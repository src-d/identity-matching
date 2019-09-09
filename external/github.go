package external

import (
	"context"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"golang.org/x/oauth2"
	"github.com/google/go-github/v28/github"
)

// GitHubMatcher matches emails and GitHub users.
type GitHubMatcher struct {
	client *github.Client
	emailToUserCache map[string]string
	userToNameCache map[string]string
}

// NewGitHubMatcher creates a new matcher given a GitHub token.
// https://github.com/settings/tokens
func NewGitHubMatcher(apiURL, token string) (Matcher, error) {
	if apiURL == "" {
		apiURL = "https://api.github.com/"
	}
	var c *http.Client
	if token != "" {
		c = oauth2.NewClient(
			context.Background(),
			oauth2.StaticTokenSource(&oauth2.Token{AccessToken: token}),
		)
	}
	// The actual upload URL does not matter - we are not going to upload anything.
	client, err := github.NewEnterpriseClient(apiURL, apiURL, c)
	if err != nil {
		return GitHubMatcher{}, err
	}
	return GitHubMatcher{
		client: client,
		emailToUserCache: make(map[string]string),
		userToNameCache: make(map[string]string),
	}, nil
}

var searchOpts = &github.SearchOptions{
	Sort:        "joined",
	ListOptions: github.ListOptions{PerPage: 1},
}

// CommitScan
func (m GitHubMatcher) ScanCommit(ctx context.Context, repo, email, commit string) error {
	logrus.Infof("scanning commit %s %s", repo, commit)
	p := regexp.MustCompile(`.*github.com/([^/]+)/([^/]+?)(?:\.git)?$`) //TODO: initialize statically
	match := p.FindStringSubmatch(repo)
	if len(match) == 0 {
		logrus.Infof("no github", repo, commit)
		return nil
	}

	// do not check more than 1 commit per author/committer email
	if _, ok := m.emailToUserCache[email]; ok {
		logrus.Infof("email %s already checked", email)
		return nil
	}

	//TODO: refactor out, repeated in MatchByEmail
	var numFailures uint64
		const maxNumFailures = 8
		const (
			success = 0
			retry   = 1
			fail    = 2
		)
		check := func(response *github.Response, err error) int {
			code := response.Response.StatusCode
			if err == nil && code >= 200 && code < 300 {
				return success
			}

			rateLimitHit := false
			if val, exists := response.Response.Header["X-Ratelimit-Remaining"]; code == 403 &&
				exists && len(val) == 1 && val[0] == "0" {
				rateLimitHit = true
			}
			if rateLimitHit {
				t, err := strconv.ParseInt(
					response.Response.Header["X-Ratelimit-Reset"][0], 10, 64)
				if err != nil {
					logrus.Errorf("Bad X-Ratelimit-Reset header: %v. %s",
						err)
					return fail
				}
				resetTime := time.Unix(t, 0).Add(time.Second)
				logrus.Warnf("rate limit was hit, waiting until %s", resetTime.String())
				time.Sleep(resetTime.Sub(time.Now().UTC()))
				return retry
			}

			if err != nil || code >= 500 && code < 600 || code == 408 || code == 429 {
				sleepTime := time.Duration((1 << numFailures) * int64(time.Second))
				logrus.Warnf("HTTP %d: %s. Sleeping until %s", code, err,
					time.Now().UTC().Add(sleepTime))
				time.Sleep(sleepTime)
				numFailures++
				if numFailures > maxNumFailures {
					return fail
				}
				return retry
			}
			logrus.Warnf("HTTP %d: %s", code, err)
			return fail
		}

	for {
		c, resp, err := m.client.Repositories.GetCommit(ctx, match[1], match[2], commit)
		status := check(resp, err)
		if status == retry {
	      continue
		} else if status == fail {
			return err
		}

		//TODO: c.Author and c.Committer are the same type, refactor when
		//      committer support is added.
		if c.Author != nil {
			if c.Author.Login != nil {
				logrus.Infof("found login: %s -> %s", email, *c.Author.Login)
				m.emailToUserCache[email] = *c.Author.Login
			} else {
				logrus.Infof("login not found: %s", email)
				m.emailToUserCache[email] = ""
			}
		}

		break
	}

	return nil
}

// MatchByEmail returns the latest GitHub user with the given email.
func (m GitHubMatcher) MatchByEmail(ctx context.Context, email string) (user, name string, err error) {
	var numFailures uint64
	const maxNumFailures = 8
	const (
		success = 0
		retry   = 1
		fail    = 2
	)
	check := func(response *github.Response, err error) int {
		code := response.Response.StatusCode
		if err == nil && code >= 200 && code < 300 {
			return success
		}

		rateLimitHit := false
		if val, exists := response.Response.Header["X-Ratelimit-Remaining"]; code == 403 &&
			exists && len(val) == 1 && val[0] == "0" {
			rateLimitHit = true
		}
		if rateLimitHit {
			t, err := strconv.ParseInt(
				response.Response.Header["X-Ratelimit-Reset"][0], 10, 64)
			if err != nil {
				logrus.Errorf("Bad X-Ratelimit-Reset header: %v",
					err)
				return fail
			}
			resetTime := time.Unix(t, 0).Add(time.Second)
			logrus.Warnf("rate limit was hit, waiting until %s", resetTime.String())
			time.Sleep(resetTime.Sub(time.Now().UTC()))
			return retry
		}

		if err != nil || code >= 500 && code < 600 || code == 408 || code == 429 {
			sleepTime := time.Duration((1 << numFailures) * int64(time.Second))
			logrus.Warnf("HTTP %d: %s. Sleeping until %s", code, err,
				time.Now().UTC().Add(sleepTime))
			time.Sleep(sleepTime)
			numFailures++
			if numFailures > maxNumFailures {
				return fail
			}
			return retry
		}
		logrus.Warnf("HTTP %d: %s", code, err)
		return fail
	}

	if isNoReplyEmail(email) {
		user = userFromEmail(email)
	} else {
		u, ok := m.emailToUserCache[email]
		if ok {
			if u == "" {
				logrus.Infof("MatchByEmail commit scan cache empty of %s", email)
				return
			}

			logrus.Infof("MatchByEmail succeeded with commit scan cache %s", email)
			user = u
		} else {
			logrus.Infof("MatchByEmail no cache %s", email)
			return
		}
	}

	if n, ok := m.userToNameCache[user]; ok {
		name = n
		return
	}

	for { // api rate limit retry loop
		var u *github.User
		var response *github.Response
		u, response, err = m.client.Users.Get(ctx, user)
		status := check(response, err)
		if status == retry {
			continue
		} else if status == fail {
			return
		}
		user = u.GetLogin()
		name = u.GetName()
		m.userToNameCache[user] = name
		return
	}
}

func isNoReplyEmail(email string) bool {
	return strings.HasSuffix(email, "@users.noreply.github.com")
}

func userFromEmail(email string) string {
	user := strings.Split(email, "@")[0]

	// Some emails can be of the form xxxxx+yyyyyy@users.noreply.github.com
	if strings.Contains(user, "+") {
		user = strings.Split(user, "+")[1]
	}

	return user
}
