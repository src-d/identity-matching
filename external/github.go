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
	"gopkg.in/google/go-github.v15/github"
)

// GitHubMatcher matches emails and GitHub users.
type GitHubMatcher struct {
	client *github.Client
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
	return GitHubMatcher{client}, nil
}

var searchOpts = &github.SearchOptions{
	Sort:        "joined",
	ListOptions: github.ListOptions{PerPage: 1},
}
var gitHubRepoRe = regexp.MustCompile(`(.*://|^)github.com/([^/]+)/([^/]+?)(?:\.git)?$`)

const (
	responseSuccess = 0
	responseRetry   = 1
	responseFail    = 2
	maxNumFailures  = 8
)

// MatchByEmail returns the latest GitHub user with the given email.
func (m GitHubMatcher) MatchByEmail(ctx context.Context, email string) (user string, err error) {
	finished := make(chan struct{})
	go func() {
		defer func() { finished <- struct{}{} }()

		var numFailures uint64
		query := email + " in:email"
		for { // api rate limit retry loop
			if isNoReplyEmail(email) {
				user = userFromEmail(email)
			} else {
				var result *github.UsersSearchResult
				var response *github.Response
				result, response, err = m.client.Search.Users(ctx, query, searchOpts)
				status := checkResponse(response, err, &numFailures)
				if status == responseRetry {
					continue
				} else if status == responseFail {
					return
				}
				if len(result.Users) == 0 {
					if strings.Contains(query, "@") {
						// Hacking time! user+domain may work instead of user@domain
						query = strings.Replace(query, "@", " ", 1)
						continue
					}
					logrus.Warnf("unable to find users for email: %s", email)
					err = ErrNoMatches
					return
				}
				user = result.Users[0].GetLogin()
				break
			}
		}
	}()
	select {
	case <-finished:
		return
	case <-ctx.Done():
		return "", context.Canceled
	}
}

// SupportsMatchingByCommit indicates whether this Matcher allows querying identities by commit metadata.
func (m GitHubMatcher) SupportsMatchingByCommit() bool {
	return true
}

// MatchByCommit queries the identity of a given email address in a particular commit context.
func (m GitHubMatcher) MatchByCommit(
	ctx context.Context, email, repo, commit string) (user string, err error) {
	parsedRepo := gitHubRepoRe.FindStringSubmatch(repo)
	if len(parsedRepo) < 4 {
		logrus.Panicf("not a GitHub repository: %s", repo)
	}
	if len(commit) != 40 {
		logrus.Panicf("not a Git hash: %s", commit)
	}
	repoUser := parsedRepo[2]
	repoName := parsedRepo[3]
	finished := make(chan struct{})
	go func() {
		defer func() { finished <- struct{}{} }()

		var numFailures uint64
		for { // api rate limit retry loop
			if isNoReplyEmail(email) {
				user = userFromEmail(email)
			} else {
				var c *github.RepositoryCommit
				var response *github.Response
				c, response, err = m.client.Repositories.GetCommit(ctx, repoUser, repoName, commit)
				status := checkResponse(response, err, &numFailures)
				if status == responseRetry {
					continue
				} else if status == responseFail {
					return
				}
				if c.Author != nil && c.Author.Login != nil && c.Commit.Author != nil &&
					c.Commit.Author.Email != nil && *c.Commit.Author.Email == email {
					user = *c.Author.Login
				} else if c.Committer != nil && c.Committer.Login != nil && c.Commit.Committer != nil &&
					c.Commit.Committer.Email != nil && *c.Commit.Committer.Email == email {
					user = *c.Committer.Login
				} else {
					logrus.Warnf("unable to find users by commit for email: %s", email)
					err = ErrNoMatches
				}
				break
			}
		}
	}()
	select {
	case <-finished:
		return
	case <-ctx.Done():
		return "", context.Canceled
	}
}

// OnIdle does nothing here.
func (m GitHubMatcher) OnIdle() error {
	return nil
}

func checkResponse(response *github.Response, err error, numFailures *uint64) int {
	code := response.Response.StatusCode
	if err == nil && code >= 200 && code < 300 {
		return responseSuccess
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
			logrus.Errorf("Bad X-Ratelimit-Reset header: %v", err)
			return responseFail
		}
		resetTime := time.Unix(t, 0).Add(time.Second)
		logrus.Warnf("rate limit was hit, waiting until %s", resetTime.String())
		time.Sleep(resetTime.Sub(time.Now().UTC()))
		return responseRetry
	}

	if err != nil || code >= 500 && code < 600 || code == 408 || code == 429 {
		sleepTime := time.Duration((1 << *numFailures) * int64(time.Second))
		logrus.Warnf("HTTP %d: %s, sleeping until %s", code, err,
			time.Now().UTC().Add(sleepTime))
		time.Sleep(sleepTime)
		*numFailures++
		if *numFailures > maxNumFailures {
			return responseFail
		}
		return responseRetry
	}
	logrus.Warnf("HTTP %d: %s", code, err)
	return responseFail
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
