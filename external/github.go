package external

import (
	"context"
	"net/http"
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

// MatchByEmail returns the latest GitHub user with the given email.
func (m GitHubMatcher) MatchByEmail(ctx context.Context, email string) (user, name string, err error) {
	finished := make(chan struct{})
	go func() {
		defer func() { finished <- struct{}{} }()

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
						err, response.String())
					return fail
				}
				resetTime := time.Unix(t, 0).Add(time.Second)
				logrus.Warnf("rate limit was hit, waiting until %s", resetTime.String())
				time.Sleep(resetTime.Sub(time.Now().UTC()))
				return retry
			}

			if err != nil || code >= 500 && code < 600 || code == 408 || code == 429 {
				sleepTime := time.Duration((1 << numFailures) * int64(time.Second))
				logrus.Warnf("HTTP %d: %s. %s. Sleeping until %s", code, err, response.String(),
					time.Now().UTC().Add(sleepTime))
				time.Sleep(sleepTime)
				numFailures++
				if numFailures > maxNumFailures {
					return fail
				}
				return retry
			}
			logrus.Warnf("HTTP %d: %s. %s", code, err, response.String())
			return fail
		}

		query := email + " in:email"
		for { // api rate limit retry loop
			if isNoReplyEmail(email) {
				user = userFromEmail(email)
			} else {
				var result *github.UsersSearchResult
				var response *github.Response
				result, response, err = m.client.Search.Users(ctx, query, searchOpts)
				status := check(response, err)
				if status == retry {
					continue
				} else if status == fail {
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
			return
		}
	}()
	select {
	case <-finished:
		return
	case <-ctx.Done():
		return "", "", context.Canceled
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
