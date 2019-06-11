package external

import (
	"context"

	"github.com/sirupsen/logrus"
	"github.com/xanzy/go-gitlab"
)

// GitLabMatcher matches emails and GitLab users.
type GitLabMatcher struct {
	client *gitlab.Client
}

// NewGitLabMatcher creates a new matcher given a GitLab OAuth token.
func NewGitLabMatcher(apiURL, token string) (Matcher, error) {
	if apiURL == "" {
		apiURL = "https://gitlab.com/api/v4"
	}
	m := GitLabMatcher{gitlab.NewClient(nil, token)}
	err := m.client.SetBaseURL(apiURL)
	if err != nil {
		return GitLabMatcher{}, err
	}
	return m, nil
}

// MatchByEmail returns the latest GitLab user with the given email.
func (m GitLabMatcher) MatchByEmail(ctx context.Context, email string) (user, name string, err error) {
	opts := &gitlab.ListUsersOptions{Search: &email}
	for {
		users, _, err := m.client.Users.ListUsers(opts)
		if err != nil {
			// TODO(vmarkovtsev): handle rate limit
			// https://github.com/xanzy/go-gitlab/issues/630
			return "", "", err
		}
		if len(users) == 0 {
			logrus.Warnf("unable to find users for email: %s", email)
			return "", "", ErrNoMatches
		}
		return users[0].Username, users[0].Name, nil
	}
}
