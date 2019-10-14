package external

import (
	"context"
	"errors"
	"net/http"

	"github.com/wbrefvem/go-bitbucket"
)

// BitBucketMatcher matches emails and BitBucket users.
type BitBucketMatcher struct {
	authContext context.Context
	client      *bitbucket.APIClient
}

// NewBitBucketMatcher creates a new matcher given a BitBucket personal access token.
// https://id.atlassian.com/manage/api-tokens
func NewBitBucketMatcher(apiURL, token string) (Matcher, error) {
	if apiURL == "" {
		apiURL = "https://api.bitbucket.org/2.0"
	}
	ctx := context.WithValue(
		context.Background(),
		bitbucket.ContextAPIKey,
		bitbucket.APIKey{Key: token},
	)
	client := bitbucket.NewAPIClient(bitbucket.NewConfiguration())
	return BitBucketMatcher{authContext: ctx, client: client}, nil
}

// MatchByEmail returns the latest BitBucket user with the given email.
func (m BitBucketMatcher) MatchByEmail(ctx context.Context, email string) (user, name string, err error) {
	finished := make(chan struct{})
	go func() {
		defer func() { finished <- struct{}{} }()
		var u bitbucket.User
		var r *http.Response
		u, r, err = m.client.UsersApi.UsersUsernameGet(m.authContext, email)
		if err != nil {
			// According to https://confluence.atlassian.com/bitbucket/rate-limits-668173227.html
			// this API is not rate-limited.
			if r.StatusCode == 404 {
				err = ErrNoMatches
			}
			return
		}
		user = u.AccountId
		name = u.DisplayName
	}()
	select {
	case <-finished:
		return
	case <-ctx.Done():
		return "", "", context.Canceled
	}
}

// SupportsMatchingByCommit indicates whether this Matcher allows querying identities by commit metadata.
func (m BitBucketMatcher) SupportsMatchingByCommit() bool {
	return false
}

// MatchByCommit queries the identity of a given email address in a particular commit context.
func (m BitBucketMatcher) MatchByCommit(
	ctx context.Context, email, repo, commit string) (user, name string, err error) {
	return "", "", errors.New("not implemented")
}
