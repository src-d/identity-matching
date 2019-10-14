package external

import (
	"context"
	"errors"
)

// Matcher defines the external matching service API, either by email or by commit.
type Matcher interface {
	// MatchByEmail queries the identity of a given email address.
	MatchByEmail(ctx context.Context, email string) (user string, err error)
	// SupportsMatchingByCommit indicates whether this Matcher allows querying identities by commit metadata.
	SupportsMatchingByCommit() bool
	// MatchByCommit queries the identity of a given email address in a particular commit context.
	MatchByCommit(ctx context.Context, email, repo, commit string) (user string, err error)
}

// MatcherConstructor is the Matcher constructor function type.
type MatcherConstructor func(apiURL, token string) (Matcher, error)

// ErrNoMatches is returned when no matches were found.
var ErrNoMatches = errors.New("no matches found")

// Matchers is the registered external matcher constructors mapped to shorthands.
var Matchers = map[string]MatcherConstructor{
	"github":    NewGitHubMatcher,
	"gitlab":    NewGitLabMatcher,
	"bitbucket": NewBitBucketMatcher,
}
