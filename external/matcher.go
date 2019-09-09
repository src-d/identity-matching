package external

import (
	"context"
	"errors"
)

// Matcher defines the external matching service API, currently only by email.
type Matcher interface {
	MatchByEmail(ctx context.Context, email string) (user, name string, err error)
}

// CommitScanner defines an external matching service that can scan commit
// hashes before matching emails.
type CommitScanner interface {
	ScanCommit(ctx context.Context, repo, email, commit string) error
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
