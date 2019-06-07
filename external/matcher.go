package external

import (
	"context"
	"errors"
)

// Matcher defines the external matching service API, currently only by email.
type Matcher interface {
	MatchByEmail(ctx context.Context, email string) (user, name string, err error)
}

// ErrNoMatches is returned when no matches were found.
var ErrNoMatches = errors.New("no matches found")
