package idmatch

import (
	"testing"

	"github.com/src-d/eee-identity-matching/external"
	"github.com/stretchr/testify/require"
)

func TestReducePeople(t *testing.T) {
	var people = People{
		1: {ID: "_1", Names: []string{"Bob 1"}, Emails: []string{"Bob@google.com"}},
		2: {ID: "_2", Names: []string{"Bob 2"}, Emails: []string{"Bob@google.com"}},
		3: {ID: "_3", Names: []string{"Alice"}, Emails: []string{"alice@google.com"}},
		4: {ID: "_4", Names: []string{"Bob"}, Emails: []string{"Bob@google.com"}},
		5: {ID: "_5", Names: []string{"popular"}, Emails: []string{"Bob@google.com"}},
		6: {ID: "_6", Names: []string{"popular"}, Emails: []string{"email@google.com"}},
		7: {ID: "_7", Names: []string{"Alice"}, Emails: []string{"popular@google.com"}},
	}

	var reducedPeople = People{
		1: {ID: "_1", Names: []string{"Bob", "Bob 1", "Bob 2", "popular"}, Emails: []string{"Bob@google.com"}},
		3: {ID: "_3", Names: []string{"Alice"}, Emails: []string{"alice@google.com", "popular@google.com"}},
		6: {ID: "_6", Names: []string{"popular"}, Emails: []string{"email@google.com"}},
	}

	blacklist := newTestBlacklist(t)

	err := ReducePeople(people, external.BitBucketMatcher{}, blacklist)
	require.Equal(t, err, nil)
	require.Equal(t, people, reducedPeople)
}
