package idmatch

import (
	"testing"

	"github.com/src-d/eee-identity-matching/external"
	"github.com/stretchr/testify/require"
)

func TestReducePeople(t *testing.T) {
	var people = People{
		1: {ID: 1, NamesWithRepos: []NameWithRepo{{"Bob 1", ""}}, Emails: []string{"Bob@google.com"}},
		2: {ID: 2, NamesWithRepos: []NameWithRepo{{"Bob 2", ""}}, Emails: []string{"Bob@google.com"}},
		3: {ID: 3, NamesWithRepos: []NameWithRepo{{"Alice", ""}}, Emails: []string{"alice@google.com"}},
		4: {ID: 4, NamesWithRepos: []NameWithRepo{{"Bob", ""}}, Emails: []string{"Bob@google.com"}},
		5: {ID: 5, NamesWithRepos: []NameWithRepo{{"popular", ""}}, Emails: []string{"Bob@google.com"}},
		6: {ID: 6, NamesWithRepos: []NameWithRepo{{"popular", ""}}, Emails: []string{"email@google.com"}},
		7: {ID: 7, NamesWithRepos: []NameWithRepo{{"Alice", ""}}, Emails: []string{"popular@google.com"}},
	}

	var reducedPeople = People{
		1: {ID: 1, NamesWithRepos: []NameWithRepo{
			{"Bob", ""},
			{"Bob 1", ""},
			{"Bob 2", ""},
			{"popular", ""}}, Emails: []string{"Bob@google.com"}},
		3: {ID: 3, NamesWithRepos: []NameWithRepo{{"Alice", ""}}, Emails: []string{"alice@google.com", "popular@google.com"}},
		6: {ID: 6, NamesWithRepos: []NameWithRepo{{"popular", ""}}, Emails: []string{"email@google.com"}},
	}

	blacklist := newTestBlacklist(t)

	err := ReducePeople(people, external.BitBucketMatcher{}, blacklist)
	require.Equal(t, err, nil)
	require.Equal(t, people, reducedPeople)
}
