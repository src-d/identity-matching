package idmatch

import (
	"testing"

	"github.com/src-d/eee-identity-matching/external"
	"github.com/stretchr/testify/require"
)

var GithubTestToken = "58f7c94cece3b0828426e5d015e8d910169abd2d"

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

	err := ReducePeople(people, nil, blacklist)
	require.Equal(t, err, nil)
	require.Equal(t, people, reducedPeople)
}

func TestReducePeopleExternalMatching(t *testing.T) {
	var people = People{
		1: {ID: 1,
			NamesWithRepos: []NameWithRepo{{"Máximo Cuadros", ""}},
			Emails:         []string{"mcuadros@gmail.com"}},
		2: {ID: 2,
			NamesWithRepos: []NameWithRepo{{"Máximo", ""}},
			Emails:         []string{"mcuadros@gmail.com"}},
		3: {ID: 3,
			NamesWithRepos: []NameWithRepo{{"Konstantin Slavnov", ""}},
			Emails:         []string{"kslavnov@gmail.com"}},
	}

	var reducedPeople = People{
		1: {ID: 1, NamesWithRepos: []NameWithRepo{
			{"Máximo", ""},
			{"Máximo Cuadros", ""}},
			Emails: []string{"mcuadros@gmail.com"}},
		3: {ID: 3,
			NamesWithRepos: []NameWithRepo{{"Konstantin Slavnov", ""}},
			Emails:         []string{"kslavnov@gmail.com"}},
	}

	blacklist := newTestBlacklist(t)
	matcher, _ := external.NewGitHubMatcher("", GithubTestToken)

	err := ReducePeople(people, matcher, blacklist)

	require.Equal(t, err, nil)
	require.Equal(t, people, reducedPeople)
}

func TestReducePeopleBothMatching(t *testing.T) {
	var people = People{
		1: {ID: 1,
			NamesWithRepos: []NameWithRepo{{"Máximo Cuadros", ""}},
			Emails:         []string{"mcuadros@gmail.com"}},
		2: {ID: 2,
			NamesWithRepos: []NameWithRepo{{"Máximo", ""}},
			Emails:         []string{"mcuadros@gmail.com"}},
		3: {ID: 3,
			NamesWithRepos: []NameWithRepo{{"Konstantin Slavnov", ""}},
			Emails:         []string{"kslavnov@gmail.com"}},
		4: {ID: 4,
			NamesWithRepos: []NameWithRepo{{"Konstantin Slavnov", ""}},
			Emails:         []string{"kslavnov@ggmail.com"}},
		5: {ID: 5,
			NamesWithRepos: []NameWithRepo{{"Konstantin Slavnov", ""}},
			Emails:         []string{"kslavnov@ggmail.com"}},
		6: {ID: 6, NamesWithRepos: []NameWithRepo{{"Bob 1", ""}}, Emails: []string{"Bob@ggoogle.com"}},
		7: {ID: 7, NamesWithRepos: []NameWithRepo{{"Bob 2", ""}}, Emails: []string{"Bob@ggoogle.com"}},
		8: {ID: 8, NamesWithRepos: []NameWithRepo{{"Alice", ""}}, Emails: []string{"alice@ggoogle.com"}},
		9: {ID: 9, NamesWithRepos: []NameWithRepo{{"Bob", ""}}, Emails: []string{"Bob@ggoogle.com"}},
	}

	var reducedPeople = People{
		1: {
			ID:             0x1,
			NamesWithRepos: []NameWithRepo{{Name: "Máximo", Repo: ""}, {Name: "Máximo Cuadros", Repo: ""}},
			Emails:         []string{"mcuadros@gmail.com"},
		},
		3: {
			ID:             0x3,
			NamesWithRepos: []NameWithRepo{{Name: "Konstantin Slavnov", Repo: ""}},
			Emails:         []string{"kslavnov@ggmail.com", "kslavnov@gmail.com"},
		},
		6: {
			ID: 0x6,
			NamesWithRepos: []NameWithRepo{
				{Name: "Bob", Repo: ""},
				{Name: "Bob 1", Repo: ""},
				{Name: "Bob 2", Repo: ""},
			},
			Emails: []string{"Bob@ggoogle.com"},
		},
		8: {
			ID:             0x8,
			NamesWithRepos: []NameWithRepo{{Name: "Alice", Repo: ""}},
			Emails:         []string{"alice@ggoogle.com"},
		},
	}

	blacklist := newTestBlacklist(t)
	matcher, _ := external.NewGitHubMatcher("", GithubTestToken)

	err := ReducePeople(people, matcher, blacklist)

	require.Equal(t, err, nil)
	require.Equal(t, people, reducedPeople)
}
