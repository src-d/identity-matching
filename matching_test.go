package idmatch

import (
	"context"
	"os"
	"testing"

	"github.com/src-d/eee-identity-matching/external"
	"github.com/stretchr/testify/require"
)

var githubTestToken = os.Getenv("GITHUB_TEST_TOKEN")

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

	err := ReducePeople(people, nil, blacklist, 100)
	require.Equal(t, err, nil)
	require.Equal(t, people, reducedPeople)
}

func TestReducePeopleMaxIdentities(t *testing.T) {
	var people = People{
		1:  {ID: 1, NamesWithRepos: []NameWithRepo{{"Bob", ""}}, Emails: []string{"Bob2@google.com"}},
		2:  {ID: 2, NamesWithRepos: []NameWithRepo{{"Bob 1", ""}}, Emails: []string{"Bob@google.com"}},
		3:  {ID: 3, NamesWithRepos: []NameWithRepo{{"Bob 2", ""}}, Emails: []string{"Bob@google.com"}},
		4:  {ID: 4, NamesWithRepos: []NameWithRepo{{"Bob 3", ""}}, Emails: []string{"Bob@google.com"}},
		5:  {ID: 5, NamesWithRepos: []NameWithRepo{{"Bob", ""}}, Emails: []string{"Bob@google.com"}},
		6:  {ID: 6, NamesWithRepos: []NameWithRepo{{"Bob", ""}}, Emails: []string{"Bob3@google.com"}},
		7:  {ID: 7, NamesWithRepos: []NameWithRepo{{"Bob", ""}}, Emails: []string{"Bob4@google.com"}},
		8:  {ID: 8, NamesWithRepos: []NameWithRepo{{"Alice 1", ""}}, Emails: []string{"alice@google.com"}},
		9:  {ID: 9, NamesWithRepos: []NameWithRepo{{"Alice 2", ""}}, Emails: []string{"alice@google.com"}},
		10: {ID: 10, NamesWithRepos: []NameWithRepo{{"Alice 2", ""}}, Emails: []string{"alice1@google.com"}},
	}

	var reducedPeople = People{
		1: {ID: 1,
			NamesWithRepos: []NameWithRepo{{"Bob", ""}},
			Emails:         []string{"Bob2@google.com", "Bob3@google.com", "Bob4@google.com"}},
		2: {ID: 2,
			NamesWithRepos: []NameWithRepo{
				{"Bob", ""},
				{"Bob 1", ""},
				{"Bob 2", ""},
				{"Bob 3", ""}},
			Emails: []string{"Bob@google.com"}},
		8: {ID: 8,
			NamesWithRepos: []NameWithRepo{{"Alice 1", ""}, {"Alice 2", ""}},
			Emails:         []string{"alice1@google.com", "alice@google.com"}},
	}

	blacklist := newTestBlacklist(t)

	err := ReducePeople(people, nil, blacklist, 4)
	require.Equal(t, err, nil)
	require.Equal(t, reducedPeople, people)
}

func TestReducePeopleExternalMatching(t *testing.T) {
	if githubTestToken == "" {
		panic("GITHUB_TEST_TOKEN environment variable is not set")
	}
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
			Emails:     []string{"mcuadros@gmail.com"},
			ExternalID: "mcuadros"},
		3: {ID: 3,
			NamesWithRepos: []NameWithRepo{{"Konstantin Slavnov", ""}},
			Emails:         []string{"kslavnov@gmail.com"},
			ExternalID:     "zurk"},
	}

	blacklist := newTestBlacklist(t)
	matcher, _ := external.NewGitHubMatcher("", githubTestToken)

	err := ReducePeople(people, matcher, blacklist, 100)

	require.Equal(t, err, nil)
	require.Equal(t, people, reducedPeople)
}

func TestReducePeopleBothMatching(t *testing.T) {
	if githubTestToken == "" {
		panic("GITHUB_TEST_TOKEN environment variable is not set")
	}
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
			ExternalID:     "mcuadros",
		},
		3: {
			ID:             0x3,
			NamesWithRepos: []NameWithRepo{{Name: "Konstantin Slavnov", Repo: ""}},
			Emails:         []string{"kslavnov@ggmail.com", "kslavnov@gmail.com"},
			ExternalID:     "zurk",
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
	matcher, _ := external.NewGitHubMatcher("", githubTestToken)

	err := ReducePeople(people, matcher, blacklist, 100)

	require.Equal(t, err, nil)
	require.Equal(t, people, reducedPeople)
}

func TestReducePeopleBothMatchingDifferentExternalIdsNoMerge(t *testing.T) {
	githubTestToken := "a7f979a7c45e7d3517ad7eeeb8cba5e16e813aef"
	if githubTestToken == "" {
		panic("GITHUB_TEST_TOKEN environment variable is not set")
	}
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
			Emails:         []string{"vadim@sourced.tech"}},
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
			ExternalID:     "mcuadros",
		},
		3: {ID: 3,
			NamesWithRepos: []NameWithRepo{{"Konstantin Slavnov", ""}},
			Emails:         []string{"kslavnov@gmail.com"},
			ExternalID:     "zurk"},
		4: {ID: 4,
			NamesWithRepos: []NameWithRepo{{"Konstantin Slavnov", ""}},
			Emails:         []string{"vadim@sourced.tech"},
			ExternalID:     "vmarkovtsev"},
		5: {ID: 5,
			NamesWithRepos: []NameWithRepo{{"Konstantin Slavnov", ""}},
			Emails:         []string{"kslavnov@ggmail.com"}},
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
	matcher, _ := external.NewGitHubMatcher("", githubTestToken)

	err := ReducePeople(people, matcher, blacklist, 100)

	require.Equal(t, err, nil)
	require.Equal(t, people, reducedPeople)
}

type TestMatcher struct {
}

func (m TestMatcher) MatchByEmail(ctx context.Context, email string) (user, name string, err error) {
	usernames := map[string]string{
		"Bob@google.com":   "bob_username",
		"Bob2@google.com":  "not_bob_username",
		"alice@google.com": "alice_username",
	}
	return usernames[email], "", nil
}

func TestReducePeopleSameNameDifferentExternalIds(t *testing.T) {
	var people = People{
		1: {ID: 1, NamesWithRepos: []NameWithRepo{{"Bob", ""}}, Emails: []string{"Bob@google.com"}},
		2: {ID: 2, NamesWithRepos: []NameWithRepo{{"Bob", ""}}, Emails: []string{"Bob2@google.com"}},
		3: {ID: 3, NamesWithRepos: []NameWithRepo{{"Alice", ""}}, Emails: []string{"alice@google.com"}},
		4: {ID: 4, NamesWithRepos: []NameWithRepo{{"Bob 2", ""}}, Emails: []string{"Bob@google.com"}},
	}

	var reducedPeople = People{
		1: {ID: 1,
			NamesWithRepos: []NameWithRepo{{"Bob", ""}, {"Bob 2", ""}},
			Emails:         []string{"Bob@google.com"},
			ExternalID:     "bob_username"},
		2: {ID: 2,
			NamesWithRepos: []NameWithRepo{{"Bob", ""}},
			Emails:         []string{"Bob2@google.com"},
			ExternalID:     "not_bob_username"},
		3: {ID: 3,
			NamesWithRepos: []NameWithRepo{{"Alice", ""}},
			Emails:         []string{"alice@google.com"},
			ExternalID:     "alice_username"},
	}

	blacklist := newTestBlacklist(t)

	err := ReducePeople(people, TestMatcher{}, blacklist, 100)
	require.Equal(t, err, nil)
	require.Equal(t, people, reducedPeople)
}
