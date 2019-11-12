package idmatch

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gonum.org/v1/gonum/graph/simple"

	"github.com/src-d/identity-matching/external"
)

var githubTestToken = os.Getenv("GITHUB_TEST_TOKEN")

func TestReducePeople(t *testing.T) {
	commit := &Commit{"xxx", "repo"}
	var people = People{
		1: {ID: 1, NamesWithRepos: []NameWithRepo{{"Bob 1", ""}}, Emails: []string{"Bob@google.com"}},
		2: {ID: 2, NamesWithRepos: []NameWithRepo{{"Bob 2", ""}}, Emails: []string{"Bob@google.com"}},
		3: {ID: 3, NamesWithRepos: []NameWithRepo{{"Alice", ""}}, Emails: []string{"alice@google.com"}},
		4: {ID: 4, NamesWithRepos: []NameWithRepo{{"Bob", ""}}, Emails: []string{"Bob@google.com"}},
		5: {ID: 5, NamesWithRepos: []NameWithRepo{{"popular", ""}}, Emails: []string{"Bob@google.com"}},
		6: {ID: 6, NamesWithRepos: []NameWithRepo{{"popular", ""}}, Emails: []string{"email@google.com"}},
		7: {ID: 7, NamesWithRepos: []NameWithRepo{{"Alice", ""}}, Emails: []string{"popular@google.com"}},
	}
	for _, p := range people {
		p.SampleCommit = commit
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

func printTestSkippedNoToken() {
	fmt.Println("GITHUB_TEST_TOKEN environment variable is not set, skipping the test")
}

func TestReducePeopleExternalMatching(t *testing.T) {
	if githubTestToken == "" {
		printTestSkippedNoToken()
		return
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
		printTestSkippedNoToken()
		return
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
	if githubTestToken == "" {
		printTestSkippedNoToken()
		return
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

func (m TestMatcher) MatchByEmail(ctx context.Context, email string) (user string, err error) {
	usernames := map[string]string{
		"Bob@google.com":   "bob_username",
		"Bob2@google.com":  "not_bob_username",
		"alice@google.com": "alice_username",
	}
	return usernames[email], nil
}

func (m TestMatcher) SupportsMatchingByCommit() bool {
	return false
}

func (m TestMatcher) MatchByCommit(ctx context.Context, email, repo, commit string) (user string, err error) {
	return "", nil
}

func (m TestMatcher) OnIdle() error {
	return nil
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

func TestSetPrimaryValue(t *testing.T) {
	people := People{
		1: {ID: 1, NamesWithRepos: []NameWithRepo{
			{"Bob", ""},
			{"Bob 1", ""},
			{"Bob 2", ""},
			{"popular", ""}},
			Emails: []string{"Bob@google.com", "bobby@google.com", "12345@gmail.com"}},
		3: {ID: 3, NamesWithRepos: []NameWithRepo{{"Alice", ""}, {"Alice 1", ""}},
			Emails: []string{"alice@google.com", "al@google.com"}},
		6: {ID: 6, NamesWithRepos: []NameWithRepo{{"popular", ""}},
			Emails: []string{"email@google.com"}},
	}
	emailFreqs := map[string]*Frequency{
		"Bob@google.com":   {5, 8},
		"bobby@google.com": {2, 4},
		"12345@gmail.com":  {1, 1},
		"email@google.com": {2, 4},
		"alice@google.com": {1, 5},
		"al@google.com":    {3, 3},
		"admin@google.com": {6, 6},
	}
	expected := People{
		1: {ID: 1, NamesWithRepos: []NameWithRepo{
			{"Bob", ""},
			{"Bob 1", ""},
			{"Bob 2", ""},
			{"popular", ""}},
			Emails:       []string{"Bob@google.com", "bobby@google.com", "12345@gmail.com"},
			PrimaryEmail: "Bob@google.com"},
		3: {ID: 3, NamesWithRepos: []NameWithRepo{{"Alice", ""}, {"Alice 1", ""}},
			Emails:       []string{"alice@google.com", "al@google.com"},
			PrimaryEmail: "al@google.com"},
		6: {ID: 6, NamesWithRepos: []NameWithRepo{{"popular", ""}},
			Emails:       []string{"email@google.com"},
			PrimaryEmail: "email@google.com"},
	}
	setPrimaryValue(people, emailFreqs, func(p *Person) []string { return p.Emails },
		func(p *Person, email string) { p.PrimaryEmail = email }, 2)
	require.Equal(t, expected, people)
}

func TestSetPrimaryValues(t *testing.T) {
	people := People{
		1: {ID: 1, NamesWithRepos: []NameWithRepo{
			{"Bob", ""},
			{"Bob 1", ""},
			{"Bob 2", ""},
			{"popular", ""}},
			Emails: []string{"Bob@google.com", "bobby@google.com", "12345@gmail.com"}},
		3: {ID: 3, NamesWithRepos: []NameWithRepo{{"Alice", ""}, {"Alice 1", ""}},
			Emails: []string{"alice@google.com", "al@google.com"}},
		6: {ID: 6, NamesWithRepos: []NameWithRepo{{"popular", ""}},
			Emails: []string{"email@google.com"}},
	}
	nameFreqs := map[string]*Frequency{
		"Bob":     {5, 10},
		"Bob 1":   {1, 3},
		"Bob 2":   {1, 1},
		"popular": {4, 20},
		"Alice":   {3, 4},
		"Alice 1": {1, 5},
		"admin":   {3, 5},
	}
	emailFreqs := map[string]*Frequency{
		"Bob@google.com":   {5, 8},
		"bobby@google.com": {2, 4},
		"12345@gmail.com":  {1, 1},
		"email@google.com": {2, 4},
		"alice@google.com": {1, 5},
		"al@google.com":    {3, 3},
		"admin@google.com": {6, 6},
	}
	expected := People{
		1: {ID: 1, NamesWithRepos: []NameWithRepo{
			{"Bob", ""},
			{"Bob 1", ""},
			{"Bob 2", ""},
			{"popular", ""}},
			Emails:      []string{"Bob@google.com", "bobby@google.com", "12345@gmail.com"},
			PrimaryName: "Bob", PrimaryEmail: "Bob@google.com"},
		3: {ID: 3, NamesWithRepos: []NameWithRepo{{"Alice", ""}, {"Alice 1", ""}},
			Emails:      []string{"alice@google.com", "al@google.com"},
			PrimaryName: "Alice 1", PrimaryEmail: "alice@google.com"},
		6: {ID: 6, NamesWithRepos: []NameWithRepo{{"popular", ""}},
			Emails:      []string{"email@google.com"},
			PrimaryName: "popular", PrimaryEmail: "email@google.com"},
	}
	SetPrimaryValues(people, nameFreqs, emailFreqs, 5)
	require.Equal(t, expected, people)
}

func TestAddEdgesWithMatcherCommits(t *testing.T) {
	people := People{}
	people[1] = &Person{ID: 1, NamesWithRepos: []NameWithRepo{{"Vadim", ""}},
		Emails: []string{"vadim@sourced.tech"}, SampleCommit: &Commit{
			Hash: "d78a9c8b0c077b5ecdb3cf1e1efab4635c97dd7b",
			Repo: "git://github.com/src-d/hercules.git",
		}}
	matcher, _ := external.NewGitHubMatcher("", githubTestToken)
	peopleGraph := simple.NewUndirectedGraph()
	for index, person := range people {
		peopleGraph.AddNode(node{person, index})
	}
	unprocessedEmails, err := addEdgesWithMatcher(people, peopleGraph, matcher)
	req := require.New(t)
	req.NoError(err)
	req.Equal(0, len(unprocessedEmails))
	req.Equal("vmarkovtsev", people[1].ExternalID)
}
