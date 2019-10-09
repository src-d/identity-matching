package idmatch

import (
	"context"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

var Commits = []Commit{
	{repo: "repo1", name: "Bob", email: "Bob@google.com",
		time: time.Now().AddDate(0, -6, 0)},
	{repo: "repo2", name: "Bob", email: "Bob@google.com",
		time: time.Now().AddDate(0, -18, 0)},
	{repo: "repo1", name: "Alice", email: "alice@google.com",
		time: time.Now().AddDate(0, -15, 0)},
	{repo: "repo1", name: "Bob", email: "Bob@google.com",
		time: time.Now().AddDate(0, -2, 0)},
	{repo: "repo1", name: "Bob", email: "bad-email@domen",
		time: time.Now().AddDate(0, -20, 0)},
	{repo: "repo1", name: "admin", email: "someone@google.com",
		time: time.Now().AddDate(0, -4, 0)},
}

func TestPeopleNew(t *testing.T) {
	expected := People{
		1: {ID: 1, NamesWithRepos: []NameWithRepo{{"bob", ""}}, Emails: []string{"bob@google.com"}},
		2: {ID: 2, NamesWithRepos: []NameWithRepo{{"bob", ""}}, Emails: []string{"bob@google.com"}},
		3: {ID: 3, NamesWithRepos: []NameWithRepo{{"alice", ""}}, Emails: []string{"alice@google.com"}},
		4: {ID: 4, NamesWithRepos: []NameWithRepo{{"bob", ""}}, Emails: []string{"bob@google.com"}},
	}
	people, err := newPeople(Commits, newTestBlacklist(t))
	require.NoError(t, err)
	require.Equal(t, expected, people)
}

func TestTwoPeopleMerge(t *testing.T) {
	require := require.New(t)
	people, err := newPeople(Commits, newTestBlacklist(t))
	require.NoError(err)
	mergedID, err := people.Merge(1, 2)
	expected := People{
		1: {ID: 1, NamesWithRepos: []NameWithRepo{{"bob", ""}}, Emails: []string{"bob@google.com"}},
		3: {ID: 3, NamesWithRepos: []NameWithRepo{{"alice", ""}}, Emails: []string{"alice@google.com"}},
		4: {ID: 4, NamesWithRepos: []NameWithRepo{{"bob", ""}}, Emails: []string{"bob@google.com"}},
	}
	require.Equal(int64(1), mergedID)
	require.Equal(expected, people)
	require.NoError(err)

	mergedID, err = people.Merge(3, 4)
	expected = People{
		1: {ID: 1, NamesWithRepos: []NameWithRepo{{"bob", ""}}, Emails: []string{"bob@google.com"}},
		3: {ID: 3,
			NamesWithRepos: []NameWithRepo{{"alice", ""}, {"bob", ""}},
			Emails:         []string{"alice@google.com", "bob@google.com"}},
	}
	require.Equal(int64(3), mergedID)
	require.Equal(expected, people)
	require.NoError(err)

	mergedID, err = people.Merge(1, 3)
	expected = People{
		1: {ID: 1,
			NamesWithRepos: []NameWithRepo{{"alice", ""}, {"bob", ""}},
			Emails:         []string{"alice@google.com", "bob@google.com"}},
	}
	require.Equal(int64(1), mergedID)
	require.Equal(expected, people)
	require.NoError(err)
}

func TestFourPeopleMerge(t *testing.T) {
	people, err := newPeople(Commits, newTestBlacklist(t))
	require.NoError(t, err)
	mergedID, err := people.Merge(1, 2, 3, 4)
	expected := People{
		1: {ID: 1,
			NamesWithRepos: []NameWithRepo{{"alice", ""}, {"bob", ""}},
			Emails:         []string{"alice@google.com", "bob@google.com"}},
	}
	require.Equal(t, int64(1), mergedID)
	require.Equal(t, expected, people)
	require.NoError(t, err)
}

func TestDifferentExternalIdsMerge(t *testing.T) {
	people, err := newPeople(Commits, newTestBlacklist(t))
	require.NoError(t, err)
	people[1].ExternalID = "id1"
	people[2].ExternalID = "id2"
	_, err = people.Merge(1, 2)
	require.Error(t, err)
}

func TestPeopleForEach(t *testing.T) {
	people, err := newPeople(Commits, newTestBlacklist(t))
	require.NoError(t, err)
	var keys = make([]int64, 0, len(people))
	people.ForEach(func(key int64, val *Person) bool {
		keys = append(keys, key)
		return false
	})
	require.Equal(t, []int64{1, 2, 3, 4}, keys)
}

func tempFile(t *testing.T, pattern string) (*os.File, func()) {
	t.Helper()
	f, err := ioutil.TempFile("", pattern)
	require.NoError(t, err)
	return f, func() {
		require.NoError(t, f.Close())
		require.NoError(t, os.Remove(f.Name()))
	}
}

func TestFindCommits(t *testing.T) {
	peopleFile, cleanup := tempFile(t, "*.csv")
	defer cleanup()

	err := storePeopleOnDisk(peopleFile.Name(), Commits)
	if err != nil {
		return
	}
	people, err := findCommits(context.TODO(), "0.0.0.0:3306", peopleFile.Name())
	if err != nil {
		return
	}
	require.Equal(t, []Commit{
		{repo: "repo1", name: "bob", email: "bob@google.com"},
		{repo: "repo2", name: "bob", email: "bob@google.com"},
		{repo: "repo1", name: "alice", email: "alice@google.com"},
		{repo: "repo1", name: "bob", email: "bob@google.com"},
		{repo: "repo1", name: "bob", email: "bad-email@domen"},
		{repo: "repo1", name: "admin", email: "someone@google.com"},
	}, people)
}

func TestFindPeople(t *testing.T) {
	peopleFile, cleanup := tempFile(t, "*.csv")
	defer cleanup()

	err := storePeopleOnDisk(peopleFile.Name(), Commits)
	if err != nil {
		return
	}
	people, nameFreqs, emailFreqs, err := FindPeople(
		context.TODO(), "0.0.0.0:3306", peopleFile.Name(), newTestBlacklist(t), 12)
	if err != nil {
		return
	}
	expected := People{
		1: {ID: 1, NamesWithRepos: []NameWithRepo{{"bob", ""}}, Emails: []string{"bob@google.com"}},
		2: {ID: 2, NamesWithRepos: []NameWithRepo{{"bob", ""}}, Emails: []string{"bob@google.com"}},
		3: {ID: 3, NamesWithRepos: []NameWithRepo{{"alice", ""}}, Emails: []string{"alice@google.com"}},
		4: {ID: 4, NamesWithRepos: []NameWithRepo{{"bob", ""}}, Emails: []string{"bob@google.com"}},
	}
	require.Equal(t, expected, people)
	require.Equal(t, map[string]*Frequency{"alice": {0, 1},
		"admin": {1, 1}, "bob": {2, 4}}, nameFreqs)
	require.Equal(t, map[string]*Frequency{"bob@google.com": {2, 3},
		"alice@google.com": {0, 1}, "bad-email@domen": {0, 1},
		"someone@google.com": {1, 1}}, emailFreqs)
}

func TestReadPeopleFromDatabase(t *testing.T) {
	// TODO(zurk)
}

func TestStoreAndReadPeopleOnDisk(t *testing.T) {
	peopleFile, cleanup := tempFile(t, "*.csv")
	defer cleanup()

	err := storePeopleOnDisk(peopleFile.Name(), Commits)
	if err != nil {
		return
	}
	peopleFileContent, err := ioutil.ReadFile(peopleFile.Name())
	if err != nil {
		return
	}
	expectedContent := `repo,name,email,time
repo1,Bob,Bob@google.com,` + Commits[0].time.String() + `
repo2,Bob,Bob@google.com,` + Commits[1].time.String() + `
repo1,Alice,alice@google.com,` + Commits[2].time.String() + `
repo1,Bob,Bob@google.com,` + Commits[3].time.String() + `
repo1,Bob,bad-email@domen,` + Commits[4].time.String() + `
repo1,admin,someone@google.com,` + Commits[5].time.String() + `
`
	require.Equal(t, expectedContent, string(peopleFileContent))

	commitsRead, err := readCommitsFromDisk(peopleFile.Name())
	if err != nil {
		return
	}
	expectedPersonsRead := []Commit{
		0: {repo: "repo1", name: "bob", email: "bob@google.com", time: Commits[0].time},
		1: {repo: "repo2", name: "bob", email: "bob@google.com", time: Commits[1].time},
		2: {repo: "repo1", name: "alice", email: "alice@google.com", time: Commits[2].time},
		3: {repo: "repo1", name: "bob", email: "bob@google.com", time: Commits[3].time},
		4: {repo: "repo1", name: "bob", email: "bad-email@domen", time: Commits[4].time},
		5: {repo: "repo1", name: "admin", email: "someone@google.com", time: Commits[5].time},
	}
	require.Equal(t, expectedPersonsRead, commitsRead)
}

func TestWriteAndReadParquet(t *testing.T) {
	tmpfile, cleanup := tempFile(t, "*.parquet")
	defer cleanup()

	expectedPeople, err := newPeople(Commits, newTestBlacklist(t))
	require.NoError(t, err)

	err = expectedPeople.WriteToParquet(tmpfile.Name(), "")
	if err != nil {
		logrus.Fatal(err)
	}
	people, provider, err := readFromParquet(tmpfile.Name())
	require.Equal(t, expectedPeople, people)
	require.Equal(t, "", provider)
}

func TestWriteAndReadParquetWithExternalID(t *testing.T) {
	tmpfile, cleanup := tempFile(t, "*.parquet")
	defer cleanup()

	expectedPeople, err := newPeople(Commits, newTestBlacklist(t))
	require.NoError(t, err)

	expectedIDProvider := "test"
	expectedPeople[1].ExternalID = "username1"
	expectedPeople[2].ExternalID = "username2"

	err = expectedPeople.WriteToParquet(tmpfile.Name(), expectedIDProvider)
	require.NoError(t, err)
	people, provider, err := readFromParquet(tmpfile.Name())
	require.Equal(t, expectedPeople, people)
	require.Equal(t, expectedIDProvider, provider)
}

func TestCleanName(t *testing.T) {
	require := require.New(t)
	for _, names := range [][]string{
		{"  name", "name"},
		{"name  	name  ", "name name"},
		{"name  	name\nsurname", "name name surname"},
		{"name　name", "name name"}, // special space %u3000
	} {
		cName, err := cleanName(names[0])
		require.NoError(err)
		require.Equal(names[1], cName)
	}
}

func TestRemoveParens(t *testing.T) {
	require := require.New(t)
	require.Equal("something something2", removeParens("something (delete it) something2"))
	require.Equal("something () something2", removeParens("something () something2"))
	require.Equal("something (2) something2", removeParens("something (1) (2) something2"))
	require.Equal("something(nospace)something2", removeParens("something(nospace)something2"))
}

func TestNormalizeSpaces(t *testing.T) {
	require := require.New(t)
	require.Equal("1 2", normalizeSpaces("1 2"))
	require.Equal("1 2", normalizeSpaces("1  \t  2 \n\n"))
	require.Equal("12", normalizeSpaces("12"))
}

func TestGetStats(t *testing.T) {
	nameFreqs, emailFreqs, err := getStats(Commits, time.Now().AddDate(0, -12, 0))
	require.NoError(t, err)
	require.Equal(t, map[string]*Frequency{"alice": {0, 1}, "admin": {1, 1}, "bob": {2, 4}},
		nameFreqs)
	require.Equal(t, map[string]*Frequency{"bob@google.com": {2, 3},
		"alice@google.com": {0, 1}, "bad-email@domen": {0, 1},
		"someone@google.com": {1, 1}}, emailFreqs)
}
