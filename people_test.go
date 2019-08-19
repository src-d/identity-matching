package idmatch

import (
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

var rawPersons = []rawPerson{
	{repo: "repo1", name: "Bob", email: "Bob@google.com"},
	{repo: "repo2", name: "Bob", email: "Bob@google.com"},
	{repo: "repo1", name: "Alice", email: "alice@google.com"},
	{repo: "repo1", name: "Bob", email: "Bob@google.com"},
	{repo: "repo1", name: "Bob", email: "bad-email@domen"},
	{repo: "repo1", name: "admin", email: "someone@google.com"},
}

func TestPeopleNew(t *testing.T) {
	expected := People{
		1: {ID: 1, NamesWithRepos: []NameWithRepo{{"bob", ""}}, Emails: []string{"bob@google.com"}},
		2: {ID: 2, NamesWithRepos: []NameWithRepo{{"bob", ""}}, Emails: []string{"bob@google.com"}},
		3: {ID: 3, NamesWithRepos: []NameWithRepo{{"alice", ""}}, Emails: []string{"alice@google.com"}},
		4: {ID: 4, NamesWithRepos: []NameWithRepo{{"bob", ""}}, Emails: []string{"bob@google.com"}},
	}
	people, err := newPeople(rawPersons, newTestBlacklist(t))
	require.NoError(t, err)
	require.Equal(t, expected, people)
}

func TestTwoPeopleMerge(t *testing.T) {
	require := require.New(t)
	people, err := newPeople(rawPersons, newTestBlacklist(t))
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
	people, err := newPeople(rawPersons, newTestBlacklist(t))
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
	people, err := newPeople(rawPersons, newTestBlacklist(t))
	require.NoError(t, err)
	people[1].ExternalID = "id1"
	people[2].ExternalID = "id2"
	_, err = people.Merge(1, 2)
	require.Error(t, err)
}

func TestPeopleForEach(t *testing.T) {
	people, err := newPeople(rawPersons, newTestBlacklist(t))
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

func TestFindRawPersons(t *testing.T) {
	peopleFile, cleanup := tempFile(t, "*.csv")
	defer cleanup()

	err := storePeopleOnDisk(peopleFile.Name(), rawPersons)
	if err != nil {
		return
	}
	people, err := findRawPersons(context.TODO(), "0.0.0.0:3306", peopleFile.Name())
	if err != nil {
		return
	}
	require.Equal(t, []rawPerson{
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

	err := storePeopleOnDisk(peopleFile.Name(), rawPersons)
	if err != nil {
		return
	}
	people, err := FindPeople(context.TODO(), "0.0.0.0:3306", peopleFile.Name(), newTestBlacklist(t))
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
}

func TestReadPeopleFromDatabase(t *testing.T) {
	// TODO(zurk)
}

func TestStoreAndReadPeopleOnDisk(t *testing.T) {
	peopleFile, cleanup := tempFile(t, "*.csv")
	defer cleanup()

	err := storePeopleOnDisk(peopleFile.Name(), rawPersons)
	if err != nil {
		return
	}
	peopleFileContent, err := ioutil.ReadFile(peopleFile.Name())
	if err != nil {
		return
	}
	expectedContent := `repo,name,email
repo1,Bob,Bob@google.com
repo2,Bob,Bob@google.com
repo1,Alice,alice@google.com
repo1,Bob,Bob@google.com
repo1,Bob,bad-email@domen
repo1,admin,someone@google.com
`
	require.Equal(t, expectedContent, string(peopleFileContent))

	personsRead, err := readRawPersonsFromDisk(peopleFile.Name())
	if err != nil {
		return
	}
	expectedPersonsRead := []rawPerson{
		0: {repo: "repo1", name: "bob", email: "bob@google.com"},
		1: {repo: "repo2", name: "bob", email: "bob@google.com"},
		2: {repo: "repo1", name: "alice", email: "alice@google.com"},
		3: {repo: "repo1", name: "bob", email: "bob@google.com"},
		4: {repo: "repo1", name: "bob", email: "bad-email@domen"},
		5: {repo: "repo1", name: "admin", email: "someone@google.com"},
	}
	require.Equal(t, expectedPersonsRead, personsRead)
}

func TestWriteAndReadParquet(t *testing.T) {
	tmpfile, cleanup := tempFile(t, "*.parquet")
	defer cleanup()

	expectedPeople, err := newPeople(rawPersons, newTestBlacklist(t))
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

	expectedPeople, err := newPeople(rawPersons, newTestBlacklist(t))
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

func TestPostgresOutputFormat(t *testing.T) {
	expectedPeople, err := newPeople(rawPersons, newTestBlacklist(t))
	require.NoError(t, err)

	expectedIDProvider := "test"
	expectedPeople[1].ExternalID = "username1"
	expectedPeople[2].ExternalID = "username2"

	host, port, user, pass, dbname, table := "0.0.0.0", "5432", "superset", "superset", "superset", "test"
	err = expectedPeople.WriteToPostgres(host, port, user, pass, dbname, table, expectedIDProvider)
	require.NoError(t, err)

	connStr := fmt.Sprintf("host=%s port=%s user=%s dbname=%s sslmode=disable",
		host, port, user, dbname)
	if pass != "" {
		connStr += fmt.Sprintf(" password='%s'", pass)
	}
	db, err := sql.Open("postgres", connStr)
	require.NoError(t, err)
	defer func() {
		errClose := db.Close()
		require.NoError(t, errClose)
	}()

	rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s", table))
	require.NoError(t, err)
	cols, err := rows.Columns()
	require.NoError(t, err)
	require.Equal(t, cols, []string{
		"id", "email", "name", "repo", "external_id_provider", "external_id"})
	expectedRowsNumber := 8
	rowsNumber := 0
	var id int
	var email, name, repo, externalIDProvider, externalID string
	expectedIDs := []int{1, 1, 2, 2, 3, 3, 4, 4}
	expectedEmails := []string{"bob@google.com", "", "bob@google.com", "", "alice@google.com", "", "bob@google.com", ""}
	expectedNames := []string{"", "bob", "", "bob", "", "alice", "", "bob"}
	expectedRepos := []string{"", "", "", "", "", "", "", ""}
	expectedExternalIDProviders := []string{"test", "", "test", "", "", "", "", ""}
	expectedExternalID := []string{"username1", "", "username2", "", "", "", "", ""}
	for rows.Next() {
		err = rows.Scan(&id, &email, &name, &repo, &externalIDProvider, &externalID)
		require.NoError(t, err, rowsNumber)
		require.Equal(t, expectedIDs[rowsNumber], id, rowsNumber)
		require.Equal(t, expectedEmails[rowsNumber], email, rowsNumber)
		require.Equal(t, expectedNames[rowsNumber], name, rowsNumber)
		require.Equal(t, expectedRepos[rowsNumber], repo, rowsNumber)
		require.Equal(t, expectedExternalIDProviders[rowsNumber], externalIDProvider, rowsNumber)
		require.Equal(t, expectedExternalID[rowsNumber], externalID, rowsNumber)
		rowsNumber++
	}
	require.Equal(t, expectedRowsNumber, rowsNumber)
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
