package idmatch

import (
	"context"
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
		1: {ID: "_1", Names: []string{"Bob"}, Emails: []string{"Bob@google.com"}},
		2: {ID: "_2", Names: []string{"Bob"}, Emails: []string{"Bob@google.com"}},
		3: {ID: "_3", Names: []string{"Alice"}, Emails: []string{"alice@google.com"}},
		4: {ID: "_4", Names: []string{"Bob"}, Emails: []string{"Bob@google.com"}},
	}
	require.Equal(t, expected, newPeople(rawPersons))
}

func TestTwoPeopleMerge(t *testing.T) {
	require := require.New(t)
	people := newPeople(rawPersons)
	mergedID := people.Merge(1, 2)
	expected := People{
		1: {ID: "_1", Names: []string{"Bob"}, Emails: []string{"Bob@google.com"}},
		3: {ID: "_3", Names: []string{"Alice"}, Emails: []string{"alice@google.com"}},
		4: {ID: "_4", Names: []string{"Bob"}, Emails: []string{"Bob@google.com"}},
	}
	require.Equal(uint64(1), mergedID)
	require.Equal(expected, people)

	mergedID = people.Merge(3, 4)
	expected = People{
		1: {ID: "_1", Names: []string{"Bob"}, Emails: []string{"Bob@google.com"}},
		3: {ID: "_3",
			Names:  []string{"Alice", "Bob"},
			Emails: []string{"Bob@google.com", "alice@google.com"}},
	}
	require.Equal(uint64(3), mergedID)
	require.Equal(expected, people)

	mergedID = people.Merge(1, 3)
	expected = People{
		1: {ID: "_1",
			Names:  []string{"Alice", "Bob"},
			Emails: []string{"Bob@google.com", "alice@google.com"}},
	}
	require.Equal(uint64(1), mergedID)
	require.Equal(expected, people)
}

func TestFourPeopleMerge(t *testing.T) {
	people := newPeople(rawPersons)
	mergedID := people.Merge(1, 2, 3, 4)
	expected := People{
		1: {ID: "_1",
			Names:  []string{"Alice", "Bob"},
			Emails: []string{"Bob@google.com", "alice@google.com"}},
	}
	require.Equal(t, uint64(1), mergedID)
	require.Equal(t, expected, people)
}

func TestPeopleForEach(t *testing.T) {
	people := newPeople(rawPersons)
	var keys = make([]uint64, 0, len(people))
	people.ForEach(func(key uint64, val *Person) bool {
		keys = append(keys, key)
		return false
	})
	require.Equal(t, []uint64{1, 2, 3, 4}, keys)
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
	require.Equal(t, rawPersons, people)
}

func TestFindPeople(t *testing.T) {
	peopleFile, cleanup := tempFile(t, "*.csv")
	defer cleanup()

	err := storePeopleOnDisk(peopleFile.Name(), rawPersons)
	if err != nil {
		return
	}
	people, err := FindPeople(context.TODO(), "0.0.0.0:3306", peopleFile.Name())
	if err != nil {
		return
	}
	expected := People{
		1: {ID: "_1", Names: []string{"Bob"}, Emails: []string{"Bob@google.com"}},
		2: {ID: "_2", Names: []string{"Bob"}, Emails: []string{"Bob@google.com"}},
		3: {ID: "_3", Names: []string{"Alice"}, Emails: []string{"alice@google.com"}},
		4: {ID: "_4", Names: []string{"Bob"}, Emails: []string{"Bob@google.com"}},
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
	require.Equal(t, rawPersons, personsRead)
}

func TestWriteAndReadParquet(t *testing.T) {
	tmpfile, cleanup := tempFile(t, "*.parquet")
	defer cleanup()

	expectedPeople := newPeople(rawPersons)

	err := expectedPeople.WriteToParquet(tmpfile.Name())
	if err != nil {
		logrus.Fatal(err)
	}
	people, err := readFromParquet(tmpfile.Name())
	require.Equal(t, expectedPeople, people)
}

func TestCleanName(t *testing.T) {
	require := require.New(t)
	require.Equal("name", cleanName("  name"))
	require.Equal("name name", cleanName("name  	name  "))
	require.Equal("name name surname", cleanName("name  	name\nsurname"))
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
	require.Equal("1 2 ", normalizeSpaces("1  \t  2 \n\n"))
	require.Equal("12", normalizeSpaces("12"))
}
