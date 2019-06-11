package idmatch

import (
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/sirupsen/logrus"
)

// Person is a single individual that can have multiple names and emails.
type Person struct {
	id     uint64
	names  []string
	emails []string
}

func (p Person) String() string {
	var parts = make([]string, 0, len(p.names)+len(p.emails))

	for _, n := range p.names {
		parts = append(parts, n)
	}

	for _, e := range p.emails {
		parts = append(parts, e)
	}

	return strings.Join(parts, "|")
}

// People is a map of persons indexed by their ID.
type People map[uint64]*Person

func newPeople(persons []rawPerson) People {
	result := make(People)
	var id uint64

	for _, p := range persons {
		if isIgnoredName(p.name) || isIgnored(p.email) {
			continue
		}

		id++
		result[id] = &Person{
			id:     id,
			names:  []string{cleanName(p.name)},
			emails: []string{p.email},
		}
	}

	return result
}

// Merge two persons with the given ids.
func (p People) Merge(id1, id2 uint64) {
	p1 := p[id1]
	p2 := p[id2]
	p1.emails = unique(merge(p1.emails, p2.emails))
	p1.names = unique(merge(p1.names, p2.names))
	delete(p, id2)
}

func (p People) Iter(f func(uint64, *Person) bool) {
	var keys = make([]int, len(p))
	for k := range p {
		keys = append(keys, int(k))
	}
	sort.Ints(keys)

	for _, k := range keys {
		k := uint64(k)
		if _, ok := p[k]; ok {
			if stop := f(k, p[k]); stop {
				return
			}
		}
	}
}

func merge(a, b []string) []string {
	var result = make([]string, 0, len(a)+len(b))
	for _, aa := range a {
		result = append(result, aa)
	}
	for _, bb := range b {
		result = append(result, bb)
	}
	return result
}

// FindPeople returns all the people in the database or the disk, if it was
// already cached.
func FindPeople(
	ctx context.Context,
	connString string,
	dataPath string,
) (People, error) {
	persons, err := findRawPersons(ctx, connString, dataPath)
	if err != nil {
		return nil, err
	}

	return newPeople(persons), nil
}

const findPeopleSQL = `
SELECT DISTINCT commit_author_name, commit_author_email
FROM commits
INNER JOIN (
	SELECT commit_author_email as email, COUNT(*) as num
	FROM commits
	GROUP BY email
) t
ON commit_author_email = email AND num > 1
`

type matches map[string][]string

type rawPerson struct {
	name  string
	email string
}

func (p rawPerson) String() string {
	return fmt.Sprintf("%s|%s", p.name, p.email)
}

func readPeopleFromDisk(filePath string) ([]rawPerson, error) {
	bytes, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var result []rawPerson
	for _, line := range strings.Split(string(bytes), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}

		parts := strings.Split(line, "|")
		if strings.Contains(parts[1], ",") {
			continue
		}
		result = append(result, rawPerson{parts[0], parts[1]})
	}

	return result, nil
}

func readPeopleFromDatabase(ctx context.Context, conn string) ([]rawPerson, error) {
	db, err := sql.Open("mysql", conn)
	if err != nil {
		return nil, err
	}

	rows, err := db.QueryContext(ctx, findPeopleSQL)
	if err != nil {
		return nil, err
	}

	var result []rawPerson
	for rows.Next() {
		var name, email string
		if err := rows.Scan(&name, &email); err != nil {
			return nil, err
		}

		if strings.Contains(email, ",") {
			emails := strings.Split(email, ",")
			names := strings.Split(name, " and ")

			for i := 0; i < len(emails); i++ {
				emails[i] = strings.TrimSpace(emails[i])
			}

			for i := 0; i < len(names); i++ {
				names[i] = strings.TrimSpace(names[i])
			}

			if len(names) == len(emails) {
				for i, n := range names {
					if isIgnored(emails[i]) {
						logrus.Warnf("ignored email: %s", emails[i])
						continue
					}

					result = append(result, rawPerson{n, emails[i]})
				}

				continue
			}
		}

		if isIgnored(email) {
			logrus.Warnf("ignored email: %s", email)
			continue
		}

		result = append(result, rawPerson{name, email})
	}

	return result, rows.Err()
}

func storePeopleInDisk(filePath string, result []rawPerson) error {
	f, err := os.Create(filePath)
	if err != nil {
		return err
	}

	defer f.Close()

	var lines = make([]string, len(result))
	for i, p := range result {
		lines[i] = p.String()
	}

	_, err = f.Write([]byte(strings.Join(lines, "\n")))
	return err
}

func findRawPersons(
	ctx context.Context,
	connStr string,
	path string,
) ([]rawPerson, error) {
	filePath := filepath.Join(path, "people.txt")
	if _, err := os.Stat(filePath); err == nil {
		return readPeopleFromDisk(filePath)
	} else if !os.IsNotExist(err) {
		return nil, err
	}

	result, err := readPeopleFromDatabase(ctx, connStr)
	if err != nil {
		return nil, err
	}

	if err := storePeopleInDisk(filePath, result); err != nil {
		return nil, err
	}

	return result, nil
}

func cleanName(name string) string {
	return removeParens(name)
}

var parensRegex = regexp.MustCompile(`([^\(]+)\s+\(([^\)]+)\)`)

func removeParens(name string) string {
	return parensRegex.ReplaceAllString(name, "$1")
}
