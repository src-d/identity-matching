package idmatch

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"
)

// Person is a single individual that can have multiple names and emails.
type Person struct {
	id     string   `parquet:"name=id, type=UTF8"`
	names  []string `parquet:"name=names, type=LIST, valuetype=UTF8"`
	emails []string `parquet:"name=emails, type=LIST, valuetype=UTF8"`
}

func (p Person) String() string {
	return strings.Join(p.names, "|") + "|" + strings.Join(p.emails, "|")
}

// People is a map of persons indexed by their ID.
type People map[uint64]*Person

func newPeople(persons []rawPerson) People {
	result := make(People)
	var id uint64

	for _, p := range persons {
		if isIgnoredName(p.name) || isIgnoredEmail(p.email) {
			continue
		}

		id++
		result[id] = &Person{
			id:     "_" + strconv.FormatUint(id, 10),
			names:  []string{cleanName(p.name)},
			emails: []string{p.email},
		}
	}

	return result
}

// Merge two persons with the given ids.
func (p People) Merge(ids... uint64) uint64 {
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	p0 := p[ids[0]]
	for _, id := range ids[1:] {
		p0.emails = append(p0.emails, p[id].emails...)
		p0.names = append(p0.names, p[id].names...)
		delete(p, id)
	}
	p0.emails = unique(p0.emails)
	p0.names = unique(p0.names)
	return ids[0]
}

func (p People) ForEach(f func(uint64, *Person) bool) {
	var keys = make([]uint64, len(p))
	for k := range p {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	for _, k := range keys {
		if stop := f(k, p[k]); stop {
			return
		}
	}
}

// FindPeople returns all the people in the database or the disk, if it was
// already cached.
func FindPeople(ctx context.Context, connString string, cachePath string) (People, error) {
	persons, err := findRawPersons(ctx, connString, cachePath)
	if err != nil {
		return nil, err
	}

	return newPeople(persons), nil
}

const findPeopleSQL = `
SELECT DISTINCT repository_id, commit_author_name, commit_author_email
FROM commits;
`

type rawPerson struct {
	repo  string
	name  string
	email string
}

func readPeopleFromDisk(filePath string) (persons []rawPerson, err error) {
	var file *os.File
	file, err = os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer func() {
		errClose := file.Close()
		if err == nil {
			err = errClose
		}
	}()

	reader := csv.NewReader(file)
	for err == nil {
		var r []string
		r, err = reader.Read()
		if len(r) != 3 {
			err = fmt.Errorf("invalid CSV record: %s", strings.Join(r, ","))
		}
		persons = append(persons, rawPerson{r[0], r[1], r[2]})
	}
	if err == io.EOF {
		err = nil
	}
	return
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
		var repo, name, email string
		if err := rows.Scan(&repo, &name, &email); err != nil {
			return nil, err
		}
		result = append(result, rawPerson{repo, name, email})
	}

	return result, rows.Err()
}

func storePeopleOnDisk(filePath string, result []rawPerson) (err error) {
	var file *os.File
	file, err = os.Create(filePath)
	if err != nil {
		return
	}
	defer func() {
		errClose := file.Close()
		if err == nil {
			err = errClose
		}
	}()

	writer := csv.NewWriter(file)
	defer func() {
		writer.Flush()
		if err == nil {
			err = writer.Error()
		}
	}()
	for _, p := range result {
		err = writer.Write([]string{p.repo, p.name, p.email})
		if err != nil {
			return
		}
	}
	return
}

func findRawPersons(ctx context.Context, connStr string, path string) ([]rawPerson, error) {
	if _, err := os.Stat(path); err == nil {
		return readPeopleFromDisk(path)
	} else if !os.IsNotExist(err) {
		return nil, err
	}

	logrus.Printf("not cached in %s, loading from the database", path)
	result, err := readPeopleFromDatabase(ctx, connStr)
	if err != nil {
		return nil, err
	}

	if err := storePeopleOnDisk(path, result); err != nil {
		return nil, err
	}

	return result, nil
}

func cleanName(name string) string {
	return strings.TrimSpace(normalizeSpaces(removeParens(name)))
}

var parensRegex = regexp.MustCompile(`([^\(]+)\s+\(([^\)]+)\)`)
var spacesRegex = regexp.MustCompile(`\s+`)

func removeParens(name string) string {
	return parensRegex.ReplaceAllString(name, "$1")
}

func normalizeSpaces(name string) string {
	return spacesRegex.ReplaceAllString(name, " ")
}
