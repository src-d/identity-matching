package idmatch

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"

	"github.com/sirupsen/logrus"
)

// rawPerson is taken from a single commit signature with only one name and one email
type rawPerson struct {
	repo  string
	name  string
	email string
}

// Person is a single individual that can have multiple names and emails.
type Person struct {
	ID     string   `parquet:"name=id, type=UTF8"`
	Names  []string `parquet:"name=names, type=LIST, valuetype=UTF8"`
	Emails []string `parquet:"name=emails, type=LIST, valuetype=UTF8"`
}

// String describes the person's identity parts.
func (p Person) String() string {
	return strings.Join(p.Names, "|") + "|" + strings.Join(p.Emails, "|")
}

// People is a map of persons indexed by their ID.
type People map[uint64]*Person

func newPeople(persons []rawPerson, blacklist Blacklist) (People, error) {
	result := make(People)
	var id uint64

	for _, p := range persons {
		name, err := cleanName(p.name)
		if err != nil {
			return nil, err
		}
		email, err := cleanEmail(p.email)
		if err != nil {
			return nil, err
		}
		if blacklist.isPopularName(name) {
			name = fmt.Sprintf("(%s, %s)", name, p.repo)
		}

		if blacklist.isIgnoredName(name) || blacklist.isIgnoredEmail(email) {
			continue
		}

		id++
		result[id] = &Person{
			ID:     "_" + strconv.FormatUint(id, 10),
			Names:  []string{name},
			Emails: []string{email},
		}
	}

	return result, nil
}

func toPeople(persons []Person) People {
	people := make(People, len(persons))
	for index := range persons {
		people[uint64(index)+1] = &persons[index]
	}
	return people
}

func readFromParquet(path string) (People, error) {
	fr, err := local.NewLocalFileReader(path)
	if err != nil {
		logrus.Fatal("Read error", err)
	}
	defer func() {
		err = fr.Close()
		if err != nil {
			logrus.Fatal("Failed to close the file.", err)
		}
	}()

	pr, err := reader.NewParquetReader(fr, new(Person), int64(runtime.NumCPU()))
	if err != nil {
		logrus.Fatal("Read error", err)
	}
	num := int(pr.GetNumRows())
	persons := make([]Person, num)
	if err = pr.Read(&persons); err != nil {
		logrus.Println("Read error", err)
	}
	pr.ReadStop()
	return toPeople(persons), err
}

// WriteToParquet saves People structure to parquet file.
func (p People) WriteToParquet(path string) (err error) {
	pf, err := local.NewLocalFileWriter(path)
	defer func() {
		errClose := pf.Close()
		if err == nil {
			err = errClose
		}
		if err != nil {
			logrus.Errorf("failed to store the matches to %s: %v", path, err)
		}
	}()
	pw, err := writer.NewParquetWriter(pf, new(Person), int64(runtime.NumCPU()))
	if err != nil {
		logrus.Fatal("Failed to create new parquet writer.", err)
	}
	pw.CompressionType = parquet.CompressionCodec_UNCOMPRESSED
	p.ForEach(func(key uint64, val *Person) bool {
		err = pw.Write(*val)
		return err != nil
	})
	err = pw.WriteStop()
	return
}

// Merge several persons with the given ids.
func (p People) Merge(ids ...uint64) uint64 {
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	p0 := p[ids[0]]
	for _, id := range ids[1:] {
		p0.Emails = append(p0.Emails, p[id].Emails...)
		p0.Names = append(p0.Names, p[id].Names...)
		delete(p, id)
	}
	p0.Emails = unique(p0.Emails)
	p0.Names = unique(p0.Names)
	return ids[0]
}

// ForEach executes a function over each person in the collection.
// The order is fixed and constant.
func (p People) ForEach(f func(uint64, *Person) bool) {
	var keys = make([]uint64, 0, len(p))
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

// FindPeople returns all the people in the database or from the disk cache.
func FindPeople(ctx context.Context, connString string, cachePath string, blacklist Blacklist) (People, error) {
	persons, err := findRawPersons(ctx, connString, cachePath)
	if err != nil {
		return nil, err
	}
	people, err := newPeople(persons, blacklist)
	if err != nil {
		return nil, err
	}
	return people, nil
}

const findPeopleSQL = `
SELECT DISTINCT repository_id, commit_author_name, commit_author_email
FROM commits;
`

func readRawPersonsFromDisk(filePath string) (persons []rawPerson, err error) {
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

	r := csv.NewReader(file)
	header := make(map[string]int)
	rowIndex := 0
	for {
		record, err := r.Read()
		rowIndex++

		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if len(header) == 0 {
			if len(record) != 3 {
				err = fmt.Errorf("invalid CSV file: should have 3 columns")
				return nil, err
			}
			for index, name := range record {
				header[name] = index
			}
		} else {
			if len(record) != len(header) {
				err = fmt.Errorf("invalid CSV record: %s", strings.Join(record, ","))
				return nil, err
			}

			for key := range header {
				normValue, _, err := removeDiacritical(record[header[key]])
				if err != nil {
					return nil, err
				}
				record[header[key]] = strings.TrimSpace(normalizeSpaces(strings.ToLower(normValue)))
			}

			person := rawPerson{
				repo:  record[header["repo"]],
				name:  record[header["name"]],
				email: record[header["email"]]}
			if person.repo == "" || person.email == "" || person.name == "" {
				continue
			}
			persons = append(persons, person)
		}
	}

	if err == io.EOF {
		err = nil
	}
	return
}

func readRawPersonsFromDatabase(ctx context.Context, conn string) ([]rawPerson, error) {
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
	err = writer.Write([]string{"repo", "name", "email"})
	if err != nil {
		return
	}
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
		return readRawPersonsFromDisk(path)
	} else if !os.IsNotExist(err) {
		return nil, err
	}

	logrus.Printf("not cached in %s, loading from the database", path)
	result, err := readRawPersonsFromDatabase(ctx, connStr)
	if err != nil {
		return nil, err
	}

	if err := storePeopleOnDisk(path, result); err != nil {
		return nil, err
	}

	return result, nil
}

func cleanName(name string) (string, error) {
	name, _, err := removeDiacritical(name)
	if err != nil {
		return name, err
	}
	return strings.TrimSpace(normalizeSpaces(strings.ToLower(name))), err
}

func cleanEmail(email string) (string, error) {
	email, _, err := removeDiacritical(email)
	if err != nil {
		return email, err
	}
	return strings.TrimSpace(normalizeSpaces(strings.ToLower(email))), err
}

var parensRegex = regexp.MustCompile(`([^\(]+)\s+\(([^\)]+)\)`)

func removeParens(name string) string {
	return parensRegex.ReplaceAllString(name, "$1")
}

func normalizeSpaces(name string) string {
	return strings.Join(strings.Fields(name), " ")
}
