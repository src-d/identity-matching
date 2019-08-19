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
	"strings"
	"time"

	"github.com/briandowns/spinner"
	_ "github.com/lib/pq" // postgres driver registration
	"github.com/sirupsen/logrus"
	"github.com/src-d/eee-identity-matching/reporter"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
)

// rawPerson is taken from a single commit signature with only one name and one email
type rawPerson struct {
	repo  string
	name  string
	email string
}

// NameWithRepo is a Name that can be linked to a specific repo.
type NameWithRepo struct {
	Name string
	Repo string
}

// Person is a single individual that can have multiple names and emails.
type Person struct {
	ID             int64
	NamesWithRepos []NameWithRepo
	Emails         []string
	ExternalID     string
}

func uniqueNamesWithRepo(names []NameWithRepo) []NameWithRepo {
	seen := map[string]struct{}{}
	var result []NameWithRepo
	for _, n := range names {
		nameStr := n.String()
		if _, ok := seen[nameStr]; ok {
			continue
		}

		seen[nameStr] = struct{}{}
		result = append(result, n)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].String() < result[j].String()
	})
	return result
}

// String describes the person's identity parts.
func (rn NameWithRepo) String() string {
	if rn.Repo == "" {
		return rn.Name
	}
	return fmt.Sprintf("{%s, %s}", rn.Name, rn.Repo)
}

// String describes the person's identity parts.
func (p Person) String() string {
	var namesWithRepos []string
	for _, name := range p.NamesWithRepos {
		namesWithRepos = append(namesWithRepos, name.String())
	}
	sort.Strings(namesWithRepos)
	sort.Strings(p.Emails)
	return fmt.Sprintf("%s:%s||%s", p.ExternalID,
		strings.Join(namesWithRepos, "|"), strings.Join(p.Emails, "|"))
}

// People is a map of persons indexed by their ID.
type People map[int64]*Person

func newPeople(persons []rawPerson, blacklist Blacklist) (People, error) {
	result := make(People)
	var id int64
	var nameWithRepo NameWithRepo

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
			reporter.Increment("popular names")
			nameWithRepo = NameWithRepo{name, p.repo}
		} else {
			nameWithRepo = NameWithRepo{name, ""}
		}

		ignoredName := blacklist.isIgnoredName(name)
		ignoredEmail := blacklist.isIgnoredEmail(email)
		if ignoredName {
			reporter.Increment("ignored names")
		}
		if ignoredEmail {
			reporter.Increment("ignored emails")
		}
		if ignoredEmail || ignoredName {
			continue
		}

		id++
		result[id] = &Person{
			ID:             id,
			NamesWithRepos: []NameWithRepo{nameWithRepo},
			Emails:         []string{email},
		}
	}
	reporter.Commit("people after filtering", len(result))

	return result, nil
}

type parquetPerson struct {
	ID                 int64  `parquet:"name=id, type=INT_64"`
	Email              string `parquet:"name=email, type=UTF8"`
	Name               string `parquet:"name=name, type=UTF8"`
	Repo               string `parquet:"name=repo, type=UTF8"`
	ExternalIDProvider string `parquet:"name=external_id_provider, type=UTF8"`
	ExternalID         string `parquet:"name=external_id, type=UTF8"`
}

func readFromParquet(path string) (People, string, error) {
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

	pr, err := reader.NewParquetReader(fr, new(parquetPerson), int64(runtime.NumCPU()))
	if err != nil {
		logrus.Fatal("Read error", err)
	}
	num := int(pr.GetNumRows())
	parquetPersons := make([]parquetPerson, num)
	if err = pr.Read(&parquetPersons); err != nil {
		logrus.Println("Read error", err)
		return nil, "", err
	}
	pr.ReadStop()
	people := make(People)
	var externalIDProvider string
	for _, person := range parquetPersons {
		if _, ok := people[person.ID]; !ok {
			people[person.ID] = &Person{person.ID, nil, nil, ""}
		}
		if person.Email != "" {
			people[person.ID].Emails = append(people[person.ID].Emails, person.Email)
		}
		if person.Name != "" {
			people[person.ID].NamesWithRepos = append(people[person.ID].NamesWithRepos,
				NameWithRepo{person.Name, person.Repo})
		}
		if person.ExternalID != "" {
			if externalIDProvider != "" && externalIDProvider != person.ExternalIDProvider {
				return people, externalIDProvider, fmt.Errorf(
					"there are multiple ExternalIDProvider-s for %s: %s %s",
					people[person.ID].String(), externalIDProvider, person.ExternalIDProvider)
			}
			if people[person.ID].ExternalID != "" && person.ExternalID != people[person.ID].ExternalID {
				return people, externalIDProvider, fmt.Errorf(
					"there are multiple usernames for %s: %s %s",
					people[person.ID].String(), person.ExternalID, people[person.ID].ExternalID)
			}
			externalIDProvider = person.ExternalIDProvider
			people[person.ID].ExternalID = person.ExternalID
		}
	}
	return people, externalIDProvider, err
}

// WriteToParquet saves People structure to parquet file.
func (p People) WriteToParquet(path string, externalIDProvider string) (err error) {
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

	pw, err := writer.NewParquetWriter(pf, new(parquetPerson), int64(runtime.NumCPU()))
	if err != nil {
		logrus.Fatal("Failed to create new parquet writer.", err)
	}
	provider := ""
	pw.CompressionType = parquet.CompressionCodec_UNCOMPRESSED
	p.ForEach(func(key int64, val *Person) bool {
		for _, email := range val.Emails {
			if val.ExternalID == "" {
				provider = ""
			} else {
				provider = externalIDProvider
			}
			if err := pw.Write(parquetPerson{
				val.ID, email, "", "", provider, val.ExternalID}); err != nil {
				return true
			}
		}
		for _, name := range val.NamesWithRepos {
			if err = pw.Write(parquetPerson{val.ID, "", name.Name, name.Repo, "", ""}); err != nil {
				return true
			}
		}
		return false
	})
	err = pw.WriteStop()
	return
}

// Merge several persons with the given ids.
func (p People) Merge(ids ...int64) (int64, error) {
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	p0 := p[ids[0]]
	newExternalID := p0.ExternalID
	for _, id := range ids[1:] {
		if newExternalID == "" {
			newExternalID = p[id].ExternalID
		} else if p[id].ExternalID != "" && p[id].ExternalID != newExternalID {
			return -1, fmt.Errorf("cannot merge ids %v with different ExternalIDs: %s %s",
				ids, newExternalID, p[id].ExternalID)
		}
		p0.Emails = append(p0.Emails, p[id].Emails...)
		p0.NamesWithRepos = append(p0.NamesWithRepos, p[id].NamesWithRepos...)
		delete(p, id)
	}
	p0.Emails = unique(p0.Emails)
	p0.NamesWithRepos = uniqueNamesWithRepo(p0.NamesWithRepos)

	return ids[0], nil
}

// ForEach executes a function over each person in the collection.
// The order is fixed and constant.
func (p People) ForEach(f func(int64, *Person) bool) {
	var keys = make([]int64, 0, len(p))
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

// WriteToPostgres saves people identities to a Postgres database
func (p People) WriteToPostgres(host, port, user, pass, dbname, table,
	externalIDProvider string) error {

	connStr := fmt.Sprintf("host=%s port=%s user=%s dbname=%s sslmode=disable",
		host, port, user, dbname)
	if pass != "" {
		connStr += fmt.Sprintf(" password='%s'", pass)
	}
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return err
	}
	defer func() {
		errClose := db.Close()
		if errClose != nil {
			err = errClose
		}
	}()

	rows, err := db.Query(fmt.Sprintf(existsTableSQL, table))
	if err != nil {
		return err
	}
	var exists bool
	rows.Next()
	err = rows.Scan(&exists)
	if err != nil {
		return err
	}
	if exists {
		return fmt.Errorf("table %s already exists in the database", table)
	}
	_, err = db.Exec(fmt.Sprintf(createTableSQL, table))
	if err != nil {
		return err
	}

	values := make([]string, 0, writeBatchSize)

	commit := func() bool {
		req := fmt.Sprintf(writeToTableSQL, table, strings.Join(values, ","))
		values = values[:0]
		if _, err = db.Exec(req); err != nil {
			logrus.Errorf("Postgres write error for sql request %s", req)
			return false
		}
		return true
	}

	provider := ""
	p.ForEach(func(key int64, val *Person) bool {
		for _, email := range val.Emails {
			if val.ExternalID == "" {
				provider = ""
			} else {
				provider = externalIDProvider
			}
			values = append(values, fmt.Sprintf(valuesTemplate,
				val.ID, email, "", "", provider, val.ExternalID))
		}
		for _, name := range val.NamesWithRepos {
			values = append(values, fmt.Sprintf(valuesTemplate,
				val.ID, "", strings.Replace(name.Name, "'", "''", -1), name.Repo, "", ""))
		}
		if len(values) >= writeBatchSize {
			return !commit()
		}
		return false
	})
	if ok := commit(); !ok {
		return fmt.Errorf("postgres write error")
	}

	return err
}

// FindPeople returns all the people in the database or from the disk cache.
func FindPeople(ctx context.Context, connString string, cachePath string, blacklist Blacklist) (People, error) {
	persons, err := findRawPersons(ctx, connString, cachePath)
	reporter.Commit("people found", len(persons))
	if err != nil {
		return nil, err
	}
	people, err := newPeople(persons, blacklist)
	if err != nil {
		return nil, err
	}
	return people, nil
}

const existsTableSQL = `
SELECT EXISTS (
	SELECT 1
	FROM   information_schema.tables 
	WHERE  table_name = '%s'
);
`
const createTableSQL = `
CREATE TABLE %s (
	id int NOT NULL,
	email text NOT NULL,
	name text NOT NULL,
	repo text NOT NULL,
	external_id_provider text NOT NULL,
	external_id text NOT NULL
);
`
const writeBatchSize = 2000
const valuesTemplate = `(%d, '%s', '%s', '%s', '%s', '%s')`
const writeToTableSQL = `INSERT INTO %s VALUES %s;`

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

	spin := spinner.New(spinner.CharSets[11], 100*time.Millisecond)
	spin.Start()
	defer spin.Stop()
	var result []rawPerson
	i := 0
	for rows.Next() {
		spin.Suffix = fmt.Sprintf(" %d", i+1)
		i++
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

	if path != "" {
		logrus.Printf("Caching the result to %s", path)
		if err := storePeopleOnDisk(path, result); err != nil {
			return nil, err
		}
	}

	return result, nil
}

func cleanName(name string) (string, error) {
	name, _, err := removeDiacritical(name)
	if err != nil {
		return name, err
	}
	cleanName := strings.TrimSpace(normalizeSpaces(strings.ToLower(name)))
	if cleanName == name {
		reporter.Increment("clean names")
	}
	return cleanName, err
}

func cleanEmail(email string) (string, error) {
	email, _, err := removeDiacritical(email)
	if err != nil {
		return email, err
	}
	cleanEmail := strings.TrimSpace(normalizeSpaces(strings.ToLower(email)))
	if cleanEmail == email {
		reporter.Increment("clean emails")
	}
	return cleanEmail, err
}

var parensRegex = regexp.MustCompile(`([^\(]+)\s+\(([^\)]+)\)`)

func removeParens(name string) string {
	return parensRegex.ReplaceAllString(name, "$1")
}

func normalizeSpaces(name string) string {
	return strings.Join(strings.Fields(name), " ")
}
