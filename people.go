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
	"github.com/sirupsen/logrus"
	"github.com/src-d/eee-identity-matching/external"
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
	PrimaryName    string
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

type parquetPersonAlias struct {
	ID    int64  `parquet:"name=id, type=INT_64"`
	Email string `parquet:"name=email, type=UTF8"`
	Name  string `parquet:"name=name, type=UTF8"`
	Repo  string `parquet:"name=repo, type=UTF8"`
}

type parquetPersonIdentity struct {
	ID                 int64  `parquet:"name=id, type=INT_64"`
	PrimaryName        string `parquet:"name=primary_name, type=UTF8"`
	ExternalIDProvider string `parquet:"name=external_id_provider, type=UTF8"`
	ExternalID         string `parquet:"name=external_id, type=UTF8"`
}

func readFromParquet(pathAliases string) (People, string, error) {
	pathAliases, pathIDs := preparePaths(pathAliases)
	getParquetReader := func(path string, obj interface{}) (*reader.ParquetReader, func()) {
		fr, err := local.NewLocalFileReader(path)
		if err != nil {
			logrus.Fatal("read error", err)
		}
		cleanup := func() {
			err = fr.Close()
			if err != nil {
				logrus.Fatal("failed to close the file", err)
			}
		}

		pr, err := reader.NewParquetReader(fr, obj, int64(runtime.NumCPU()))
		if err != nil {
			logrus.Fatal("read error", err)
		}
		return pr, cleanup
	}

	pr, cleanupAliases := getParquetReader(pathAliases, new(parquetPersonAlias))
	defer cleanupAliases()
	num := int(pr.GetNumRows())
	parquetPersonAliases := make([]parquetPersonAlias, num)
	if err := pr.Read(&parquetPersonAliases); err != nil {
		logrus.Printf("read error in %s: %v", pathAliases, err)
		return nil, "", err
	}
	pr.ReadStop()

	prIDs, cleanupIds := getParquetReader(pathIDs, new(parquetPersonIdentity))
	defer cleanupIds()
	numIds := int(prIDs.GetNumRows())
	parquetPersonsIDs := make([]parquetPersonIdentity, numIds)
	if err := prIDs.Read(&parquetPersonsIDs); err != nil {
		logrus.Printf("read error in %s: %v", pathIDs, err)
		return nil, "", err
	}
	prIDs.ReadStop()
	id2PersonID := map[int64]parquetPersonIdentity{}
	for _, pp := range parquetPersonsIDs {
		id2PersonID[pp.ID] = pp
	}

	people := make(People)
	var externalIDProvider, curExternalIDProvider string
	for _, person := range parquetPersonAliases {
		if _, ok := people[person.ID]; !ok {
			people[person.ID] = &Person{person.ID, nil, nil, "", ""}
		}
		if person.Email != "" {
			people[person.ID].Emails = append(people[person.ID].Emails, person.Email)
		}
		if person.Name != "" {
			people[person.ID].NamesWithRepos = append(people[person.ID].NamesWithRepos,
				NameWithRepo{person.Name, person.Repo})
		}
	}
	for _, p := range people {
		people[p.ID].PrimaryName = id2PersonID[p.ID].PrimaryName
		people[p.ID].ExternalID = id2PersonID[p.ID].ExternalID
		curExternalIDProvider = id2PersonID[p.ID].ExternalIDProvider
		if people[p.ID].ExternalID != "" {
			if externalIDProvider != "" && externalIDProvider != curExternalIDProvider {
				return people, externalIDProvider, fmt.Errorf(
					"there are multiple ExternalIDProvider-s for %s: %s %s",
					people[p.ID].String(), externalIDProvider, curExternalIDProvider)
			}
			externalIDProvider = curExternalIDProvider
		}
	}
	return people, externalIDProvider, nil
}

// WriteToParquet saves People structure to parquet file.
func (p People) WriteToParquet(path string, externalIDProvider string) (err error) {
	path, pathIDs := preparePaths(path)
	getParquetWriter := func(path string, obj interface{}) (*writer.ParquetWriter, func()) {
		pf, err := local.NewLocalFileWriter(path)
		if err != nil {
			logrus.Fatalf("failed to create a new local file writer at %s: %v", path, err)
		}
		pw, err := writer.NewParquetWriter(pf, obj, int64(runtime.NumCPU()))
		if err != nil {
			logrus.Fatalf("failed to create a new parquet writer: %v", err)
		}
		pw.CompressionType = parquet.CompressionCodec_UNCOMPRESSED
		cleanup := func() {
			err = pw.WriteStop()
			if err != nil {
				logrus.Fatal("failed to stop write to parquet", err)
			}
			errClose := pf.Close()
			if err == nil {
				err = errClose
			}
			if err != nil {
				logrus.Errorf("failed to store the matches to %s: %v", path, err)
			}
		}
		return pw, cleanup
	}

	pw, cleanup := getParquetWriter(path, new(parquetPersonAlias))
	defer cleanup()
	pwIDs, cleanupIDs := getParquetWriter(pathIDs, new(parquetPersonIdentity))
	defer cleanupIDs()

	p.ForEach(func(key int64, val *Person) bool {
		provider := ""
		if val.ExternalID != "" {
			provider = externalIDProvider
		}
		if err := pwIDs.Write(parquetPersonIdentity{
			val.ID, val.PrimaryName, provider, val.ExternalID}); err != nil {
			return true
		}
		for _, email := range val.Emails {
			if err := pw.Write(parquetPersonAlias{
				val.ID, email, "", ""}); err != nil {
				return true
			}
		}
		for _, name := range val.NamesWithRepos {
			if err = pw.Write(parquetPersonAlias{
				val.ID, "", name.Name, name.Repo}); err != nil {
				return true
			}
		}
		return false
	})
	return
}

func preparePaths(rawPath string) (pathAliases, pathIDs string) {
	if strings.HasSuffix(rawPath, ".parquet") {
		rawPath = rawPath[:len(rawPath)-len(".parquet")]
	}
	pathAliases = rawPath + "-aliases.parquet"
	pathIDs = rawPath + "-identities.parquet"
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

// FindPeople returns all the people in the database or from the disk cache.
func FindPeople(ctx context.Context, connString string, cachePath string, blacklist Blacklist, extmatcher external.Matcher) (
	People, map[string]int, error) {
	persons, err := findRawPersons(ctx, connString, cachePath, extmatcher)
	reporter.Commit("people found", len(persons))
	if err != nil {
		return nil, nil, err
	}
	people, err := newPeople(persons, blacklist)
	if err != nil {
		return nil, nil, err
	}
	nameFreqs, err := getNamesFreqs(persons)
	if err != nil {
		return people, nil, err
	}

	return people, nameFreqs, nil
}

// getNamesFreqs calculates frequencies of rawPerson names
func getNamesFreqs(persons []rawPerson) (map[string]int, error) {
	freqs := map[string]int{}
	for _, p := range persons {
		name, err := cleanName(p.name)
		if err != nil {
			return nil, err
		}
		freqs[name]++
	}
	return freqs, nil
}

const findPeopleSQL = `SELECT c.repository_id, c.commit_author_name, c.commit_author_email, MAX(c.commit_hash) AS commit_hash
FROM commits c
GROUP BY 1, 2, 3;`

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

func readRawPersonsFromDatabase(ctx context.Context, conn string, extmatcher external.Matcher) ([]rawPerson, error) {
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
		var p rawPerson
		var commitHash string
		if err := rows.Scan(&p.repo, &p.name, &p.email, &commitHash); err != nil {
			return nil, err
		}

		if cs, ok := extmatcher.(external.CommitScanner); ok {
			if err := cs.ScanCommit(ctx, p.repo, p.email, commitHash); err != nil {
				logrus.Errorf("error scanning commit", err)
			}
		} else {
			logrus.Fatalf("no commitscanner") //FIXME: remove
		}

		result = append(result, p)
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

func findRawPersons(ctx context.Context, connStr string, path string, extmatcher external.Matcher) ([]rawPerson, error) {
	if _, err := os.Stat(path); err == nil {
		return readRawPersonsFromDisk(path)
	} else if !os.IsNotExist(err) {
		return nil, err
	}

	logrus.Printf("not cached in %s, loading from the database", path)
	result, err := readRawPersonsFromDatabase(ctx, connStr, extmatcher)
	if err != nil {
		return nil, err
	}

	if path != "" {
		logrus.Printf("caching the result to %s", path)
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
