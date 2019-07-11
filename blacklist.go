package idmatch

//go:generate esc -o blacklists.go -pkg idmatch -prefix blacklists -modtime 1562752805 blacklists
// -modtime flag is required to make `make check-generate` work.
// Otherwise, the regenerated file has a different modtime value.
// `1562752805` corresponds to 2019-07-10 12:00:05 CEST.
import (
	"bufio"
	"compress/gzip"
	"fmt"
	"regexp"
	"strings"
)

// Blacklist contains all the data to filter identities or identities connection
type Blacklist struct {
	Domains         map[string]struct{}
	TopLevelDomains map[string]struct{}
	Names           map[string]struct{}
	Emails          map[string]struct{}
	PopularEmails   map[string]struct{}
	PopularNames    map[string]struct{}
}

var blacklistFiles = []string{"domains", "top_level_domains", "names", "emails", "popular_emails", "popular_names"}

// NewBlacklist generates Blacklist from the data files embedded to blacklists.go
func NewBlacklist() (Blacklist, error) {
	var blacklist []map[string]struct{}
	for _, name := range blacklistFiles {
		lines, err := readFileLinesSet(fmt.Sprintf("/%s.csv.gz", name))
		if err != nil {
			return Blacklist{}, err
		}
		blacklist = append(blacklist, lines)
	}

	return Blacklist{Domains: blacklist[0], TopLevelDomains: blacklist[1], Names: blacklist[2],
		Emails: blacklist[3], PopularEmails: blacklist[4], PopularNames: blacklist[5]}, nil
}

func readFileLinesSet(filename string) (map[string]struct{}, error) {
	files := FS(false)
	file, err := files.Open(filename)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = file.Close()
	}()

	reader, err := gzip.NewReader(file)
	if err != nil {
		return nil, err
	}

	scanner := bufio.NewScanner(reader)
	scanner.Split(bufio.ScanLines)
	lines := make(map[string]struct{})

	for scanner.Scan() {
		line := scanner.Text()
		normLine, _, err := removeDiacritical(line)
		if err != nil {
			return nil, err
		}
		lines[strings.ToLower(strings.TrimSpace(normalizeSpaces(normLine)))] = struct{}{}
	}

	return lines, err
}

func (b Blacklist) isIgnoredEmail(s string) bool {
	if !strings.Contains(s, "@") || b.isBlacklistedEmail(s) || isMultipleEmail(s) {
		return true
	}
	parts := strings.Split(s, "@")
	domain := parts[1]
	return b.isIgnoredDomain(domain) ||
		b.isIgnoredTopLevelDomain(domain) ||
		isSingleLabelDomain(domain) ||
		isIPDomain(domain)

}

func isMultipleEmail(s string) bool {
	return strings.Index(s, "@") != strings.LastIndex(s, "@")
}

func (b Blacklist) isPopularEmail(s string) bool {
	_, ok := b.PopularEmails[s]
	return ok
}

func (b Blacklist) isPopularName(s string) bool {
	_, ok := b.PopularNames[s]
	return ok
}

func (b Blacklist) isBlacklistedEmail(s string) bool {
	_, ok := b.Emails[s]
	return ok
}

func (b Blacklist) isIgnoredDomain(s string) bool {
	parts := strings.Split(s, "@")
	_, ok := b.Domains[parts[len(parts)-1]]
	return ok
}

func (b Blacklist) isIgnoredTopLevelDomain(s string) bool {
	parts := strings.Split(s, "@")
	topLevelDomain := strings.Split(parts[len(parts)-1], ".")
	_, ok := b.TopLevelDomains[topLevelDomain[len(topLevelDomain)-1]]
	return ok
}

func (b Blacklist) isIgnoredName(name string) bool {
	_, ok := b.Names[strings.ToLower(name)]
	return ok
}

var isIP4EmailRegex = regexp.MustCompile(`\d+\.\d+\.\d+\.\d+$`)
var isIP6EmailRegex = regexp.MustCompile(`(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))`)

func isIPDomain(s string) bool {
	return isIP4EmailRegex.MatchString(s) || isIP6EmailRegex.MatchString(s)
}

func isSingleLabelDomain(s string) bool {
	return strings.Count(s, ".") == 0
}
