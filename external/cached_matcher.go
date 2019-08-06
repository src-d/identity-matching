package external

import (
	"context"
	"encoding/csv"
	"fmt"
	"github.com/sirupsen/logrus"
	"io"
	"os"
	"strings"
)

// UserName represents personal name and username of a person
type UserName struct {
	User  string
	Name  string
	Match bool // false if there in no match from the external API
}

// CachedMatcher is a wrapper for Matcher with a cache
type CachedMatcher struct {
	matcher      Matcher
	cachePath    string
	cache        map[string]UserName
	saveFreq     int // Dump cache to file each saveFreq calls of matcher
}

// Exists reports whether the named file or directory exists.
func Exists(name string) bool {
	if _, err := os.Stat(name); err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}
	return true
}

// NewCachedMatcher creates a new matcher with a cache for a given matcher interface.
func NewCachedMatcher(matcher Matcher, cachePath string) (CachedMatcher, error) {
	if cachePath == "" {
		return CachedMatcher{}, fmt.Errorf("cachePath cannot be empty")
	}
	logrus.Info("Using caching at %s for external matching", cachePath)
	cachedMatcher := CachedMatcher{
		matcher: matcher, cachePath: cachePath, cache: make(map[string]UserName),
		saveFreq: 20}
	var err error
	if Exists(cachePath) {
		err = cachedMatcher.LoadCache()
	} else {
		// Dump empty cache to make sure that it is possible to write to the file
		err = cachedMatcher.DumpCache()
	}
	return cachedMatcher, err
}

// MatchByEmail returns the latest GitHub user with the given email.
// If email was fetched already it uses cached value.
// MatchByEmail runs `matcher.MatchByEmail` if not.
func (m CachedMatcher) MatchByEmail(ctx context.Context, email string) (user, name string, err error) {
	if username, exist := m.cache[email]; exist {
		if username.Match {
			return username.User, username.Name, nil
		}
		return "", "", ErrNoMatches
	}
	user, name, err = m.matcher.MatchByEmail(ctx, email)
	if err == nil {
		m.cache[email] = UserName{user, name, true}
	}
	if err == ErrNoMatches {
		m.cache[email] = UserName{user, name, false}
	}
	if len(m.cache) % m.saveFreq == 0 {
		err = m.DumpCache()
	}
	return user, name, err
}

// LoadCache reads GitHubMatcher cache from disk
func (m CachedMatcher) LoadCache() error {
	var file *os.File
	file, err := os.Open(m.cachePath)
	if err != nil {
		return err
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
			return err
		}
		if len(header) == 0 {
			if len(record) != 4 {
				return fmt.Errorf("invalid CSV file: should have 4 columns")
			}
			for index, name := range record {
				header[name] = index
			}
		} else {
			if len(record) != len(header) {
				return fmt.Errorf("invalid CSV record: %s", strings.Join(record, ","))
			}
			m.cache[record[header["email"]]] = UserName{
				record[header["user"]], record[header["name"]], record[header["match"]] == "true"}
		}
	}

	if err == io.EOF {
		err = nil
	}

	return nil
}

// DumpCache saves GitHubMatcher cache on disk
func (m CachedMatcher) DumpCache() error {
	logrus.Info("Dumping CachedMatcher cache.")
	var file *os.File
	file, err := os.Create(m.cachePath)
	if err != nil {
		return err
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
	err = writer.Write([]string{"email", "user", "name", "match"})
	if err != nil {
		return err
	}
	for email, username := range m.cache {
		match := "false"
		if username.Match {
			match = "true"
		}
		err = writer.Write([]string{email, username.User, username.Name, match})
		if err != nil {
			return err
		}
	}
	return nil
}
