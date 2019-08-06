package external

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
)

// UserName represents personal name and username of a person
type UserName struct {
	User    string
	Name    string
	Matched bool // false if there is no match from the external API
}

type safeCache struct {
	cache     map[string]UserName
	lock      sync.RWMutex // mutex to make cache mapping safe for concurrent use
	cachePath string
}

// CachedMatcher is a wrapper for Matcher with a cache
type CachedMatcher struct {
	matcher Matcher
	cache   safeCache
}

const saveFreq int = 20 // Dump cache to file each saveFreq usernames fetched
const csvTrue string = "1"
const csvFalse string = "0"

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
		panic("cachePath cannot be empty")
	}
	logrus.WithFields(logrus.Fields{
		"cachePath": cachePath,
	}).Info("Using caching for external matching")
	cache := safeCache{cache: make(map[string]UserName), cachePath: cachePath, lock: sync.RWMutex{}}
	cachedMatcher := CachedMatcher{matcher: matcher, cache: cache}
	var err error
	if Exists(cachePath) {
		err = cachedMatcher.LoadCache()
	} else {
		// Dump empty cache to make sure that it is possible to write to the file
		err = cachedMatcher.DumpCache()
	}
	return cachedMatcher, err
}

// LoadCache reads CachedMatcher cache from disk.
// It is a proxy for safeCache.LoadCache() function
func (m CachedMatcher) LoadCache() error {
	return m.cache.LoadCache()
}

// DumpCache saves CachedMatcher cache on disk
// It is a proxy for safeCache.DumpCache() function
func (m CachedMatcher) DumpCache() error {
	return m.cache.DumpCache()
}

// MatchByEmail returns the latest GitHub user with the given email.
// If email was fetched already it uses cached value.
// MatchByEmail runs `matcher.MatchByEmail` if not.
func (m CachedMatcher) MatchByEmail(ctx context.Context, email string) (user, name string, err error) {
	if username, exists := m.cache.ReadFromCache(email); exists {
		if username.Matched {
			return username.User, username.Name, nil
		}
		return "", "", ErrNoMatches
	}
	user, name, err = m.matcher.MatchByEmail(ctx, email)
	if err == nil {
		m.cache.AddToCache(email, user, name, true)
	}
	if err == ErrNoMatches {
		m.cache.AddToCache(email, user, name, false)
	}
	if len(m.cache.cache)%saveFreq == 0 {
		err = m.DumpCache()
	}
	return user, name, err
}

// Add to cache safely
func (m safeCache) AddToCache(email string, user string, name string, matched bool) {
	m.lock.Lock()
	m.cache[email] = UserName{user, name, matched}
	m.lock.Unlock()
}

// Read from cache safely
func (m safeCache) ReadFromCache(email string) (UserName, bool) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	val, exists := m.cache[email]
	return val, exists
}

// LoadCache reads cache from disk
func (m safeCache) LoadCache() error {
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
	m.lock.Lock()
	defer m.lock.Unlock()
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
				record[header["user"]], record[header["name"]],
				record[header["match"]] == csvTrue}
		}
	}
	if err == io.EOF {
		err = nil
	}
	return nil
}

// DumpCache saves cache on disk
func (m safeCache) DumpCache() error {
	// TODO(zurk): DumpCache rewrite the whole file every time, which is not very efficient.
	// Instead, we should try to read the existing file, record the existing matches,
	// diff to the new matches and write the difference only. It is not a bottleneck,
	// so keeping as is for now.
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
	m.lock.RLock()
	defer m.lock.RUnlock()
	for email, username := range m.cache {
		match := csvFalse
		if username.Matched {
			match = csvTrue
		}
		err = writer.Write([]string{email, username.User, username.Name, match})
		if err != nil {
			return err
		}
	}
	return nil
}
