// +build !cipr

package external

import (
	"context"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func tempFile(t *testing.T, pattern string) (*os.File, func()) {
	t.Helper()
	f, err := ioutil.TempFile("", pattern)
	require.NoError(t, err)
	return f, func() {
		require.NoError(t, f.Close())
		require.NoError(t, os.Remove(f.Name()))
	}
}

func TestNewCachedMatcher(t *testing.T) {
	req := require.New(t)
	matcher, _ := NewGitHubMatcher("", githubTestToken)
	cache, cleanup := tempFile(t, "*.csv")
	defer cleanup()
	_, err := cache.Write([]byte("email,user,match"))
	req.NoError(err)
	cachedMatcher, err := NewCachedMatcher(matcher, cache.Name())
	scache := safeUserCache{
		cache: make(map[string]CachedUser), cachePath: cache.Name(), lock: sync.RWMutex{}}
	expectedCachedMatcher := &CachedMatcher{matcher: matcher, cache: scache}
	req.NoError(err)
	req.Equal(expectedCachedMatcher, cachedMatcher)
}

func TestMatchByEmailAndDump(t *testing.T) {
	req := require.New(t)
	matcher, _ := NewGitHubMatcher("", githubTestToken)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cache, cleanup := tempFile(t, "*.csv")
	defer cleanup()
	_, err := cache.Write([]byte("email,user,match"))
	req.NoError(err)
	cachedMatcher, err := NewCachedMatcher(matcher, cache.Name())
	req.NoError(err)

	user, err := cachedMatcher.MatchByEmail(ctx, "mcuadros@gmail.com")
	req.Equal("mcuadros", user)
	req.NoError(err)

	err = cachedMatcher.DumpCache()
	req.NoError(err)
	cacheContent, err := ioutil.ReadFile(cache.Name())
	req.NoError(err)
	expectedCacheContent := "email,user,match\nmcuadros@gmail.com,mcuadros,1\n"
	req.Equal(expectedCacheContent, string(cacheContent))
}

// TestNoMatchMatcher does not match any emails.
type TestNoMatchMatcher struct {
}

var ErrTest = errors.New("API error")

// MatchByEmail returns the latest GitHub user with the given email.
func (m TestNoMatchMatcher) MatchByEmail(ctx context.Context, email string) (user string, err error) {
	if email == "new@gmail.com" {
		return "new_user", nil
	}
	return "", ErrTest
}

func (m TestNoMatchMatcher) SupportsMatchingByCommit() bool {
	return true
}

func (m TestNoMatchMatcher) MatchByCommit(
	ctx context.Context, email, repo, commit string) (user string, err error) {
	if email == "new@gmail.com" {
		return "new_user", nil
	}
	return "", ErrTest
}

func (m TestNoMatchMatcher) OnIdle() error {
	return nil
}

func TestMatchCacheOnly(t *testing.T) {
	req := require.New(t)
	matcher := TestNoMatchMatcher{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cache, cleanup := tempFile(t, "*.csv")
	defer cleanup()
	_, err := cache.Write([]byte(
		"email,user,match\n" +
			"mcuadros@gmail.com,mcuadros,1\n" +
			"mcuadros-clone@gmail.com,,0\n"))
	req.NoError(err)
	cachedMatcher, err := NewCachedMatcher(matcher, cache.Name())
	req.NoError(err)

	user, err := cachedMatcher.MatchByEmail(ctx, "mcuadros@gmail.com")
	req.Equal("mcuadros", user)
	req.NoError(err)

	user, err = cachedMatcher.MatchByEmail(ctx, "mcuadros-clone@gmail.com")
	req.Equal("", user)
	req.Equal(ErrNoMatches, err)

	user, err = cachedMatcher.MatchByEmail(ctx, "errored@gmail.com")
	req.Equal("", user)
	req.Equal(ErrTest, err)

	user, err = cachedMatcher.MatchByEmail(ctx, "new@gmail.com")
	req.Equal("new_user", user)
	req.NoError(err)

	err = cachedMatcher.DumpCache()
	req.NoError(err)
	cacheContent, err := ioutil.ReadFile(cache.Name())
	req.NoError(err)
	expectedCacheContent := map[string]struct{}{
		"email,user,match":              {},
		"mcuadros@gmail.com,mcuadros,1": {},
		"mcuadros-clone@gmail.com,,0":   {},
		"new@gmail.com,new_user,1":      {},
		"":                              {},
	}
	cacheContentMap := map[string]struct{}{}
	for _, line := range strings.Split(string(cacheContent), "\n") {
		cacheContentMap[line] = struct{}{}
	}

	req.Equal(expectedCacheContent, cacheContentMap)
}

func TestMatchCacheAppend(t *testing.T) {
	req := require.New(t)
	cache, cleanup := tempFile(t, "*.csv")
	defer cleanup()
	matcher := safeUserCache{
		cache: make(map[string]CachedUser), cachePath: cache.Name(), lock: sync.RWMutex{}}
	_, err := cache.Write([]byte(
		"email,user,match\n" +
			"mcuadros@gmail.com,mcuadros,1\n" +
			"mcuadros-clone@gmail.com,,0\n"))
	cache.Sync()
	req.NoError(err)
	matcher.AddUserToCache("mcuadros@gmail.com", "mcuadros", true)
	matcher.AddUserToCache("mcuadros-clone@gmail.com", "mcuadros", true)
	matcher.AddUserToCache("vadim@sourced.tech", "vmarkovtsev", true)
	req.NoError(matcher.DumpOnDisk())
	cache.Seek(0, io.SeekStart)
	txt, _ := ioutil.ReadAll(cache)
	req.Equal(`email,user,match
mcuadros@gmail.com,mcuadros,1
mcuadros-clone@gmail.com,,0
mcuadros-clone@gmail.com,mcuadros,1
vadim@sourced.tech,vmarkovtsev,1
`, string(txt))
}

func TestMatchCacheCommit(t *testing.T) {
	req := require.New(t)
	matcher := TestNoMatchMatcher{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cache, cleanup := tempFile(t, "*.csv")
	defer cleanup()
	_, err := cache.Write([]byte(
		"email,user,match\n" +
			"mcuadros@gmail.com,mcuadros,1\n" +
			"mcuadros-clone@gmail.com,,0\n"))
	req.NoError(err)
	cachedMatcher, err := NewCachedMatcher(matcher, cache.Name())
	req.NoError(err)
	req.True(cachedMatcher.SupportsMatchingByCommit())

	user, err := cachedMatcher.MatchByCommit(ctx, "mcuadros@gmail.com", "repo", "commit_hash")
	req.Equal("mcuadros", user)
	req.NoError(err)

	user, err = cachedMatcher.MatchByCommit(ctx, "mcuadros-clone@gmail.com", "repo", "commit_hash")
	req.Equal("", user)
	req.Equal(ErrNoMatches, err)

	user, err = cachedMatcher.MatchByCommit(ctx, "errored@gmail.com", "repo", "commit_hash")
	req.Equal("", user)
	req.Equal(ErrTest, err)

	user, err = cachedMatcher.MatchByCommit(ctx, "new@gmail.com", "repo", "commit_hash")
	req.Equal("new_user", user)
	req.NoError(err)
}

func TestMatchCacheScheduledDump(t *testing.T) {
	req := require.New(t)
	matcher := TestNoMatchMatcher{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cache, cleanup := tempFile(t, "*.csv")
	defer cleanup()
	fixture := []byte(`email,user,match
mcuadros-clone1@gmail.com,,0
mcuadros-clone2@gmail.com,,0
mcuadros-clone3@gmail.com,,0
mcuadros-clone4@gmail.com,,0
mcuadros-clone5@gmail.com,,0
mcuadros-clone6@gmail.com,,0
mcuadros-clone7@gmail.com,,0
mcuadros-clone8@gmail.com,,0
mcuadros-clone9@gmail.com,,0
mcuadros-clone10@gmail.com,,0
mcuadros-clone11@gmail.com,,0
mcuadros-clone12@gmail.com,,0
mcuadros-clone13@gmail.com,,0
mcuadros-clone14@gmail.com,,0
mcuadros-clone15@gmail.com,,0
mcuadros-clone16@gmail.com,,0
mcuadros-clone17@gmail.com,,0
mcuadros-clone18@gmail.com,,0
mcuadros-clone19@gmail.com,,0
`)
	expected := `email,user,match
mcuadros-clone1@gmail.com,,0
mcuadros-clone2@gmail.com,,0
mcuadros-clone3@gmail.com,,0
mcuadros-clone4@gmail.com,,0
mcuadros-clone5@gmail.com,,0
mcuadros-clone6@gmail.com,,0
mcuadros-clone7@gmail.com,,0
mcuadros-clone8@gmail.com,,0
mcuadros-clone9@gmail.com,,0
mcuadros-clone10@gmail.com,,0
mcuadros-clone11@gmail.com,,0
mcuadros-clone12@gmail.com,,0
mcuadros-clone13@gmail.com,,0
mcuadros-clone14@gmail.com,,0
mcuadros-clone15@gmail.com,,0
mcuadros-clone16@gmail.com,,0
mcuadros-clone17@gmail.com,,0
mcuadros-clone18@gmail.com,,0
mcuadros-clone19@gmail.com,,0
new@gmail.com,new_user,1
`
	_, err := cache.Write(fixture)
	req.NoError(err)
	cachedMatcher, err := NewCachedMatcher(matcher, cache.Name())
	req.NoError(err)
	user, err := cachedMatcher.MatchByCommit(
		ctx, "new@gmail.com", "git://github.com/src-d/go-git.git",
		"8d20cc5916edf7cfa6a9c5ed069f0640dc823c12")
	req.Equal("new_user", user)
	req.NoError(err)
	newCache, err := ioutil.ReadFile(cache.Name())
	req.NoError(err)
	req.Equal(expected, string(newCache))

	cache, cleanup = tempFile(t, "*.csv")
	defer cleanup()
	_, err = cache.Write(fixture)
	req.NoError(err)
	cachedMatcher, err = NewCachedMatcher(matcher, cache.Name())
	req.NoError(err)
	user, err = cachedMatcher.MatchByEmail(ctx, "new@gmail.com")
	req.Equal("new_user", user)
	req.NoError(err)
	newCache, err = ioutil.ReadFile(cache.Name())
	req.NoError(err)
	req.Equal(expected, string(newCache))
}
