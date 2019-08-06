package external

import (
	"context"
	"errors"
	"io/ioutil"
	"os"
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
	matcher, _ := NewGitHubMatcher("", githubTestToken)
	cache, cleanup := tempFile(t, "*.csv")
	defer cleanup()
	_, err := cache.Write([]byte("email,user,name,match"))
	require.NoError(t, err)
	cachedMatcher, err := NewCachedMatcher(matcher, cache.Name())
	expectedCachedMatcher := CachedMatcher{
		matcher: matcher, cachePath: cache.Name(), cache: make(map[string]UserName), saveFreq: 20}
	require.NoError(t, err)
	require.Equal(t, expectedCachedMatcher, cachedMatcher)
}

func TestMatchByEmailAndDump(t *testing.T) {
	matcher, _ := NewGitHubMatcher("", githubTestToken)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cache, cleanup := tempFile(t, "*.csv")
	defer cleanup()
	_, err := cache.Write([]byte("email,user,name,match"))
	require.NoError(t, err)
	cachedMatcher, err := NewCachedMatcher(matcher, cache.Name())
	require.NoError(t, err)

	user, name, err := cachedMatcher.MatchByEmail(ctx, "mcuadros@gmail.com")
	require.Equal(t, "mcuadros", user)
	require.Equal(t, "Máximo Cuadros", name)
	require.NoError(t, err)

	err = cachedMatcher.DumpCache()
	require.NoError(t, err)
	cacheContent, err := ioutil.ReadFile(cache.Name())
	require.NoError(t, err)
	expectedCacheContent := "email,user,name,match\nmcuadros@gmail.com,mcuadros,Máximo Cuadros,true\n"
	require.Equal(t, expectedCacheContent, string(cacheContent))
}

// TestNoMatchMatcher does not match any emails.
type TestNoMatchMatcher struct {
}

func NewTestNoMatchMatcher(apiURL, token string) (Matcher, error) {
	return TestNoMatchMatcher{}, nil
}

var ErrTest = errors.New("API error")

// MatchByEmail returns the latest GitHub user with the given email.
func (m TestNoMatchMatcher) MatchByEmail(ctx context.Context, email string) (user, name string, err error) {
	if email == "new@gmail.com" {
		return "new_user", "new_name", nil
	}
	return "", "", ErrTest
}

func TestMatchCacheOnly(t *testing.T) {
	matcher, _ := NewTestNoMatchMatcher("", "")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cache, cleanup := tempFile(t, "*.csv")
	defer cleanup()
	_, err := cache.Write([]byte(
		"email,user,name,match\n" +
			"mcuadros@gmail.com,mcuadros,Máximo Cuadros,true\n" +
			"mcuadros-clone@gmail.com,,,false\n"))
	require.NoError(t, err)
	cachedMatcher, err := NewCachedMatcher(matcher, cache.Name())
	require.NoError(t, err)

	user, name, err := cachedMatcher.MatchByEmail(ctx, "mcuadros@gmail.com")
	require.Equal(t, "mcuadros", user)
	require.Equal(t, "Máximo Cuadros", name)
	require.NoError(t, err)

	user, name, err = cachedMatcher.MatchByEmail(ctx, "mcuadros-clone@gmail.com")
	require.Equal(t, "", user)
	require.Equal(t, "", name)
	require.Equal(t, ErrNoMatches, err)

	user, name, err = cachedMatcher.MatchByEmail(ctx, "errored@gmail.com")
	require.Equal(t, "", user)
	require.Equal(t, "", name)
	require.Equal(t, ErrTest, err)

	user, name, err = cachedMatcher.MatchByEmail(ctx, "new@gmail.com")
	require.Equal(t, "new_user", user)
	require.Equal(t, "new_name", name)
	require.NoError(t, err)

	err = cachedMatcher.DumpCache()
	require.NoError(t, err)
	cacheContent, err := ioutil.ReadFile(cache.Name())
	require.NoError(t, err)
	expectedCacheContent :=
		"email,user,name,match\n" +
			"mcuadros@gmail.com,mcuadros,Máximo Cuadros,true\n" +
			"mcuadros-clone@gmail.com,,,false\n" +
			"new@gmail.com,new_user,new_name,true\n"
	require.Equal(t, expectedCacheContent, string(cacheContent))
}
