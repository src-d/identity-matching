// +build !cipr

package external

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var githubTestToken = os.Getenv("GITHUB_TEST_TOKEN")

func init() {
	if githubTestToken == "" {
		panic("GITHUB_TEST_TOKEN environment variable is not set")
	}
}

func TestGitHubMatcherValidEmail(t *testing.T) {
	matcher, _ := NewGitHubMatcher("", githubTestToken)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	user, err := matcher.MatchByEmail(ctx, "mcuadros@gmail.com")
	require.Equal(t, "mcuadros", user)
	require.NoError(t, err)
}

// TestGitHubMatcherValidEmailWorkaround checks some strange cases when querying the email
// directly does not work, however, it is possible to filter by left and right parts.
func TestGitHubMatcherValidEmailWorkaround(t *testing.T) {
	matcher, _ := NewGitHubMatcher("", githubTestToken)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	user, err := matcher.MatchByEmail(ctx, "eiso@sourced.tech")
	require.Equal(t, "eiso", user)
	require.NoError(t, err)
}

func TestGitHubMatcherInvalidEmail(t *testing.T) {
	matcher, _ := NewGitHubMatcher("", githubTestToken)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := matcher.MatchByEmail(ctx, "vadim-evil-clone@sourced.tech")
	require.EqualError(t, err, ErrNoMatches.Error())
}

func TestGitHubMatcherCancel(t *testing.T) {
	matcher, _ := NewGitHubMatcher("", githubTestToken)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	user, err := matcher.MatchByEmail(ctx, "mcuadros@gmail.com")
	require.Equal(t, "", user)
	require.Equal(t, context.Canceled, err)
}

func TestGitHubMatcherValidEmailByCommitAuthor(t *testing.T) {
	matcher, _ := NewGitHubMatcher("", githubTestToken)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	user, err := matcher.MatchByCommit(ctx, "mcuadros@gmail.com", "github.com/src-d/go-git",
		"8d20cc5916edf7cfa6a9c5ed069f0640dc823c12")
	require.Equal(t, "mcuadros", user)
	require.NoError(t, err)
}

func TestGitHubMatcherValidEmailByCommitCommitter(t *testing.T) {
	matcher, _ := NewGitHubMatcher("", githubTestToken)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	user, err := matcher.MatchByCommit(ctx, "mcuadros@gmail.com", "https://github.com/src-d/go-git",
		"e5c9c0dd9ff1f42dcdaba7a51919cf43abdb79f9")
	require.Equal(t, "mcuadros", user)
	require.NoError(t, err)
}

func TestGitHubMatcherInvalidEmailByCommit(t *testing.T) {
	matcher, _ := NewGitHubMatcher("", githubTestToken)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	user, err := matcher.MatchByCommit(ctx, "ladron@gmail.com", "github.com/src-d/go-git",
		"8d20cc5916edf7cfa6a9c5ed069f0640dc823c12")
	require.Equal(t, "", user)
	require.EqualError(t, err, ErrNoMatches.Error())
}

func TestGitHubMatcherByCommitInvalidRepoCommit(t *testing.T) {
	matcher, _ := NewGitHubMatcher("", githubTestToken)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.Panics(t, func() {
		matcher.MatchByCommit(ctx, "ladron@gmail.com", "wtf.com/src-d/go-git",
			"8d20cc5916edf7cfa6a9c5ed069f0640dc823c12")
	})
	require.Panics(t, func() {
		matcher.MatchByCommit(ctx, "ladron@gmail.com", "github.com/src-d/go-git", "xxx")
	})
}
