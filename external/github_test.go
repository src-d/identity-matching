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
	user, name, err := matcher.MatchByEmail(ctx, "mcuadros@gmail.com")
	require.Equal(t, "mcuadros", user)
	require.Equal(t, "Máximo Cuadros", name)
	require.NoError(t, err)
}

// TestGitHubMatcherValidEmailWorkaround checks some strange cases when querying the email
// directly does not work, however, it is possible to filter by left and right parts.
func TestGitHubMatcherValidEmailWorkaround(t *testing.T) {
	matcher, _ := NewGitHubMatcher("", githubTestToken)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	user, name, err := matcher.MatchByEmail(ctx, "eiso@sourced.tech")
	require.Equal(t, "eiso", user)
	require.Equal(t, "Eiso Kant", name)
	require.NoError(t, err)
}

func TestGitHubMatcherInvalidEmail(t *testing.T) {
	matcher, _ := NewGitHubMatcher("", githubTestToken)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, _, err := matcher.MatchByEmail(ctx, "vadim-evil-clone@sourced.tech")
	require.EqualError(t, err, ErrNoMatches.Error())
}

func TestGitHubMatcherCancel(t *testing.T) {
	matcher, _ := NewGitHubMatcher("", githubTestToken)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	user, name, err := matcher.MatchByEmail(ctx, "mcuadros@gmail.com")
	require.Equal(t, "", user)
	require.Equal(t, "", name)
	require.Equal(t, context.Canceled, err)
}
