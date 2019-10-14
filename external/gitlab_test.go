// +build !cipr

package external

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var gitlabTestToken = os.Getenv("GITLAB_TEST_TOKEN")

func init() {
	if gitlabTestToken == "" {
		panic("GITLAB_TEST_TOKEN environment variable is not set")
	}
}

func TestGitLabMatcherValidEmail(t *testing.T) {
	matcher, _ := NewGitLabMatcher("", gitlabTestToken)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	user, err := matcher.MatchByEmail(ctx, "vadim@sourced.tech")
	require.Equal(t, "vmarkovtsev", user)
	require.NoError(t, err)
}

func TestGitLabMatcherInvalidEmail(t *testing.T) {
	matcher, _ := NewGitLabMatcher("", gitlabTestToken)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := matcher.MatchByEmail(ctx, "vadim-evil-clone@sourced.tech")
	require.EqualError(t, err, ErrNoMatches.Error())
}

func TestGitLabMatcherCancel(t *testing.T) {
	matcher, _ := NewGitLabMatcher("", gitlabTestToken)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	user, err := matcher.MatchByEmail(ctx, "vadim@sourced.tech")
	require.Equal(t, "", user)
	require.Equal(t, context.Canceled, err)
}
