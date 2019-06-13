package external

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGitLabMatcherValidEmail(t *testing.T) {
	matcher, _ := NewGitLabMatcher("", "RZtZsqZ3FckbHB-YRYzG")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	user, name, err := matcher.MatchByEmail(ctx, "vadim@sourced.tech")
	require.Equal(t, "vmarkovtsev", user)
	require.Equal(t, "Vadim Markovtsev", name)
	require.NoError(t, err)
}

func TestGitLabMatcherInvalidEmail(t *testing.T) {
	matcher, _ := NewGitLabMatcher("", "RZtZsqZ3FckbHB-YRYzG")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, _, err := matcher.MatchByEmail(ctx, "vadim-evil-clone@sourced.tech")
	require.EqualError(t, err, ErrNoMatches.Error())
}

func TestGitLabMatcherCancel(t *testing.T) {
	matcher, _ := NewGitLabMatcher("", "RZtZsqZ3FckbHB-YRYzG")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	user, name, err := matcher.MatchByEmail(ctx, "vadim@sourced.tech")
	require.Equal(t, "", user)
	require.Equal(t, "", name)
	require.Equal(t, context.Canceled, err)
}
