package external

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGitLabMatcherValidEmail(t *testing.T) {
	matcher, _ := NewGitLabMatcher("", "RZtZsqZ3FckbHB-YRYzG")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	user, name, err := matcher.MatchByEmail(ctx, "vadim@sourced.tech")
	assert.Equal(t, "vmarkovtsev", user)
	assert.Equal(t, "Vadim Markovtsev", name)
	assert.NoError(t, err)
}

func TestGitLabMatcherInvalidEmail(t *testing.T) {
	matcher, _ := NewGitLabMatcher("", "RZtZsqZ3FckbHB-YRYzG")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, _, err := matcher.MatchByEmail(ctx, "vadim-evil-clone@sourced.tech")
	assert.EqualError(t, err, ErrNoMatches.Error())
}

func TestGitLabMatcherCancel(t *testing.T) {
	matcher, _ := NewGitLabMatcher("", "RZtZsqZ3FckbHB-YRYzG")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	user, name, err := matcher.MatchByEmail(ctx, "vadim@sourced.tech")
	assert.Equal(t, "", user)
	assert.Equal(t, "", name)
	assert.Errorf(t, err, context.Canceled.Error())
}
