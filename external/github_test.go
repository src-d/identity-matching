package external

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGitHubMatcherValidEmail(t *testing.T) {
	matcher, _ := NewGitHubMatcher("", "58f7c94cece3b0828426e5d015e8d910169abd2d")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	user, name, err := matcher.MatchByEmail(ctx, "mcuadros@gmail.com")
	assert.Equal(t, "mcuadros", user)
	assert.Equal(t, "Máximo Cuadros", name)
	assert.NoError(t, err)
}

// TestGitHubMatcherValidEmailWorkaround checks some strange cases when querying the email
// directly does not work, however, it is possible to filter by left and right parts.
func TestGitHubMatcherValidEmailWorkaround(t *testing.T) {
	matcher, _ := NewGitHubMatcher("", "58f7c94cece3b0828426e5d015e8d910169abd2d")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	user, name, err := matcher.MatchByEmail(ctx, "eiso@sourced.tech")
	assert.Equal(t, "eiso", user)
	assert.Equal(t, "Eiso Kant", name)
	assert.NoError(t, err)
}

func TestGitHubMatcherInvalidEmail(t *testing.T) {
	matcher, _ := NewGitHubMatcher("", "58f7c94cece3b0828426e5d015e8d910169abd2d")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, _, err := matcher.MatchByEmail(ctx, "vadim-evil-clone@sourced.tech")
	assert.EqualError(t, err, ErrNoMatches.Error())
}
