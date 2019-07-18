package external

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var bitbucketTestToken = os.Getenv("BITBUCKET_TEST_TOKEN")

func init() {
	if bitbucketTestToken == "" {
		panic("BITBUCKET_TEST_TOKEN environment variable is not set")
	}
}

func TestBitBucketMatcherMatchByEmail(t *testing.T) {
	m, err := NewBitBucketMatcher("", bitbucketTestToken)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	user, name, err := m.MatchByEmail(ctx, "victor.stinner@gmail.com")
	require.NoError(t, err)
	require.Equal(t, "557058:7bfcfebe-074d-4f48-9983-a8f959cf4a65", user)
	require.Equal(t, "Victor Stinner", name)
}

func TestBitBucketMatcherInvalidEmail(t *testing.T) {
	m, err := NewBitBucketMatcher("", bitbucketTestToken)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	user, name, err := m.MatchByEmail(ctx, "vadim-ladron-xxx@gmail.com")
	require.Equal(t, ErrNoMatches, err)
	require.Equal(t, "", user)
	require.Equal(t, "", name)
}

func TestBitBucketMatcherCancel(t *testing.T) {
	m, err := NewBitBucketMatcher("", bitbucketTestToken)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	user, name, err := m.MatchByEmail(ctx, "victor.stinner@gmail.com")
	require.Equal(t, context.Canceled, err)
	require.Equal(t, "", user)
	require.Equal(t, "", name)
}
