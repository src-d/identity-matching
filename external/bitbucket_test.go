package external

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBitBucketMatcherMatchByEmail(t *testing.T) {
	m, err := NewBitBucketMatcher("", "JOHRfFo9NG2npndvCXmkD82D")
	assert.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	user, name, err := m.MatchByEmail(ctx, "victor.stinner@gmail.com")
	assert.NoError(t, err)
	assert.Equal(t, "557058:7bfcfebe-074d-4f48-9983-a8f959cf4a65", user)
	assert.Equal(t, "Victor Stinner", name)
}

func TestBitBucketMatcherInvalidEmail(t *testing.T) {
	m, err := NewBitBucketMatcher("", "JOHRfFo9NG2npndvCXmkD82D")
	assert.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	user, name, err := m.MatchByEmail(ctx, "vadim-ladron-xxx@gmail.com")
	assert.Errorf(t, err, ErrNoMatches.Error())
	assert.Equal(t, "", user)
	assert.Equal(t, "", name)
}

func TestBitBucketMatcherCancel(t *testing.T) {
	m, err := NewBitBucketMatcher("", "JOHRfFo9NG2npndvCXmkD82D")
	assert.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	user, name, err := m.MatchByEmail(ctx, "victor.stinner@gmail.com")
	assert.Errorf(t, err, context.Canceled.Error())
	assert.Equal(t, "", user)
	assert.Equal(t, "", name)
}
