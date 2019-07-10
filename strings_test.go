package idmatch

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUnique(t *testing.T) {
	var nilArray []string
	require := require.New(t)
	require.Equal(nilArray, unique([]string{}))
	require.Equal(nilArray, unique(nilArray))
	require.Equal([]string{"a", "b"}, unique([]string{"b", "a"}))
	require.Equal([]string{"a", "b", "c"},
		unique([]string{"a", "b", "c", "c"}))
	require.Equal([]string{"a", "b", "c"},
		unique([]string{"a", "b", "c", "a", "b", "c", "a", "b", "c"}))

}

func TestStringInSlice(t *testing.T) {
	require := require.New(t)
	require.False(stringInSlice([]string{}, ""))
	require.True(stringInSlice([]string{""}, ""))
	require.True(stringInSlice([]string{"a", "b", "c"}, "b"))
	require.False(stringInSlice([]string{"a", "b", "c"}, "d"))
}

func TestIsCapitalized(t *testing.T) {
	require := require.New(t)
	require.False(isCapitalized("notCapitalized"))
	require.True(isCapitalized("Capitalized"))
	require.False(isCapitalized(""))
}
