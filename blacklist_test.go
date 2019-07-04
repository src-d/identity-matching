package idmatch

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsMultipleEmail(t *testing.T) {
	require := require.New(t)
	for _, email := range []string{"first@mail.com second@mail.com", "first@mail.com;second@mail.com"} {
		require.True(isMultipleEmail(email))
	}
	require.False(isMultipleEmail("first@mail.com"))
}

func TestIsBlacklistedEmail(t *testing.T) {
	require := require.New(t)
	require.True(isBlacklistedEmail("nobody@android.com"))
	require.False(isBlacklistedEmail("somebody@android.com"))
}

func TestIsIgnoredDomain(t *testing.T) {
	require := require.New(t)
	for _, email := range []string{
		"1@localhost.localdomain", "admin@example.com", "max@example.com", "localhost.localdomain", "example.com"} {
		require.True(isIgnoredDomain(email))
	}
	for _, email := range []string{"somebody@android.com", "android.com"} {
		require.False(isIgnoredDomain(email))
	}
}

func TestIsIPDomain(t *testing.T) {
	require := require.New(t)
	for _, ip := range []string{
		"0.0.0.0", "192.168.0.1", "88.35.10.128", "2001:db8:85a3::8a2e:370:7334", "2001:db8:85a3:0:0:8a2e:370:7334",
		"2001:db8:85a3::8a2e:370:7334", "0:0:0:0:0:0:0:1"} {
		require.True(isIPDomain(ip))
	}
	for _, ip := range []string{"notip.com", "notip"} {
		require.False(isIPDomain(ip))
	}
}

func TestIsSingleLabelDomain(t *testing.T) {
	require := require.New(t)
	for _, domain := range []string{"singlelabel", ""} {
		require.True(isSingleLabelDomain(domain))
	}
	for _, domain := range []string{"not.singlelabel", "."} {
		require.False(isSingleLabelDomain(domain))
	}
}

func TestIsIgnoredName(t *testing.T) {
	require := require.New(t)
	require.True(isIgnoredName("unknown"))
	require.False(isIgnoredDomain("known"))
}

func TestIsIgnoredEmail(t *testing.T) {
	require := require.New(t)
	for _, email := range []string{
		"bad@email", "root@0.0.0.0", "admin@2001:db8:85a3::8a2e:370:7334", "no-domain-mail@",
		"admin1@google.com admin2@google.com", "bad-domain@example.com"} {
		require.True(isIgnoredEmail(email))
	}
	for _, email := range []string{
		"good-email@google.com", "dot.in.name@is.ok.com", "dash-in-name@is.ok.com", "max@google.com",
		"admin-vadim@google.com", "also+ok-mail@inbox.org"} {
		require.False(isIgnoredEmail(email))
	}
}
