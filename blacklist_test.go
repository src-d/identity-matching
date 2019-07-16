package idmatch

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func newTestBlacklist(t *testing.T) Blacklist {
	t.Helper()
	return Blacklist{
		Domains: map[string]struct{}{
			"localhost.localdomain": {},
			"example.com":           {},
			"test.com":              {},
			"domain.com":            {},
		},
		TopLevelDomains: map[string]struct{}{
			"ignored_tld": {},
		},
		Names: map[string]struct{}{
			"unknown": {},
			"ubuntu":  {},
			"admin":   {},
		},
		Emails: map[string]struct{}{
			"nobody@android.com": {},
			"badger@gitter.im":   {},
		},
		PopularEmails: map[string]struct{}{
			"popular@email.com": {},
		},
		PopularNames: map[string]struct{}{
			"popular": {},
		},
	}
}

func TestNewBlacklist(t *testing.T) {
	require := require.New(t)
	blacklist, err := NewBlacklist()
	require.NoError(err)
	require.Contains(blacklist.Domains, "users.noreply.github.com")
	require.Contains(blacklist.TopLevelDomains, "localdomain")
	require.Contains(blacklist.Names, "your name")
	require.Contains(blacklist.Emails, "badges@fossa.io")
	require.Contains(blacklist.PopularEmails, "a@a.a")
	require.Contains(blacklist.PopularNames, "alex")
}

func TestIsMultipleEmail(t *testing.T) {
	require := require.New(t)
	for _, email := range []string{"first@mail.com second@mail.com", "first@mail.com;second@mail.com"} {
		require.True(isMultipleEmail(email))
	}
	require.False(isMultipleEmail("first@mail.com"))
}

func TestIsBlacklistedEmail(t *testing.T) {
	require := require.New(t)
	blacklist := newTestBlacklist(t)
	require.True(blacklist.isBlacklistedEmail("nobody@android.com"))
	require.False(blacklist.isBlacklistedEmail("somebody@android.com"))
}

func TestIsIgnoredDomain(t *testing.T) {
	require := require.New(t)
	blacklist := newTestBlacklist(t)
	for _, email := range []string{
		"1@localhost.localdomain", "admin@example.com", "max@example.com", "localhost.localdomain", "example.com"} {
		require.True(blacklist.isIgnoredDomain(email))
	}
	for _, email := range []string{"somebody@android.com", "android.com"} {
		require.False(blacklist.isIgnoredDomain(email))
	}
}

func TestIsIgnoredTopLevelDomain(t *testing.T) {
	require := require.New(t)
	blacklist := newTestBlacklist(t)
	for _, tld := range []string{"not_ignored", "full.domain.not_ignored", "email@full.domain.not_ignored"} {
		require.False(blacklist.isIgnoredTopLevelDomain(tld))
	}
	for _, tld := range []string{"ignored_tld", "full.domain.ignored_tld", "email@full.domain.ignored_tld"} {
		require.True(blacklist.isIgnoredTopLevelDomain(tld))
	}
}

func TestIsIPDomain(t *testing.T) {
	require := require.New(t)
	for _, ip := range []string{
		"0.0.0.0", "192.168.0.1", "88.35.10.128", "2001:db8:85a3::8a2e:370:7334", "2001:db8:85a3:0:0:8a2e:370:7334",
		"2001:db8:85a3::8a2e:370:7334", "0:0:0:0:0:0:0:1", "blockchaindev34.172.20.180.160"} {
		require.True(isIPDomain(ip))
	}
	for _, ip := range []string{"notip.com", "notip", "88.131.110"} {
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
	blacklist := newTestBlacklist(t)
	require.True(blacklist.isIgnoredName("unknown"))
	require.False(blacklist.isIgnoredName("known"))
}

func TestIsIgnoredEmail(t *testing.T) {
	require := require.New(t)
	blacklist := newTestBlacklist(t)
	for _, email := range []string{
		"bad@email", "root@0.0.0.0", "admin@2001:db8:85a3::8a2e:370:7334", "no-domain-mail@",
		"admin1@google.com admin2@google.com", "bad-domain@example.com", "nobody@android.com",
		"not a mail"} {
		require.True(blacklist.isIgnoredEmail(email))
	}
	for _, email := range []string{
		"good-email@google.com", "dot.in.name@is.ok.com", "dash-in-name@is.ok.com", "max@google.com",
		"admin-vadim@google.com", "also+ok-mail@inbox.org"} {
		require.False(blacklist.isIgnoredEmail(email))
	}
}
