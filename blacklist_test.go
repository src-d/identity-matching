package idmatch

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsMultipleEmail(t *testing.T) {
	assert.True(t, isMultipleEmail("first@mail.com second@mail.com"))
	assert.True(t, isMultipleEmail("first@mail.com;second@mail.com"))
	assert.False(t, isMultipleEmail("first@mail.com"))
}

func TestIsBlacklistedEmail(t *testing.T) {
	assert.True(t, isBlacklistedEmail("nobody@android.com"))
	assert.False(t, isBlacklistedEmail("somebody@android.com"))
}

func TestIsIgnoredDomain(t *testing.T) {
	assert.True(t, isIgnoredDomain("1@localhost.localdomain"))
	assert.True(t, isIgnoredDomain("admin@example.com"))
	assert.True(t, isIgnoredDomain("max@example.com"))
	assert.False(t, isIgnoredDomain("somebody@android.com"))
	assert.True(t, isIgnoredDomain("localhost.localdomain"))
	assert.True(t, isIgnoredDomain("example.com"))
	assert.False(t, isIgnoredDomain("android.com"))
}

func TestIsIPDomain(t *testing.T) {
	assert.True(t, isIPDomain("0.0.0.0"))
	assert.True(t, isIPDomain("192.168.0.1"))
	assert.True(t, isIPDomain("88.35.10.128"))
	assert.True(t, isIPDomain("2001:db8:85a3::8a2e:370:7334"))
	assert.True(t, isIPDomain("2001:db8:85a3:0:0:8a2e:370:7334"))
	assert.True(t, isIPDomain("2001:db8:85a3::8a2e:370:7334"))
	assert.True(t, isIPDomain("0:0:0:0:0:0:0:1"))
	assert.False(t, isIPDomain("notip.com"))
	assert.False(t, isIPDomain("notip"))
}

func TestIsSingleLabelDomain(t *testing.T) {
	assert.True(t, isSingleLabelDomain("singlelabel"))
	assert.True(t, isSingleLabelDomain(""))
	assert.False(t, isSingleLabelDomain("not.singlelabel"))
	assert.False(t, isSingleLabelDomain("."))
}

func TestIsIgnoredName(t *testing.T) {
	assert.True(t, isIgnoredName("unknown"))
	assert.False(t, isIgnoredDomain("known"))
}

func TestIsIgnoredEmail(t *testing.T) {
	assert.True(t, isIgnoredEmail("bad@email"))
	assert.True(t, isIgnoredEmail("root@0.0.0.0"))
	assert.True(t, isIgnoredEmail("admin@2001:db8:85a3::8a2e:370:7334"))
	assert.True(t, isIgnoredEmail("no-domain-mail@"))
	assert.True(t, isIgnoredEmail("admin1@google.com admin2@google.com"))
	assert.True(t, isIgnoredEmail("bad-domain@example.com"))

	assert.False(t, isIgnoredEmail("good-email@google.com"))
	assert.False(t, isIgnoredEmail("dot.in.name@is.ok.com"))
	assert.False(t, isIgnoredEmail("dash-in-name@is.ok.com"))
	assert.False(t, isIgnoredEmail("max@google.com"))
	assert.False(t, isIgnoredEmail("admin-vadim@google.com"))
	assert.False(t, isIgnoredEmail("also+ok-mail@inbox.org"))
}
