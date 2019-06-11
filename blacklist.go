package idmatch

import (
	"regexp"
	"strings"
)

func isIgnored(s string) bool {
	s = strings.TrimSpace(s)
	return !strings.Contains(s, "@") ||
		isIgnoredEmail(s) ||
		isIgnoredDomain(s) ||
		isIgnoredTLD(s) ||
		isSingleLabelDomain(s) ||
		isIPEmail(s) ||
		isMultipleEmail(s)
}

func isMultipleEmail(s string) bool {
	return strings.Index(s, "@") != strings.LastIndex(s, "@")
}

var ignoredEmails = map[string]struct{}{
	"nobody@android.com": {},
	"badger@gitter.im":   {},
}

func isIgnoredEmail(s string) bool {
	_, ok := ignoredEmails[s]
	return ok
}

var ignoredDomains = map[string]struct{}{
	"localhost.localdomain": {},
	"example.com":           {},
	"test.com":              {},
	"DOMAIN.COM":            {},
}

func isIgnoredDomain(s string) bool {
	parts := strings.Split(s, "@")
	_, ok := ignoredDomains[parts[len(parts)-1]]
	return ok
}

var ignoredTLD = map[string]struct{}{
	"localhost":   {},
	"localdomain": {},
	"local":       {},
	"test":        {},
	"internal":    {},
	"private":     {},
	"lan":         {},
	"hq":          {},
	"domain":      {},
	"(none)":      {},
	"home":        {},
}

var isIPEmailRegex = regexp.MustCompile(`@\d+\.\d+\.\d+\.\d+$`)

func isIPEmail(s string) bool {
	return isIPEmailRegex.MatchString(s)
}

func isIgnoredTLD(s string) bool {
	parts := strings.Split(s, ".")
	_, ok := ignoredTLD[parts[len(parts)-1]]
	return ok
}

func isSingleLabelDomain(s string) bool {
	parts := strings.Split(s, "@")
	last := parts[len(parts)-1]
	labels := strings.Split(last, ".")
	return len(labels) == 1
}

var ignoredNames = map[string]struct{}{
	"unknown": {},
	"ubuntu":  {},
	"admin":   {},
}

func isIgnoredName(name string) bool {
	_, ok := ignoredNames[strings.ToLower(name)]
	return ok
}
