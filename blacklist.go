package idmatch

import (
	"regexp"
	"strings"
)

func isIgnoredEmail(s string) bool {
	s = strings.ToLower(strings.TrimSpace(s))
	if !strings.Contains(s, "@") || isBlacklistedEmail(s) || isMultipleEmail(s) {
		return true
	}
	parts := strings.Split(s, "@")
	domain := parts[1]
	return isIgnoredDomain(domain) ||
		isSingleLabelDomain(s) ||
		isIPDomain(domain)

}

func isMultipleEmail(s string) bool {
	return strings.Index(s, "@") != strings.LastIndex(s, "@")
}

var blacklistedEmails = map[string]struct{}{
	"nobody@android.com": {},
	"badger@gitter.im":   {},
}

func isBlacklistedEmail(s string) bool {
	_, ok := blacklistedEmails[s]
	return ok
}

var ignoredDomains = map[string]struct{}{
	"localhost.localdomain": {},
	"example.com":           {},
	"test.com":              {},
	"domain.com":            {},
}

func isIgnoredDomain(s string) bool {
	parts := strings.Split(s, "@")
	_, ok := ignoredDomains[parts[len(parts)-1]]
	return ok
}

var isIP4EmailRegex = regexp.MustCompile(`\d+\.\d+\.\d+\.\d+$`)
var isIP6EmailRegex = regexp.MustCompile(`(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))`)

func isIPDomain(s string) bool {
	return isIP4EmailRegex.MatchString(s) || isIP6EmailRegex.MatchString(s)
}

func isSingleLabelDomain(s string) bool {
	return strings.Count(s, ".") == 0
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
