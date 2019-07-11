package idmatch

import (
	"sort"
	"unicode"
	"unicode/utf8"

	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
)

func unique(slice []string) []string {
	var seen = make(map[string]struct{})
	var result []string
	for _, s := range slice {
		if _, ok := seen[s]; ok {
			continue
		}

		seen[s] = struct{}{}
		result = append(result, s)
	}
	sort.Strings(result)
	return result
}

func stringInSlice(slice []string, s string) bool {
	for _, str := range slice {
		if str == s {
			return true
		}
	}
	return false
}

func isCapitalized(word string) bool {
	ru, _ := utf8.DecodeRune([]byte(word))
	return unicode.IsUpper(ru)
}

func isMn(r rune) bool {
	return unicode.Is(unicode.Mn, r) // Mn: nonspacing marks
}

func removeDiacritical(s string) (string, int, error) {
	return transform.String(transform.Chain(norm.NFD, transform.RemoveFunc(isMn), norm.NFC), s)
}
