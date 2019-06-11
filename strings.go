package idmatch

import (
	"unicode"
	"unicode/utf8"
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
