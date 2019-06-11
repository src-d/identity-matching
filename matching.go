package idmatch

import (
	"context"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/src-d/eee-identity-matching/external"
)

// MatchByEmail groups persons inside the given people by their email.
func MatchByEmail(p People) {
	p.Iter(func(id1 uint64, p1 *Person) bool {
		p.Iter(func(id2 uint64, p2 *Person) bool {
			if id1 != id2 && hasCommonEmail(p1.emails, p2.emails) {
				p.Merge(id1, id2)
				return true
			}

			return false
		})

		return false
	})
}

// MatchByGitHub groups persons inside the given people by their GitHub
// accounts.
func MatchByGitHub(ctx context.Context, p People, m external.GitHubMatcher) {
	var matching = make(map[string][]uint64)

	p.Iter(func(id uint64, person *Person) bool {
		for _, e := range person.emails {
			user, name, err := m.MatchByEmail(ctx, e)
			if err != nil {
				if err != external.ErrNoMatches {
					logrus.Errorf("error matching email %s: %s", e, err)
				}
				continue
			}

			person.names = unique(append(person.names, name))
			matching[user] = append(matching[user], id)
			break
		}

		return false
	})

	for _, ids := range matching {
		id1 := ids[0]
		for _, id2 := range ids[1:] {
			p.Merge(id1, id2)
		}
	}
}

// MatchByNames groups persons inside the given people by their names.
func MatchByNames(p People) {
	p.Iter(func(id1 uint64, p1 *Person) bool {
		p.Iter(func(id2 uint64, p2 *Person) bool {
			if id1 != id2 && hasCommonName(p1.names, p2.names) {
				p.Merge(id1, id2)
				return true
			}

			return false
		})

		return false
	})
}

func hasCommonEmail(e1, e2 []string) bool {
	for _, e := range e1 {
		if stringInSlice(e2, e) {
			return true
		}
	}
	return false
}

func hasCommonName(n1, n2 []string) bool {
	for _, n := range n1 {
		if stringInSlice(n2, n) {
			return true
		}
	}
	return false
}

func isFullName(n string) bool {
	parts := strings.Split(n, " ")
	var words []string
	for _, p := range parts {
		if strings.TrimSpace(p) != "" {
			words = append(words, p)
		}
	}

	return len(words) > 1 && isCapitalized(words[0])
}
