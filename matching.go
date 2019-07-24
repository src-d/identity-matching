package idmatch

import (
	"context"

	"github.com/sirupsen/logrus"
	"github.com/src-d/eee-identity-matching/external"
	"github.com/src-d/eee-identity-matching/reporter"
	simplegraph "gonum.org/v1/gonum/graph"
	"gonum.org/v1/gonum/graph/simple"
	"gonum.org/v1/gonum/graph/topo"
)

type node struct {
	Value *Person
	id    int64
}

func (g node) ID() int64 {
	return g.id
}

// addEdgesWithMatcher adds edges by the groundtruth fetched with external matcher.
func addEdgesWithMatcher(people People, peopleGraph *simple.UndirectedGraph,
	matcher external.Matcher) (map[string]struct{}, error) {
	unprocessedEmails := map[string]struct{}{}
	// Add edges by the groundtruth fetched with external matcher.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	email2extID := make(map[string]simplegraph.Node)
	for index, person := range people {
		for _, email := range person.Emails {
			username, _, err := matcher.MatchByEmail(ctx, email)
			if err != nil {
				if err == external.ErrNoMatches {
					logrus.Warnf("no matches for person %s.", person.String())
					unprocessedEmails[email] = struct{}{}
				} else {
					return unprocessedEmails, err
				}
			} else {
				if val, ok := email2extID[username]; ok {
					peopleGraph.SetEdge(peopleGraph.NewEdge(val, peopleGraph.Node(int64(index))))
					reporter.Increment("graph edges")
				} else {
					email2extID[username] = peopleGraph.Node(int64(index))
				}
				reporter.Increment("external API emails found")
			}
		}
	}
	reporter.Commit("external API components", len(email2extID))
	reporter.Commit("external API emails not found", len(unprocessedEmails))
	return unprocessedEmails, nil
}

// ReducePeople merges the identities together by following the fixed set of rules.
// 1. Run the external matching, if available.
// 2. Run the series of heuristics on those items which were left untouched in the list (everything
//    in case of ext == nil, not found in case of ext != nil).
//
// The heuristics are:
// TODO(vmarkovtsev): describe the current approach
func ReducePeople(people People, matcher external.Matcher, blacklist Blacklist) error {
	peopleGraph := simple.NewUndirectedGraph()
	for index, person := range people {
		peopleGraph.AddNode(node{person, index})
	}

	unmatchedEmails := map[string]struct{}{}
	var err error
	if matcher != nil {
		unmatchedEmails, err = addEdgesWithMatcher(people, peopleGraph, matcher)
		if err != nil {
			return err
		}
	}

	// Add edges by the same unpopular email
	email2id := make(map[string]simplegraph.Node)
	for index, person := range people {
		for _, email := range person.Emails {
			if matcher != nil {
				if _, unmatched := unmatchedEmails[email]; !unmatched {
					// Do not process emails which were matched with external matcher
					continue
				}
			}
			if blacklist.isPopularEmail(email) {
				reporter.Increment("popular emails found")
				continue
			}
			if val, ok := email2id[email]; ok {
				peopleGraph.SetEdge(peopleGraph.NewEdge(val, peopleGraph.Node(index)))
				reporter.Increment("graph edges")
			} else {
				email2id[email] = peopleGraph.Node(index)
			}
		}
	}
	reporter.Commit("people matched by email", len(email2id))

	// Add edges by the same unpopular name
	name2id := make(map[string]simplegraph.Node)
	for index, person := range people {
		for _, name := range person.NamesWithRepos {
			if blacklist.isPopularName(name.String()) {
				reporter.Increment("popular names found")
				continue
			}
			if val, ok := name2id[name.String()]; ok {
				peopleGraph.SetEdge(peopleGraph.NewEdge(val, peopleGraph.Node(index)))
				reporter.Increment("graph edges")
			} else {
				name2id[name.String()] = peopleGraph.Node(index)
			}
		}
	}
	reporter.Commit("people matched by name", len(name2id))

	for _, component := range topo.ConnectedComponents(peopleGraph) {
		var toMerge []int64
		for _, node := range component {
			toMerge = append(toMerge, node.ID())
		}
		people.Merge(toMerge...)
	}
	reporter.Commit("people after reduce", len(people))

	return nil

}
