package idmatch

import (
	"context"
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/src-d/eee-identity-matching/external"
	"github.com/src-d/eee-identity-matching/reporter"
	"gonum.org/v1/gonum/floats"
	simplegraph "gonum.org/v1/gonum/graph"
	"gonum.org/v1/gonum/graph/simple"
	"gonum.org/v1/gonum/graph/topo"
	"gonum.org/v1/gonum/graph/traverse"
	"gonum.org/v1/gonum/stat"
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

	username2extID := make(map[string]node)
	var username string
	var err error
	for index, person := range people {
		for _, email := range person.Emails {
			username, _, err = matcher.MatchByEmail(ctx, email)
			if err != nil {
				if err == external.ErrNoMatches {
					logrus.Warnf("no matches for person %s.", person.String())
				} else {
					logrus.Errorf("Unexpected error for person %s: %s", person.String(), err)
				}
				unprocessedEmails[email] = struct{}{}
			} else {
				if person.ExternalID != "" && username != person.ExternalID {
					return unprocessedEmails, fmt.Errorf(
						"person %s has emails with different external ids: %s %s",
						person.String(), person.ExternalID, username)
				}
				person.ExternalID = username
				if val, ok := username2extID[username]; ok {
					err := setEdge(people, peopleGraph, val, peopleGraph.Node(int64(index)).(node))
					if err != nil {
						return unprocessedEmails, nil
					}
				} else {
					username2extID[username] = peopleGraph.Node(int64(index)).(node)
				}
				reporter.Increment("external API emails found")
			}
		}
	}
	reporter.Commit("external API components", len(username2extID))
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
	email2id := make(map[string]node)
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
				err = setEdge(people, peopleGraph, val, peopleGraph.Node(index).(node))
				if err != nil {
					return err
				}
			} else {
				email2id[email] = peopleGraph.Node(index).(node)
			}
		}
	}
	reporter.Commit("people matched by email", len(email2id))

	// Add edges by the same unpopular name
	name2id := make(map[string]map[string]node)
	for index, person := range people {
		for _, name := range person.NamesWithRepos {
			if blacklist.isPopularName(name.String()) {
				reporter.Increment("popular names found")
				continue
			}
			for { // this for is to exit with break from the block when required
				externals, exists := name2id[name.String()]
				if exists {
					if n, exists := externals[person.ExternalID]; exists {
						err = setEdge(people, peopleGraph, n, peopleGraph.Node(index).(node))
						if err != nil {
							return err
						}
						break
					}
				} else {
					externals = map[string]node{}
					name2id[name.String()] = externals
				}
				externals[person.ExternalID] = peopleGraph.Node(index).(node)
				break
			}
		}
	}

	// Merge names with only one found external id
	for _, externalIDs := range name2id {
		if len(externalIDs) == 2 { // if there is more than 2 externalIDs then there are at least two external id found
			toMerge := false
			var edge []node
			for externalID, node := range externalIDs {
				if externalID == "" {
					toMerge = true
				}
				edge = append(edge, node)
			}
			if toMerge {
				err = setEdge(people, peopleGraph, edge[0], edge[1])
				// err can occur here and it is fine.
			}
		}
	}

	reporter.Commit("people matched by name", len(name2id))

	var componentsSize []float64
	for _, component := range topo.ConnectedComponents(peopleGraph) {
		var toMerge []int64
		for _, node := range component {
			toMerge = append(toMerge, node.ID())
		}
		componentsSize = append(componentsSize, float64(len(toMerge)))
		_, err := people.Merge(toMerge...)
		if err != nil {
			return err
		}
	}
	mean, std := stat.MeanStdDev(componentsSize, nil)
	reporter.Commit("connected component size mean", mean)
	reporter.Commit("connected component size std", std)
	reporter.Commit("connected component size max", floats.Max(componentsSize))
	reporter.Commit("people after reduce", len(people))

	return nil
}

// setEdge propagates ExternalID when you connect two components
func setEdge(people People, graph *simple.UndirectedGraph, node1, node2 node) error {
	node1ID := node1.ID()
	node2ID := node2.ID()
	ExternalID1 := people[node1ID].ExternalID
	ExternalID2 := people[node2ID].ExternalID
	if ExternalID1 != "" && ExternalID2 != "" && ExternalID1 != ExternalID2 {
		return fmt.Errorf(
			"cannot set edge between nodes with different ExternalIDs: %s %s",
			ExternalID1, ExternalID2)
	}
	var nodeToFix node
	newExternalID := ""
	if ExternalID1 == "" && ExternalID2 != "" {
		newExternalID = ExternalID2
		nodeToFix = node1
	} else if ExternalID1 != "" && ExternalID2 == "" {
		newExternalID = ExternalID1
		nodeToFix = node2
	}
	if newExternalID != "" {
		var w traverse.DepthFirst
		w.Walk(graph, nodeToFix, func(sn simplegraph.Node) bool {
			n := sn.(node)
			if n.Value.ExternalID != "" && n.Value.ExternalID != newExternalID {
				panic(fmt.Errorf(
					"cannot set edge between components with different ExternalIDs: |%s| |%s|",
					newExternalID, n.Value.ExternalID))
			}
			n.Value.ExternalID = newExternalID
			return false
		})
	}

	graph.SetEdge(graph.NewEdge(node1, node2))
	reporter.Increment("graph edges")
	return nil
}
