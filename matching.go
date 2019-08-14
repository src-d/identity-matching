package idmatch

import (
	"context"
	"fmt"
	"sort"

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

// Int64Slice attaches the methods of Interface to []int64, sorting in increasing order.
type Int64Slice []int64

func (p Int64Slice) Len() int           { return len(p) }
func (p Int64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p Int64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// Sort is a convenience method.
func (p Int64Slice) Sort() { sort.Sort(p) }

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
func ReducePeople(people People, matcher external.Matcher, blacklist Blacklist,
	maxIdentities int) error {
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
	// we need to sort keys because algorithm is order dependent
	keys := make([]int64, 0, len(people))
	for k := range people {
		keys = append(keys, k)
	}
	Int64Slice(keys).Sort()
	for _, index := range keys {
		person := people[index]
		iNode := peopleGraph.Node(index).(node)
		for _, name := range person.NamesWithRepos {
			if blacklist.isPopularName(name.String()) {
				reporter.Increment("popular names found")
				continue
			}
			for { // this for is to exit with break from the block when required
				externals, exists := name2id[name.String()]
				if exists {
					if n, exists := externals[person.ExternalID]; exists {
						if topo.PathExistsIn(peopleGraph, n, iNode) ||
							!passIdentitiesLimit(peopleGraph, maxIdentities, iNode, n) {
							break
						}
						err = setEdge(people, peopleGraph, n, iNode)
						if err != nil {
							return err
						}
						break
					}
				} else {
					externals = map[string]node{}
					name2id[name.String()] = externals
				}
				externals[person.ExternalID] = iNode
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
				if topo.PathExistsIn(peopleGraph, edge[0], edge[1]) ||
					!passIdentitiesLimit(peopleGraph, maxIdentities, edge[0], edge[1]) {
					break
				}
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

func passIdentitiesLimit(graph *simple.UndirectedGraph, maxIdentities int, node1, node2 node) bool {
	n1Emails, n1Names := componentUniqueEmailsAndNames(graph, node1)
	n2Emails, n2Names := componentUniqueEmailsAndNames(graph, node2)
	if n1Emails+n1Names >= maxIdentities || n2Names+n2Emails >= maxIdentities {
		logrus.Debugf(
			"Edge is not added between %s (%d emails, %d names) and %s (%d emails, %d names).",
			node1.Value.String(), n1Emails, n1Names, node2.Value.String(), n2Emails, n2Names)
		return false
	}
	return true
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

// componentUniqueEmailsAndNames calculates the number of unique emails and names in the component
// with n node inside
func componentUniqueEmailsAndNames(graph *simple.UndirectedGraph, n simplegraph.Node) (int, int) {
	emails := map[string]struct{}{}
	names := map[string]struct{}{}
	var w traverse.DepthFirst
	w.Walk(graph, n, func(sn simplegraph.Node) bool {
		for _, email := range sn.(node).Value.Emails {
			emails[email] = struct{}{}
		}
		for _, name := range sn.(node).Value.NamesWithRepos {
			names[name.String()] = struct{}{}
		}
		return false
	})
	return len(emails), len(names)
}
