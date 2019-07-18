package idmatch

import (
	"github.com/src-d/eee-identity-matching/external"
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

// ReducePeople merges the identities together by following the fixed set of rules.
// 1. Run the external matching, if available.
// 2. Run the series of heuristics on those items which were left untouched in the list (everything
//    in case of ext == nil, not found in case of ext != nil).
//
// The heuristics are:
// TODO(vmarkovtsev): describe the current approach
func ReducePeople(people People, ext external.Matcher, blacklist Blacklist) error {
	// TODO(zurk): implement external matching

	peopleGraph := simple.NewUndirectedGraph()
	for index, person := range people {
		peopleGraph.AddNode(node{person, index})
	}

	// Add edges by the same unpopular email
	email2id := make(map[string]simplegraph.Node)
	for index, person := range people {
		for _, email := range person.Emails {
			if blacklist.isPopularEmail(email) {
				continue
			}
			if val, ok := email2id[email]; ok {
				peopleGraph.SetEdge(peopleGraph.NewEdge(val, peopleGraph.Node(index)))
			} else {
				email2id[email] = peopleGraph.Node(index)
			}
		}
	}

	// Add edges by the same unpopular name
	name2id := make(map[string]simplegraph.Node)
	for index, person := range people {
		for _, name := range person.NamesWithRepos {
			if blacklist.isPopularName(name.String()) {
				continue
			}
			if val, ok := name2id[name.String()]; ok {
				peopleGraph.SetEdge(peopleGraph.NewEdge(val, peopleGraph.Node(index)))
			} else {
				name2id[name.String()] = peopleGraph.Node(index)
			}
		}
	}

	for _, component := range topo.ConnectedComponents(peopleGraph) {
		var toMerge []int64
		for _, node := range component {
			toMerge = append(toMerge, node.ID())
		}
		people.Merge(toMerge...)
	}

	return nil

}
