package rdfwriter

import (
	"fmt"
	"github.com/RishabhBhatnagar/gordf/rdfloader/parser"
)

// returns an adjacency list from a list of triples
func getAdjacencyList(triples []*parser.Triple) (adjList map[*parser.Node][]*parser.Node) {
	// triples are analogous to the edges of a graph.
	// For a (Subject, Predicate, Object) triple,
	// it forms a directed edge from Subject to Object
	// Graphically,
	//                          predicate
	//             (Subject) ---------------> (Object)

	// initialising the adjacency list:
	adjList = make(map[*parser.Node][]*parser.Node)
	for _, triple := range triples {
		// create a new entry in the adjList if the key is not already seen.
		if adjList[triple.Subject] == nil {
			adjList[triple.Subject] = []*parser.Node{triple.Object}
		} else {
			// the key is already seen and we can directly append the child
			adjList[triple.Subject] = append(adjList[triple.Subject], triple.Object)
		}

		// ensure that there is a key entry for all the children.
		if adjList[triple.Object] == nil {
			adjList[triple.Object] = []*parser.Node{}
		}
	}
	return adjList
}


// same as dfs function. Just that after each every neighbor of the node is visited, it is appended in a queue.
// Params:
//     node: Current node to perform dfs on.
//     lastIdx: index where a new node should be added in the resultList
//     visited: if visited[node] is true, we've already serviced the node before.
//     resultList: list of all the nodes after topological sorting.
func topologicalSortHelper(node *parser.Node, lastIndex *int, adjList map[*parser.Node][]*parser.Node, visited *map[*parser.Node]bool, resultList *[]*parser.Node) (err error) {
	if node == nil {
		return
	}

	// marking current node as visited
	(*visited)[node] = true

	// visiting all the neighbors of the node and it's children recursively
	for _, neighbor := range adjList[node] {
		// recurse neighbor only if and only if it is not visited yet.
		if !(*visited)[neighbor] {
			err = topologicalSortHelper(neighbor, lastIndex, adjList, visited, resultList)
			if err != nil {
				return err
			}
		}
	}

	if *lastIndex == -1 {
		// there is at least one node which is a neighbor of some node
		// whose entry doesn't exist in the adjList
		return fmt.Errorf("found more nodes than the number of keys in the adjacency list")
	}

	// appending from the right hand side to have a queue effect.
	(*resultList)[*lastIndex] = node
	*lastIndex--

	return nil
}


// A wrapper function to initialize the data structures required by the
// topological sort algorithm. It provides an interface for the user to
// directly get the sorted triples without knowing the internal variables
// required for sorting.
// Params:
//   adjList   : adjacency list: a map with key as the node and value as a
//  			 list of it's neighbor nodes.
// Assumes: all the nodes in the graph are present in the adjList keys.
func TopologicalSort(adjList map[*parser.Node][]*parser.Node) ([]*parser.Node, error) {
	// variable declaration
	numberNodes := len(adjList)
	resultList := make([]*parser.Node, numberNodes) //  this will be returned
	visited := make(map[*parser.Node]bool, numberNodes)
	lastIndex := numberNodes - 1

	// iterate through nodes and perform a dfs starting from that node.
	for node := range adjList {
		if !visited[node] {
			err := topologicalSortHelper(node, &lastIndex, adjList, &visited, &resultList)
			if err != nil {
				return resultList, err
			}
		}
	}
	return resultList, nil
}

