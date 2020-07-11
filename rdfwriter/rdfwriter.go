package rdfwriter

import (
	"fmt"
	"github.com/RishabhBhatnagar/gordf/rdfloader/parser"
	"github.com/RishabhBhatnagar/gordf/uri"
	"strings"
)

func shortenURI(uri string, invSchemaDefinition map[string]string) (string, error) {
	splitIndex := strings.LastIndex(uri, "#")
	if splitIndex == -1 {
		return "", fmt.Errorf("uri doesn't have two parts of type schemaName:tagName. URI: %s", uri)
	}
	baseURI := strings.Trim(uri[:splitIndex], "#")
	fragment := strings.Trim(uri[splitIndex + 1:], "#")
	if abbrev, exists := invSchemaDefinition[baseURI]; exists {
		return fmt.Sprintf("%s:%s", abbrev, fragment), nil
	}
	return "", fmt.Errorf("declaration of URI(%s) not found in the schemaDefinition", baseURI)
}

// from a given adjacency list, return a list of root-nodes which will be used
// to generate string forms of the nodes to be written.
func getRootNodes(triples []*parser.Triple, adjList map[*parser.Node][]*parser.Node) (rootNodes []*parser.Node) {
	// getting all the subject nodes from which the root nodes will be selected
	var subjects []*parser.Node
	subjects = getAllSubjects(adjList)


	// In a disjoint set, indices with root nodes will point to nil
	// that means, if disjointSet[1] is nil, subjects[1] has no parent.
	// that is, subject[1] is not the object of any of the triples.
	var parent map[*parser.Node]*parser.Node
	parent = DisjointSet(triples, subjects)

	for node := range parent {
		if parent[node] == nil {
			rootNodes = append(rootNodes, node)
		}
	}
	return rootNodes
}

func getAllSubjects(adjList map[*parser.Node][]*parser.Node) (subjects []*parser.Node){
	for sub := range adjList {
		if len(adjList[sub]) > 0 {
			subjects = append(subjects, sub)
		}
	}
	return subjects
}

func filterTriples(triples []*parser.Triple, subject, predicate, object *string) (result []*parser.Triple) {
	for _, triple := range triples {
		if (subject == nil || *subject == triple.Subject.ID) && (predicate == nil || *predicate == triple.Predicate.ID) && (object == nil || *object == triple.Object.ID) {
			result = append(result, triple)
		}
	}
	return
}

func stringify(node *parser.Node, nodeToTriples map[*parser.Node][]*parser.Triple, invSchemaDefinition map[string]string, depth int) (output string, err error) {
	rdfTypeURI := parser.RDFNS + "type"
	rdfNodeIDURI := parser.RDFNS + "nodeID"
	rdfResourceURI := parser.RDFNS + "resource"
	rdfNSAbbrev := "rdf"
	if abbrev, exists := invSchemaDefinition[parser.RDFNS]; exists {
		rdfNSAbbrev = abbrev
	}

	openingTagFormat := "<%s%s%s%s>"
	closingTagFormat := "</%s>"
	// taking example of the following tag:
	//   <spdx:name rdf:nodeID="ID" rdf:about="https://sample.com#name">Apache License 2.0</spdx:name>
	// Description of the %s used in the openingTagFormat
	// 1st %s: node's name and schemaName
	// 		   spdx:name in case of the example
	// 2nd %s: nodeId attribute
	//         rdf:nodeID="ID" for the given example
	// 3rd %s: rdf:about property
	//         rdf:about="https://sample.com#name" for the given example
	// 4th %s: rdf:resource attribute
	//       : not in the example
	// NOTE: 2nd, 3rd and 4th %s can be given in any order. won't affect the semantics of the output.
	// Description of %s in closingTagFormat
	// 6th %%s: Same as 1st %s of openingTagFormat

	var openingTag, closingTag, childrenString string
	rdfTypeTriples := filterTriples(nodeToTriples[node], nil, &rdfTypeURI, nil)
	if n := len(rdfTypeTriples); n != 1 {
		return "", fmt.Errorf("every subject node must be associated with exactly 1 triple of type rdf:type predicate. Found %v triples", n)
	}
	rdfnodeIDTriples := filterTriples(nodeToTriples[node], nil, &rdfNodeIDURI, nil)
	if n := len(rdfnodeIDTriples); n > 1 {
		return "", fmt.Errorf("there must be atmost nodeID attribute. found %v nodeID attributes", n)
	}
	rdfResourceTriples := filterTriples(nodeToTriples[node], nil, &rdfResourceURI, nil)
	if n := len(rdfResourceTriples); n > 1 {
		return "", fmt.Errorf("there must be atmost 1 nodeID attribute. found %v nodeID attributes", n)
	}

	tagName, err := shortenURI(rdfTypeTriples[0].Object.ID, invSchemaDefinition)
	if err != nil {
		return "", err
	}
	rdfNodeID := ""
	if len(rdfnodeIDTriples) == 1 {
		rdfNodeID = fmt.Sprintf(` %s:nodeID="%s"`, rdfNSAbbrev, rdfnodeIDTriples[0].Object.ID)
	}
	rdfAbout := ""
	if node.NodeType == parser.IRI {
		rdfAbout = fmt.Sprintf(` %s:about="%s"`, rdfNSAbbrev, node.ID)
	}
	rdfResource := ""
	if len(rdfResourceTriples) == 1 {
		rdfResource = fmt.Sprintf(` %s:resource=\%s"`, rdfNSAbbrev, rdfResourceTriples[0].Object.ID)
	}
	openingTag = strings.Repeat("\t", depth) + fmt.Sprintf(openingTagFormat, tagName, rdfNodeID, rdfAbout, rdfResource)
	closingTag = strings.Repeat("\t", depth) + fmt.Sprintf(closingTagFormat, tagName)
	// parsing all other triple
	depth++ // we'll be parsing one level deep now.
	for _, triple := range nodeToTriples[node] {
		if !any(triple.Predicate.ID, []string{rdfNodeIDURI, rdfResourceURI, rdfTypeURI}) {
			predicateURI, err := shortenURI(triple.Predicate.ID, invSchemaDefinition)
			if err != nil {
				return "", err
			}
			var childString string
			// adding opening tag to the child tag:
			childString += strings.Repeat("\t", depth) + fmt.Sprintf("<%s>", predicateURI) + "\n"
			if len(nodeToTriples[triple.Object]) == 0 {
				// the tag ends here and doesn't have any further childs.
				// object is even one level deep
				// number of tabs increases.
				childString += strings.Repeat("\t", depth + 1) + triple.Object.ID
			} else {
				// we have a sub-child which is not a literal type. it can be a blank or a IRI node.
				temp, err := stringify(triple.Object, nodeToTriples, invSchemaDefinition, depth + 1)
				if err != nil {
					return "", err
				}
				childString += temp
			}
			// adding the closing tag
			childString += "\n" + strings.Repeat("\t", depth) +  fmt.Sprintf(closingTagFormat, predicateURI)
			childrenString += childString + "\n"
		}
	}
	childrenString = strings.TrimSuffix(childrenString, "\n")
	return fmt.Sprintf("%s\n%v\n%s", openingTag, childrenString, closingTag), nil
}


func TriplesToString(triples []*parser.Triple, schemaDefinition map[string]uri.URIRef) (outputString string, err error) {
	// linearly ordering the triples in a non-increasing order of depth.
	sortedTriples, err := TopologicalSortTriples(triples)
	if err != nil {
		return outputString, err
	}

	invSchemaDefinition := invertSchemaDefinition(schemaDefinition)
	adjList, nodeToTriples := getAdjacencyList(sortedTriples)
	rootTags := getRootNodes(sortedTriples, adjList)

	// now, we can iterate over all the root-nodes and generate the string representation of the nodes.
	for _, tag := range rootTags {
		currString, err := stringify(tag, nodeToTriples, invSchemaDefinition, 0)
		if err != nil {
			return outputString, nil
		}
		outputString += currString + "\n"
	}
	return outputString, nil
}