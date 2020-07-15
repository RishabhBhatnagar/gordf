package rdfwriter

import (
	"fmt"
	"github.com/RishabhBhatnagar/gordf/rdfloader/parser"
	"github.com/RishabhBhatnagar/gordf/uri"
	"io/ioutil"
	"strings"
)

func shortenURI(uri string, invSchemaDefinition map[string]string) (string, error) {
	splitIndex := strings.LastIndex(uri, "#")
	if splitIndex == -1 {
		return "", fmt.Errorf("uri doesn't have two parts of type schemaName:tagName. URI: %s", uri)
	}
	baseURI := strings.Trim(uri[:splitIndex], "#")
	fragment := strings.Trim(uri[splitIndex+1:], "#")
	fragment = strings.TrimSpace(fragment)
	if len(fragment) == 0 {
		return "", fmt.Errorf(`fragment "%v" doesn't exist`, fragment)
	}
	if abbrev, exists := invSchemaDefinition[baseURI]; exists {
		return fmt.Sprintf("%s:%s", abbrev, fragment), nil
	}
	return "", fmt.Errorf("declaration of URI(%s) not found in the schemaDefinition", baseURI)
}

// from a given adjacency list, return a list of root-nodes which will be used
// to generate string forms of the nodes to be written.
func getRootNodes(triples []*parser.Triple) (rootNodes []*parser.Node) {

	// In a disjoint set, indices with root nodes will point to nil
	// that means, if disjointSet[1] is nil, subjects[1] has no parent.
	// that is, subject[1] is not the object of any of the triples.
	var parent map[*parser.Node]*parser.Node
	parent = DisjointSet(triples)

	for node := range parent {
		if parent[node] == nil {
			rootNodes = append(rootNodes, node)
		}
	}
	return rootNodes
}

func filterTriples(triples []*parser.Triple, subject, predicate, object *string) (result []*parser.Triple) {
	for _, triple := range triples {
		if (subject == nil || *subject == triple.Subject.ID) && (predicate == nil || *predicate == triple.Predicate.ID) && (object == nil || *object == triple.Object.ID) {
			result = append(result, triple)
		}
	}
	return
}

func getRootTagFromSchemaDefinition(schemaDefinition map[string]uri.URIRef, tab string) string {
	rootTag := "<rdf:RDF\n"
	for tag := range schemaDefinition {
		tagURI := schemaDefinition[tag]
		rootTag += tab + fmt.Sprintf(`%s:%s="%s"`, "xmlns", tag, tagURI.String()) + "\n"
	}
	rootTag = rootTag[:len(rootTag)-1] // removing the last \n char.
	rootTag += ">"
	return rootTag
}

func getRestTriples(triples []*parser.Triple) (restTriples []*parser.Triple) {
	rdfTypeURI := parser.RDFNS + "type"
	rdfNodeIDURI := parser.RDFNS + "nodeID"
	rdfResourceURI := parser.RDFNS + "resource"
	for _, triple := range triples {
		if !any(triple.Predicate.ID, []string{rdfNodeIDURI, rdfResourceURI, rdfTypeURI}) {
			restTriples = append(restTriples, triple)
		}
	}
	return restTriples
}

func stringify(node *parser.Node, nodeToTriples map[*parser.Node][]*parser.Triple, invSchemaDefinition map[string]string, depth int, tab string) (output string, err error) {
	rdfTypeURI := parser.RDFNS + "type"
	rdfNodeIDURI := parser.RDFNS + "nodeID"
	rdfNSAbbrev := "rdf"
	if abbrev, exists := invSchemaDefinition[parser.RDFNS]; exists {
		rdfNSAbbrev = abbrev
	}

	openingTagFormat := "<%s%s%s>"
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

	openingTag = strings.Repeat(tab, depth) + fmt.Sprintf(openingTagFormat, tagName, rdfNodeID, rdfAbout)
	closingTag = strings.Repeat(tab, depth) + fmt.Sprintf(closingTagFormat, tagName)

	// getting rest of the triples after rdf triples are parsed
	restTriples := getRestTriples(nodeToTriples[node])

	depth++ // we'll be parsing one level deep now.
	for _, triple := range restTriples {
		predicateURI, err := shortenURI(triple.Predicate.ID, invSchemaDefinition)
		if err != nil {
			return "", err
		}

		if triple.Object.NodeType == parser.RESOURCELITERAL {
			childrenString += strings.Repeat(tab, depth) + fmt.Sprintf(`<%s %s:resource="%s"/>`, predicateURI, rdfNSAbbrev, triple.Object.ID) + "\n"
			continue
		}

		var childString string
		// adding opening tag to the child tag:
		childString += strings.Repeat(tab, depth) + fmt.Sprintf("<%s>", predicateURI) + "\n"
		if len(nodeToTriples[triple.Object]) == 0 {
			// the tag ends here and doesn't have any further childs.
			// object is even one level deep
			// number of tabs increases.
			childString += strings.Repeat(tab, depth+1) + triple.Object.ID
		} else {
			// we have a sub-child which is not a literal type. it can be a blank or a IRI node.
			temp, err := stringify(triple.Object, nodeToTriples, invSchemaDefinition, depth+1, tab)
			if err != nil {
				return "", err
			}
			childString += temp
		}
		// adding the closing tag
		childString += "\n" + strings.Repeat(tab, depth) + fmt.Sprintf(closingTagFormat, predicateURI)
		childrenString += childString + "\n"
	}
	childrenString = strings.TrimSuffix(childrenString, "\n")
	return fmt.Sprintf("%s\n%v\n%s", openingTag, childrenString, closingTag), nil
}

func TriplesToString(triples []*parser.Triple, schemaDefinition map[string]uri.URIRef, tab string) (outputString string, err error) {
	// linearly ordering the triples in a non-increasing order of depth.
	sortedTriples, err := TopologicalSortTriples(triples)
	if err != nil {
		return outputString, err
	}

	invSchemaDefinition := invertSchemaDefinition(schemaDefinition)
	_, nodeToTriples := getAdjacencyList(sortedTriples)
	rootTags := getRootNodes(sortedTriples)

	// now, we can iterate over all the root-nodes and generate the string representation of the nodes.
	for _, tag := range rootTags {
		currString, err := stringify(tag, nodeToTriples, invSchemaDefinition, 1, "  ")
		if err != nil {
			return outputString, nil
		}
		outputString += currString + "\n"
	}
	rootTagString := getRootTagFromSchemaDefinition(schemaDefinition, tab)
	rootEndTag := "</rdf:RDF>"
	return fmt.Sprintf("%s\n%s%s", rootTagString, outputString, rootEndTag), nil
}

// converts the input triples to string and writes it to the file.
func WriteToFile(triples []*parser.Triple, schemaDefinition map[string]uri.URIRef, tab, filePath string) error {
	opString, err := TriplesToString(triples, schemaDefinition, tab)
	if err != nil {
		return err
	}

	// Default file permission is hardcoded. Maybe leave it to the user.
	return ioutil.WriteFile(filePath, []byte(opString), 655)
}
