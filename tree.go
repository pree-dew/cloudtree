package cloudtree

import (
	"strings"
)

// create a Tree with each tree with a structure
// resources -> namespace -> component_type -> tag -> instances
// give a capability to search via tag, exporter, sort

type walkFn func(string, interface{}) bool

type edge struct {
	label string
	n     *node
}

type leafNode struct {
	key   string
	value interface{}
}

type node struct {
	// use to hold the actual data given by user
	leaf *leafNode

	// gives no. of leaf nodes under a prefix to
	// find the no. of instances quickly
	depth int

	// used to facilitate easy search using prefix
	prefix string

	// use to locate the prefix and correct leaf quickly
	edges []edge
}

type Tree struct {
	root          *node
	value         string
	size          int
	resourceClass string
}

func New(resourceClass string) *Tree {
	return NewFromMap(resourceClass, nil)
}

func NewFromMap(resourceClass string, m map[string]interface{}) *Tree {
	t := &Tree{
		root:          &node{},
		resourceClass: resourceClass,
	}

	for k, v := range m {
		t.Insert(k, v)
	}

	return t
}

func (n *node) isLeaf() bool {
	return n.leaf != nil
}

func (n *node) addEdge(e edge) {
	n.edges = append(n.edges, e)
}

func (n *node) delEdge(label string) {
	index := -1
	for i, e := range n.edges {
		if e.label == label {
			index = i
			break
		}
	}

	if index == -1 {
		panic("no edge found")
	}

	// make copy of edges then remove that edge and copy the
	// remaining ones
	copy(n.edges[index:], n.edges[index+1:])
	n.edges[len(n.edges)-1] = edge{}
	n.edges = n.edges[:len(n.edges)-1]
}

func (n *node) updateEdge(label string, newNode *node) {
	if len(n.edges) == 0 {
		panic("No edge found to update")
	}

	for ix, e := range n.edges {
		if e.label == label {
			n.edges[ix].n = newNode
			break
		}
	}
}

func (n *node) getEdge(label string) *node {
	for _, e := range n.edges {
		if e.label == label {
			return e.n
		}
	}

	return nil
}

func longestPrefix(k1, k2 string) int {
	max := len(k1)
	if l := len(k2); l < max {
		max = l
	}

	var i int
	for i = 0; i < max; i++ {
		if k1[i] != k2[i] {
			break
		}
	}
	return i
}

// Len is used to return the number of elements in the tree
func (tr *Tree) Len() int {
	return tr.size
}

func (tr *Tree) Insert(nodeKey string, nodeValue interface{}) (interface{}, bool) {
	if nodeKey == "" {
		return nil, false
	}

	keyToBeInsterted := nodeKey
	parent := tr.root
	currNode := parent

	for {

		// if parent is empty it means it's the
		// parent tree, here it measns it's the namespace
		// tree, so insert in namespace tree
		// otherwise first find parent by prefix
		// then use that prefix as search for the child tree

		// if all relevant edges and leaf node is inserted
		// correctly after iterating over nodeKey

		if len(nodeKey) == 0 {
			if currNode.isLeaf() {
				old := currNode.leaf.value
				currNode.leaf.value = nodeValue
				return old, true
			}

			currNode.leaf = &leafNode{
				key:   keyToBeInsterted,
				value: nodeValue,
			}

			tr.size++
			return nil, false
		}

		// find the edge whose label matches the first character
		// of search key, to traverse only the relevant tree path

		parent = currNode
		currNode = currNode.getEdge(string(nodeKey[0]))

		// no edge found matching the nodekey, add an edge
		if currNode == nil {
			// add edge here and return
			e := edge{
				label: string(nodeKey[0]),
				n: &node{
					leaf: &leafNode{
						key:   keyToBeInsterted,
						value: nodeValue,
					},

					prefix: nodeKey,
				},
			}

			tr.size++
			parent.addEdge(e)
			return nil, false
		}

		// check that does the nodeKey matches the current node prefix
		commonPrefix := longestPrefix(nodeKey, currNode.prefix)

		// if current node prefix matches completly with nodeKey
		// then add the new edge to currNode
		if commonPrefix == len(currNode.prefix) {
			nodeKey = nodeKey[commonPrefix:]
			continue
		}

		// if prefix is not matching the length
		// then split the node into matched part
		// then add new edge for remaining part

		tr.size++
		child := &node{
			prefix: nodeKey[:commonPrefix],
		}
		parent.updateEdge(string(nodeKey[0]), child)

		child.addEdge(edge{
			label: string(currNode.prefix[commonPrefix]),
			n:     currNode,
		})
		currNode.prefix = currNode.prefix[commonPrefix:]

		leaf := &leafNode{
			key:   keyToBeInsterted,
			value: nodeValue,
		}

		nodeKey = nodeKey[commonPrefix:]
		if len(nodeKey) == 0 {
			child.leaf = leaf
			return nil, false
		}

		child.addEdge(edge{
			label: string(nodeKey[0]),
			n: &node{
				leaf:   leaf,
				prefix: nodeKey,
			},
		})

		return nil, false

	}
}

func (tr *Tree) Get(pattern string) (interface{}, bool) {
	n := tr.root
	for {
		if len(pattern) == 0 {
			if n.isLeaf() {
				return n.leaf.value, true
			}

			break
		}

		n = n.getEdge(string(pattern[0]))
		if n == nil {
			break
		}

		if strings.HasPrefix(pattern, n.prefix) {
			pattern = pattern[len(n.prefix):]
		} else {
			break
		}

	}
	return nil, false
}

func (tr *Tree) Delete(pattern string) (interface{}, bool) {
	n := tr.root
	parent := n

	var label string
	for {
		if len(pattern) == 0 {
			if n.isLeaf() {
				goto DELETE
			}

			break
		}

		parent = n
		label = string(pattern[0])
		n = n.getEdge(string(pattern[0]))
		if n == nil {
			break
		}

		if strings.HasPrefix(pattern, n.prefix) {
			pattern = pattern[len(n.prefix):]
		} else {
			break
		}

	}

	return nil, false

DELETE:
	leaf := n.leaf
	n.leaf = nil
	tr.size--

	if parent != nil && len(n.edges) == 0 {
		parent.delEdge(label)
	}

	// if parent is not nil and node has only 1 edge
	if parent != nil && len(n.edges) == 1 {
		n.mergeChild()
	}

	if parent != nil && len(parent.edges) == 1 && parent != tr.root && !parent.isLeaf() {
		parent.mergeChild()
	}

	return leaf.value, true
}

func (tr *Tree) DeletePrefix(pattern string) int {
	return tr.deletePrefix(tr.root, tr.root, pattern)
}

func (tr *Tree) deletePrefix(parent, n *node, pattern string) int {
	// To delete a prefix reach the leaf node
	// make leaf = nil

	if len(pattern) == 0 {
		// find the subtree size
		subTreeSize := 0
		recursiveWalk(n, func(k string, v interface{}) bool {
			subTreeSize++
			return false
		})

		n.leaf = nil
		if n.isLeaf() {
			n.leaf = nil
		}
		n.edges = nil // deletes the entire subtree

		// Check if we should merge the parent's other child
		if parent != nil && parent != tr.root && len(parent.edges) == 1 && !parent.isLeaf() {
			parent.mergeChild()
		}

		tr.size = tr.size - subTreeSize
		return subTreeSize
	}

	child := n.getEdge(string(pattern[0]))
	if child == nil {
		return 0
	}

	if !strings.HasPrefix(pattern, child.prefix) && !strings.HasPrefix(child.prefix, pattern) {
		return 0
	}

	if len(pattern) > len(child.prefix) {
		pattern = pattern[len(child.prefix):]
	} else {
		pattern = pattern[len(pattern):]
	}

	return tr.deletePrefix(parent, child, pattern)
}

func (tr *Tree) Walk(fn walkFn) bool {
	return recursiveWalk(tr.root, fn)
}

// WalkPrefix is used to walk the tree under a prefix
func (tr *Tree) WalkPrefix(prefix string, fn walkFn) {
	n := tr.root
	pattern := prefix
	for {
		// Check for key exhaustion
		if len(pattern) == 0 {
			recursiveWalk(n, fn)
			return
		}

		// Look for an edge
		n = n.getEdge(string(pattern[0]))
		if n == nil {
			return
		}

		// Consume the search prefix
		if strings.HasPrefix(pattern, n.prefix) {
			pattern = pattern[len(n.prefix):]
			continue
		}

		if strings.HasPrefix(n.prefix, pattern) {
			// Child may be under our search prefix
			recursiveWalk(n, fn)
		}
		return
	}
}

func (tr *Tree) WalkPath(path string, fn walkFn) {
	n := tr.root
	pattern := path
	for {
		// Visit the leaf values if any
		if n.leaf != nil && fn(n.leaf.key, n.leaf.value) {
			return
		}

		if len(pattern) == 0 {
			return
		}

		// Look for an edge
		n = n.getEdge(string(pattern[0]))
		if n == nil {
			return
		}

		// Consume the search prefix
		if strings.HasPrefix(pattern, n.prefix) {
			pattern = pattern[len(n.prefix):]
		} else {
			break
		}
	}
}

func recursiveWalk(n *node, fn walkFn) bool {
	// base case
	if n.leaf != nil && fn(n.leaf.key, n.leaf.value) {
		return true
	}

	i := 0
	k := len(n.edges)
	for i < k {
		e := n.edges[i]
		if recursiveWalk(e.n, fn) {
			return true
		}

		if len(n.edges) == 0 {
			return recursiveWalk(n, fn)
		}

		if len(n.edges) >= k {
			i++
		}

		k = len(n.edges)
	}

	return false
}

func (n *node) mergeChild() {
	e := n.edges[0]
	child := e.n
	n.prefix = n.prefix + child.prefix
	n.leaf = child.leaf
	n.edges = child.edges
}

func (tr *Tree) LongestPrefix(pattern string) (string, interface{}, bool) {
	var last *leafNode
	n := tr.root

	for {

		// Look for a leaf node
		if n.isLeaf() {
			last = n.leaf
		}

		// Check for key exhaution
		if len(pattern) == 0 {
			break
		}

		// Look for an edge
		n = n.getEdge(string(pattern[0]))
		if n == nil {
			break
		}

		// Consume the search prefix
		if strings.HasPrefix(pattern, n.prefix) {
			pattern = pattern[len(n.prefix):]
		} else {
			break
		}

	}

	if last != nil {
		return last.key, last.value, true
	}

	return "", nil, false
}

// ToMap is used to walk the tree and convert it into a map
func (tr *Tree) ToMap() map[string]interface{} {
	out := make(map[string]interface{}, tr.size)
	tr.Walk(func(k string, v interface{}) bool {
		out[k] = v
		return false
	})
	return out
}
