package handlers

import (
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// node is our maelstrom node
var node *maelstrom.Node

// nodeID is our parsed node id, we will only use 10 bits of this
var nodeID int16

func HandleThings(n *maelstrom.Node) {
	node = n

	node.Handle("init", Initialize)
	node.Handle("echo", Echo)
	node.Handle("generate", Generate)
}
