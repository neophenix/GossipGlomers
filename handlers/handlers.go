package handlers

import (
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// node is our maelstrom node
var node *maelstrom.Node

// nodeID is our parsed node id, we will only use 10 bits of this
var nodeID int16

// topology is a place to store our node topology, going to use the same format maelstrom sends until I see a need
// to change it
var topology = map[string][]string{}

func HandleThings(n *maelstrom.Node) {
	node = n

	node.Handle("init", Initialize)
	node.Handle("echo", Echo)
	node.Handle("generate", Generate)
	node.Handle("broadcast", Broadcast)
	node.Handle("broadcast_ok", Noop)
	node.Handle("rebroadcast", Rebroadcast)
	node.Handle("rebroadcast_ok", Noop)
	node.Handle("read", Read)
	node.Handle("topology", Topology)
}

// Noop is a do nothing handler, at this moment it is for broadcast-ok messages
func Noop(msg maelstrom.Message) error {
	// do nothing
	return nil
}
