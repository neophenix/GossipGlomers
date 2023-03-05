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

// broadcastOrGrow stores which test we have determined we are doing.  This is because I wanted to do all the tests in
// one binary, and the broadcast and g-counter tests both use the Read method.
// default to grow, because all broadcast tests start with a topology message and we can set it then.  Others might use
// the topology later but we will deal with that
var broadcastOrGrow = "grow"

func HandleThings(n *maelstrom.Node) {
	node = n
	nodeKV = maelstrom.NewSeqKV(node)

	node.Handle("init", Initialize)
	node.Handle("echo", Echo)
	node.Handle("generate", Generate)
	node.Handle("broadcast", Broadcast)
	node.Handle("broadcast_ok", Noop)
	node.Handle("rebroadcast", Rebroadcast)
	node.Handle("rebroadcast_ok", Noop)
	node.Handle("read", Read)
	node.Handle("topology", Topology)
	node.Handle("add", Add)
}

// Noop is a do nothing handler, at this moment it is for broadcast-ok messages
func Noop(msg maelstrom.Message) error {
	// do nothing
	return nil
}
