package handlers

import (
	"encoding/json"
	"fmt"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type InitMsg struct {
	Type    string   `json:"type"`
	MsgID   int      `json:"msg_id"`
	NodeID  string   `json:"node_id"`
	NodeIDs []string `json:"node_ids"`
}

// Initialize is our init handler.  Normally this is done for us but I wanted to parse out the node ID once so this
// seemed like the way to do it.
func Initialize(msg maelstrom.Message) error {
	// unmarshal into a defined struct to make getting the right types for node id, etc easier.
	var body InitMsg
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	// init our node
	node.Init(body.NodeID, body.NodeIDs)

	// parse our node id into an int for later
	parsed, err := fmt.Sscanf(node.ID(), "n%d", &nodeID)
	// make sure we parse something
	if err != nil || parsed == 0 {
		log.Fatalf("could not parse node id: %v", node.ID())
	}
	// make sure our node id fits in our 10 bit allotment
	if nodeID < 0 || nodeID > 1024 {
		log.Fatal("node id should be between 0 and 1024")
	}

	// set our ok message type and send it back
	body.Type = "init_ok"
	return node.Reply(msg, body)
}
