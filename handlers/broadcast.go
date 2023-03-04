package handlers

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// BroadcastMsg is our broadcast message request / response type
type broadcastMsg struct {
	Type    string `json:"type"`
	ID      int64  `json:"msg_id"`
	Message int64  `json:"message"`
}

// ReadMsg is our read message request / response type
type readMsg struct {
	Type     string  `json:"type"`
	Messages []int64 `json:"messages"`
}

// TopoMsg is our topology message request / response type
type topoMsg struct {
	Type     string              `json:"type"`
	Topology map[string][]string `json:"topology,omitempty"`
}

// bcastMutex protects our messages slice from concurrent operations
var bcastMutex sync.RWMutex

// bcastMessages will store all our received broadcast messages
var bcastMessages = []int64{}

// broadcast messages we've seen.  Probably not an ideal way for something that would run for a long time, but I think
// for this its reasonable.
var bcastSeen = map[int64]bool{}

// rebroadcastMsg stores information about a message we need to rebroadcast
type rebroadcastMsg struct {
	Dest    string // where the message is going
	Message int64  // the message
}

// Broadcast handles the broadcast message type and appends values into our slice
func Broadcast(msg maelstrom.Message) error {
	var body broadcastMsg
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	// lock our slice and map lookups / writing
	bcastMutex.Lock()
	defer bcastMutex.Unlock()

	// propogate our message to other nodes if this is the first time we've seen it
	if !bcastSeen[body.Message] {
		// Mark it seen
		bcastSeen[body.Message] = true
		// add it to our list of messages
		bcastMessages = append(bcastMessages, body.Message)

		// now, if this came from a client, send it off to every other node in the cluster, if it isn't from a client
		// for now we aren't going to rebroadcast to keep our messages per op low
		if msg.Src[0] == 'c' {
			for _, dest := range node.NodeIDs() {
				if dest != node.ID() {
					msg := &rebroadcastMsg{Dest: dest, Message: body.Message}
					msg.rebroadcast()
				}
			}
		}
	}

	// let the sender know we got it
	return node.Reply(msg, map[string]any{"type": "broadcast_ok"})
}

// Read will return the broadcasted messages we've seen
func Read(msg maelstrom.Message) error {
	var body readMsg
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	// set our return type, grab a read lock, read our slice
	body.Type = "read_ok"
	bcastMutex.RLock()
	defer bcastMutex.RUnlock()
	body.Messages = bcastMessages

	// send our messages back
	return node.Reply(msg, body)
}

// Topology handles topology messages and sets up queues for our neighbor nodes
func Topology(msg maelstrom.Message) error {
	var body topoMsg
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	// just set our topology the same as we get it for now
	topology = body.Topology

	// set our response
	body.Type = "topology_ok"
	body.Topology = nil
	// send our messages back
	return node.Reply(msg, body)
}

// rebroadcast keeps trying to broadcast our message to the destination.
func (msg *rebroadcastMsg) rebroadcast() {
	go func() {
		// normally I'd have some retry limit here but for the purposes of these tests not at the moment
		for {
			// some arbitrary timeout to get a response to our message back
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			_, err := node.SyncRPC(ctx, msg.Dest, broadcastMsg{Type: "broadcast", Message: msg.Message})
			cancel() // call cancel so the linter doesn't yell at me
			if err == nil {
				// no error we are done with this message
				return
			}
		}
	}()
}
