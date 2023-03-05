package handlers

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// broadcastMsg is our broadcast message request / response type
type broadcastMsg struct {
	Type    string `json:"type"`
	Message int64  `json:"message"`
}

// rebroadcastMsg is our message between nodes when they rebroadcast what they've seen
type rebroadcastMsg struct {
	Type     string
	Messages []int64 // the messages accumulated for this rebroadcast
}

// readMsg is our read message request / response type
type readMsg struct {
	Type     string  `json:"type"`
	Messages []int64 `json:"messages"`
}

// topoMsg is our topology message request / response type
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

// nodeRebroadcastInfo stores information about a node we need to rebroadcast to
type nodeRebroadcastInfo struct {
	Dest     string // destination node id
	Position int    // position in our rebroadcast queue
	Delay    int64  // average delay in communication (ms)
	Count    int64  // count for our running average
}

// rebroadcastQueue is our queue of messages from clients that we need to send to other nodes
var rebroadcastQueue = []int64{}

// rebroadQueueMu locks access to our rebroadcastQueue
var rebroadQueueMu sync.Mutex

// nodeInfoLookup stores our node info in a map for easier access
var nodeInfoLookup = map[string]*nodeRebroadcastInfo{}

// nodeInfoMu locks access to our info lookup map
var nodeInfoMu sync.Mutex

// Broadcast handles the broadcast message type and appends values into our slice
func Broadcast(msg maelstrom.Message) error {
	var body broadcastMsg
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	// lock our slice and map lookups / writing
	bcastMutex.Lock()
	// propogate our message to other nodes if this is the first time we've seen it
	if !bcastSeen[body.Message] {
		// Mark it seen
		bcastSeen[body.Message] = true
		// add it to our list of messages
		bcastMessages = append(bcastMessages, body.Message)

		// now, if this came from a client, send it off to every other node in the cluster, if it isn't from a client
		// for now we aren't going to rebroadcast to keep our messages per op low
		if msg.Src[0] == 'c' {
			rebroadQueueMu.Lock()
			rebroadcastQueue = append(rebroadcastQueue, body.Message)
			rebroadQueueMu.Unlock()
		}
	}
	bcastMutex.Unlock()

	// let the sender know we got it
	return node.Reply(msg, map[string]any{"type": "broadcast_ok"})
}

// Rebroadcast is a similar handler to Broadcast, except it operates on a list of messages from another node and does
// not rebroadcast them.
func Rebroadcast(msg maelstrom.Message) error {
	var body rebroadcastMsg
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	// lock our slice and map lookups / writing
	bcastMutex.Lock()
	for _, value := range body.Messages {
		if !bcastSeen[value] {
			// Mark it seen
			bcastSeen[value] = true
			// add it to our list of messages
			bcastMessages = append(bcastMessages, value)
		}
	}
	bcastMutex.Unlock()

	// let the sender know we got it
	return node.Reply(msg, map[string]any{"type": "rebroadcast_ok"})
}

// Read will return the broadcasted messages we've seen
func Read(msg maelstrom.Message) error {
	// before we do anything else, if we think we are in the grow test hand it off to that handler
	if broadcastOrGrow != "broadcast" {
		return ReadGrow(msg)
	}

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

	broadcastOrGrow = "broadcast"

	// just set our topology the same as we get it for now
	topology = body.Topology

	for _, id := range node.NodeIDs() {
		// setup our node info for an initial small delay
		info := &nodeRebroadcastInfo{Dest: id, Delay: 100, Count: 1}
		// start our accumulator
		info.accumulate()
		nodeInfoLookup[id] = info
	}

	// set our response
	body.Type = "topology_ok"
	body.Topology = nil
	// send our messages back
	return node.Reply(msg, body)
}

// accumulate fires of a goroutine that waits for some delay (currently 500ms) to accumulate messages then rebroadcasts
// them to the other node.
func (info *nodeRebroadcastInfo) accumulate() {
	go func() {
		for {
			select {
			// 500ms to keep our messages per op below threshold
			case <-time.After(time.Duration(500) * time.Millisecond):
				if info.Position < len(rebroadcastQueue) {
					rebroadQueueMu.Lock()
					msg := &rebroadcastMsg{Type: "rebroadcast", Messages: rebroadcastQueue[info.Position:]}
					info.Position = len(rebroadcastQueue)
					rebroadQueueMu.Unlock()
					msg.rebroadcast(info.Dest)
				}
			}
		}
	}()
}

// rebroadcast keeps trying to broadcast our message to the destination.
func (msg *rebroadcastMsg) rebroadcast(dest string) {
	go func() {
		// normally I'd have some retry limit here but for the purposes of these tests not at the moment
		for {
			// some arbitrary timeout to get a response to our message back
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			_, err := node.SyncRPC(ctx, dest, msg)
			cancel() // call cancel so the linter doesn't yell at me
			if err == nil {
				// how long did this call take, keep a running average of our delay, obviously this would change if
				// we were getting timeouts, but I'm cheating for now and not including them.

				// Note, I ended up not using this.  We can use it to dynamically set the delay we wait for messages
				// to accumulate for each node, but since a lot of nodes will have a small delay this really ups the
				// messages for op, so its better to pass the challenge to just use a static delay, but I thought the
				// idea was cool so leaving it :)
				nodeInfoMu.Lock()
				info := nodeInfoLookup[dest]
				info.Count++
				deadline, _ := ctx.Deadline()
				info.Delay += deadline.Sub(time.Now()).Milliseconds()
				info.Delay /= info.Count
				nodeInfoLookup[dest] = info
				nodeInfoMu.Unlock()

				// no error we are done with this message
				return
			}
		}
	}()
}
