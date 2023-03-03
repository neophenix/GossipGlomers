package handlers

import (
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

// NodeBcastQueue contains a queue and necessary items to communicate messages to other nodes
// simple and we will expand it later
type nodeBcastQueue struct {
	Dest        string
	Mutex       sync.Mutex
	LastSent    int
	LastConfirm int
	MsgIDtoVal  map[int]int
	Messages    []broadcastMsg
}

// bcastQueues is a map linking a node to its queue
var bcastQueues = map[string]*nodeBcastQueue{}

// Broadcast handles the broadcast message type and appends values into our slice
func Broadcast(msg maelstrom.Message) error {
	var body broadcastMsg
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	// lock our slice and map access
	bcastMutex.Lock()
	defer bcastMutex.Unlock()

	// propogate our message to other nodes if this is the first time we've seen it
	if !bcastSeen[body.Message] {
		for _, nodeid := range topology[node.ID()] {
			// enqueue the message to broadcast but don't send it back to a node that just sent it to us
			if msg.Src != nodeid {
				bcastQueues[nodeid].Enqueue(body)
			}
		}

		// and since this is the first time seeing the message record that fact
		bcastSeen[body.Message] = true
		bcastMessages = append(bcastMessages, body.Message)
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

	// setup broadcast queues for our neighbor nodes
	for _, nodeid := range body.Topology[node.ID()] {
		bcastQueues[nodeid] = &nodeBcastQueue{Dest: nodeid, LastSent: -1, LastConfirm: -1}
		bcastQueues[nodeid].Drain()
	}

	// set our response
	body.Type = "topology_ok"
	body.Topology = nil
	// send our messages back
	return node.Reply(msg, body)
}

// Enqueue add a message to our queue msg slice
func (nbq *nodeBcastQueue) Enqueue(msg broadcastMsg) {
	nbq.Mutex.Lock()
	defer nbq.Mutex.Unlock()
	nbq.Messages = append(nbq.Messages, msg)
}

// Drain is called for each queue and left to do its thing.  It will check to see if it should send the next message
// and if so send it via the rpc call by way of the PrepSend function.  If it has nothing to send will sleep momentarily
// and then try again
func (nbq *nodeBcastQueue) Drain() {
	go func() {
		// wait for messages to start
		for len(nbq.Messages) == 0 {
			time.Sleep(10 * time.Millisecond)
		}
		for {
			if nbq.LastSent == nbq.LastConfirm {
				// we are ready to send the next message
				nbq.LastSent++
			} else {
				// we need to resend the last message, but going to wait first
				time.Sleep(50 * time.Millisecond)
			}
			// we moved our sent, so as long as we now have a message to send, send it
			if len(nbq.Messages) > nbq.LastSent {
				nbq.Mutex.Lock()
				node.RPC(nbq.Dest, nbq.Messages[nbq.LastSent], nbq.PrepSend())
				nbq.Mutex.Unlock()
			}
		}
	}()
}

// PrepSend wraps our RPC call in a closure so we know what value we sent out to see if we need to confirm it.  Doing
// this since I don't think I have a way to get the msg id when we send so I'm not sure I can use the reply to know
// which value or message it is confirming
func (nbq *nodeBcastQueue) PrepSend() func(msg maelstrom.Message) error {
	// we already locked this when we called the function, so it should be safe
	val := nbq.Messages[nbq.LastSent].Message
	return func(msg maelstrom.Message) error {
		nbq.Mutex.Lock()
		defer nbq.Mutex.Unlock()
		// if this is a confirmation of the last message we sent move the confirmation forward, if not ignore it
		if len(nbq.Messages) > nbq.LastSent && nbq.Messages[nbq.LastSent].Message == val {
			nbq.LastConfirm++
		}
		return nil
	}
}
