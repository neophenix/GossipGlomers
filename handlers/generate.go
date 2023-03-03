package handlers

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	NIDBITLEN = 10 // node id bit length
	SEQBITLEN = 12 // sequence bit length
)

// Generate is the unique-ids test handler
func Generate(msg maelstrom.Message) error {
	// Unmarshal the message body as an loosely-typed map.
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	// Update the message type to return back.
	body["type"] = "generate_ok"
	// normally I'd just use a UUID here but that felt like cheating
	id := genUniqueID()
	// convert our ID to a string so that the uint -> any json marshalling doesn't kill us when it uses a float64
	body["id"] = fmt.Sprintf("%d", id)

	// return our id to the sender
	return node.Reply(msg, body)
}

// genMutex ensures only one thread is generating an id at a time
var genMutex sync.Mutex

// sequence is our node specific counter, we will only use 12 bits of it
var sequence int64

// genUniqueID generates a unique ID for this node.  It returns a uint64 comprised of:
// 41 bits               + 10 bits + 12 bits
// millisecond timestamp + node id + sequence value
// yes that only uses up 63 and we are using a uint, but this is so great it will need to run forever so room to grow
func genUniqueID() uint64 {
	// lock / unlock here instead of in the sequence function since I think in theory we could get our TS + node
	// and then starve getting the sequence, and could get the same one as another thread returning the same ID
	genMutex.Lock()
	defer genMutex.Unlock()
	var id uint64
	now := time.Now().UnixMilli()

	id = uint64(now) << (NIDBITLEN + SEQBITLEN) // move our timestamp to the front of the id
	id |= uint64(nodeID) << SEQBITLEN           // plug in our node id next
	id |= uint64(getSequenceInt())              // end with our sequence
	return id
}

// getSequenceInt handles the sequence rollover and waiting if we do so pulled it out here
func getSequenceInt() int64 {
	sequence++
	// we overflowed our 12 bits so reset, decided to start at 1 instead of 0
	if sequence == 4096 {
		// sleep to get to ensure the next timestamp value
		time.Sleep(1 * time.Millisecond)
		sequence = 1
	}
	return sequence
}
