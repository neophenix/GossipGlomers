package handlers

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// addMsg is our g-counter add message type
type addMsg struct {
	Type  string `json:"type"`
	Delta int    `json:"delta"`
}

// readGrowMsg is our read message request / response type for the g-counter tests
type readGrowMsg struct {
	Type  string `json:"type"`
	Value int    `json:"value"`
}

// nodeKV is our maelstrom key value store
var nodeKV *maelstrom.KV

// checkpoints is a counter of checkpoints we insert into the kv store so we have some known point in time
var checkpoints atomic.Int64

// ReadGrow responds with the hopefully current value of our kv store counter.
func ReadGrow(msg maelstrom.Message) error {
	// add a checkpoint so we know once we hit it we are up to date
	cp := AddCheckpoint()

	kvcp := 0
	// while we have a checkpoint to look up keep reading the kv store until we reach it
	for cp != 0 && int64(kvcp) < cp {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		kvcp, _ = nodeKV.ReadInt(ctx, "checkpoint-"+node.ID())
		cancel()
		// just some small sleep so we don't fully spin on doing this
		time.Sleep(10 * time.Millisecond)
	}

	// now we should be certain we are up to date
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	val, err := nodeKV.ReadInt(ctx, "counter")
	if err != nil {
		return err
	}

	return node.Reply(msg, readGrowMsg{Type: "read_ok", Value: val})
}

// Add increments our kv store counter
func Add(msg maelstrom.Message) error {
	var body addMsg
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	// try to get the value first, if this errors its more than likely that is doesn't exist so the CaS will set it
	// anyway, we could check the error and swap then, but I thought we will be running these in multiple goroutines
	// so wanted to loop comparing so we know we set out value correctly
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	val, _ := nodeKV.ReadInt(ctx, "counter")
	for {
		ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
		// try to swap in our new value, creating it if need be
		err := nodeKV.CompareAndSwap(ctx, "counter", val, val+body.Delta, true)
		cancel()
		if err != nil {
			// if we couldn't swap we are probably out of date, so grab the latest value and try again
			ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
			val, _ = nodeKV.ReadInt(ctx, "counter")
			cancel()
		} else {
			// we swapped, we are done
			break
		}
	}

	return node.Reply(msg, map[string]any{"type": "add_ok"})
}

// AddCheckpoint takes our local counter and updates a node specific value with it.  Then we can read it back, and if
// our read value is less than what we just checkpointed, we know we are still in the past and need to keep reading.
// I did not come up with this on my own, was getting what I thought were odd results and was looking at the maelstrom
// bug reports.  https://github.com/jepsen-io/maelstrom/issues/39#issuecomment-1445414521 the second paragraph there I
// stopped to think about what he was saying and this was my take on it.
func AddCheckpoint() int64 {
	cp := checkpoints.Add(1)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	nodeKV.Write(ctx, "checkpoint-"+node.ID(), cp)
	return cp
}
