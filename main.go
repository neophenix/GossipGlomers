package main

import (
	"log"

	"github.com/neophenix/GossipGlomers/handlers"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	// broke out handlers into their own package to let this grow and now look like a mess all in main
	// this sets up all the handlers we support as well as anything else we might need to do
	handlers.HandleThings(n)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
