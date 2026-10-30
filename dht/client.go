package dht

import (
	"context"
	crand "crypto/rand"
	"net/netip"

	"github.com/anacrolix/dht/v2/krpc"
)

type NodeID [20]byte
type InfoHash [20]byte

type Client interface {
	Bootstrap(ctx context.Context) error
	Ping(node *Node) (NodeID, error)
	FindNode(node *Node, target NodeID) ([]Node, error)
	GetPeers(ctx context.Context, node *Node, infoHash InfoHash, scrape bool) (GetPeersResult, error)
	SampleInfoHashes(ctx context.Context, node *Node, target NodeID) (SampleInfoHashesResult, error)
	Nodes() []Node
	AddNode(node *Node) error
	RemoveNode(node *Node)
	Close()
}

const idClientPart = "-Bing"

func RandomNodeID() (id NodeID) {
	_, _ = crand.Read(id[:])
	return
}

// RandomNodeIDWithClientSuffix generates a node ID for the DHT client.
// We use a random byte string with the client ID encoded at the end,
// to allow identifying other bitmagnet instances in the wild.
// A suffix is used instead of a prefix, which would be incompatible with DHT,
// where ID prefixes are used for computing the distance metric).
func RandomNodeIDWithClientSuffix() (id NodeID) {
	_, _ = crand.Read(id[:])
	for i := range len(idClientPart) {
		id[20-len(idClientPart)+i] = idClientPart[i]
	}
	return
}

func (id NodeID) IsZero() bool {
	return id == [20]byte{}
}

type GetPeersResult struct {
	Nodes     []Node
	Peers     []netip.AddrPort
	Token     string
	BfPeers   *krpc.ScrapeBloomFilter
	BfSeeders *krpc.ScrapeBloomFilter
}

type SampleInfoHashesResult struct {
	Nodes    []Node
	Samples  []InfoHash
	Interval int64
	Num      int64
}

type HashPeer struct {
	Addr netip.AddrPort
}
