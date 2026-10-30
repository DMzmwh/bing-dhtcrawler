package dht

import (
	"context"
	"errors"
	"net"
	"net/netip"

	anacrolix "github.com/anacrolix/dht/v2"
	"github.com/anacrolix/dht/v2/int160"
	"github.com/anacrolix/dht/v2/krpc"
)

var ErrMethodUnknown = errors.New("method unknown")

type AnacrolixClient struct {
	server *anacrolix.Server
}

func NewAnacrolixClient(config *anacrolix.ServerConfig) (*AnacrolixClient, error) {
	if config == nil {
		config = anacrolix.NewDefaultServerConfig()
	}

	server, err := anacrolix.NewServer(config)
	if err != nil {
		return nil, err
	}

	return &AnacrolixClient{
		server: server,
	}, nil
}

func (c *AnacrolixClient) Bootstrap(ctx context.Context) error {
	_, err := c.server.BootstrapContext(ctx)
	return err
}

func (c *AnacrolixClient) Ping(node *Node) (NodeID, error) {
	result := c.server.Ping(toUDPAddr(node.Addr))
	if err := result.ToError(); err != nil {
		return node.ID, err
	}

	nodeID := result.Reply.SenderID()
	if nodeID == nil {
		return node.ID, errors.New("ping response missing node id")
	}
	return NodeID(*nodeID), nil
}

func (c *AnacrolixClient) FindNode(node *Node, target NodeID) ([]Node, error) {
	result := c.server.FindNode(
		anacrolix.NewAddr(toUDPAddr(node.Addr)),
		int160.FromByteArray(target),
		anacrolix.QueryRateLimiting{},
	)
	if err := result.ToError(); err != nil {
		return nil, err
	}

	return nodesFromReturn(result.Reply.R), nil
}

func (c *AnacrolixClient) GetPeers(
	ctx context.Context,
	node *Node,
	infoHash InfoHash,
	scrape bool,
) (GetPeersResult, error) {
	result := c.server.GetPeers(
		ctx,
		anacrolix.NewAddr(toUDPAddr(node.Addr)),
		int160.FromByteArray(infoHash),
		scrape,
		anacrolix.QueryRateLimiting{},
	)
	if err := result.ToError(); err != nil {
		return GetPeersResult{}, err
	}

	ret := result.Reply.R
	if ret == nil {
		return GetPeersResult{}, nil
	}

	return GetPeersResult{
		Nodes:     nodesFromReturn(ret),
		Peers:     peersFromReturn(ret.Values),
		Token:     stringValue(ret.Token),
		BfSeeders: ret.BFsd,
		BfPeers:   ret.BFpe,
	}, nil
}

func (c *AnacrolixClient) SampleInfoHashes(
	ctx context.Context,
	node *Node,
	target NodeID,
) (SampleInfoHashesResult, error) {
	result := c.server.Query(
		ctx,
		anacrolix.NewAddr(toUDPAddr(node.Addr)),
		"sample_infohashes",
		anacrolix.QueryInput{
			MsgArgs: krpc.MsgArgs{
				Target: krpc.ID(target),
			},
		},
	)
	if err := result.ToError(); err != nil {
		var krpcErr *krpc.Error
		if errors.As(err, &krpcErr) && krpcErr.Code == 204 {
			return SampleInfoHashesResult{}, ErrMethodUnknown
		}
		return SampleInfoHashesResult{}, err
	}

	ret := result.Reply.R
	if ret == nil {
		return SampleInfoHashesResult{}, nil
	}

	resultValue := SampleInfoHashesResult{
		Nodes: nodesFromReturn(ret),
	}

	if ret.Samples != nil {
		resultValue.Samples = make([]InfoHash, 0, len(*ret.Samples))
		for _, hash := range *ret.Samples {
			resultValue.Samples = append(resultValue.Samples, InfoHash(hash))
		}
	}
	if ret.Interval != nil && *ret.Interval != 0 {
		resultValue.Interval = *ret.Interval
	}
	if ret.Num != nil && *ret.Num != 0 {
		resultValue.Num = *ret.Num
	}

	return resultValue, nil
}

func (c *AnacrolixClient) Nodes() []Node {
	items := c.server.Nodes()
	result := make([]Node, 0, len(items))

	for _, item := range items {
		node, ok := nodeFromInfo(item)
		if ok {
			result = append(result, node)
		}
	}

	return result
}

func (c *AnacrolixClient) AddNode(node *Node) error {
	return c.server.AddNode(krpc.NodeInfo{
		ID:   krpc.ID(node.ID),
		Addr: toNodeAddr(node.Addr),
	})
}

func (c *AnacrolixClient) RemoveNode(node *Node) {
	// anacrolix/dht v2.24.0 没有公开 RemoveNode API。
}

func (c *AnacrolixClient) Close() {
	c.server.Close()
}

func nodesFromReturn(ret *krpc.Return) []Node {
	if ret == nil {
		return nil
	}

	result := make([]Node, 0, len(ret.Nodes)+len(ret.Nodes6))

	for _, item := range ret.Nodes {
		if node, ok := nodeFromInfo(item); ok {
			result = append(result, node)
		}
	}

	for _, item := range ret.Nodes6 {
		if node, ok := nodeFromInfo(item); ok {
			result = append(result, node)
		}
	}

	return result
}

func nodeFromInfo(info krpc.NodeInfo) (Node, bool) {
	addr := info.Addr.ToNodeAddrPort().AddrPort
	if !addr.IsValid() || info.ID.IsZero() {
		return Node{}, false
	}

	return Node{
		ID:   NodeID(info.ID),
		Addr: addr,
	}, true
}

func peersFromReturn(peers []krpc.NodeAddr) []netip.AddrPort {
	result := make([]netip.AddrPort, 0, len(peers))

	for _, peer := range peers {
		addr := peer.ToNodeAddrPort().AddrPort
		if addr.IsValid() {
			result = append(result, addr)
		}
	}

	return result
}

func toNodeAddr(addr netip.AddrPort) krpc.NodeAddr {
	var result krpc.NodeAddr
	result.FromAddrPort(addr)
	return result
}

func toUDPAddr(addr netip.AddrPort) *net.UDPAddr {
	return &net.UDPAddr{
		IP:   addr.Addr().AsSlice(),
		Port: int(addr.Port()),
	}
}

func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

var _ Client = (*AnacrolixClient)(nil)
