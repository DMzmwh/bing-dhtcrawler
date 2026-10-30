package crawler

import (
	"sync"
	"time"

	"github.com/DMzmwh/bing-dhtcrawler/crawler/metrics"
	"github.com/DMzmwh/bing-dhtcrawler/dht"
)

type BingData struct {
	mu sync.RWMutex
	// InfoHash -> Peers
	PeersMapByInfoHash map[dht.InfoHash][]*dht.Node
	// IP -> InfoHashes
	InfoHashMapByNode map[*dht.Node][]dht.InfoHash
}

func NewBingData() *BingData {
	return &BingData{
		PeersMapByInfoHash: make(map[dht.InfoHash][]*dht.Node),
		InfoHashMapByNode:  make(map[*dht.Node][]dht.InfoHash),
	}
}

// GetPeersByInfoHash 获取 InfoHash 对应的所有 Peer。
func (d *BingData) GetPeersByInfoHash(hash dht.InfoHash) []*dht.Node {
	d.mu.RLock()
	defer d.mu.RUnlock()

	peers := d.PeersMapByInfoHash[hash]
	if len(peers) == 0 {
		return nil
	}

	result := make([]*dht.Node, len(peers))
	copy(result, peers)
	return result
}

// PutNode 保存一个新发现的 Node。
// Node 第一次出现时，即使还没有关联 InfoHash，也会创建 IP -> []InfoHash。
func (d *BingData) PutNode(node *dht.Node, bc *BingCrawler) {
	if node == nil {
		return
	}

	//ip := node.Addr.Addr().String()

	d.mu.Lock()
	defer d.mu.Unlock()
	node.LastAt()
	if _, exists := d.InfoHashMapByNode[node]; exists {
		return
	}
	bc.metrics.Inc(metrics.NodesTotal)
	d.InfoHashMapByNode[node] = nil
	return
}

// RemoveNode 删除 Node，同时删除该 Node 对应的所有 InfoHash 关系。
func (d *BingData) RemoveNode(node *dht.Node) {
	if node == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	addr := node.Addr
	//ip := addr.Addr().String()

	hashes := d.InfoHashMapByNode[node]
	if len(hashes) == 0 {
		return
	}
	for _, hash := range hashes {
		peers := d.PeersMapByInfoHash[hash]
		result := peers[:0]
		for _, peer := range peers {
			if peer == nil {
				continue
			}

			if peer.Addr != addr {
				result = append(result, peer)
			}
		}
		if len(result) == 0 {
			delete(d.PeersMapByInfoHash, hash)
		} else {
			d.PeersMapByInfoHash[hash] = result
		}
	}
	delete(d.InfoHashMapByNode, node)
}

// PutHash 添加 InfoHash 对应的 Peers，同时维护 InfoHash 和 IP 的双向关系。
func (d *BingData) PutHash(hash dht.InfoHash, peers []dht.Node) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	existing := d.PeersMapByInfoHash[hash]

	peerSet := make(map[string]struct{}, len(existing)+len(peers))
	for _, peer := range existing {
		if peer == nil {
			continue
		}
		peerSet[peer.Addr.Addr().String()] = struct{}{}
	}

	changed := false

	for i := range peers {
		peer := &peers[i]
		addr := peer.Addr

		if !addr.IsValid() {
			continue
		}

		if _, exists := peerSet[addr.Addr().String()]; exists {
			continue
		}

		peerSet[addr.Addr().String()] = struct{}{}
		existing = append(existing, peer)
		changed = true

		//ip := addr.Addr().String()
		hashes := d.InfoHashMapByNode[peer]

		if !containsHash(hashes, hash) {
			d.InfoHashMapByNode[peer] = append(hashes, hash)
		}
	}

	if len(existing) > 0 {
		d.PeersMapByInfoHash[hash] = existing
	}
	return changed
}

func containsHash(hashes []dht.InfoHash, target dht.InfoHash) bool {
	for _, hash := range hashes {
		if hash == target {
			return true
		}
	}
	return false
}

func (d *BingData) GetOldestNodes(before time.Time, limit int) []*dht.Node {
	d.mu.RLock()
	defer d.mu.RUnlock()

	unique := make(map[dht.NodeID]*dht.Node)
	for node, _ := range d.InfoHashMapByNode {
		if node.LastAtTime().Before(before) {
			unique[node.ID] = node
			if len(unique) >= limit {
				break
			}
		}
	}
	nodes := make([]*dht.Node, 0, len(unique))
	for _, node := range unique {
		nodes = append(nodes, node)
	}
	return nodes
}

// GetNodesForSampleInfoHashes 获取用于执行Sample的 Node 列表。
func (d *BingData) GetNodesForSampleInfoHashes(limit int) []*dht.Node {
	d.mu.RLock()
	defer d.mu.RUnlock()
	now := time.Now()
	nodes := make([]*dht.Node, 0, limit)
	for _, peers := range d.PeersMapByInfoHash {
		for _, node := range peers {
			if node == nil {
				continue
			}
			if node.NextSampleTimeTime().After(now) {
				continue
			}
			nodes = append(nodes, node)
			if len(nodes) >= limit {
				return nodes
			}
		}
	}

	return nodes
}
func (bd *BingData) HasNode(node *dht.Node) bool {
	bd.mu.RLock()
	defer bd.mu.RUnlock()

	_, ok := bd.InfoHashMapByNode[node]
	return ok
}
