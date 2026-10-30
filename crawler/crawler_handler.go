package crawler

import (
	"context"
	"encoding/hex"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/DMzmwh/bing-dhtcrawler/dht"
	"github.com/DMzmwh/bing-dhtcrawler/storage"
)

/*
过滤节点
*/
func (bc *BingCrawler) filterNodes(addres []*dht.Node) []*dht.Node {
	var unknown []*dht.Node
	for _, item := range addres {
		if !bc.bingData.HasNode(item) {
			unknown = append(unknown, item)
		}
	}
	return unknown
}

func (bc *BingCrawler) runDiscoveredNodes(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case nodes := <-bc.discoveredNodes.Out():
			unknown := bc.filterNodes(nodes)
			nodeMap := make(map[string]int, len(unknown))
			for _, node := range unknown {
				ipStr := node.Addr.Addr().String()
				if _, ok := nodeMap[ipStr]; ok {
					continue
				}
				nodeMap[ipStr] = 1
				select {
				case <-ctx.Done():
					return
				case bc.nodesForFindNode.In() <- node:
				case bc.nodesForSampleInfoHashes.In() <- node:
				case bc.nodesForPing.In() <- node:
				}
			}
		}
	}
}
func (bc *BingCrawler) runPing(ctx context.Context) {
	bc.nodesForPing.Run(ctx, func(node *dht.Node) {
		if node.LastAtTime().After(time.Now().Add(-bc.oldPeerThreshold)) {
			return
		}
		nodeId, err := bc.dht.Ping(node)
		if err != nil {
			bc.bingData.RemoveNode(node)
			return
		}
		if node.ID.IsZero() || node.ID != nodeId {
			node.ID = nodeId
		}
		bc.bingData.PutNode(node, bc)
	})
}
func (bc *BingCrawler) runFindNode(ctx context.Context) {
	bc.nodesForFindNode.Run(ctx, func(node *dht.Node) {
		res, err := bc.dht.FindNode(node, bc.soughtNodeID.Get())
		if err != nil {
			bc.bingData.RemoveNode(node)
			return
		}
		node.LastAt()
		for _, item := range res {
			select {
			case <-ctx.Done():
				return
			case bc.discoveredNodes.In() <- &item:
				continue
			}
		}
	})
}

func (bc *BingCrawler) runNodesForFindNode(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		nodes := bc.bingData.GetOldestNodes(time.Now().Add(-(5 * time.Second)), 10)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		for _, node := range nodes {
			bc.nodesForFindNode.In() <- node
		}
	}
}

func (bc *BingCrawler) runSampleInfoHashes(ctx context.Context) {
	bc.nodesForSampleInfoHashes.Run(ctx, func(node *dht.Node) {
		if !node.IsSampleInfoHashesCheck() {
			return
		}
		res, err := bc.dht.SampleInfoHashes(ctx, node, bc.soughtNodeID.Get())
		bep51 := 1
		if err != nil {
			if errors.Is(err, dht.ErrMethodUnknown) {
				bep51 = 2 // 2 不支持
			} else {
				bc.bingData.RemoveNode(node)
				return
			}
		}
		if res.Interval > 0 {
			node.NextSampleTime(res.Interval)
		}
		node.Bep51(bep51)
		if res.Samples != nil {
			var discoveredHashes []NodeHasPeersForHash
			for _, hash := range res.Samples {
				if !bc.ignoreHashes.testAndAdd(hash) {
					discoveredHashes = append(discoveredHashes, NodeHasPeersForHash{
						infoHash: hash,
						node:     node,
					})
				}
			}
			for _, hash := range discoveredHashes {
				select {
				case <-ctx.Done():
					return
				case bc.infoHashTriage.In() <- hash:
				}
			}
		}
		if len(res.Nodes) > 0 {
			for _, item := range res.Nodes {
				select {
				case <-ctx.Done():
					return
				case bc.discoveredNodes.In() <- &item:
				}
			}
		}
	})
}

func (bc *BingCrawler) runNodesForSampleInfoHashes(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		nodes := bc.bingData.GetNodesForSampleInfoHashes(60)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		for _, node := range nodes {
			select {
			case <-ctx.Done():
				return
			case bc.nodesForSampleInfoHashes.In() <- node:
			}
		}
	}
}

func (bc *BingCrawler) runInfoHashTriage(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case hashs := <-bc.infoHashTriage.Out():
			if len(hashs) == 0 {
				return
			}
			hashMapById := make(map[dht.InfoHash]NodeHasPeersForHash)
			result := make([]dht.InfoHash, 0)
			for _, hash := range hashs {
				if _, ok := hashMapById[hash.infoHash]; ok {
					continue
				}
				hashMapById[hash.infoHash] = hash
				result = append(result, hash.infoHash)
			}
			for _, hash := range result {
				bc.getPeers.In() <- hashMapById[hash]
			}
			//此处还可添加其他过滤逻辑
			infos, err := bc.storage.GetTriageInfo(ctx, result)
			if err != nil {
				bc.logger.Error("get triage info error", err)
				return
			}
			for key, info := range infos {
				select {
				case <-ctx.Done():
					return
				default:

				}
				if !info.Exists || !info.HasMetadata {
					bc.getPeers.In() <- hashMapById[key]
					//判断是否需要获取元数据
					continue
				}
				//if info.Leechers == 0 || info.Seeders == 0 || info.UpdatedAt.Before(time.Now().Add(-bc.rescrapeThreshold)) {
				//	bc.scrape.In() <- hashMapById[key]
				//	continue
				//}
			}
		}
	}
}
func (bc *BingCrawler) runGetHashInfoPeers(ctx context.Context) {
	bc.getPeers.Run(ctx, func(hash NodeHasPeersForHash) {
		node := hash.node
		res, err := bc.dht.GetPeers(ctx, node, hash.infoHash, false)
		if err != nil {
			bc.bingData.RemoveNode(node)
			return
		}
		node.LastAt()
		if len(res.Nodes) > 0 {
			for _, n := range res.Nodes {
				select {
				case <-ctx.Done():
					return
				case bc.discoveredNodes.In() <- &n:
					continue
				}
			}
		}
		select {
		case <-ctx.Done():
			return
		case bc.requestMetaInfo.In() <- InfoHashWithPeers{
			NodeHasPeersForHash: hash,
			peers:               res.Peers}:
			return
		}
	})
}

func (bc *BingCrawler) runRequestMetaInfo(ctx context.Context) {
	bc.requestMetaInfo.Run(ctx, func(hash InfoHashWithPeers) {
		var errs []error

		errsMutex := sync.Mutex{}
		addErr := func(err error) {
			errsMutex.Lock()
			errs = append(errs, err)
			errsMutex.Unlock()
		}
		for _, peer := range hash.peers {
			res, err := bc.metainfoRequester.Request(ctx, hash.infoHash, peer)
			if err != nil {
				addErr(err)
				continue
			}
			select {
			case <-ctx.Done():
			case bc.persistTorrents.In() <- InfoHashWithMetaInfo{
				NodeHasPeersForHash: hash.NodeHasPeersForHash,
				metaInfo:            res.Info,
				peer:                peer}:
			}
		}
	})
}

func (bc *BingCrawler) runScrape(ctx context.Context) {
	bc.scrape.Run(ctx, func(hash NodeHasPeersForHash) {
		res, err := bc.dht.GetPeers(ctx, hash.node, hash.infoHash, true)
		if err != nil {
			bc.bingData.RemoveNode(hash.node)
		}
		hash.node.LastAt()
		if len(res.Nodes) > 0 {
			for _, n := range res.Nodes {
				select {
				case <-ctx.Done():
					return
				case bc.discoveredNodes.In() <- &n:
					continue
				}
			}
		}

		select {
		case <-ctx.Done():
		case bc.persistSources.In() <- InfoHashWithScrape{
			NodeHasPeersForHash: hash,
			bfsd:                res.BfSeeders,
			bfpe:                res.BfPeers,
		}:
		}
	})
}

func (bc *BingCrawler) reseedBootstrapNodes(ctx context.Context) {
	interval := time.Duration(0)
	timer := time.NewTimer(interval)
	defer func() {
		// 安全停止定时器，排空通道避免泄漏goroutine
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			for _, strAddr := range bc.bootstrapNodes {
				addr, err := net.ResolveUDPAddr("udp", strAddr)
				if err != nil {
					bc.logger.Error("failed to resolve bootstrap node address: %s", err)
					continue
				}
				node := dht.Node{
					ID:   dht.NodeID{},
					Addr: addr.AddrPort(),
				}
				select {
				case <-ctx.Done():
					return
				case bc.nodesForPing.In() <- &node:
					continue
				}
			}
		}

		interval = bc.reseedBootstrapNodesInterval
		timer.Reset(interval)
	}
}

// runPersistTorrents 把抓取到的种子元数据转换成可序列化记录交给 Storage，
// 并把对应的 InfoHash 继续投递给 scrape 队列。
func (c *BingCrawler) runPersistTorrents(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		case items, ok := <-c.persistTorrents.Out():
			if !ok {
				return
			}
			if len(items) == 0 {
				continue
			}

			seen := make(map[dht.InfoHash]struct{}, len(items))
			records := make([]any, 0, len(items))
			torrents := make([]InfoHashWithMetaInfo, 0, len(items))

			for _, item := range items {
				if _, exists := seen[item.infoHash]; exists {
					continue
				}
				seen[item.infoHash] = struct{}{}
				records = append(records, createTorrentRecord(item, c.saveFilesThreshold))
				torrents = append(torrents, item)
			}

			if len(records) == 0 {
				continue
			}

			if err := c.storage.SaveTorrents(ctx, records); err != nil {
				c.logger.Error("error persisting torrents", "err", err, "count", len(records))
				continue
			}

			c.logger.Info("persisted torrents", "count", len(records))

			for _, item := range torrents {
				select {
				case <-ctx.Done():
					return
				case c.scrape.In() <- item.NodeHasPeersForHash:
				}
			}
		}
	}
}

// createTorrentRecord 把 metainfo 转换成 Storage 可序列化的记录。
// 文件数量超过 saveFilesThreshold 时截断，FileCount 仍保留真实数量。
func createTorrentRecord(item InfoHashWithMetaInfo, saveFilesThreshold uint) storage.TorrentRecord {
	info := item.metaInfo

	record := storage.TorrentRecord{
		InfoHash:     hex.EncodeToString(item.infoHash[:]),
		Name:         info.BestName(),
		Size:         info.TotalLength(),
		PieceLength:  info.PieceLength,
		FileCount:    len(info.Files),
		DiscoveredAt: time.Now(),
	}
	if info.Private != nil {
		record.Private = *info.Private
	}
	if item.node != nil {
		record.NodeID = hex.EncodeToString(item.node.ID[:])
		record.NodeAddr = item.node.Addr.String()
	}
	if item.peer.IsValid() {
		record.PeerAddr = item.peer.String()
	}

	limit := len(info.Files)
	if saveFilesThreshold > 0 && limit > int(saveFilesThreshold) {
		limit = int(saveFilesThreshold)
	}
	for i := 0; i < limit; i++ {
		file := info.Files[i]
		record.Files = append(record.Files, storage.TorrentFileRecord{
			Path: file.DisplayPath(&info),
			Size: file.Length,
		})
	}

	return record
}

// runPersistSources 把 DHT scrape 结果转换成可序列化记录交给 Storage。
func (c *BingCrawler) runPersistSources(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		case scrapes, ok := <-c.persistSources.Out():
			if !ok {
				return
			}
			if len(scrapes) == 0 {
				continue
			}

			seen := make(map[dht.InfoHash]struct{}, len(scrapes))
			records := make([]any, 0, len(scrapes))

			for _, item := range scrapes {
				if _, exists := seen[item.infoHash]; exists {
					continue
				}
				seen[item.infoHash] = struct{}{}
				records = append(records, createSourceRecord(item))
			}

			if len(records) == 0 {
				continue
			}

			if err := c.storage.SaveSources(ctx, records); err != nil {
				c.logger.Error("error persisting torrent sources", "err", err, "count", len(records))
				continue
			}

			c.logger.Info("persisted torrent sources", "count", len(records))
		}
	}
}

// createSourceRecord 把 scrape Bloom Filter 估算出的 Seeder / Leecher 数量转成记录。
func createSourceRecord(item InfoHashWithScrape) storage.SourceRecord {
	record := storage.SourceRecord{
		InfoHash:  hex.EncodeToString(item.infoHash[:]),
		UpdatedAt: time.Now(),
	}
	if item.node != nil {
		record.NodeAddr = item.node.Addr.String()
	}
	if item.bfsd != nil {
		record.Seeders = uint(max(0, int(item.bfsd.EstimateCount())))
	}
	if item.bfpe != nil {
		record.Leechers = uint(max(0, int(item.bfpe.EstimateCount())))
	}

	return record
}
