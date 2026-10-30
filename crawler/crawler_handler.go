package crawler

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/bing/bing-dhtcrawler/dht"
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
				metaInfo:            res.Info}:
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

//
//const classifyBatchSize = 100
//
//// runPersistTorrents persists discovered torrent metadata and forwards hashes to scrape.
//func (c *BingCrawler) runPersistTorrents(ctx context.Context) {
//	for {
//		select {
//		case <-ctx.Done():
//			return
//
//		case items, ok := <-c.persistTorrents.Out():
//			if !ok {
//				return
//			}
//			if len(items) == 0 {
//				continue
//			}
//
//			hashMap := make(map[dht.InfoHash]InfoHashWithMetaInfo, len(items))
//			torrents := make([]model.Torrent, 0, len(items))
//			hashesToClassify := make([]dht.InfoHash, 0, classifyBatchSize)
//
//			flushHashesToClassify := func() {
//				if len(hashesToClassify) == 0 {
//					return
//				}
//
//				if err := c.storage.SaveClassifyJobs(
//					ctx,
//					hashesToClassify,
//					time.Minute,
//				); err != nil {
//					c.logger.Errorf("error saving classify jobs: %s", err)
//				}
//
//				hashesToClassify = make([]dht.InfoHash, 0, classifyBatchSize)
//			}
//
//			for _, item := range items {
//				if _, exists := hashMap[item.infoHash]; exists {
//					continue
//				}
//
//				hashMap[item.infoHash] = item
//
//				torrent, err := createTorrentModel(
//					item.infoHash,
//					item.metaInfo,
//					c.savePieces,
//					c.saveFilesThreshold,
//				)
//				if err != nil {
//					c.logger.Errorf("error creating torrent model: %s", err)
//					continue
//				}
//
//				torrents = append(torrents, torrent)
//				hashesToClassify = append(hashesToClassify, item.infoHash)
//
//				if len(hashesToClassify) >= classifyBatchSize {
//					flushHashesToClassify()
//				}
//			}
//
//			flushHashesToClassify()
//
//			if len(torrents) == 0 {
//				continue
//			}
//
//			if err := c.storage.SaveTorrents(ctx, torrents); err != nil {
//				c.logger.Errorf("error persisting torrents: %s", err)
//				continue
//			}
//
//			c.logger.Debugw(
//				"persisted torrents",
//				"count", len(torrents),
//			)
//
//			for _, item := range hashMap {
//				select {
//				case <-ctx.Done():
//					return
//				case c.scrape.In() <- item.nodeHasPeersForHash:
//				}
//			}
//		}
//	}
//}
//
//// createTorrentModel converts metainfo into the crawler's persistence model.
//func createTorrentModel(
//	hash dht.InfoHash,
//	info metainfo.Info,
//	savePieces bool,
//	saveFilesThreshold uint,
//) (model.Torrent, error) {
//	name := info.BestName()
//
//	private := false
//	if info.Private != nil {
//		private = *info.Private
//	}
//
//	var filesCount *uint
//
//	filesStatus := model.FilesStatusSingle
//	if len(info.Files) > 0 {
//		filesStatus = model.FilesStatusMulti
//		count := uint(len(info.Files))
//		filesCount = &count
//	}
//
//	files := make([]model.TorrentFile, 0, min(int(saveFilesThreshold), len(info.Files)))
//
//	for i, file := range info.Files {
//		if i >= int(saveFilesThreshold) {
//			filesStatus = model.FilesStatusOverThreshold
//			break
//		}
//
//		files = append(files, model.TorrentFile{
//			InfoHash: hash,
//			Index:    uint(i),
//			Path:     file.DisplayPath(&info),
//			Size:     uint(file.Length),
//		})
//	}
//
//	var pieces *model.TorrentPieces
//	if savePieces {
//		pieces = &model.TorrentPieces{
//			InfoHash:    hash,
//			PieceLength: info.PieceLength,
//			Pieces:      info.Pieces,
//		}
//	}
//
//	return model.Torrent{
//		InfoHash:    hash,
//		Name:        name,
//		Size:        uint(info.TotalLength()),
//		Private:     private,
//		Pieces:      pieces,
//		Files:       files,
//		FilesStatus: filesStatus,
//		FilesCount:  filesCount,
//		Sources: []model.TorrentSource{
//			{
//				Source:   "dht",
//				InfoHash: hash,
//			},
//		},
//	}, nil
//}
//
//// runPersistSources persists DHT scrape results.
//func (c *crawler) runPersistSources(ctx context.Context) {
//	for {
//		select {
//		case <-ctx.Done():
//			return
//
//		case scrapes, ok := <-c.persistSources.Out():
//			if !ok {
//				return
//			}
//			if len(scrapes) == 0 {
//				continue
//			}
//
//			hashSet := make(map[dht.InfoHash]struct{}, len(scrapes))
//			sources := make([]model.TorrentSource, 0, len(scrapes))
//
//			for _, scrape := range scrapes {
//				if _, exists := hashSet[scrape.infoHash]; exists {
//					continue
//				}
//				hashSet[scrape.infoHash] = struct{}{}
//
//				source, err := createTorrentSourceModel(scrape)
//				if err != nil {
//					c.logger.Errorf(
//						"error creating torrent source model: %s",
//						err,
//					)
//					continue
//				}
//
//				sources = append(sources, source)
//			}
//
//			if len(sources) == 0 {
//				continue
//			}
//
//			if err := c.storage.SaveSources(ctx, sources); err != nil {
//				c.logger.Errorf(
//					"error persisting torrent sources: %s",
//					err,
//				)
//				continue
//			}
//
//			c.logger.Debugw(
//				"persisted torrent sources",
//				"count", len(sources),
//			)
//		}
//	}
//}
//
//// createTorrentSourceModel converts DHT scrape information into a source model.
//func createTorrentSourceModel(
//	result infoHashWithScrape,
//) (model.TorrentSource, error) {
//	seeders := uint(result.bfsd.ApproximatedSize())
//	leechers := uint(result.bfpe.ApproximatedSize())
//
//	return model.TorrentSource{
//		Source:    "dht",
//		InfoHash:  result.infoHash,
//		Seeders:   &seeders,
//		Leechers:  &leechers,
//		UpdatedAt: time.Now(),
//	}, nil
//}
