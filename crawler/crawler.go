package crawler

import (
	"context"
	"log/slog"
	"net/netip"
	"sync"
	"time"

	"github.com/DMzmwh/bing-dhtcrawler/concurrency"
	"github.com/DMzmwh/bing-dhtcrawler/crawler/metrics"
	"github.com/DMzmwh/bing-dhtcrawler/dht"
	"github.com/DMzmwh/bing-dhtcrawler/metadata"
	"github.com/DMzmwh/bing-dhtcrawler/storage"
	"github.com/anacrolix/dht/v2/krpc"
	"github.com/anacrolix/torrent/metainfo"
	boom "github.com/tylertreat/BoomFilters"
)

type BingCrawler struct {
	//DHT 客户端
	dht dht.Client
	//指标
	metrics *metrics.Metrics
	//DHT 客户端
	bingData *BingData
	// Metadata 请求器
	metainfoRequester metadata.Requester
	// Block 规则管理
	//blockingManager blocking.Manager
	// InfoHash 后续处理判断
	//triage Triage
	// Bootstrap 节点
	bootstrapNodes []string
	// Bootstrap 重连间隔
	reseedBootstrapNodesInterval time.Duration
	// 获取旧节点间隔
	getOldestNodesInterval time.Duration
	// 旧节点判断阈值
	oldPeerThreshold time.Duration
	// 新发现节点队列
	discoveredNodes concurrency.BatchingChannel[*dht.Node]
	// Ping 节点队列
	nodesForPing concurrency.BufferedConcurrentChannel[*dht.Node]
	// FindNode 节点队列
	nodesForFindNode concurrency.BufferedConcurrentChannel[*dht.Node]
	// SampleInfoHashes 节点队列
	nodesForSampleInfoHashes concurrency.BufferedConcurrentChannel[*dht.Node]
	// InfoHash 检查队列
	infoHashTriage concurrency.BatchingChannel[NodeHasPeersForHash]
	// GetPeers 队列
	getPeers concurrency.BufferedConcurrentChannel[NodeHasPeersForHash]
	// Scrape 队列
	scrape concurrency.BufferedConcurrentChannel[NodeHasPeersForHash]
	// Metadata 请求队列
	requestMetaInfo concurrency.BufferedConcurrentChannel[InfoHashWithPeers]
	// Torrent 持久化队列
	persistTorrents concurrency.BatchingChannel[InfoHashWithMetaInfo]
	// Source 持久化队列
	persistSources concurrency.BatchingChannel[InfoHashWithScrape]
	// Scrape 重新执行间隔
	rescrapeThreshold time.Duration
	// 保存文件信息的阈值
	saveFilesThreshold uint
	// 是否保存 Pieces
	savePieces bool
	// 数据持久化
	storage storage.Storage
	// 已处理 InfoHash 的 Bloom Filter
	ignoreHashes *IgnoreHashes
	// 随机 FindNode/SampleInfoHashes 目标 ID
	soughtNodeID concurrency.AtomicValue[dht.NodeID]
	// 日志
	logger *slog.Logger
	// 防止 Start 重复执行
	startOnce sync.Once
}

func NewBingCrawler(
	dhtClient dht.Client,
	metrics *metrics.Metrics,
	bingData *BingData,
	metainfoRequester metadata.Requester,
	//blockingManager blocking.Manager,
	//triage Triage,
	storage storage.Storage,
	ignoreHashes *IgnoreHashes,
	logger *slog.Logger,
) *BingCrawler {
	return &BingCrawler{
		dht:               dhtClient,
		metrics:           metrics,
		bingData:          bingData,
		metainfoRequester: metainfoRequester,
		//blockingManager:   blockingManager,
		//triage:       triage,
		storage:      storage,
		ignoreHashes: ignoreHashes,
		logger:       logger,

		bootstrapNodes: []string{
			"router.bittorrent.com:6881",
			"dht.transmissionbt.com:6881",
			"router.utorrent.com:6881",
		},

		reseedBootstrapNodesInterval: 30 * time.Minute,
		getOldestNodesInterval:       1 * time.Second,
		oldPeerThreshold:             5 * time.Second,
		rescrapeThreshold:            30 * time.Minute,

		saveFilesThreshold: 100,
		savePieces:         false,

		discoveredNodes: concurrency.NewBatchingChannel[*dht.Node](
			1000,
			80,
			10*time.Millisecond,
		),

		nodesForPing: concurrency.NewBufferedConcurrentChannel[*dht.Node](
			1000,
			32,
		),

		nodesForFindNode: concurrency.NewBufferedConcurrentChannel[*dht.Node](
			1000,
			32,
		),

		nodesForSampleInfoHashes: concurrency.NewBufferedConcurrentChannel[*dht.Node](
			1000,
			32,
		),

		infoHashTriage: concurrency.NewBatchingChannel[NodeHasPeersForHash](
			1000,
			100,
			10*time.Millisecond,
		),

		getPeers: concurrency.NewBufferedConcurrentChannel[NodeHasPeersForHash](
			1000,
			32,
		),

		scrape: concurrency.NewBufferedConcurrentChannel[NodeHasPeersForHash](
			1000,
			32,
		),

		requestMetaInfo: concurrency.NewBufferedConcurrentChannel[InfoHashWithPeers](
			1000,
			16,
		),

		persistTorrents: concurrency.NewBatchingChannel[InfoHashWithMetaInfo](
			1000,
			100,
			10*time.Millisecond,
		),

		persistSources: concurrency.NewBatchingChannel[InfoHashWithScrape](
			1000,
			100,
			10*time.Millisecond,
		),
	}
}

func (c *BingCrawler) Start(ctx context.Context) {
	c.startOnce.Do(func() {
		// 轮换 FindNode/SampleInfoHashes 的目标 ID
		go c.generateSoughtNodeID(ctx)
		// 处理新发现节点
		go c.runDiscoveredNodes(ctx)
		// Ping 节点
		go c.runPing(ctx)
		// FindNode
		go c.runFindNode(ctx)
		go c.runNodesForFindNode(ctx)
		// SampleInfoHashes
		go c.runSampleInfoHashes(ctx)
		go c.runNodesForSampleInfoHashes(ctx)
		// InfoHash 判断
		go c.runInfoHashTriage(ctx)
		// 获取 Peer
		go c.runGetHashInfoPeers(ctx)
		// 获取 Metadata
		go c.runRequestMetaInfo(ctx)
		// Scrape
		go c.runScrape(ctx)
		// Bootstrap 重连
		go c.reseedBootstrapNodes(ctx)
		// 持久化
		go c.runPersistTorrents(ctx)
		go c.runPersistSources(ctx)
		go c.runMerics(ctx)
	})
}

type NodeHasPeersForHash struct {
	infoHash dht.InfoHash
	node     *dht.Node
}

type InfoHashWithMetaInfo struct {
	NodeHasPeersForHash
	metaInfo metainfo.Info
	// peers 是 get_peers 得到的候选对等节点地址集合（已去重），
	// 可用于下载时直连。
	peers []netip.AddrPort
}

type InfoHashWithPeers struct {
	NodeHasPeersForHash
	peers []netip.AddrPort
}

type InfoHashWithScrape struct {
	NodeHasPeersForHash
	bfsd *krpc.ScrapeBloomFilter
	bfpe *krpc.ScrapeBloomFilter
}

type IgnoreHashes struct {
	mutex sync.Mutex
	bloom *boom.StableBloomFilter
}

func NewIgnoreHashes(bloom *boom.StableBloomFilter) *IgnoreHashes {
	return &IgnoreHashes{
		bloom: bloom,
	}
}

func (i *IgnoreHashes) testAndAdd(id dht.InfoHash) bool {
	i.mutex.Lock()
	defer i.mutex.Unlock()

	return i.bloom.TestAndAdd(id[:])
}

func (c *BingCrawler) generateSoughtNodeID(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(10 * time.Second):
			c.soughtNodeID.Set(dht.RandomNodeID())
		}
	}
}

func (c *BingCrawler) runMerics(ctx context.Context) {
	c.metrics.Print()
	ticker := time.NewTicker(2 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.metrics.Print()
		case <-ctx.Done():
			return
		}
	}
}
