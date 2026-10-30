package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	anacrolix "github.com/anacrolix/dht/v2"
	"github.com/DMzmwh/bing-dhtcrawler/crawler"
	metrics2 "github.com/DMzmwh/bing-dhtcrawler/crawler/metrics"
	"github.com/DMzmwh/bing-dhtcrawler/dht"
	"github.com/DMzmwh/bing-dhtcrawler/metadata"
	"github.com/DMzmwh/bing-dhtcrawler/storage"
	boom "github.com/tylertreat/BoomFilters"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	dhtClient, err := newDHTClient()
	if err != nil {
		logger.Error("failed to create dht client", "error", err)
		os.Exit(1)
	}
	defer dhtClient.Close()

	metadataRequester := metadata.NewRequester(
		dht.RandomNodeID(),
		10*time.Second,
	)

	metrics, _ := metrics2.InitMetrics()
	bingData := crawler.NewBingData()

	storage := storage.NewDefaultStorage()

	ignoreHashes := crawler.NewIgnoreHashes(boom.NewDefaultStableBloomFilter(1000000, 0.01))

	c := crawler.NewBingCrawler(
		dhtClient,
		metrics,
		bingData,
		metadataRequester,
		storage,
		ignoreHashes,
		logger,
	)

	c.Start(ctx)

	logger.Info("bing-dhtcrawler started")

	<-ctx.Done()

	logger.Info("stopping bing-dhtcrawler")

	shutdownCtx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	_ = shutdownCtx

	logger.Info("bing-dhtcrawler stopped")
}

func newDHTClient() (dht.Client, error) {
	config := anacrolix.NewDefaultServerConfig()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{
		IP:   net.IPv4zero,
		Port: 51413,
	})
	if err != nil {
		return nil, err
	}
	config.Conn = conn
	dhtClient, err := dht.NewAnacrolixClient(config)
	if err != nil {
		return nil, err
	}
	return dhtClient, nil
}
