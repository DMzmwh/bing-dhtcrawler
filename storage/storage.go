package storage

import (
	"context"
	"time"

	"github.com/DMzmwh/bing-dhtcrawler/dht"
)

type Storage interface {
	SaveTorrents(ctx context.Context, items []any) error
	SaveSources(ctx context.Context, items []any) error
	GetTriageInfo(ctx context.Context, hashes []dht.InfoHash) (map[dht.InfoHash]TriageInfo, error)
}

type TriageInfo struct {
	Exists      bool      // 数据库中是否存在该 InfoHash
	HasMetadata bool      // 是否拥有满足当前保存策略要求的可用 Metadata
	Seeders     uint      // 最近一次 DHT 统计到的 Seeder 数量，nil 表示无统计数据
	Leechers    uint      // 最近一次 DHT 统计到的 Leecher 数量，nil 表示无统计数据
	UpdatedAt   time.Time // DHT 来源数据最后更新时间
}

type DefaultStorage struct{}

func NewDefaultStorage() *DefaultStorage {
	return &DefaultStorage{}
}

func (s *DefaultStorage) SaveTorrents(ctx context.Context, items []any) error {
	return nil
}

func (s *DefaultStorage) SaveSources(ctx context.Context, items []any) error {
	return nil
}

func (s *DefaultStorage) GetTriageInfo(ctx context.Context, hashes []dht.InfoHash) (map[dht.InfoHash]TriageInfo, error) {
	return nil, nil
}

var _ Storage = (*DefaultStorage)(nil)
