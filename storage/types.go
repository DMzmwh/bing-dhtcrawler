package storage

import "time"

// TorrentFileRecord 种子内的单个文件信息。
type TorrentFileRecord struct {
	Path string `json:"path"`
	Size int64  `json:"size"`
}

// TorrentRecord 爬虫通过 BEP9 抓取到的种子元数据。
// 业务侧的 Storage 实现负责把它序列化落盘。
type TorrentRecord struct {
	InfoHash     string              `json:"info_hash"`
	Name         string              `json:"name"`
	Size         int64               `json:"size"`
	PieceLength  int64               `json:"piece_length"`
	Private      bool                `json:"private"`
	FileCount    int                 `json:"file_count"`
	Files        []TorrentFileRecord `json:"files,omitempty"`
	NodeID       string              `json:"node_id"`
	NodeAddr     string              `json:"node_addr"`
	DiscoveredAt time.Time           `json:"discovered_at"`
}

// SourceRecord DHT scrape 得到的来源统计。
type SourceRecord struct {
	InfoHash  string    `json:"info_hash"`
	NodeAddr  string    `json:"node_addr"`
	Seeders   uint      `json:"seeders"`
	Leechers  uint      `json:"leechers"`
	UpdatedAt time.Time `json:"updated_at"`
}
