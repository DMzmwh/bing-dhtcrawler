package crawler

import (
	"context"

	"github.com/bing/bing-dhtcrawler/dht"
)

type Triage interface {
	Triage(ctx context.Context, hash dht.InfoHash) (TriageResult, error)
}

type TriageResult struct {
	NeedGetPeers bool
	NeedMetadata bool
	NeedScrape   bool
}
