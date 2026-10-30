# bing-play 集成

`bing-play` 不复制 crawler 源码，只依赖：

```go
import dhtcrawler "github.com/bing/bing-dhtcrawler"
```

开发阶段可以：

```go
replace github.com/bing/bing-dhtcrawler => ../bing-dhtcrawler
```

建议 bing-play 只消费：
- Config
- Result
- Stats
- New
- Start
- Close
- Results

crawler 内部的 DHT、Kademlia、Bloom、BEP9、BEP33 不暴露给业务层。

典型数据流：

DHT -> InfoHash -> GetPeers/Scrape -> BEP9 Metadata -> Result -> bing-play ResourceService -> SQLite/torrentmgr
