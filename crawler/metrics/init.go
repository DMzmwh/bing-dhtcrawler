package metrics

func InitMetrics() (*Metrics, error) {
	m := New()

	for _, opts := range []Options{
		{Name: NodesTotal, Help: "当前已知的 DHT 节点数量。", Type: GaugeType},
		{Name: NodesDiscoveredTotal, Help: "累计发现的 DHT 节点数量。", Type: CounterType},
		{Name: NodesRemovedTotal, Help: "累计移除的 DHT 节点数量。", Type: CounterType},
		{Name: InfoHashesTotal, Help: "当前已知的 InfoHash 数量。", Type: GaugeType},
		{Name: InfoHashesDiscoveredTotal, Help: "累计从 DHT 节点获取到的 InfoHash 数量。", Type: CounterType},
		{Name: InfoHashesNewTotal, Help: "累计发现的新 InfoHash 数量（经过 Bloom Filter 去重）。", Type: CounterType},
	} {
		if err := m.Create(opts); err != nil {
			return nil, err
		}
	}

	return m, nil
}
