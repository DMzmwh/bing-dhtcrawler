package metrics

import (
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type Type int

const (
	CounterType Type = iota
	GaugeType
	HistogramType
)

type Options struct {
	Name    string
	Help    string
	Type    Type
	Buckets []float64
}

type item struct {
	typ    Type
	metric prometheus.Collector
}

type Metrics struct {
	registry  *prometheus.Registry
	startTime time.Time
	items     sync.Map
}

func New() *Metrics {
	return &Metrics{
		startTime: time.Now(),
		registry:  prometheus.NewRegistry(),
	}
}

func (m *Metrics) Registry() *prometheus.Registry {
	return m.registry
}

func (m *Metrics) Create(opts Options) error {
	if opts.Name == "" {
		return fmt.Errorf("metric name is empty")
	}
	if opts.Help == "" {
		return fmt.Errorf("metric help is empty: %s", opts.Name)
	}

	var metric prometheus.Collector

	switch opts.Type {
	case CounterType:
		metric = prometheus.NewCounter(prometheus.CounterOpts{
			Name: opts.Name,
			Help: opts.Help,
		})
	case GaugeType:
		metric = prometheus.NewGauge(prometheus.GaugeOpts{
			Name: opts.Name,
			Help: opts.Help,
		})
	case HistogramType:
		metric = prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    opts.Name,
			Help:    opts.Help,
			Buckets: opts.Buckets,
		})
	default:
		return fmt.Errorf("unsupported metric type: %d", opts.Type)
	}

	if err := m.registry.Register(metric); err != nil {
		return fmt.Errorf("register metric %q: %w", opts.Name, err)
	}

	m.items.Store(opts.Name, item{
		typ:    opts.Type,
		metric: metric,
	})

	return nil
}

func (m *Metrics) Inc(name string) error {
	item, err := m.get(name)
	if err != nil {
		return err
	}

	switch metric := item.metric.(type) {
	case prometheus.Counter:
		metric.Inc()
	case prometheus.Gauge:
		metric.Inc()
	default:
		return fmt.Errorf("metric %q does not support Inc", name)
	}
	return nil
}

func (m *Metrics) Add(name string, value float64) error {
	item, err := m.get(name)
	if err != nil {
		return err
	}

	switch metric := item.metric.(type) {
	case prometheus.Counter:
		if value < 0 {
			return fmt.Errorf("counter metric %q cannot add negative value: %v", name, value)
		}
		metric.Add(value)
	case prometheus.Gauge:
		metric.Add(value)
	default:
		return fmt.Errorf("metric %q does not support Add", name)
	}

	return nil
}

func (m *Metrics) Dec(name string) error {
	item, err := m.get(name)
	if err != nil {
		return err
	}

	switch metric := item.metric.(type) {
	case prometheus.Gauge:
		metric.Dec()
	default:
		return fmt.Errorf("metric %q does not support Dec", name)
	}

	return nil
}

func (m *Metrics) Sub(name string, value float64) error {
	item, err := m.get(name)
	if err != nil {
		return err
	}

	switch metric := item.metric.(type) {
	case prometheus.Gauge:
		metric.Sub(value)
	default:
		return fmt.Errorf("metric %q does not support Sub", name)
	}

	return nil
}

func (m *Metrics) Set(name string, value float64) error {
	item, err := m.get(name)
	if err != nil {
		return err
	}

	switch metric := item.metric.(type) {
	case prometheus.Gauge:
		metric.Set(value)
	default:
		return fmt.Errorf("metric %q does not support Set", name)
	}

	return nil
}

func (m *Metrics) Observe(name string, value float64) error {
	item, err := m.get(name)
	if err != nil {
		return err
	}

	switch metric := item.metric.(type) {
	case prometheus.Histogram:
		metric.Observe(value)
	default:
		return fmt.Errorf("metric %q does not support Observe", name)
	}

	return nil
}

func (m *Metrics) get(name string) (item, error) {
	value, ok := m.items.Load(name)
	if !ok {
		return item{}, fmt.Errorf("metric %q not found", name)
	}

	return value.(item), nil
}

func (m *Metrics) Print() error {
	families, err := m.registry.Gather()
	if err != nil {
		return err
	}

	for _, family := range families {
		fmt.Printf(
			"%s [%s]\n",
			family.GetName(),
			family.GetType().String(),
		)

		for _, metric := range family.GetMetric() {
			if metric.Counter != nil {
				fmt.Printf("  value: %v\n", metric.Counter.GetValue())
			}

			if metric.Gauge != nil {
				fmt.Printf("  value: %v\n", metric.Gauge.GetValue())
			}

			if metric.Histogram != nil {
				fmt.Printf("  count: %d\n", metric.Histogram.GetSampleCount())
				fmt.Printf("  sum:   %v\n", metric.Histogram.GetSampleSum())
			}
		}
	}

	return nil
}
