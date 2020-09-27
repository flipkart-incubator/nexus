package stats

import (
	"io"
	"time"

	"github.com/smira/go-statsd"
)

type Tag struct {
	key, val string
}

func NewTag(key, val string) Tag {
	return Tag{key, val}
}

type Client interface {
	io.Closer
	Incr(string, int64)
	Gauge(string, int64)
	GaugeDelta(string, int64)
	StartTiming() int64
	EndTiming(string, int64)
}

type noopClient struct{}

func (*noopClient) Incr(_ string, _ int64)       {}
func (*noopClient) Gauge(_ string, _ int64)      {}
func (*noopClient) GaugeDelta(_ string, _ int64) {}
func (*noopClient) StartTiming() int64           { return 0 }
func (*noopClient) EndTiming(_ string, _ int64)  {}
func (*noopClient) Close() error                 { return nil }

func NewNoOpClient() *noopClient {
	return &noopClient{}
}

type statsDClient struct {
	cli *statsd.Client
}

func NewStatsDClient(statsdAddr, metricPrfx string, defTags ...Tag) *statsDClient {
	statsTags := make([]statsd.Tag, len(defTags))
	for i, defTag := range defTags {
		statsTags[i] = statsd.StringTag(defTag.key, defTag.val)
	}
	return &statsDClient{
		statsd.NewClient(
			statsdAddr,
			statsd.TagStyle(statsd.TagFormatDatadog),
			statsd.MetricPrefix(metricPrfx),
			statsd.DefaultTags(statsTags...)),
	}
}

func (sdc *statsDClient) Incr(name string, value int64) {
	sdc.cli.Incr(name, value)
}

func (sdc *statsDClient) Gauge(name string, value int64) {
	sdc.cli.Gauge(name, value)
}

func (sdc *statsDClient) GaugeDelta(name string, value int64) {
	sdc.cli.GaugeDelta(name, value)
}

func (sdc *statsDClient) StartTiming() int64 {
	return time.Now().UnixNano() / 1e6
}

func (sdc *statsDClient) EndTiming(name string, startTime int64) {
	endTime := time.Now().UnixNano() / 1e6
	sdc.cli.Timing(name, endTime-startTime)
}

func (sdc *statsDClient) Close() error {
	return sdc.cli.Close()
}
