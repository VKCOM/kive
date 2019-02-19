package ktypes

import (
	"fmt"
	"time"
)

type StreamInfo struct {
	Width          int
	Height         int
	ChunkStartTime UnixMs
}

var (
	ApiInst Api
)

type Api interface {
	OnPublish(streamName, appName string, params map[string]string) (Stream, error)
	AllowView(streamName, salt string) (isAllowed bool)
	Stat(isError bool, event string, context string, extra string)
	ReadConfig(configPath string, configInterface interface{}) (interface{}, error)
	GetTranscoder() (Abr, error)
	Serve() error
}

type Stream interface {
	StreamName() string
	NotifyStreaming(StreamInfo)
	AllowStreaming() (isAllowed bool)
	Disconnect()
}

func TimeToStat(dt time.Duration) string {
	ms := 100 * int64(dt/(time.Millisecond*100))
	return fmt.Sprintf("%d", ms)
}

func Stat(isError bool, event string, context string, extra string) {
	ApiInst.Stat(isError, event, context, extra)
}
