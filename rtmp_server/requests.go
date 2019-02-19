package rtmp_server

import (
	"github.com/VKCOM/joy4/av"
	"github.com/VKCOM/kive/ktypes"
)

//import (
//	"github.com/mitchellh/mapstructure"
//)

type PublishRequest struct {
	StreamHandler      ktypes.Stream
	Application        string `mapstructure:"app"`
	IncomingStreamName string `mapstructure:"incoming_stream_name"`
	StreamName         string
	Params             map[string]string
	Data               av.DemuxCloser
}
