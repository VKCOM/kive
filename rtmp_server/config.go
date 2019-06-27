package rtmp_server

import (
	"regexp"
)

type RtmpServerConfig struct {
	RtmpHost      string
	RtmpPort      int
	PublishPrefix string
	publishRegexp *regexp.Regexp
}

func (rs *RtmpServerConfig) Prepare() {
	rs.publishRegexp = regexp.MustCompile(rs.PublishPrefix)
}

func NewRtmpServerConfig() RtmpServerConfig {
	res := RtmpServerConfig{
		RtmpHost: "",
		RtmpPort: 1935,

		PublishPrefix: "/(?P<app>.*)/(?P<incoming_stream_name>[^?]*)",
	}
	res.Prepare()

	return res
}
