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

func NewRtmpServerConfig() RtmpServerConfig {
	res := RtmpServerConfig{
		RtmpHost:      "",
		RtmpPort:      1935,
		PublishPrefix: "/(?P<app>.*)/(?P<incoming_stream_name>[^?]*)",
	}
	res.publishRegexp = regexp.MustCompile(res.PublishPrefix)

	return res
}
