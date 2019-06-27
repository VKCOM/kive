package worker

import (
	"github.com/VKCOM/joy4/format/rtmp"
	"github.com/sirupsen/logrus"
	"github.com/VKCOM/kive/hls_server"
	"github.com/VKCOM/kive/kfs"
	"github.com/VKCOM/kive/ktypes"
	"github.com/VKCOM/kive/rtmp_server"
)

const (
	DEFAULT_CONFIG = "default"
	TESTING_CONFIG = "testing"
	DEV_CONFIG     = "development"
)

type Config struct {
	LogLevel         string
	RtmpDebug        bool
	KfsConfig        kfs.KfsConfig
	LiveHlsConfig    hls_server.LiveHlsConfig
	RtmpServerConfig rtmp_server.RtmpServerConfig
	FfmpegBinary     string
}

func NewConfig(configPath string) Config {
	logrus.Infof("Starting with config path %+s", configPath)
	config := Config{
		LogLevel:         "debug",
		KfsConfig:        kfs.NewKfsConfig(),
		LiveHlsConfig:    hls_server.NewLiveHlsConfig(),
		RtmpServerConfig: rtmp_server.NewRtmpServerConfig(),
	}

	configInterface, err := ktypes.ApiInst.ReadConfig(configPath, config)

	if err != nil {
		logrus.Panicf("Cannot init config %+v", err)
	}

	config = configInterface.(Config)
	if config.RtmpDebug {
		rtmp.Debug = true
	}

	switch config.LogLevel {
	case "info":
		logrus.SetLevel(logrus.InfoLevel)
	case "debug":
		logrus.SetLevel(logrus.DebugLevel)
	default:
		logrus.Panicf("Bad log level: %s:", config.LogLevel)
	}
	logrus.Infof("Final config: %+v ", config)

	return config
}
