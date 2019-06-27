package noop_api

import (
	"github.com/BurntSushi/toml"
	"github.com/VKCOM/kive/ktypes"
	"github.com/VKCOM/kive/worker"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"io/ioutil"
)

type NoopApi struct {
}

func (na *NoopApi) OnPublish(streamName, appName string, params map[string]string) (ktypes.Stream, error) {
	//parse and check stream signature
	//send to blocking task queue 'streams' [owner_id, user_id]
	//error on bad signature/parsing
	return &noopStream{
		incomingStreamName: streamName,
	}, nil
}

func (na *NoopApi) AllowView(streamName, salt string) (isAllowed bool) {
	return true
}

type noopStream struct {
	incomingStreamName string
}

func (nas *noopStream) StreamName() string {
	return nas.incomingStreamName
}

func (nas *noopStream) NotifyStreaming(si ktypes.StreamInfo) {
}

func (nas *noopStream) AllowStreaming() (isAllowed bool) {
	return true
}

func (vks *noopStream) Disconnect() {
}

func (na *NoopApi) Stat(isError bool, event string, context string, extra string) {
}

func (na *NoopApi) Serve() error {
	return nil
}

func (na *NoopApi) GetTranscoder() (ktypes.Abr, error) {
	return nil, errors.New("Not implemented")
}

func (na *NoopApi) ReadConfig(configPath string, configInterface interface{}) (interface{}, error) {
	if configPath == worker.DEFAULT_CONFIG {
		return configInterface, nil
	}

	c := configInterface.(worker.Config)

	if configPath == worker.DEV_CONFIG {
		c.LiveHlsConfig.HttpPort = 8085
		c.RtmpServerConfig.RtmpPort = 1935
		c.KfsConfig.Basedir = "/tmp/kive_ts/"
		return c, nil
	}

	configData, err := ioutil.ReadFile(configPath)
	if err != nil {
		return c, errors.Wrapf(err, "Bad config file %+v", configPath)
	}

	logrus.Infof("Config data %+v", string(configData))

	if meta, err := toml.DecodeFile(configPath, &c); err != nil || len(meta.Undecoded()) != 0 {
		if len(meta.Undecoded()) != 0 {
			logrus.Errorf("Cannot apply %v: ", meta.Undecoded())
		}
		return c, errors.Wrap(err, "cannot decode config")
	}
	return c, nil
}
