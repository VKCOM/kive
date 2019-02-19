package rtmp_server

import (
	"fmt"
	"net"

	"github.com/VKCOM/joy4/format/flv"
	"github.com/VKCOM/joy4/format/rtmp"
	"github.com/VKCOM/kive/ktypes"
	"github.com/VKCOM/kive/vsync"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"runtime/debug"
	"time"
)

func init() {
	flv.MaxProbePacketCount = 500 //long wait for audio and video
	rtmp.MaxChunkSize = 10 * 1024 * 1024
}

type RtmpServer struct {
	config        RtmpServerConfig
	server        *rtmp.Server
	rtmpListener  *net.TCPListener
	HandlePublish func(*PublishRequest) error
	rtmpMutex     *vsync.Semaphore
	streamMap     *vsync.CheckedMap
}

type DeadLineConn struct {
	net.Conn
	extend time.Duration
}

func (dlc *DeadLineConn) Read(b []byte) (n int, err error) {
	dlc.SetDeadline(time.Now().Add(dlc.extend))
	return dlc.Conn.Read(b)
}

func (dlc *DeadLineConn) Write(b []byte) (n int, err error) {
	dlc.SetDeadline(time.Now().Add(dlc.extend))
	return dlc.Conn.Write(b)
}

func NewRtmpServer(config RtmpServerConfig) (*RtmpServer, error) {
	flv.MaxProbePacketCount = 500 //long wait for audio and video
	rtmp.MaxChunkSize = 10 * 1024 * 1024

	rts := &RtmpServer{
		config:    config,
		rtmpMutex: vsync.NewSemaphore(500, 200),
		streamMap: vsync.NewCheckedMap(),
	}

	rtmpConnCreate := func(netconn net.Conn) *rtmp.Conn {
		logrus.Debugf("Connection created %+v", netconn.RemoteAddr())
		ktypes.Stat(false, "connection", "created", "")

		netconn.SetDeadline(time.Now().Add(1 * time.Minute))
		conn := rtmp.NewConn(&DeadLineConn{Conn: netconn, extend: 40 * time.Second})
		conn.Prober.HasVideo = true
		conn.Prober.HasAudio = true
		return conn
	}

	rtmpServer := &rtmp.Server{
		Addr:       fmt.Sprintf("%s:%d", rts.config.RtmpHost, rts.config.RtmpPort),
		CreateConn: rtmpConnCreate,
	}

	rtmpServer.HandlePublish = func(conn *rtmp.Conn) {
		defer func() {
			if ktypes.Recover == false {
				return
			}
			if r := recover(); r != nil {
				logrus.Errorf("%s: %s", r, debug.Stack())
			}
		}()
		logrus.Debugf("Connection publish %+v", conn.NetConn().RemoteAddr())
		ktypes.Stat(false, "connection", "publish", "")

		defer ktypes.Stat(false, "connection", "close", "")
		defer conn.Close()
		logrus.Infof(conn.URL.RequestURI())

		publishRequest, err := rts.parsePublishRequest(conn.URL.RequestURI())
		if err != nil {
			logrus.Errorf(" %+v", err)
			return
		}
		publishRequest.Data = conn
		streamHandler, err := ktypes.ApiInst.OnPublish(publishRequest.IncomingStreamName, publishRequest.Application, publishRequest.Params)
		if err != nil {
			logrus.WithField("stream_name", publishRequest.StreamName).Errorf("Cannot publish %+v", err)
			return
		}
		defer streamHandler.Disconnect()

		lockSuccess := rts.streamMap.Lock(streamHandler.StreamName(), true)
		if !lockSuccess {
			logrus.WithField("stream_name", publishRequest.StreamName).Errorf("stream already running %+v", err)
			return
		}
		defer rts.streamMap.Unlock(streamHandler.StreamName())

		if !streamHandler.AllowStreaming() {
			logrus.WithField("stream_name", publishRequest.StreamName).Errorf("Streaming disabled %+v", publishRequest)
			return
		}

		if !rts.rtmpMutex.TryLock(8 * time.Second) {
			ktypes.Stat(false, "connection", "timedout", "")
			return
		}
		defer rts.rtmpMutex.Unlock()

		publishRequest.StreamHandler = streamHandler
		publishRequest.StreamName = streamHandler.StreamName()

		logrus.WithField("stream_name", publishRequest.StreamName).Infof("Streaming started %+v", publishRequest)
		err = rts.HandlePublish(publishRequest)
		logrus.WithField("stream_name", publishRequest.StreamName).Infof("Streaming stopped %+v, %+v", publishRequest, err)
		if err != nil {
			logrus.WithField("stream_name", publishRequest.StreamName).Errorf(" %+v", err)
			return
		}

	}
	rts.server = rtmpServer

	return rts, nil
}

func (rts *RtmpServer) HealthCheck(duration time.Duration) bool {
	if !rts.rtmpMutex.TryLock(duration) {
		return false
	}
	rts.rtmpMutex.Unlock()
	return true
}
func (rts *RtmpServer) parsePublishUrl(publishUrl string) (map[string]string, error) {
	result := make(map[string]string)
	match := rts.config.publishRegexp.FindStringSubmatch(publishUrl)
	indexes := rts.config.publishRegexp.SubexpNames()
	if len(indexes) != len(match) {
		return nil, errors.Errorf("bad publish request %+v", publishUrl)
	}
	for i, name := range indexes {
		if i != 0 && name != "" {
			result[name] = match[i]
		}
	}
	logrus.Debugf("rtmp paths: %+v", result)
	return result, nil
}

func (rts *RtmpServer) parsePublishRequest(url string) (*PublishRequest, error) {
	result := &PublishRequest{}
	vars, err := rts.parsePublishUrl(url)
	if err != nil {
		return result, errors.Wrap(err, "cannot parse publish url")
	}

	if err := mapstructure.Decode(vars, &result); err != nil {
		return result, errors.Wrap(err, "cannot decode publish url")
	}
	result.Params = vars
	logrus.Debugf("Publish parse %+v", result)
	return result, nil

}

type ListenerDeadLine struct {
	net.Listener
}

func (ldl *ListenerDeadLine) Accept() (net.Conn, error) {
	c, err := ldl.Listener.Accept()
	if err != nil {
		return nil, errors.Wrap(err, "cannot accept")
	}
	err = c.SetDeadline(time.Now().Add(10 * time.Second))
	if err != nil {
		return nil, errors.Wrap(err, "cannot set dedline")
	}
	return c, nil
}

func (rts *RtmpServer) Listen() error {
	listener, err := rts.server.Listen()
	if err != nil {
		errors.Wrap(err, "cannot listen rtmp")
	}
	rts.rtmpListener = listener
	return nil
}

func (rts *RtmpServer) Serve() error {
	err := rts.server.Serve(rts.rtmpListener)
	if err != nil {
		errors.Wrap(err, "cannot server rtmp")
	}
	return nil
}

func (rts *RtmpServer) Stop() error {
	return nil
}
