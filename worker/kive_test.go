package worker

import (
	"bytes"
	"fmt"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestKiveDvr(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)
	c := NewConfig(TESTING_CONFIG)
	worker, err := NewWorker(c)
	if err != nil {
		panic(err)
	}

	err = worker.Listen()
	if err != nil {
		panic(err)
	}

	err = worker.Serve()
	if err != nil {
		panic(err)
	}

	defer worker.Stop()

	inFile := "t4.mp4"
	outFile := inFile

	streamName := "test123"
	pubUrl := fmt.Sprintf("%s%d%s%s", "rtmp://127.0.0.1:", worker.Config.RtmpServerConfig.RtmpPort, "/live?publishsign=aWQ9SUJwYXVTdGRrTlY0NmtRRCZzaWduPTZiQ0hHMG9wa1U1S0dhMFE5bkhrcFE9PQ==/", streamName)

	from := time.Now().Unix()
	duration := 35
	playUrl := fmt.Sprintf("%s%d/live/%s/playlist_dvr_range-%d-%d.m3u8", "http://127.0.0.1:", worker.Config.LiveHlsConfig.HttpPort, streamName, from, duration)

	equal, err := streamAndCheck(t, inFile, outFile, pubUrl, playUrl)

	if err != nil {
		logrus.Errorf("%+v", err)
	}

	assert.NoError(t, err)
	assert.True(t, equal)

	time.Sleep(time.Second)
}

func TestKiveDvrTranscode(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)
	c := NewConfig(TESTING_CONFIG)
	worker, err := NewWorker(c)
	if err != nil {
		panic(err)
	}

	err = worker.Listen()
	if err != nil {
		panic(err)
	}

	err = worker.Serve()
	if err != nil {
		panic(err)
	}

	defer worker.Stop()

	inFile := "t4.mp4"
	outFile := inFile

	streamName := "test123"
	from := time.Now().Unix()
	duration := 35

	pubUrl := fmt.Sprintf("%s%d%s%s", "rtmp://127.0.0.1:", worker.Config.RtmpServerConfig.RtmpPort, "/abr?publishsign=aWQ9SUJwYXVTdGRrTlY0NmtRRCZzaWduPTZiQ0hHMG9wa1U1S0dhMFE5bkhrcFE9PQ==/", streamName)

	err = stream(t, inFile, pubUrl)
	assert.NoError(t, err)

	testSizes := []string{"1080p", "720p", "480p", "360p", "source"}

	for _, testSize := range testSizes {
		playUrl := fmt.Sprintf("%s%d/liveabr/%s/abr/%s_%s/playlist_dvr_range-%d-%d.m3u8", "http://127.0.0.1:", worker.Config.LiveHlsConfig.HttpPort, streamName, streamName, testSize, from, duration)

		equal, err := check(t, inFile, outFile, playUrl)
		if err != nil {
			logrus.Errorf("%+v", err)
		}

		assert.NoError(t, err)
		assert.True(t, equal)
	}
	time.Sleep(time.Second)
}

func TestKiveLive(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)
	c := NewConfig(TESTING_CONFIG)
	worker, err := NewWorker(c)
	if err != nil {
		panic(err)
	}

	err = worker.Listen()
	if err != nil {
		panic(err)
	}

	err = worker.Serve()
	if err != nil {
		panic(err)
	}

	defer worker.Stop()

	inFile := "t4.mp4"
	outFile := inFile

	streamName := "test123"
	pubUrl := fmt.Sprintf("%s%d%s%s", "rtmp://127.0.0.1:", worker.Config.RtmpServerConfig.RtmpPort, "/live?publishsign=aWQ9SUJwYXVTdGRrTlY0NmtRRCZzaWduPTZiQ0hHMG9wa1U1S0dhMFE5bkhrcFE9PQ==/", streamName)

	playUrl := fmt.Sprintf("%s%d/live/%s/playlist.m3u8", "http://127.0.0.1:", worker.Config.LiveHlsConfig.HttpPort, streamName)

	equal, err := streamAndCheck(t, inFile, outFile, pubUrl, playUrl)

	if err != nil {
		logrus.Errorf("%+v", err)
	}

	assert.NoError(t, err)
	assert.True(t, equal)

	time.Sleep(time.Second)
}

func stream(t *testing.T, inFile, pubUrl string) error {
	inFile = composeInFilename(inFile)
	cmd, err := exec.Command("ffmpeg", "-re", "-i", inFile, "-c", "copy", "-f", "flv", pubUrl).CombinedOutput()
	if err != nil {
		logrus.Fatalf("stream: %s %+v", cmd, err)
		return err
	}
	assert.NoError(t, err)
	return nil
}

func check(t *testing.T, inFile, outFile, playUrl string) (bool, error) {
	inFile = composeInFilename(inFile)
	inMd5, err := exec.Command("ffmpeg", "-y", "-i", inFile, "-c", "copy", "-map", "0:a", "-f", "md5", "-").Output()

	if err != nil {
		logrus.Errorf("%+v", err)
	}
	outFile = composeOutFilename(outFile)

	logrus.Info(playUrl, " : ", outFile)
	cmd, err := exec.Command("ffmpeg", "-y", "-i", playUrl, "-c", "copy", "-f", "mp4", outFile).CombinedOutput()

	if err != nil {
		logrus.Fatalf("%s %+v", cmd, err)
		return false, err
	}

	outMd5, err := exec.Command("ffmpeg", "-y", "-i", outFile, "-c", "copy", "-map", "0:a", "-f", "md5", "-").Output()

	if err != nil {
		logrus.Fatalf("%s %+v", cmd, err)
		return false, err
	}
	return bytes.Equal(inMd5, outMd5), err
}

func streamAndCheck(t *testing.T, inFile, outFile, pubUrl, playUrl string) (bool, error) {
	if err := stream(t, inFile, pubUrl); err != nil {
		return false, err
	}

	return check(t, inFile, outFile, playUrl) //should good compare here, not just audio
}

func composeInFilename(inFile string) string {
	return composeFilename(inFile, "/../test_data/")
}

func composeOutFilename(outFile string) string {
	return composeFilename(outFile, "/../test_result/")
}

func composeFilename(Name, infix string) string {
	_, filename, _, _ := runtime.Caller(0)

	return filepath.Dir(filename) + infix + Name
}
