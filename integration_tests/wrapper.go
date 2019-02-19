package integration_tests

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/VKCOM/kive/ktypes"
	"github.com/VKCOM/kive/noop_api"
	"github.com/VKCOM/kive/worker"
	"github.com/otiai10/curr"
	"github.com/phayes/freeport"
	"github.com/stretchr/testify/assert"
	"github.com/xfrr/goffmpeg/transcoder"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

var (
	VK_SYNC_MP4 = GetAsset("vk_sync.mp4")
)

var (
	FFROBE_CHECK = "-v quiet -show_entries stream=nb_read_frames -of default=nokey=1:noprint_wrappers=1 -count_frames %s"
)

type Wrapper struct {
	W *worker.Worker
	C worker.Config
}

func NewWrapper(t *testing.T) *Wrapper {
	ktypes.ApiInst = &noop_api.NoopApi{}

	dir, err := ioutil.TempDir("", "kive_it_")
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	t.Log(dir)
	c := worker.NewConfig(worker.DEFAULT_CONFIG)
	c.KfsConfig.Basedir = dir
	c.RtmpServerConfig.RtmpPort, err = freeport.GetFreePort()
	assert.NoError(t, err)
	t.Log(c.RtmpServerConfig.RtmpPort)

	c.LiveHlsConfig.HttpPort, err = freeport.GetFreePort()
	assert.NoError(t, err)
	t.Log(c.LiveHlsConfig.HttpPort)

	w, err := worker.NewWorker(c)
	assert.NoError(t, err)
	return &Wrapper{
		W: w,
		C: c,
	}
}

func GetAsset(n string) string {
	return filepath.Clean(curr.Dir() + "/../test_assets/" + n)
}

func BuildOutput(w *Wrapper, app string, streamName string) string {
	out := fmt.Sprintf("rtmp://localhost:%d/%s/%s", w.W.Config.RtmpServerConfig.RtmpPort, app, streamName)
	return out
}

func BuildInput(w *Wrapper, app string, streamName string) string {
	out := fmt.Sprintf("http://localhost:%d/%s/%s/playlist.m3u8", w.W.Config.LiveHlsConfig.HttpPort, app, streamName)
	return out
}

func NewStreamer(t *testing.T, in string, out string) *transcoder.Transcoder {
	trans := new(transcoder.Transcoder)
	err := trans.Initialize(in, out)
	assert.NoError(t, err)

	trans.MediaFile().SetAudioCodec("copy")
	trans.MediaFile().SetVideoCodec("copy")
	trans.MediaFile().SetOutputFormat("flv")
	trans.MediaFile().SetNativeFramerateInput(true)
	trans.MediaFile().SetCopyTs(true)

	return trans
}

func NewViewer(t *testing.T, in string, out string) *transcoder.Transcoder {
	trans := new(transcoder.Transcoder)
	err := trans.Initialize(in, out)
	assert.NoError(t, err)

	trans.MediaFile().SetAudioCodec("copy")
	trans.MediaFile().SetVideoCodec("copy")
	trans.MediaFile().SetOutputFormat("mp4")
	trans.MediaFile().SetCopyTs(true)

	return trans
}

func AssertSameSmoke(t *testing.T, l string, r string) bool {
	_, err := os.Stat(l)
	assert.NoError(t, err)

	_, err = os.Stat(r)
	assert.NoError(t, err)

	//transL := new(transcoder.Transcoder)
	//err = transL.Initialize(l, "-")
	//assert.NoError(t, err)
	//
	//transR := new(transcoder.Transcoder)
	//err = transR.Initialize(r, "-")
	//assert.NoError(t, err)
	//

	assert.Equal(t, frameCount(t, l), frameCount(t, r))

	return true
}

func frameCount(t *testing.T, f string) []int {
	cmd := exec.Command("ffprobe", strings.Split(fmt.Sprintf(FFROBE_CHECK, f), " ")...)
	out, err := cmd.CombinedOutput()
	assert.NoError(t, err)
	res := make([]int, 0, 0)
	scanner := bufio.NewScanner(bytes.NewReader(out))
	for scanner.Scan() {
		r, err := strconv.ParseInt(scanner.Text(), 10, 32)
		assert.NoError(t, err)
		res = append(res, int(r))
	}
	t.Log(f, res)

	return res
}
