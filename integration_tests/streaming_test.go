package integration_tests

import (
	"fmt"
	"github.com/VKCOM/kive/ktypes"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"testing"
	//"os"
	"time"
)

func init() {
	ktypes.Recover = false
}

func Test_StreamRunSmoke(t *testing.T) {
	w := NewWrapper(t)
	err := w.W.Listen()
	assert.NoError(t, err)

	err = w.W.Serve()
	assert.NoError(t, err)
	defer w.W.Stop()

	trans := NewStreamer(t, VK_SYNC_MP4, BuildOutput(w, "live", "123"))
	trans.MediaFile().SetDuration("2")
	done := trans.Run(false)
	err = <-done
	assert.NoError(t, err)
}

func Test_StreamRunSeveral(t *testing.T) {
	w := NewWrapper(t)
	err := w.W.Listen()
	assert.NoError(t, err)

	err = w.W.Serve()
	assert.NoError(t, err)
	defer w.W.Stop()

	wait := make([]<-chan error, 0, 0)
	for i := 0; i < 3; i++ {
		trans := NewStreamer(t, VK_SYNC_MP4, BuildOutput(w, "live", fmt.Sprintf("stream%d", i)))
		trans.MediaFile().SetDuration("15")
		done := trans.Run(false)
		wait = append(wait, done)
	}
	for _, c := range wait {
		err = <-c
		assert.NoError(t, err)
	}
}

func Test_StreamAndRecordLive(t *testing.T) {
	tempOut, err := ioutil.TempFile("", "kive_mp4_out_")
	assert.NoError(t, err)
	//defer os.Remove(tempOut.Name())
	t.Log(tempOut.Name())

	w := NewWrapper(t)
	err = w.W.Listen()
	assert.NoError(t, err)

	err = w.W.Serve()
	assert.NoError(t, err)
	defer w.W.Stop()

	transStream := NewStreamer(t, VK_SYNC_MP4, BuildOutput(w, "live", "123"))
	doneStream := transStream.Run(false)
	time.Sleep(5 * time.Second)

	transView := NewViewer(t, BuildInput(w, "live", "123"), tempOut.Name())
	doneView := transView.Run(false)

	err = <-doneView
	assert.NoError(t, err)

	err = <-doneStream
	assert.NoError(t, err)
	AssertSameSmoke(t, tempOut.Name(), VK_SYNC_MP4)
}
