package kfs

import (
	"github.com/pkg/errors"
	"github.com/VKCOM/kive/ktypes"
	"io"
	"time"
)

type Writer struct {
	io.WriteCloser
	item *bufferItem
	fs   *Filesystem
}

func (w *Writer) Write(data []byte) (int, error) {
	return w.item.queue.Write(data)
}

func (w *Writer) SetChunkDuration(dur time.Duration) {
	w.fs.m.Lock()
	defer w.fs.m.Unlock()
	w.item.resultKey.Duration = dur
}

func (w *Writer) Close() error {
	start := time.Now()
	err := w.fs.finalizeWriter(w.item)
	t := time.Now()
	elapsed := t.Sub(start)
	ktypes.Stat(err != nil, "disk_write", "timing", ktypes.TimeToStat(elapsed))

	if err != nil {
		return errors.Wrapf(err, "cannot finalize writer %+v", w.item.resultKey)
	}
	return nil
}
