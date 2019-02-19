package kfs

import (
	"github.com/pkg/errors"
	"io"
	"sync"
)

type queue struct {
	maxSize int
	lock    sync.RWMutex
	cond    *sync.Cond
	buf     []byte
	closed  bool
}

func NewQueue(maxSize int) *queue {
	q := &queue{
		buf:     make([]byte, 0, 1024000),
		maxSize: maxSize,
	}
	q.cond = sync.NewCond(q.lock.RLocker())
	return q
}

func (q *queue) Close() error {
	q.lock.Lock()
	defer q.lock.Unlock()
	q.closed = true
	q.cond.Broadcast()
	return nil
}

func (q *queue) Write(data []byte) (int, error) {
	q.lock.Lock()
	defer q.lock.Unlock()

	q.cond.Broadcast()

	if q.closed {
		return 0, errors.New("queue already closed")
	}

	if len(q.buf) > q.maxSize {
		q.closed = true
		return 0, errors.New("queue too long")
	}

	q.buf = append(q.buf, data...)
	return len(data), nil
}

func (q *queue) Oldest() *queueCursor {
	cursor := &queueCursor{
		que: q,
	}
	return cursor
}

func (q *queue) GetSize() int {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	for {
		buf := q.buf

		if q.closed {
			return len(buf)
		}

		q.cond.Wait()
	}
}

type queueCursor struct {
	que    *queue
	closed bool
	pos    int
}

func (qc *queueCursor) Read(out []byte) (n int, err error) {
	qc.que.cond.L.Lock()
	defer qc.que.cond.L.Unlock()

	for {
		buf := qc.que.buf

		if qc.que.closed && qc.pos == len(buf) {
			err = io.EOF
			return
		}

		if qc.closed {
			err = io.EOF
			return
		}

		if qc.pos < len(buf) {
			n = copy(out, buf[qc.pos:])
			qc.pos += n
			return
		}
		qc.que.cond.Wait()
	}
}
