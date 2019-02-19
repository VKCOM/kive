package vsync

import (
	"sync/atomic"
	"time"
)

type Semaphore struct {
	c          chan struct{}
	waiterSlot int32
}

func NewSemaphore(max uint, maxWaiters uint) *Semaphore {
	return &Semaphore{c: make(chan struct{}, max), waiterSlot: int32(maxWaiters + max)}
}

func (m *Semaphore) Lock() {
	atomic.AddInt32(&m.waiterSlot, -1)
	m.c <- struct{}{}
}

func (m *Semaphore) Unlock() {
	atomic.AddInt32(&m.waiterSlot, 1)
	<-m.c
}

func (m *Semaphore) TryLock(timeout time.Duration) bool {
	slotsLeft := atomic.AddInt32(&m.waiterSlot, -1)
	if slotsLeft < 0 {
		atomic.AddInt32(&m.waiterSlot, 1)
		return false
	}

	select {
	case m.c <- struct{}{}:
		return true
	case <-time.After(timeout):
		atomic.AddInt32(&m.waiterSlot, 1)
	}
	return false
}
