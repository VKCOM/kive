package kfs_test

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"io/ioutil"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/VKCOM/kive/kfs"
	"github.com/VKCOM/kive/ktypes"
)

type KfsTestsSuite struct {
	suite.Suite
	segments *kfs.Filesystem
	path     string
	tpl      ktypes.ChunkInfo
	inMemory bool
}

func (s *KfsTestsSuite) SetupTest() {
	path, err := ioutil.TempDir("", "kive_test")
	if err != nil {
		panic("Cannot run test")
	}
	s.path = path
	config := kfs.NewKfsConfig()
	config.RemovalTime = 0
	if s.inMemory {
		config.Basedir = kfs.INMEMORY
		config.RemovalTime = 0
	} else {
		config.Basedir = path
	}
	s.segments, err = kfs.NewFilesystem(config)
	if err != nil {
		panic("Cannot run test")
	}

	s.tpl = ktypes.ChunkInfo{
		StreamDesc:    "test",
		Size:          0,
		Rand:          rand.Int(),
		Discontinuity: 1,
		ChunkKey: ktypes.ChunkKey{
			StreamType: ktypes.SOURCE,
			StreamName: "test1",
			Ts:         1,
			SeqId:      0,
		},
	}

}

func (s *KfsTestsSuite) TearDownTest() {
	s.segments.Finalize()
	if s.path != "" {
		os.RemoveAll(s.path)
	}
}

func (s *KfsTestsSuite) TestWriteReadBuffer() {
	wbytes := []byte{1, 2, 3}

	w, _ := s.segments.Writer(s.tpl)

	_, _ = w.Write(wbytes)
	w.Close()

	r, _ := s.segments.Reader(s.tpl)
	rbytes := []byte{0, 0, 0}
	r.Read(rbytes)
	assert.Equal(s.T(), wbytes, rbytes)
}

func (s *KfsTestsSuite) TestReadBeforeWrite() {
	wbytes := []byte{1, 2, 3}

	w, _ := s.segments.Writer(s.tpl)
	_, _ = w.Write(wbytes)
	r, _ := s.segments.Reader(s.tpl)
	w.Close()
	rbytes := []byte{0, 0, 0}
	sw := sync.WaitGroup{}

	sw.Add(1)
	go func() {
		r.Read(rbytes)
		sw.Done()
	}()
	sw.Wait()

	assert.Equal(s.T(), wbytes, rbytes)
}

func (s *KfsTestsSuite) TestWriteReadFinalized() {
	wbytes := []byte{1, 2, 3}

	w, _ := s.segments.Writer(s.tpl)
	_, _ = w.Write(wbytes)
	w.Close()

	s.segments.Finalize()

	r, _ := s.segments.Reader(s.tpl)
	rbytes := []byte{0, 0, 0}
	r.Read(rbytes)
	assert.Equal(s.T(), wbytes, rbytes)
}

func (s *KfsTestsSuite) TestFinalize() {
	s.segments.Finalize()
}

func (s *KfsTestsSuite) TestReadWriteRange() {
	wbytes1 := []byte{1, 2, 3}
	wbytes2 := []byte{1, 2, 3, 4}
	wbytes3 := []byte{1, 2, 3, 4, 5}

	k1 := s.tpl
	k2 := s.tpl
	k3 := s.tpl

	k1.Ts = 3600 * 1000
	k2.Ts = 3700 * 1000
	k3.Ts = 7200 * 1000

	w1, _ := s.segments.Writer(k1)
	w2, _ := s.segments.Writer(k2)
	w3, _ := s.segments.Writer(k3)

	_, _ = w1.Write(wbytes1)
	w1.SetChunkDuration(1)
	w1.Close()
	_, _ = w2.Write(wbytes2)
	w2.SetChunkDuration(2)
	w2.Close()
	_, _ = w3.Write(wbytes3)
	w3.SetChunkDuration(3)
	w3.Close()

	_, err := s.segments.Reader(s.tpl)
	assert.Error(s.T(), err)

	ci, _ := s.segments.Md.Walk(k1.StreamName, k1.StreamType, k1.Ts, k3.Ts)
	require.Equal(s.T(), 3, len(ci))
	assert.Equal(s.T(), 3, ci[0].Size)
	assert.Equal(s.T(), 4, ci[1].Size)
	assert.Equal(s.T(), 5, ci[2].Size)

	assert.Equal(s.T(), time.Duration(1), ci[0].Duration)
	assert.Equal(s.T(), time.Duration(2), ci[1].Duration)
	assert.Equal(s.T(), time.Duration(3), ci[2].Duration)

}

func TestStorageTestsSuiteAll(t *testing.T) {
	suite.Run(t, new(KfsTestsSuite))
}

func TestStorageTestsSuiteInMemory(t *testing.T) {
	is := new(KfsTestsSuite)
	is.inMemory = true
	suite.Run(t, is)
}
