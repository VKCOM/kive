package kfs

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/VKCOM/kive/ktypes"
	"github.com/google/btree"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	INMEMORY = "memory"
)

type Filesystem struct {
	Md *Metadata

	config         KfsConfig
	writeQueue     chan *bufferItem
	writeMetaQueue chan innerStorage
	deleteQueue    chan innerStorage
	streamDeletion chan string

	finalizerWriters sync.WaitGroup

	m             sync.RWMutex
	buffers       *btree.BTree
	activeWriters uint32

	stopped bool
}

type bufferItem struct {
	resultKey innerStorage
	queue     *queue
}

func (bi *bufferItem) Less(rhs btree.Item) bool {
	return bi.resultKey.ChunkKey.Less(rhs.(*bufferItem).resultKey.ChunkKey)
}

func NewFilesystem(config KfsConfig) (*Filesystem, error) {
	if config.Basedir != INMEMORY {
		basedir, err := filepath.EvalSymlinks(config.Basedir)

		if err != nil {
			return nil, errors.Errorf("cannot evaluate symlinks %s", basedir)
		}

		if !filepath.IsAbs(basedir) {
			return nil, errors.Errorf("not abs path %s", basedir)
		}

		err = os.MkdirAll(basedir, os.ModePerm)

		if err != nil {
			return nil, errors.Wrapf(err, "cannot create directory %s", basedir)
		}
		config.Basedir = filepath.Clean(config.Basedir)
	}

	fs := &Filesystem{
		config:         config,
		buffers:        btree.New(2),
		writeQueue:     make(chan *bufferItem, 256),
		writeMetaQueue: make(chan innerStorage, 256),
		deleteQueue:    make(chan innerStorage, 1024*100),
		streamDeletion: make(chan string, 300),
		Md:             NewMetadata(config),
	}

	if config.Basedir != INMEMORY {
		go fs.writerGoroutine()
	}

	if config.RemovalTime.Duration != 0 {
		go fs.cleanUpGoroutine()
	}

	return fs, nil
}

func (fs *Filesystem) Finalize() {
	fs.m.Lock()
	wasStopped := fs.stopped
	fs.stopped = true
	fs.m.Unlock()
	if wasStopped {
		return
	}

	fs.finalizerWriters.Wait()
	close(fs.writeQueue)
	close(fs.deleteQueue)
	close(fs.streamDeletion)
}

func (fs *Filesystem) Delete(ci ktypes.ChunkInfo) error {
	key := innerStorage(ci)
	err := validateStorageInfo(key)

	if err != nil {
		return errors.Wrapf(err, "invalid chunk info %+v", key)
	}

	fs.deleteFromBuffer(key)
	select {
	case fs.deleteQueue <- key:
	default:
		return errors.New("cannot add to deletion queue")
	}
	return nil
}

func (fs *Filesystem) Reader(ci ktypes.ChunkInfo) (io.ReadCloser, error) {
	logrus.WithField("stream_name", ci.StreamName).Debugf("Getting %+v", ci)
	key := innerStorage(ci)
	err := validateStorageInfo(key)

	if err != nil {
		return nil, errors.Wrapf(err, "cannot get reader %+v", key)
	}

	reader, err := fs.readerBuffer(key)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot get reader %+v", key)
	}
	if reader != nil {
		return reader, nil
	}

	return fs.fileReader(fs.config.buildFullPath(key))
}

func (fs *Filesystem) Writer(ch ktypes.ChunkInfo) (*Writer, error) {
	key := innerStorage(ch)
	err := validateStorageInfo(key)
	key.Duration = ktypes.DEFAULT_DURATION
	if err != nil {
		return nil, errors.Wrapf(err, "cannot get writer %+v", key)
	}

	if fs.config.Basedir != INMEMORY {
		err = os.MkdirAll(fs.config.buildDirPath(key), os.ModePerm)
		if err != nil {
			return nil, errors.Wrapf(err, "cannot create dir for  %+v", key)
		}
	}

	maxQueueSize := fs.config.MaxSize
	if ch.StreamType == ktypes.SOURCE {
		maxQueueSize = fs.config.MaxSourceSize
	}

	bufi := &bufferItem{
		resultKey: key,
		queue:     NewQueue(maxQueueSize),
	}

	fs.m.Lock()
	defer fs.m.Unlock()
	if fs.stopped {
		return nil, errors.New("already stopped")
	}
	item := fs.buffers.Get(bufi)

	if item != nil {
		return nil, errors.Errorf("already written %+v", key)
	}

	fs.buffers.ReplaceOrInsert(bufi)
	fs.finalizerWriters.Add(1)
	return &Writer{item: bufi, fs: fs}, nil
}

func (fs *Filesystem) WriteMeta(key ktypes.ChunkInfo) error {
	return fs.writeMeta(innerStorage(key))
}

func (fs *Filesystem) fileReader(fullname string) (io.ReadCloser, error) {
	if fs.config.Basedir == INMEMORY {
		return nil, errors.Errorf("cannot get file reader %s", fullname)
	}
	b, err := ioutil.ReadFile(fullname)

	if err != nil {
		return nil, errors.Wrapf(err, "cannot get file reader %s", fullname)
	}
	return ioutil.NopCloser(bytes.NewReader(b)), nil
}

func (fs *Filesystem) deleteFromBuffer(rkey innerStorage) {
	bufi := bufferItem{
		resultKey: rkey,
	}

	fs.m.Lock()
	defer fs.m.Unlock()
	fs.buffers.Delete(&bufi)
}

func (fs *Filesystem) readerBuffer(rkey innerStorage) (io.ReadCloser, error) {
	fs.m.Lock()
	defer fs.m.Unlock()

	bufi := bufferItem{
		resultKey: rkey,
	}
	item := fs.buffers.Get(&bufi)
	if item == nil {
		return nil, nil
	}

	return ioutil.NopCloser(item.(*bufferItem).queue.Oldest()), nil
}

func (fs *Filesystem) eraseBuffer(bufi *bufferItem) error {
	fs.m.Lock()
	defer fs.m.Unlock()

	item := fs.buffers.Delete(bufi)
	if item == nil {
		return errors.Errorf("no such buffer %+v", bufi)
	}
	fs.finalizerWriters.Done()
	return nil
}

func (fs *Filesystem) finalizeWriter(bufi *bufferItem) error {
	err := bufi.queue.Close()
	if err != nil {
		return errors.Wrapf(err, "cannot finalize writer %+v", bufi)
	}

	if fs.config.Basedir == INMEMORY {
		fs.finalizerWriters.Done()
		return nil
	}

	fs.m.Lock()
	bufi.resultKey.Size = bufi.queue.GetSize()
	item := fs.buffers.Get(bufi)
	fs.m.Unlock()

	if item == nil {
		return errors.Errorf("cannot finalize %+v", bufi)
	}

	select {
	case fs.writeQueue <- bufi:
		ktypes.Stat(false, "disk_write", "done", "")
		return nil
	case <-time.After(fs.config.WriteTimeout.Duration):
		ktypes.Stat(true, "disk_write", "first_timeout", "")
	}

	fs.writeQueue <- bufi
	return nil
}

func (fs *Filesystem) writeMeta(key innerStorage) error {
	fs.writeMetaQueue <- key
	return nil
}

func (fs *Filesystem) deleteFile(key innerStorage) error {
	fullname := fs.config.buildFullPath(key)
	logrus.WithField("stream_name", key.StreamName).Debugf("Deleting %s", fullname)
	return os.Remove(fullname)
}

func (fs *Filesystem) writeFile(bufi *bufferItem) (int64, error) {
	fullname := fs.config.buildFullPath(bufi.resultKey)

	err := fs.Md.writeFsInfo(bufi.resultKey, fullname)
	if err != nil {
		return 0, errors.Wrapf(err, "cannot write json %s", fullname)
	}

	file, err := os.OpenFile(fullname, os.O_WRONLY|os.O_CREATE|os.O_EXCL, os.ModePerm)
	if err != nil {
		return 0, errors.Wrapf(err, "cannot open file  %s", fullname)
	}
	defer file.Close()

	written, err := io.Copy(file, bufi.queue.Oldest())
	if err != nil {
		return written, errors.Wrapf(err, "cannot read from buffer %s %d", fullname, written)
	}
	return written, nil
}

func (fs *Filesystem) walkBuffer(from ktypes.ChunkKey, to ktypes.ChunkKey) (ktypes.SChunkInfo, error) {
	if to.Less(from) {
		return nil, errors.Errorf("from to out of order")
	}

	result := make(ktypes.SChunkInfo, 0, 30)
	fs.m.RLock()
	defer fs.m.RUnlock()
	bufiFrom := &bufferItem{
		resultKey: innerStorage{ChunkKey: from},
	}

	iterator := func(i btree.Item) bool {
		val := i.(*bufferItem)

		if to.Less(val.resultKey.ChunkKey) {
			return false
		}

		result = append(result, ktypes.ChunkInfo(val.resultKey))
		return true
	}
	fs.buffers.AscendGreaterOrEqual(bufiFrom, iterator)
	return result, nil
}

func readDirNames(dirname string) ([]string, error) {
	info, err := os.Stat(dirname)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot stat %s", dirname)
	}

	if !info.IsDir() {
		return nil, errors.Errorf("not an directory %s", dirname)
	}

	f, err := os.Open(dirname)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	names, err := f.Readdirnames(-1)

	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	return names, nil
}

func (fs *Filesystem) writerGoroutine() {
	for {
		select {
		case bufi := <-fs.writeQueue:
			if bufi == nil {
				return
			}
			_, err := fs.writeFile(bufi)
			if err != nil {
				logrus.WithField("stream_name", bufi.resultKey.StreamName).Fatalf("Cannot write file %+v", err)
			}
			err = fs.eraseBuffer(bufi)
			if err != nil {
				logrus.WithField("stream_name", bufi.resultKey.StreamName).Fatalf("Cannot erase buffer %+v", err)
			}
		case key := <-fs.writeMetaQueue:
			err := fs.Md.writeFsInfo(key, path.Join(fs.config.Basedir, key.buildDirStreamType(key.VirtualStreamType), "metadata.json"))
			if err != nil {
				logrus.WithField("stream_name", key.StreamName).Fatalf("Cannot write meta file %+v", err)
			}
		case key, more := <-fs.deleteQueue:
			if !more {
				continue
			}
			err := fs.deleteFile(key)
			if err != nil {
				logrus.WithField("stream_name", key.StreamName).Errorf("Cannot erase file %+v", err)
			}
		case key, more := <-fs.streamDeletion:
			if !more {
				continue
			}
			err := fs.removeEmptystream(key)
			if err != nil {
				logrus.WithField("stream_name", key).Errorf("Cannot erase stream %+v", err)
			}
		}

	}
}

func (fs *Filesystem) removeEmptystream(streamName string) error {
	sourceDir := fmt.Sprintf("%s/%s/%s", fs.config.Basedir, streamName, ktypes.SOURCE)
	err := os.MkdirAll(sourceDir, os.ModePerm)
	if err != nil {
		return errors.Wrapf(err, "cannot ensure dir, %s", sourceDir)
	}

	hourListFile, err := os.Open(sourceDir)
	if err != nil {
		return errors.Wrapf(err, "cannot open hours dir for %s, %s", streamName, sourceDir)
	}
	defer hourListFile.Close()
	hours, err := hourListFile.Readdirnames(-1)
	if err != nil {
		return errors.Wrapf(err, "cannot get hours list for %s", streamName)
	}

	if len(hours) != 0 {
		ktypes.Stat(false, "removal_check", "remove_whole_dir_skipped", fmt.Sprintf("%d", len(hours)))
		return nil
	} else {
		streamDir := fmt.Sprintf("%s/%s", fs.config.Basedir, streamName)
		err := os.RemoveAll(streamDir)
		ktypes.Stat(err != nil, "removal_check", "removed_whole_dir", fmt.Sprintf("%d", len(hours)))
	}
	return nil
}

func (fs *Filesystem) cleanUpGoroutine() {
	for {
		free, _ := diskFree(fs.config.Basedir)
		now := time.Now()
		latestRemoval := now.Add(-fs.config.RemovalTime.Duration)
		latestRemovalString := ktypes.HourString(ktypes.TimeToMillis(latestRemoval))

		logrus.Debugf("Walk started %+v free %+v removal after %+v", fs.config.Basedir, free, latestRemovalString)

		walkStream := fs.removeOldFiles

		walkStreams := func() error {

			streams, err := readDirNames(fs.config.Basedir)
			if err != nil {
				return errors.Wrap(err, "cannot get stream list")
			}

			for _, streamName := range streams {
				time.Sleep(1 * time.Second)
				streamTypes, err := readDirNames(path.Join(fs.config.Basedir, streamName))
				if err != nil {
					return errors.Wrap(err, "cannot get stream list")
				}
				for _, streamType := range streamTypes {
					err := walkStream(streamName, streamType, latestRemovalString)
					ktypes.Stat(err != nil, "removal_check", "walk_stream", "")
					if err != nil {
						logrus.Errorf("cannon delete %+v ", err)
					}
				}
			}
			return nil
		}

		err := walkStreams()
		logrus.Infof("Walk finished %+v", err)

		time.Sleep(10 * time.Second)
	}
}

func diskFree(path string) (uint64, error) {
	fs := syscall.Statfs_t{}
	err := syscall.Statfs(path, &fs)
	if err != nil {
		return 0, errors.Wrap(err, "cannot get disk usage")
	}
	return fs.Bfree * uint64(fs.Bsize), nil
}

func (fs *Filesystem) ensureEmptyDir(streamName string) error {
	select {
	case fs.streamDeletion <- streamName:
	default:
		return errors.Errorf("cannot add deletion check")
	}
	return nil
}

func (fs *Filesystem) removeOldFiles(streamName, streamType, latestRemovalString string) error {
	sourceDir := fmt.Sprintf("%s/%s/%s", fs.config.Basedir, streamName, streamType)
	if _, err := os.Stat(sourceDir); os.IsNotExist(err) {
		err := fs.ensureEmptyDir(streamName)
		ktypes.Stat(err != nil, "removal_check", "send_check_whole_dir_no_source", "")
		return nil
	}

	hourListFile, err := os.Open(sourceDir)
	if err != nil {
		return errors.Wrapf(err, "cannot open hours dir for %s, %s", streamName, sourceDir)
	}
	defer hourListFile.Close()
	hours, err := hourListFile.Readdirnames(-1)
	if err != nil {
		return errors.Wrapf(err, "cannot get hours list for %s", streamName)
	}

	removedCount := 0
	for _, hourName := range hours {
		time.Sleep(1 * time.Millisecond)

		if strings.Compare(hourName, latestRemovalString) < 0 {
			removeHourDir := fmt.Sprintf("%s/%s", sourceDir, hourName)
			err := os.RemoveAll(removeHourDir)
			ktypes.Stat(err != nil, "removal_check", "removed_hour", "")
			logrus.Debugf("removing %s %s %s", hourName, latestRemovalString, removeHourDir)
			time.Sleep(100 * time.Millisecond)
			removedCount += 1
		} else {
			ktypes.Stat(false, "removal_check", "skipped_hour", "")
		}

	}

	if len(hours) == removedCount {
		err := fs.ensureEmptyDir(streamName)
		ktypes.Stat(err != nil, "removal_check", "send_check_whole_dir", fmt.Sprintf("%d", len(hours)))
	} else {
		ktypes.Stat(err != nil, "removal_check", "skip_check_whole_dir", fmt.Sprintf("%d", len(hours)))
	}

	return nil
}
