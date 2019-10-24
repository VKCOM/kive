package kfs

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/VKCOM/kive/ktypes"
	"github.com/karlseguin/ccache"
	"github.com/pkg/errors"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

type Metadata struct {
	m         sync.RWMutex
	config    KfsConfig
	infoCache *ccache.Cache
}

func NewMetadata(config KfsConfig) *Metadata {
	return &Metadata{
		config:    config,
		infoCache: ccache.New(ccache.Configure().MaxSize(config.MaxCacheSize).Buckets(64)),
	}
}

//TODO duration instead to
func (md *Metadata) Walk(streamName string, streamType ktypes.StreamType, fromTs ktypes.UnixMs, toTs ktypes.UnixMs) ([]ktypes.ChunkInfo, error) {

	dirData, err := md.getMetadata(streamName, string(streamType), fromTs, toTs)

	if err != nil {
		return nil, errors.Wrap(err, "bad walk")
	}

	sort.Sort(dirData)

	return []ktypes.ChunkInfo(dirData), nil
}

func (md *Metadata) GetSources(streamName string) ([]string, error) {
	sources, err := readDirNames(path.Join(md.config.Basedir, streamName))
	if err != nil {
		return nil, err
	}
	return sources, nil
}

func (md *Metadata) GetAllChunksInfo(streamName string, from, to ktypes.UnixMs) (map[string]ktypes.SChunkInfo, error) {
	sources, err := md.GetSources(streamName)
	if err != nil {
		return nil, nil
	}
	result := make(map[string]ktypes.SChunkInfo, 3)
	for _, source := range sources {
		streamChunksInfo, err := md.getMetadata(streamName, source, from, to)
		if err != nil {
			continue
		}
		result[source] = streamChunksInfo
	}
	return result, nil
}

func (md *Metadata) GetLast(streamName, sourceType string, count int, timeoutLimit ktypes.UnixMs) (ktypes.SChunkInfo, error) {
	timeMs := ktypes.UnixMs(time.Duration(time.Now().Unix() * 1000))
	chunks, err := md.getMetadata(streamName, sourceType, timeMs-timeoutLimit, timeMs)
	if err != nil {
		return nil, err
	}
	if len(chunks) > count {
		return append(ktypes.SChunkInfo{}, chunks[len(chunks)-count:]...), nil
	} else {
		return chunks, nil
	}
}

func (md *Metadata) getMetadata(streamName string, sourceName string, from ktypes.UnixMs, to ktypes.UnixMs) (ktypes.SChunkInfo, error) {
	result := make(ktypes.SChunkInfo, 0)
	for _, hr := range ktypes.HoursRange(from, to) {
		hourInfo, _ := md.fetchManifest(streamName, sourceName, hr)
		for _, chunk := range hourInfo {
			if chunk.Ts >= from && chunk.Ts <= to {
				result = append(result, chunk)
			}
		}
	}
	return result, nil
}

func (md *Metadata) fetchManifest(streamName, sourceName, hour string) ([]ktypes.ChunkInfo, error) {
	value := md.infoCache.Get(ktypes.ComposeHourKeyCache(streamName, sourceName, hour))
	if value != nil && !value.Expired() {
		return value.Value().([]ktypes.ChunkInfo), nil
	}

	result, err := md.fetchManifestFromFile(streamName, sourceName, hour)
	if err != nil {
		return nil, err
	}

	md.m.RLock()
	defer md.m.RUnlock()
	value = md.infoCache.Get(ktypes.ComposeHourKeyCache(streamName, sourceName, hour))
	if value != nil {
		md.infoCache.Set(ktypes.ComposeHourKeyCache(streamName, sourceName, hour), &result, md.config.RemovalTime.Duration-2*time.Hour)
	}
	return result, nil
}

func (md *Metadata) fetchManifestFromFile(streamName, sourceName, hour string) ([]ktypes.ChunkInfo, error) {
	file, err := os.OpenFile(md.config.buildMetadataPathPlain(streamName, sourceName, hour), os.O_RDONLY, os.ModeExclusive)
	if err != nil {
		return nil, err
	}

	result := make([]ktypes.ChunkInfo, 0, 300)
	entry := innerStorage{}
	reader := bufio.NewScanner(file)
	for reader.Scan() {
		if err := json.Unmarshal(reader.Bytes(), &entry); err == nil {
			result = append(result, ktypes.ChunkInfo(entry))
		}
	}
	return result, nil
}

func (md *Metadata) writeFsInfo(key innerStorage, fullname string) error {
	if _, err := os.Stat(filepath.Dir(fullname)); !os.IsExist(err) {
		err := os.MkdirAll(filepath.Dir(fullname), os.ModePerm)
		if err != nil {
			return errors.Wrapf(err, "cannot create directories for  %s", fullname)
		}
		streamTypePath := key.StreamType
		if key.Virtual == true {
			streamTypePath = key.VirtualStreamType
		}
		fileMetadata, err := os.OpenFile(md.config.buildMetadataPathStreamType(key, streamTypePath), os.O_CREATE, os.ModePerm)
		if err != nil {
			return err
		}
		fileMetadata.Close()
	}

	streamTypePath := key.StreamType
	if key.Virtual == true {
		streamTypePath = key.VirtualStreamType
	}
	f, err := os.OpenFile(md.config.buildMetadataPathStreamType(key, streamTypePath), os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		return md.writeFsInfoFallback(key, fullname) //fallback for an old scheme
	}

	jBytes, err := json.Marshal(key)
	if err != nil {
		return errors.Wrapf(err, "cannot serialize file %+v", key)
	}

	n, err := f.Write(append([]byte{'\n'}, jBytes...))
	if err == nil && n < len(jBytes) {
		return errors.Wrap(io.ErrShortWrite, "error while write meta")
	}
	if err := f.Close(); err == nil {
		return errors.Wrap(err, "error while write meta")
	}

	var manifestContent []ktypes.ChunkInfo
	value := md.infoCache.Get(ktypes.ComposeHourKeyCache(key.StreamName, string(key.StreamType), key.HourString()))
	if value == nil {
		manifestContent, err = md.fetchManifestFromFile(key.StreamName, string(key.StreamType), key.HourString())
		if err != nil {
			return err
		}
	} else {
		manifestContent = value.Value().([]ktypes.ChunkInfo)
	}

	var updatedChunkInfoList []ktypes.ChunkInfo
	updatedChunkInfoList = append(updatedChunkInfoList, manifestContent...)
	updatedChunkInfoList = append(updatedChunkInfoList, ktypes.ChunkInfo(key))

	md.m.Lock()
	defer md.m.Unlock()
	md.infoCache.Set(ktypes.ComposeHourKeyCache(key.StreamName, string(key.StreamType), key.HourString()), &updatedChunkInfoList, md.config.RemovalTime.Duration-2*time.Hour)
	return nil
}

func (md *Metadata) writeFsInfoFallback(key innerStorage, fullname string) error {
	jBytes, err := json.Marshal(key)
	if err != nil {
		return errors.Wrapf(err, "cannot serialize file %+v", key)
	}

	if err := ioutil.WriteFile(fmt.Sprintf("%s.json", fullname), jBytes, os.ModePerm); err != nil {
		return errors.Wrap(err, "cannot write meta")
	}
	md.infoCache.Set(fullname, &key, md.config.RemovalTime.Duration-2*time.Hour)
	return nil
}

func (md *Metadata) readFsInfo(key innerStorage, fullname string) (*innerStorage, error) {
	cached := md.infoCache.Get(fullname)
	if cached != nil && !cached.Expired() {
		return cached.Value().(*innerStorage), nil
	}
	jData := innerStorage{}
	jBytes, err := ioutil.ReadFile(fmt.Sprintf("%s.json", fullname))
	if err != nil {
		return &innerStorage{}, errors.Wrap(err, "Cannot read chunk info")
	}

	err = json.Unmarshal(jBytes, &jData)
	if err != nil {
		return &jData, errors.Wrap(err, "cannot unmarshal")
	}
	md.infoCache.Set(fullname, &key, time.Duration(key.Ts)-md.config.RemovalTime.Duration-2*time.Hour)
	return &jData, nil
}

func (md *Metadata) getExtended(ci ktypes.ChunkInfo) (int, time.Duration, error) {
	key := innerStorage(ci)

	fullname := md.config.buildFullPath(key)
	jData, err := md.readFsInfo(key, fullname)
	if err != nil {
		return 0, 0, errors.Wrap(err, "Cannot unmarshall chunk info")
	}

	return int(jData.Size), jData.Duration, nil
}
