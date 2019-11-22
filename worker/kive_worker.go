package worker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"strings"
	"time"

	"github.com/VKCOM/joy4/av"
	"github.com/VKCOM/joy4/av/avutil"
	"github.com/mihail812/m3u8"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/VKCOM/kive/hls_server"
	"github.com/VKCOM/kive/kfs"
	"github.com/VKCOM/kive/ktypes"
	"github.com/VKCOM/kive/media"
	"github.com/VKCOM/kive/rtmp_server"
	"math"
	"sort"
)

type Worker struct {
	storage    *kfs.Filesystem
	hlsServer  *hls_server.LiveHls
	rtmpServer *rtmp_server.RtmpServer
	Config     Config

	desiredOutputSizes []int
	streamTypeToSize   map[ktypes.StreamType]int
	sizeToStreamType   map[int]ktypes.StreamType
}

func NewWorker(config Config) (*Worker, error) {
	worker := Worker{
		Config: config,
	}
	storage, err := kfs.NewFilesystem(config.KfsConfig)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create file storage")
	}
	worker.storage = storage

	hlsServer, err := hls_server.NewLiveHls(config.LiveHlsConfig)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create hls server")
	}

	hlsServer.HandleVodChunk = worker.handleVodChunk
	hlsServer.HandleVodManifest = worker.handleVodManifest
	hlsServer.HandleLiveChunk = worker.handleLiveChunk
	hlsServer.HandleLivePlaylist = worker.handleLivePlayList
	hlsServer.HandleDvrChunk = worker.handleDvrChunk
	hlsServer.HandleDvrPlayList = worker.handleDvrPlayList

	worker.hlsServer = hlsServer

	if err != nil {
		return nil, errors.Wrap(err, "cannot create playlist storage")
	}

	rtmpServer, err := rtmp_server.NewRtmpServer(config.RtmpServerConfig)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create rtmp server")
	}
	rtmpServer.HandlePublish = worker.handlePublish
	worker.rtmpServer = rtmpServer
	hlsServer.HandleRtmpHealth = rtmpServer.HealthCheck

	worker.desiredOutputSizes = worker.composeDesiredSizes()

	return &worker, nil
}

func (w *Worker) Listen() error {
	err := w.rtmpServer.Listen()
	if err != nil {
		return errors.Wrap(err, "cannot listen rtmp")
	}

	err = w.hlsServer.Listen()
	if err != nil {
		return errors.Wrap(err, "cannot listen hls ")
	}

	return nil
}

func (w *Worker) Serve() error {
	go func() {
		err := w.rtmpServer.Serve()
		if err != nil {
			logrus.Panicf("cannot serve %+v", err)
		}
		time.Sleep(30 * time.Second)
		panic("serve failed")
	}()

	go func() {
		err := w.hlsServer.Serve()
		if err != nil {
			logrus.Panicf("cannot serve %+v", err)
		}
	}()

	return nil
}

func (w *Worker) Stop() error {
	err := w.rtmpServer.Stop()
	if err != nil {
		logrus.Errorf("cannot stop %+v", err)
	}
	w.storage.Finalize()
	err = w.hlsServer.Stop()
	if err != nil {
		logrus.Errorf("cannot stop %+v", err)
	}
	return nil
}

func (w *Worker) handleLiveMasterPlayList(r *hls_server.LivePlaylistRequest) (hls_server.HttpResponse, error) {
	pl := m3u8.NewMasterPlaylist()

	return hls_server.HttpResponse{
		HttpStatus: http.StatusOK,
		Reader:     ioutil.NopCloser(strings.NewReader(pl.String())),
	}, nil
}

func (w *Worker) handleLivePlayList(r *hls_server.LivePlaylistRequest) (hls_server.HttpResponse, error) {
	var livePlaylistLen = int(4)
	var liveTimeoutLimit = ktypes.UnixMs(50 * 1000)

	if r.StreamType == "" && (r.Application == "kiveabr") {
		res, err := w.storage.Md.GetLast(r.StreamName, string(w.sizeToStreamType[w.getLowestTranscoderSize()]), livePlaylistLen, liveTimeoutLimit) // falback for source playback
		if err != nil || len(res) == 0 {
			r.StreamType = string(ktypes.SOURCE)
		} else {
			return w.handleMasterPlaylist(r.StreamName, "chunks.m3u8")
		}
	} else if r.Application != "kiveabr" {
		r.StreamType = string(ktypes.SOURCE)
	}

	if !ktypes.ApiInst.AllowView(r.StreamName, r.ViewSalt) {
		return hls_server.HttpResponse{HttpStatus: http.StatusForbidden}, errors.New("forbiden")
	}

	pl, err := m3u8.NewMediaPlaylist(0, uint(livePlaylistLen))
	if err != nil {
		return hls_server.HttpResponse{HttpStatus: http.StatusNotFound}, errors.Wrap(err, "cannot creale playlist")
	}

	var res ktypes.SChunkInfo
	if r.Offset != 0 {
		res, err = w.storage.Md.GetFirst(r.StreamName, string(r.StreamType), livePlaylistLen, ktypes.UnixMs(r.Offset))
		if len(res) == 0 || err != nil {
			res, err = w.storage.Md.GetLast(r.StreamName, string(r.StreamType), livePlaylistLen, liveTimeoutLimit)
		}
	} else {
		res, err = w.storage.Md.GetLast(r.StreamName, string(r.StreamType), livePlaylistLen, liveTimeoutLimit)
	}

	logrus.Infof("livePlaylist: %+v %+v %+v", res, r.StreamName, r.StreamType)
	lcr := hls_server.LiveChunkRequest{
		StreamName:      r.StreamName,
		Application:     r.Application,
		StreamNameChunk: r.StreamName,
	}

	for _, i := range res {
		lcr.ChunkName = i.BuildChunkName()
		pl.Append(w.hlsServer.BuildLiveChunkName(&lcr), float64(i.Duration)/float64(time.Second), "")
		if i.Discontinuity != 0 {
			pl.SetDiscontinuity()
		}
	}

	if len(res) != 0 {
		pl.SeqNo = uint64(res[len(res)-1].SeqId - int64(len(res)))
	} else {
		return hls_server.HttpResponse{HttpStatus: http.StatusNotFound}, errors.Wrap(err, "empty_playlist")
	}

	return hls_server.HttpResponse{
		HttpStatus: http.StatusOK,
		Reader:     ioutil.NopCloser(strings.NewReader(pl.String())),
	}, nil
}

func (w *Worker) handleMasterPlaylist(streamName, playlistSufix string) (hls_server.HttpResponse, error) {
	pl := m3u8.NewMasterPlaylist()
	sizes := append([]int{}, w.desiredOutputSizes...)
	sort.Sort(sort.IntSlice(sizes))
	for it, desireSize := range sizes {
		pl.Append(w.ComposeFullMediaPlaylistName(streamName, playlistSufix, desireSize), nil, m3u8.VariantParams{
			ProgramId:  uint32(it),
			Bandwidth:  uint32(4 * 1024 * desireSize),
			Resolution: fmt.Sprintf("%dx%d", int(math.Round(1.7777777*float64(desireSize))), desireSize)})
	}
	return hls_server.HttpResponse{
		HttpStatus: http.StatusOK,
		Reader:     ioutil.NopCloser(strings.NewReader(pl.String())),
	}, nil
}

func (w *Worker) handleLiveChunk(r *hls_server.LiveChunkRequest) (hls_server.HttpResponse, error) {
	key, err := ktypes.ParseChunkName(r.ChunkName)
	if err != nil {
		return hls_server.HttpResponse{HttpStatus: http.StatusBadRequest}, errors.Wrap(err, "bad params")
	}

	reader, err := w.storage.Reader(key)
	if err != nil {
		return hls_server.HttpResponse{HttpStatus: http.StatusNotFound}, errors.Wrap(err, "no such stream")
	}

	return hls_server.HttpResponse{
		HttpStatus: http.StatusOK,
		Reader:     reader,
	}, nil
}
func (w *Worker) handleDvrPlayList(r *hls_server.DvrPlaylistRequest) (hls_server.HttpResponse, error) {
	fromUnixMs := ktypes.UnixMs(r.From * 1000)
	toUnixMs := ktypes.UnixMs(r.From*1000 + r.Duration*1000)
	if r.StreamType == "" && (r.Application == "kiveabr") {
		res, err := w.storage.Md.Walk(r.StreamName, w.sizeToStreamType[w.getLowestTranscoderSize()], fromUnixMs, toUnixMs)
		if err != nil || len(res) == 0 {
			r.StreamType = string(ktypes.SOURCE)
		} else {
			return w.handleMasterPlaylist(r.StreamName, w.hlsServer.BuildDvrChunksPlaylist(r))
		}
	} else if r.Application != "kiveabr" {
		r.StreamType = string(ktypes.SOURCE)
	}

	res, err := w.storage.Md.Walk(r.StreamName, ktypes.StreamType(r.StreamType), fromUnixMs, toUnixMs)
	if err != nil {
		return hls_server.HttpResponse{HttpStatus: http.StatusNotFound}, errors.Wrap(err, "bad params")
	}

	if len(res) == 0 {
		return hls_server.HttpResponse{HttpStatus: http.StatusNotFound}, errors.Wrap(err, "empty_playlist")
	}

	pl, err := m3u8.NewMediaPlaylist(0, uint(len(res)))
	if err != nil {
		return hls_server.HttpResponse{HttpStatus: http.StatusNotFound}, errors.Wrap(err, "cannot creale playlist")
	}

	pl.MediaType = m3u8.VOD

	dcr := hls_server.DvrChunkRequest{
		StreamName:      r.StreamName,
		Application:     r.Application,
		StreamNameChunk: r.StreamName,
	}
	for _, i := range res {
		dcr.ChunkName = i.BuildChunkName()
		pl.Append(w.hlsServer.BuildDvrChunkName(&dcr), float64(i.Duration)/float64(time.Second), "")
		if i.Discontinuity != 0 {
			pl.SetDiscontinuity()
		}
	}
	pl.Close()

	return hls_server.HttpResponse{
		HttpStatus: http.StatusOK,
		Reader:     ioutil.NopCloser(strings.NewReader(pl.String())),
	}, nil
}

func (w *Worker) handleDvrChunk(r *hls_server.DvrChunkRequest) (hls_server.HttpResponse, error) {
	key, err := ktypes.ParseChunkName(r.ChunkName)
	if err != nil {
		return hls_server.HttpResponse{HttpStatus: http.StatusBadRequest}, errors.Wrap(err, "bad params")
	}

	reader, err := w.storage.Reader(key)
	if err != nil {
		return hls_server.HttpResponse{HttpStatus: http.StatusNotFound}, errors.Wrap(err, "no such stream")
	}

	return hls_server.HttpResponse{
		HttpStatus: http.StatusOK,
		Reader:     reader,
	}, nil
}

func (w *Worker) handleVodManifest(r *hls_server.VodManifestRequest) (hls_server.HttpResponse, error) {
	res, err := w.storage.Md.Walk(r.StreamName, ktypes.SOURCE, ktypes.UnixMs(r.From*1000), ktypes.UnixMs(r.From*1000+r.Duration*1000))
	if err != nil {
		return hls_server.HttpResponse{HttpStatus: http.StatusNotFound}, errors.Wrap(err, "bad params")
	}
	if len(res) == 0 {
		return hls_server.HttpResponse{HttpStatus: http.StatusNotFound}, errors.New("Zero len")
	}
	logrus.Infof("Vod manifest len %d", len(res))

	pl, err := m3u8.NewMediaPlaylist(0, uint(len(res)))
	if err != nil {
		return hls_server.HttpResponse{HttpStatus: http.StatusNotFound}, errors.Wrap(err, "bad params")
	}

	pl.MediaType = m3u8.VOD

	vodManifest := VodManifest{}
	vodChunks := make([]ManifestEntry, 0)

	dcr := hls_server.DvrChunkRequest{
		StreamName:      r.StreamName,
		Application:     "live",
		StreamNameChunk: r.StreamName,
	}

	vodManifest.DeletionChunks = make([]ktypes.ChunkInfo, 0)
	duration := 0.
	for _, i := range res {
		playlistPathName := fmt.Sprintf("%s/%s", i.HourString(), i.BuildVodChunkName())
		vodManifestEntry := ManifestEntry{
			Size:      i.Size,
			Fullname:  playlistPathName,
			Directory: i.HourString(),
			Duration:  i.Duration,
		}
		chunkDuration := float64(i.Duration) / float64(time.Second)
		duration += chunkDuration

		dcr.ChunkName = i.BuildChunkName()
		vodManifestEntry.DownloadUrl = w.hlsServer.BuildDvrChunkName(&dcr)
		vodManifestEntry.DvrChunkName = vodManifestEntry.DownloadUrl
		pl.Append(
			playlistPathName,
			chunkDuration,
			"",
		)
		if i.Discontinuity != 0 {
			pl.SetDiscontinuity()
		}
		vodManifestEntry.ChunkInfo = i
		vodManifestEntry.DvrChunkRequest = dcr
		vodChunks = append(vodChunks, vodManifestEntry)
		vodManifest.DeletionChunks = append(vodManifest.DeletionChunks, i)
	}
	pl.Close()
	vodManifest.Playlist = pl.String()
	vodManifest.Chunks = vodChunks
	vodManifest.Duration = int64(duration)
	vodManifest.StreamName = r.StreamName
	vodManifest.From = r.From

	b, err := json.Marshal(vodManifest)
	if err != nil {
		return hls_server.HttpResponse{HttpStatus: http.StatusInternalServerError}, errors.Wrapf(err, "cannot encode %+v", vodManifest)
	}

	return hls_server.HttpResponse{
		HttpStatus: http.StatusOK,
		Reader:     ioutil.NopCloser(bytes.NewReader(b)),
	}, nil
}

func (w *Worker) handleVodChunk(r *hls_server.VodChunkRequest) (hls_server.HttpResponse, error) {
	key, err := ktypes.ParseChunkName(r.ChunkName)
	if err != nil {
		return hls_server.HttpResponse{HttpStatus: http.StatusBadRequest}, errors.Wrap(err, "bad params")
	}

	reader, err := w.storage.Reader(key)
	if err != nil {
		return hls_server.HttpResponse{HttpStatus: http.StatusNotFound}, errors.Wrap(err, "no such stream")
	}

	return hls_server.HttpResponse{
		HttpStatus: http.StatusOK,
		Reader:     reader,
	}, nil
}

func (w *Worker) handlePublish(request *rtmp_server.PublishRequest) error {
	switch request.Application {
	case "live":
		return w.handlePublishInner(request.Data, ktypes.SOURCE, request, nil)
	case "abr":

		transcoder, err := ktypes.ApiInst.GetTranscoder()
		if err != nil {
			return errors.Wrap(err, "failed intialize transcoder")
		}

		defer transcoder.Close()

		demuxers, actualSizes, err := transcoder.Init(w.desiredOutputSizes, request.Data, request.StreamName)
		if err != nil {
			return errors.Wrap(err, "failed intialize transcoder")
		}

		demux := request.Data

		for idx := range demuxers {
			request.Application = "live"
			demuxer := demuxers[idx]
			go func() {
				err := w.handlePublishInner(demuxer, ktypes.StreamType(demuxer.Desc()), request, actualSizes)
				if err != nil {
					logrus.WithField("stream_name", request.StreamName).Errorf("cannot write chunk in demuxer %s: %+v", demuxer.Desc(), err)
				}
			}()
		}
		return avutil.CopyPackets(transcoder, demux)
	default:
		return errors.New("not supported")
	}
}

func (w *Worker) handlePublishInner(demux av.DemuxCloser, streamType ktypes.StreamType, request *rtmp_server.PublishRequest, actualSizes []int) error {
	chunker := media.NewSegmentDemuxer(demux, 3100*time.Millisecond)

	streamToSeqId := make(map[int]int64)
	virtualStreams, _ := composeVirtual(w.streamTypeToSize[streamType], w.desiredOutputSizes, actualSizes)
	for _, vStream := range virtualStreams {
		streamToSeqId[vStream] = w.getLastSeqId(request.StreamName, w.sizeToStreamType[vStream])
	}

	key := ktypes.NewChunkInfo(request.StreamName, streamType)
	key.SeqId = w.getLastSeqId(request.StreamName, streamType)
	keyOut := &key

	for {
		if !request.StreamHandler.AllowStreaming() {
			return errors.Errorf("Streaming is forbidden %+v ", request)
		}

		keyOut, err := w.writeChunk(keyOut, request, chunker)
		if err == nil && keyOut != nil {
			w.writeVirtualChunk(*keyOut, streamToSeqId, virtualStreams)
		}
		key.Discontinuity = 0
		if err == io.EOF {
			break
		} else if err != nil {
			return errors.Wrap(err, "cannot write chunk")
		}
	}

	return nil
}

func (w *Worker) writeChunk(key *ktypes.ChunkInfo, request *rtmp_server.PublishRequest, chunker *media.SegmentDemuxer) (*ktypes.ChunkInfo, error) {
	key.SeqId = key.SeqId + 1
	key.Ts = ktypes.TimeToMillis(time.Now())
	key.Rand = rand.Int()
	key.StreamDesc = "nodesc"
	key.Virtual = false
	key.VirtualStreamType = ktypes.SOURCE
	writer, err := w.storage.Writer(*key)
	if err != nil {
		return nil, errors.Wrap(err, "cannot get storage")
	}
	defer writer.Close()

	si, err := chunker.WriteNext(writer)
	writer.SetChunkDuration(si.Duration)
	key.Duration = si.Duration

	if err != nil && err != io.EOF {
		return nil, errors.Wrap(err, "cannot chunk")
	}

	if key.StreamType == ktypes.SOURCE {
		request.StreamHandler.NotifyStreaming(ktypes.StreamInfo{
			Width:          si.Width,
			Height:         si.Height,
			ChunkStartTime: key.Ts,
		})
	}
	return key, err
}

func (w *Worker) writeVirtualChunk(key ktypes.ChunkInfo, streamToSeqId map[int]int64, virtualStreams []int) {
	for _, vStream := range virtualStreams {
		key.Virtual = true
		key.SeqId = streamToSeqId[vStream] + 1
		streamToSeqId[vStream] = key.SeqId
		key.VirtualStreamType = ktypes.StreamType(w.sizeToStreamType[vStream])
		w.storage.WriteMeta(key)
	}
}

func (w *Worker) getLastSeqId(streamName string, streamType ktypes.StreamType) (seqId int64) {
	chunks, err := w.storage.Md.GetLast(streamName, string(streamType), 1, 3600*1000)
	seqId = 0
	if err == nil && len(chunks) != 0 {
		seqId = chunks[0].SeqId
	}
	return seqId
}

func (w *Worker) GetChunk(streamName string, streamType ktypes.StreamType, from ktypes.UnixMs, to ktypes.UnixMs) ([]byte, error) {
	res, err := w.storage.Md.Walk(streamName, streamType, from, to)
	if err != nil {
		return nil, errors.Wrap(err, "cannot get chunk list")
	}

	if len(res) == 0 {
		return nil, errors.Wrap(err, "no chunks")
	}
	chunkReader, err := w.storage.Reader(res[0])

	if err != nil {
		return nil, errors.Wrap(err, "cannot get chunk")
	}
	defer chunkReader.Close()

	tsData := bytes.NewBuffer(make([]byte, 0))
	n, err := io.Copy(tsData, chunkReader)

	logrus.Debugf("Copied %d bytes for image", n)
	if err != nil {
		return nil, errors.Wrap(err, "cannot get chunk")
	}
	return tsData.Bytes(), nil
}

func (w *Worker) DeleteChunk(ci ktypes.ChunkInfo) error {
	return w.storage.Delete(ci)
}

func (w *Worker) composeDesiredSizes() []int {
	sizes := make([]int, 0, 0)
	w.streamTypeToSize = make(map[ktypes.StreamType]int)
	w.sizeToStreamType = make(map[int]ktypes.StreamType)
	w.streamTypeToSize["1080p"] = 1080
	w.streamTypeToSize["720p"] = 720
	w.streamTypeToSize["480p"] = 480
	w.streamTypeToSize["360p"] = 360
	for key, value := range w.streamTypeToSize {
		w.sizeToStreamType[value] = key
		sizes = append(sizes, value)
	}
	sort.Ints(sizes)
	return sizes
}

func (w *Worker) getLowestTranscoderSize() int {
	min := w.desiredOutputSizes[0]
	for _, size := range w.desiredOutputSizes {
		if size < min {
			min = size
		}
	}
	return min
}

func (w *Worker) ComposeFullMediaPlaylistName(streamName, playlistSufix string, size int) string {
	return fmt.Sprintf("%s/%s", w.sizeToStreamType[size], playlistSufix)
}

func hasVideoSize(desiredSize int, sizes []int) bool {
	for _, size := range sizes {
		if size == desiredSize {
			return true
		}
	}
	return false
}

func findSizePosition(desiredSize int, sizes []int) (int, error) {
	for el, size := range sizes {
		if size == desiredSize {
			return el, nil
		}
	}
	return 0, errors.New("size_not_found")
}

func composeVirtual(size int, desiredSizes, actualSizes []int) ([]int, error) {
	desiredSizePos, err := findSizePosition(size, desiredSizes)
	if err != nil {
		return nil, err
	}
	result := make([]int, 0, 0)
	for i := desiredSizePos + 1; i < len(desiredSizes); i++ {
		if hasVideoSize(desiredSizes[i], actualSizes) == true {
			return result, nil
		}
		result = append(result, desiredSizes[i])
	}
	return result, nil
}
