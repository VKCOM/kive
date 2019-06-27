package media

import (
	"github.com/VKCOM/joy4/av"
	"github.com/VKCOM/joy4/av/avutil"
	"github.com/VKCOM/joy4/format/flv"
	"github.com/VKCOM/joy4/format/ts"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"io"
	"time"
)

type kfChunkerState int

const (
	INITIAL kfChunkerState = iota
	WAITING_KF
	SPLITTING
	HOLD_ON_KF
	FINISHED
	ERROR

	maxPacketPerChunk = 2000
)

type SegmentInfo struct {
	Duration time.Duration
	Width    int
	Height   int
}

type keyFrameChunker struct {
	av.Demuxer
	state           kfChunkerState
	lastKeyframe    av.Packet
	durMin          time.Duration
	lastTs          time.Duration
	currentDuration time.Duration
	pktCounter      int
}

func (kf *keyFrameChunker) isVideo(pk *av.Packet) bool {
	streams, err := kf.Demuxer.Streams()
	if err != nil {
		return false
	}

	if len(streams) <= int(pk.Idx) {
		return false
	}

	t := streams[pk.Idx]
	return t.Type().IsVideo()
}

func (kf *keyFrameChunker) readChecked() (av.Packet, error) {
	pk, err := kf.Demuxer.ReadPacket()
	kf.pktCounter += 1
	if err != nil {
		return pk, err
	}

	if kf.pktCounter > maxPacketPerChunk {
		return pk, errors.New("too many packets")
	}
	return pk, err
}

func (kf *keyFrameChunker) ReadPacket() (av.Packet, error) {
	switch kf.state {
	case FINISHED:
		return av.Packet{}, io.EOF
	case ERROR:
		return av.Packet{}, io.ErrUnexpectedEOF
	case INITIAL:
		pk, err := kf.readChecked()
		if err == io.EOF || err != nil {
			kf.state = ERROR
			return pk, errors.Wrap(err, "error on reading")
		} else if pk.IsKeyFrame {
			kf.state = SPLITTING
			kf.lastTs = pk.Time
			return pk, nil
		} else if kf.isVideo(&pk) {
			kf.state = ERROR
			return pk, errors.Wrap(err, "error on reading")
		}

		if !kf.isVideo(&pk) {
			kf.lastTs = pk.Time
		}
		return pk, nil
	case WAITING_KF:
		pk, err := kf.readChecked()
		if err == io.EOF || err != nil {
			kf.state = ERROR
			return pk, errors.Wrap(err, "error on reading")
		} else if pk.IsKeyFrame {
			kf.state = SPLITTING
		} else if kf.isVideo(&pk) {
			kf.state = ERROR
			return pk, errors.Wrap(err, "error on reading")
		}
		return pk, nil
	case SPLITTING:
		pk, err := kf.readChecked()
		if err == io.EOF {
			kf.state = FINISHED
			return pk, io.EOF
		} else if err != nil {
			kf.state = ERROR
			return pk, errors.Wrap(err, "error on reading")
		}

		if pk.IsKeyFrame && pk.Time-kf.lastTs > kf.durMin {
			kf.lastKeyframe = pk
			kf.state = HOLD_ON_KF
			return pk, io.EOF
		}

		if !kf.isVideo(&pk) {
			kf.currentDuration = pk.Time - kf.lastTs
		}

		return pk, nil

	case HOLD_ON_KF:
		kf.state = SPLITTING
		kf.pktCounter = 0
		kf.lastTs = kf.lastKeyframe.Time
		return kf.lastKeyframe, nil
	}

	return av.Packet{}, errors.New("unreachable state")
}

type SegmentDemuxer struct {
	chunker *keyFrameChunker
	muxer   *ts.Muxer
}

func NewSegmentDemuxer(demuxer av.Demuxer, minSegmentDuration time.Duration) *SegmentDemuxer {
	return &SegmentDemuxer{
		chunker: &keyFrameChunker{
			Demuxer: demuxer,
			state:   INITIAL,
			durMin:  minSegmentDuration,
		},
		muxer: ts.NewMuxer(nil),
	}
}

func (sd *SegmentDemuxer) WriteNext(w io.Writer) (SegmentInfo, error) {
	sd.muxer = ts.NewMuxer(w)
	si := SegmentInfo{}
	err := avutil.CopyFile(sd.muxer, sd.chunker)

	if sd.chunker.currentDuration <= 0 {
		logrus.Error("bad duration:")
		si.Duration = 4
	} else {
		si.Duration = sd.chunker.currentDuration
	}

	if err != nil {
		return si, errors.Wrap(err, "error on copy")
	}

	if streams, err := sd.Streams(); err == nil {
		if md, err := flv.NewMetadataByStreams(streams); err == nil {
			h, _ := md["height"]
			w, _ := md["width"]
			si.Height = h.(int)
			si.Width = w.(int)
		}
	}

	if sd.chunker.state == FINISHED || sd.chunker.state == ERROR {
		return si, io.EOF
	}

	return si, nil
}

func (sd *SegmentDemuxer) Streams() ([]av.CodecData, error) {
	return sd.chunker.Streams()
}
