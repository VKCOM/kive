package ktypes

import (
	"github.com/VKCOM/joy4/av"
	"io"
)

type Abr interface {
	Init(desiredOutputSizes []int, incomingStream av.Demuxer, streamName string) ([]AbrDemuxer, []int, error)
	av.PacketWriter
	io.Closer
}

type AbrDemuxer interface {
	Size() int
	Desc() string
	av.DemuxCloser
}
