package ktypes

import (
	"encoding/base32"
	"fmt"
	"github.com/pkg/errors"
	"strings"
	"time"
)

type StreamType string

const (
	SOURCE           StreamType = "source"
	DEFAULT_DURATION            = 4 * time.Second
)

type UnixMs int64
type ChunkKey struct {
	Ts                UnixMs     `json:"ts"`
	SeqId             int64      `json:"seqId"`
	StreamName        string     `json:"stream_name"`
	StreamType        StreamType `json:"stream_type"`
	Virtual           bool       `json:"virtual"`
	VirtualStreamType StreamType `json:"virtual_stream_type"`
}

func (ck *ChunkKey) StreamName32Enc() string {
	return strings.Replace(base32.StdEncoding.EncodeToString([]byte(ck.StreamName)), "=", "", -1)
}

func (ck *ChunkKey) HourString() string {
	return HourString(ck.Ts)
}

func (ck *ChunkKey) Duration() time.Duration {
	return time.Duration(ck.Ts) * time.Millisecond
}

func HourString(ts UnixMs) string {
	hour := int64(time.Duration(ts)*time.Millisecond) / int64(time.Hour)
	return fmt.Sprintf("%015d", hour)
}

func HoursRange(from, to UnixMs) []string {
	fromHr := int64(time.Duration(from)*time.Millisecond) / int64(time.Hour)
	toHr := int64(time.Duration(to)*time.Millisecond) / int64(time.Hour)

	result := make([]string, 0)
	if fromHr > toHr {
		return nil
	}

	result = append(result, fmt.Sprintf("%015d", fromHr))
	var i int64
	for i = 0; i < toHr-fromHr; i++ {
		result = append(result, fmt.Sprintf("%015d", fromHr+i+1))
	}
	return result
}

func ComposeHourKeyCache(streamName, sourceName, hourName string) string {
	return fmt.Sprintf("%s/%s/%s", streamName, sourceName, hourName)
}

type ChunkInfo struct {
	ChunkKey
	StreamDesc    string        `json:"stream_desc"`
	Discontinuity int           `json:"disc"`
	Rand          int           `json:"rand"`
	Size          int           `json:"size"`     //not used in path
	Duration      time.Duration `json:"duration"` //not used in path
}

func NewChunkInfo(streamName string, sourceType StreamType) ChunkInfo {
	key := ChunkInfo{
		ChunkKey: ChunkKey{
			StreamType: sourceType,
			StreamName: streamName,
		},
		Discontinuity: 1,
	}
	return key
}

func (ck *ChunkInfo) StreamDesc32Enc() string {
	return strings.Replace(base32.StdEncoding.EncodeToString([]byte(ck.StreamDesc)), "=", "", -1)
}

func (ck *ChunkKey) Less(rhs ChunkKey) bool {
	nameLess := strings.Compare(ck.StreamName, rhs.StreamName)
	if nameLess < 0 {
		return true
	} else if nameLess > 0 {
		return false
	}
	typeLess := strings.Compare(string(ck.StreamType), string(rhs.StreamType))
	if typeLess < 0 {
		return true
	} else if typeLess > 0 {
		return false
	}

	if ck.Ts < rhs.Ts {
		return true
	} else if ck.Ts > rhs.Ts {
		return false
	}

	if ck.SeqId < rhs.SeqId {
		return true
	} else if ck.SeqId > rhs.SeqId {
		return false
	}

	return false
}

type SChunkInfo []ChunkInfo

func (p SChunkInfo) Len() int           { return len(p) }
func (p SChunkInfo) Less(i, j int) bool { return p[i].ChunkKey.Less(p[j].ChunkKey) }
func (p SChunkInfo) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func TimeToMillis(ts time.Time) UnixMs {
	return UnixMs(int64(ts.UnixNano()) / int64(time.Millisecond))
}

func (key ChunkInfo) BuildChunkName() string {
	return fmt.Sprintf("%015d_%015d_%s_%s_d%d_%s_r%d.ts", key.Ts, 0, key.StreamName32Enc(), key.StreamType, key.Discontinuity, key.StreamDesc32Enc(), key.Rand)
}

func (key ChunkInfo) BuildVodChunkName() string {
	return fmt.Sprintf("%015d_%015d_%s_d%d_%s_r%d.ts", key.Ts, 0, key.StreamType, key.Discontinuity, key.StreamDesc32Enc(), key.Rand)
}

func ParseChunkName(chunkNameEncoded string) (ChunkInfo, error) {
	chunkNameEncoded = strings.Replace(chunkNameEncoded, "_", " ", -1)
	key := ChunkInfo{}
	var (
		streamNameEnc32 []byte
		streamDescEnc32 []byte
	)
	n, err := fmt.Sscanf(chunkNameEncoded, "%015d 000000000000000 %s %s d%d %s r%d.ts", &key.Ts, &streamNameEnc32, &key.StreamType, &key.Discontinuity, &streamDescEnc32, &key.Rand)
	if n != 6 || err != nil {
		return key, errors.Wrapf(err, "cannot parse %+s", chunkNameEncoded)
	}

	padding := ""
	switch len(streamNameEnc32) % 8 {
	case 2:
		padding = "======"
	case 4:
		padding = "===="
	case 5:
		padding = "==="
	case 7:
		padding = "=="
	}
	streamNameEnc32 = append(streamNameEnc32, []byte(padding)...)

	streamName, err := base32.StdEncoding.DecodeString(string(streamNameEnc32))
	if err != nil {
		return key, errors.Wrap(err, "cannot decode")
	}
	key.StreamName = string(streamName)

	padding = ""
	switch len(streamDescEnc32) % 8 {
	case 2:
		padding = "======"
	case 4:
		padding = "===="
	case 5:
		padding = "==="
	case 7:
		padding = "=="
	}
	streamDescEnc32 = append(streamDescEnc32, []byte(padding)...)

	streamDesc, err := base32.StdEncoding.DecodeString(string(streamDescEnc32))
	if err != nil {
		return key, errors.Wrap(err, "cannot decode")
	}
	key.StreamDesc = string(streamDesc)

	return key, nil
}
