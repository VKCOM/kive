package kfs

import (
	"fmt"
	"github.com/VKCOM/kive/ktypes"
	"github.com/pkg/errors"
	"regexp"
	"sort"
	"strings"
)

var (
	isValidName = regexp.MustCompile(`^[[:alnum:]|[_-]{2,}$`).MatchString
)

type innerStorage ktypes.ChunkInfo

func (key *innerStorage) buildRelative() string {
	return fmt.Sprintf("%s/%s/%s/%015d.%s.%s.%d.%s.%d.ts", key.StreamName, key.StreamType, key.HourString(), key.Ts, key.StreamName, key.StreamType, key.Discontinuity, key.StreamDesc, key.Rand)
}

func (key *innerStorage) buildDir() string {
	return fmt.Sprintf("%s/%s/%s", key.StreamName, key.StreamType, key.HourString())
}

func (key *innerStorage) buildDirStreamType(streamType ktypes.StreamType) string {
	return fmt.Sprintf("%s/%s/%s", key.StreamName, streamType, key.HourString())
}

func validateStorageInfo(info innerStorage) error {
	if !isValidName(info.StreamDesc) {
		return errors.Errorf("bad filename")
	}
	return validateStorageKey(info.ChunkKey)
}

func validateStorageKey(key ktypes.ChunkKey) error {
	if key.Ts == 0 {
		return errors.Errorf("bad time")
	}

	if !isValidName(key.StreamName) {
		return errors.Errorf("bad stream name")
	}
	return nil
}

func parseStorageKey(fname string) (innerStorage, error) {
	fname = strings.Replace(fname, ".", " ", -1)
	key := innerStorage{}
	streamType := ""
	n, err := fmt.Sscanf(fname, "%015d_%015d %s %s %d %s %d ts", &key.Ts, &key.SeqId, &key.StreamName, &streamType, &key.Discontinuity, &key.StreamDesc, &key.Rand)
	key.StreamType = ktypes.StreamType(streamType)
	if n != 7 || err != nil {
		return key, errors.Wrapf(err, "%d cannot parse +%s", n, fname)
	}
	err = validateStorageInfo(key)
	if err != nil {
		return key, errors.Wrapf(err, "cannot parse +%s", fname)
	}
	key.Duration = ktypes.DEFAULT_DURATION
	return key, nil
}

func SearchSstorage(a ktypes.SChunkInfo, x innerStorage) int {
	return sort.Search(len(a), func(i int) bool { return !a[i].ChunkKey.Less(x.ChunkKey) })
}
