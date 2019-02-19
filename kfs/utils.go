package kfs

import (
	"fmt"
	"github.com/VKCOM/kive/ktypes"
	"path"
)

func (config KfsConfig) buildFullPath(key innerStorage) string {
	return fmt.Sprintf("%s/%s", config.Basedir, key.buildRelative())
}

func (config KfsConfig) buildDirPath(key innerStorage) string {
	return fmt.Sprintf("%s/%s", config.Basedir, key.buildDir())
}

func (config KfsConfig) buildMetadataPath(key innerStorage) string {
	return path.Join(config.Basedir, key.buildDir(), "metadata.json")
}

func (config KfsConfig) buildMetadataPathStreamType(key innerStorage, streamType ktypes.StreamType) string {
	return path.Join(config.Basedir, key.buildDirStreamType(streamType), "metadata.json")
}

func (config KfsConfig) buildMetadataPathPlain(streamName, sourceName, hour string) string {
	return path.Join(config.Basedir, streamName, sourceName, hour, "metadata.json")
}
