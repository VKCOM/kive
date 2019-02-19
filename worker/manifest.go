package worker

import (
	"github.com/VKCOM/kive/hls_server"
	"github.com/VKCOM/kive/ktypes"
	"time"
)

type ManifestEntry struct {
	Fullname        string                     `json:"fullname"`
	DvrChunkName    string                     `json:"dvr_chunk_name"`
	Directory       string                     `json:"dir"`
	Duration        time.Duration              `json:"duration"`
	Size            int                        `json:"size"`
	DownloadUrl     string                     `json:"download_url"` //postfix after internal
	StorageUrl      string                     `json:"storage_url"`
	ChunkInfo       ktypes.ChunkInfo           `json:"chunk_info"`
	DvrChunkRequest hls_server.DvrChunkRequest `json:"dvr_request"`
}

type VodManifest struct {
	Playlist       string             `json:"playlist"`
	Chunks         []ManifestEntry    `json:"chunks"`
	StreamName     string             `json:"stream_name"`
	Duration       int64              `json:"duration"`
	From           int64              `json:"from"`
	DeletionChunks []ktypes.ChunkInfo `json:"deletion_chunks"`
}
