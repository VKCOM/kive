package hls_server

import "fmt"

type LivePlaylistRequest struct {
	Application string `mapstructure:"app"`
	StreamName  string `mapstructure:"stream_name"`
	ViewSalt    string `mapstructure:"view_salt"`
	StreamType  string `mapstructure:"stream_type"`
}

type DvrPlaylistRequest struct {
	Application string `mapstructure:"app"`
	StreamName  string `mapstructure:"stream_name"`
	From        int64  `mapstructure:"from"`
	Duration    int64  `mapstructure:"duration"`
	StreamType  string `mapstructure:"stream_type"`
}

type ChunkRequest struct {
	Application     string `mapstructure:"app",json:"application"`
	StreamName      string `mapstructure:"stream_name",json:"stream_name"`
	StreamNameChunk string `mapstructure:"stream_name_chunk",json:"stream_chunk_name"`
	ChunkName       string `mapstructure:"chunk_name",json:"chunk_name"`
	ViewSalt        string `mapstructure:"view_salt",json:"-"`
}

type LiveChunkRequest ChunkRequest
type DvrChunkRequest ChunkRequest

func (cr *DvrChunkRequest) Key() string {
	return fmt.Sprintf("%s/%s/%s/%s", cr.Application, cr.StreamName, cr.StreamNameChunk, cr.ChunkName)
}

type VodManifestRequest DvrPlaylistRequest
type VodChunkRequest struct {
	StreamType string `mapstructure:"stream_type"`
	StreamName string `mapstructure:"stream_name"`
	ChunkName  string `mapstructure:"chunk_name"`
}
