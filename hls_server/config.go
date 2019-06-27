package hls_server

type LiveHlsConfig struct {
	HttpHost string
	HttpPort int

	LiveStreamPrefix      string
	AbrStreamPrefix       string
	AbrStreamMasterPrefix string
	LivePlaylist          string
	ChunksPlaylist        string
	LiveChunk             string

	DvrStreamPrefix string
	DvrPlaylist     string
	DvrChunk        string

	VodPrefix   string
	VodManifest string
	VodChunk    string

	Player string
}

func (c *LiveHlsConfig) HandleDvrPlaylistUrl() string {
	return c.DvrStreamPrefix + "/" + c.DvrPlaylist
}

func (c *LiveHlsConfig) HandleDvrAbrPlaylistUrl() string {
	return c.AbrStreamMasterPrefix + c.AbrStreamPrefix + "/" + c.DvrPlaylist
}

func (c *LiveHlsConfig) HandleLiveChunkUrl() string {
	return c.LiveStreamPrefix + "/" + c.LiveChunk
}

func (c *LiveHlsConfig) HandleAbrChunkUrl() string {
	return c.AbrStreamMasterPrefix + c.AbrStreamPrefix + "/" + c.LiveChunk
}

func (c *LiveHlsConfig) HandleDvrChunkUrl() string {
	return c.DvrStreamPrefix + "/" + c.DvrChunk
}

func (c *LiveHlsConfig) HandleDvrAbrChunkUrl() string {
	return c.AbrStreamMasterPrefix + c.AbrStreamPrefix + "/" + c.DvrChunk
}

func (c *LiveHlsConfig) HandleLivePlaylistUrl() string {
	return c.LiveStreamPrefix + "/" + c.LivePlaylist
}

func (c *LiveHlsConfig) HandleAbrChunksPlaylistUrl() string {
	return c.AbrStreamMasterPrefix + c.AbrStreamPrefix + "/" + c.ChunksPlaylist
}

func (c *LiveHlsConfig) HandleAbrMasterPlaylistUrl() string {
	return c.AbrStreamMasterPrefix + "/" + c.LivePlaylist
}

func (c *LiveHlsConfig) HandleVodManifestUrl() string {
	return c.VodPrefix + "/" + c.VodManifest
}

func (c *LiveHlsConfig) HandleVodChunkUrl() string {
	return c.VodPrefix + "/" + c.VodChunk
}

func (c *LiveHlsConfig) HandleLivePlayerUrl() string {
	return c.Player + c.AbrStreamMasterPrefix
}

func NewLiveHlsConfig() LiveHlsConfig {
	c := LiveHlsConfig{
		HttpHost: "",
		HttpPort: 8080,

		AbrStreamPrefix:       "/{stream_type:source|256p|352p|360p|384p|480p|512p|720p|1080p}",
		AbrStreamMasterPrefix: "/{app:kiveabr}/{stream_name:[0-9a-zA-Z_-]+}",
		LiveStreamPrefix:      "/{app:live|kiveabr}/{stream_name:[0-9a-zA-Z_-]+}",
		LiveChunk:             "{stream_name_chunk}-{chunk_name}",
		LivePlaylist:          "playlist.m3u8",
		ChunksPlaylist:        "chunks.m3u8",
		DvrStreamPrefix:       "/{app}/{stream_name}",
		DvrChunk:              "{stream_name_chunk}-dvr-{chunk_name}",
		DvrPlaylist:           "{playlist|chunks}_dvr_range-{from:[0-9]+}-{duration:[0-9]+}.m3u8",
		VodPrefix:             "/internal/{stream_type}/{stream_name}",
		VodChunk:              "{stream_name_chunk}-{chunk_name}",
		VodManifest:           "manifest_{stream_name}_{from:[0-9]+}_{duration:[0-9]+}.json",
		Player:                "/player",
	}
	return c
}
