package hls_server

import (
	"github.com/gorilla/mux"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"net/http"
	"net/http/httptest"
)

func TestLiveHls_LivePlaylistPattern(t *testing.T) {
	r := mux.NewRouter()
	c := NewLiveHlsConfig()
	rr := httptest.NewRecorder()
	called := false
	h := func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"alive": true}`)
		called = true
		v := mux.Vars(r)
		require.Equal(t, "", v["view_salt"])
	}
	r.Path(c.HandleLivePlaylistUrl()).Queries("vid", "{view_salt}").Name("LivePlaylistVid").HandlerFunc(h)
	r.HandleFunc(c.HandleLivePlaylistUrl(), h).Name("LivePlaylist")

	req, _ := http.NewRequest("GET", "/live/aa/playlist.m3u8", nil)
	r.ServeHTTP(rr, req)
	logrus.Debug(rr)
	assert.True(t, called)
}

func TestLiveHls_LivePlaylistPatternVid(t *testing.T) {
	r := mux.NewRouter()
	c := NewLiveHlsConfig()
	rr := httptest.NewRecorder()
	called := false
	h := func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"alive": true}`)
		called = true
		v := mux.Vars(r)
		require.Equal(t, "10", v["view_salt"])
	}
	r.Path(c.HandleLivePlaylistUrl()).Queries("vid", "{view_salt}").Name("LivePlaylistVid").HandlerFunc(h)
	r.HandleFunc(c.HandleLivePlaylistUrl(), h).Name("LivePlaylist")

	req, _ := http.NewRequest("GET", "/live/aa/playlist.m3u8?vid=10", nil)
	r.ServeHTTP(rr, req)
	logrus.Debug(rr)
	assert.True(t, called)

}
