package hls_server

import (
	"fmt"
	"github.com/VKCOM/kive/ktypes"
	"github.com/VKCOM/kive/vsync"
	"github.com/gorilla/mux"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"io"
	"net/http"
	"net/http/pprof"
	"reflect"
	"strconv"
	"strings"
	"time"
)

func LogHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logrus.Debugf("req: %+v,  map: %+v, %+v", r.RequestURI, mux.Vars(r), r)
		next.ServeHTTP(w, r)
		logrus.Infof("req: %+v, response headers %+v", r, w.Header())
	})
}

type HttpResponse struct {
	HttpStatus int
	Reader     io.ReadCloser
}

type LiveHls struct {
	httpServer *http.Server
	httpRouter *mux.Router

	config LiveHlsConfig

	HandleLivePlaylist func(*LivePlaylistRequest) (HttpResponse, error)
	HandleLiveChunk    func(*LiveChunkRequest) (HttpResponse, error)
	HandleDvrPlayList  func(*DvrPlaylistRequest) (HttpResponse, error)
	HandleDvrChunk     func(*DvrChunkRequest) (HttpResponse, error)
	HandleVodManifest  func(*VodManifestRequest) (HttpResponse, error)
	HandleVodChunk     func(*VodChunkRequest) (HttpResponse, error)

	HandleRtmpHealth func(duration time.Duration) bool

	VodManifestMutex  *vsync.Semaphore
	VodChunkMutex     *vsync.Semaphore
	dvrPlaylistMutex  *vsync.Semaphore
	dvrChunkMutex     *vsync.Semaphore
	livePlaylistMutex *vsync.Semaphore
	liveChunkMutex    *vsync.Semaphore
}

func parseRequest(req interface{}, r *http.Request) error {
	vars := mux.Vars(r)
	if err := mapstructure.WeakDecode(vars, req); err != nil {
		return errors.Wrapf(err, "error parsing %+v, on %+v", req, vars)
	}
	logrus.Debugf("Request parse %+v", req)
	return nil
}

func (lhls *LiveHls) handleReqTyped(req interface{}) (HttpResponse, error) {
	switch v := req.(type) {
	case *VodChunkRequest:
		return func() (HttpResponse, error) {
			if !lhls.VodChunkMutex.TryLock(time.Second * 20) {
				return HttpResponse{HttpStatus: http.StatusRequestTimeout}, errors.New("timeout")
			}
			defer lhls.VodChunkMutex.Unlock()

			return lhls.HandleVodChunk(req.(*VodChunkRequest))
		}()
	case *VodManifestRequest:
		return func() (HttpResponse, error) {
			if !lhls.VodManifestMutex.TryLock(time.Second * 10) {
				return HttpResponse{HttpStatus: http.StatusRequestTimeout}, errors.New("timeout")
			}
			defer lhls.VodManifestMutex.Unlock()

			return lhls.HandleVodManifest(req.(*VodManifestRequest))
		}()
	case *DvrChunkRequest:
		return func() (HttpResponse, error) {
			if !lhls.dvrChunkMutex.TryLock(time.Second * 10) {
				return HttpResponse{HttpStatus: http.StatusRequestTimeout}, errors.New("timeout")
			}
			defer lhls.dvrChunkMutex.Unlock()

			return lhls.HandleDvrChunk(req.(*DvrChunkRequest))
		}()
	case *DvrPlaylistRequest:
		return func() (HttpResponse, error) {
			if !lhls.dvrPlaylistMutex.TryLock(time.Second * 10) {
				return HttpResponse{HttpStatus: http.StatusRequestTimeout}, errors.New("timeout")
			}
			defer lhls.dvrPlaylistMutex.Unlock()

			return lhls.HandleDvrPlayList(req.(*DvrPlaylistRequest))
		}()
	case *LiveChunkRequest:
		return func() (HttpResponse, error) {
			if !lhls.liveChunkMutex.TryLock(time.Second * 10) {
				return HttpResponse{HttpStatus: http.StatusRequestTimeout}, errors.New("timeout")
			}
			defer lhls.liveChunkMutex.Unlock()

			return lhls.HandleLiveChunk(req.(*LiveChunkRequest))
		}()
	case *LivePlaylistRequest:
		return func() (HttpResponse, error) {
			if !lhls.livePlaylistMutex.TryLock(time.Second * 15) {
				return HttpResponse{HttpStatus: http.StatusRequestTimeout}, errors.New("timeout")
			}
			logrus.Infof("%+v", req.(*LivePlaylistRequest).StreamName)
			logrus.Infof("%+v", req.(*LivePlaylistRequest).StreamType)
			defer lhls.livePlaylistMutex.Unlock()

			return lhls.HandleLivePlaylist(req.(*LivePlaylistRequest))
		}()
	default:
		return HttpResponse{
			HttpStatus: http.StatusInternalServerError,
			Reader:     nil,
		}, errors.Errorf("unknown type %+v", v)
	}
}

func (lhls *LiveHls) handleReq(req interface{}, w http.ResponseWriter, r *http.Request) error {
	methodName := reflect.TypeOf(req).String()

	err := parseRequest(req, r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		ktypes.Stat(true, "http_handle", methodName, http.StatusText(http.StatusBadRequest))
		return err
	}

	res, err := lhls.handleReqTyped(req)

	if err != nil && res.HttpStatus == 0 {
		ktypes.Stat(true, "http_handle", methodName, http.StatusText(http.StatusBadRequest))
		w.WriteHeader(http.StatusBadRequest)
		return err
	} else if err != nil && res.HttpStatus != 0 {
		ktypes.Stat(true, "http_handle", methodName, http.StatusText(res.HttpStatus))
		w.WriteHeader(res.HttpStatus)
		return err
	}

	w.WriteHeader(res.HttpStatus)
	if res.Reader != nil {
		_, err = io.Copy(w, res.Reader)
	}
	if err != nil {
		ktypes.Stat(true, "http_handle", methodName, http.StatusText(http.StatusInternalServerError))
		logrus.Errorf("Bad response %+v", res)
		return err
	}
	ktypes.Stat(false, "http_handle", methodName, http.StatusText(http.StatusOK))

	return nil
}

func NewLiveHls(config LiveHlsConfig) (*LiveHls, error) {
	httpRouter := mux.NewRouter()
	httpRouter.Use(LogHandler)

	lhls := &LiveHls{
		config:            config,
		httpRouter:        httpRouter,
		VodManifestMutex:  vsync.NewSemaphore(1, 4),
		VodChunkMutex:     vsync.NewSemaphore(2, 3),
		dvrPlaylistMutex:  vsync.NewSemaphore(2, 20),
		dvrChunkMutex:     vsync.NewSemaphore(2, 40),
		livePlaylistMutex: vsync.NewSemaphore(50, 300),
		liveChunkMutex:    vsync.NewSemaphore(20, 300),
	}

	livePlaylistHandler := func(w http.ResponseWriter, r *http.Request) {
		req := &LivePlaylistRequest{}
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		lhls.handleReq(req, w, r)
	}

	liveChunkHandler := func(w http.ResponseWriter, r *http.Request) {
		req := &LiveChunkRequest{}
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "video/m2ts")
		lhls.handleReq(req, w, r)
	}

	dvrPlaylistHandler := func(w http.ResponseWriter, r *http.Request) {
		req := &DvrPlaylistRequest{}
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		lhls.handleReq(req, w, r)
	}

	dvrChunkHandler := func(w http.ResponseWriter, r *http.Request) {
		req := &DvrChunkRequest{}
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "video/m2ts")
		lhls.handleReq(req, w, r)
	}

	httpRouter.Path(lhls.config.HandleLivePlaylistUrl()).Queries("vid", "{view_salt}", "offset", "{offset}").Name("LivePlaylistVid").HandlerFunc(livePlaylistHandler)
	httpRouter.Path(lhls.config.HandleAbrMasterPlaylistUrl()).Queries("vid", "{view_salt}", "offset", "{offset}").Name("LivePlaylistVid").HandlerFunc(livePlaylistHandler)
	httpRouter.Path(lhls.config.HandleAbrChunksPlaylistUrl()).Queries("vid", "{view_salt}", "offset", "{offset}").Name("LivePlaylistVid").HandlerFunc(livePlaylistHandler)

	httpRouter.Path(lhls.config.HandleLivePlaylistUrl()).Queries("offset", "{offset}").Name("LivePlaylistVid").HandlerFunc(livePlaylistHandler)
	httpRouter.Path(lhls.config.HandleAbrMasterPlaylistUrl()).Queries("offset", "{offset}").Name("LivePlaylistVid").HandlerFunc(livePlaylistHandler)
	httpRouter.Path(lhls.config.HandleAbrChunksPlaylistUrl()).Queries("offset", "{offset}").Name("LivePlaylistVid").HandlerFunc(livePlaylistHandler)

	httpRouter.Path(lhls.config.HandleLivePlaylistUrl()).Queries("vid", "{view_salt}").Name("LivePlaylistVid").HandlerFunc(livePlaylistHandler)
	httpRouter.Path(lhls.config.HandleAbrMasterPlaylistUrl()).Queries("vid", "{view_salt}").Name("LivePlaylistVid").HandlerFunc(livePlaylistHandler)
	httpRouter.Path(lhls.config.HandleAbrChunksPlaylistUrl()).Queries("vid", "{view_salt}").Name("LivePlaylistVid").HandlerFunc(livePlaylistHandler)

	httpRouter.HandleFunc(lhls.config.HandleLivePlaylistUrl(), livePlaylistHandler).Name("LivePlaylist")
	httpRouter.HandleFunc(lhls.config.HandleAbrChunksPlaylistUrl(), livePlaylistHandler).Name("LivePlaylist")
	httpRouter.HandleFunc(lhls.config.HandleAbrMasterPlaylistUrl(), livePlaylistHandler).Name("LivePlaylist")

	httpRouter.HandleFunc(lhls.config.HandleDvrPlaylistUrl(), dvrPlaylistHandler).Name("DvrPlaylist")
	httpRouter.HandleFunc(lhls.config.HandleDvrAbrPlaylistUrl(), dvrPlaylistHandler).Name("DvrPlaylist")

	httpRouter.HandleFunc(lhls.config.HandleDvrChunkUrl(), dvrChunkHandler).Name("DvrChunkUrl")
	httpRouter.HandleFunc(lhls.config.HandleDvrAbrChunkUrl(), dvrChunkHandler).Name("DvrChunkUrl")

	httpRouter.HandleFunc(lhls.config.HandleLiveChunkUrl(), liveChunkHandler).Name("LiveChunk")
	httpRouter.HandleFunc(lhls.config.HandleAbrChunkUrl(), liveChunkHandler).Name("LiveChunk")

	httpRouter.HandleFunc(lhls.config.HandleVodManifestUrl(), func(w http.ResponseWriter, r *http.Request) {
		req := &VodManifestRequest{}
		w.Header().Set("Content-Type", "text/plain")
		lhls.handleReq(req, w, r)
	}).Name("VodManifest")

	httpRouter.HandleFunc(lhls.config.HandleVodChunkUrl(), func(w http.ResponseWriter, r *http.Request) {
		ktypes.Stat(false, "chunk", "vod", "")
		req := &VodChunkRequest{}
		w.Header().Set("Content-Type", "video/m2ts")
		lhls.handleReq(req, w, r)
	}).Name("VodChunk")

	pprofr := httpRouter.PathPrefix("/debug/pprof").Subrouter()
	pprofr.HandleFunc("/", pprof.Index)
	pprofr.HandleFunc("/cmdline", pprof.Cmdline)
	pprofr.HandleFunc("/symbol", pprof.Symbol)
	pprofr.HandleFunc("/trace", pprof.Trace)

	profile := pprofr.PathPrefix("/profile").Subrouter()
	profile.HandleFunc("", pprof.Profile)
	profile.Handle("/goroutine", pprof.Handler("goroutine"))
	profile.Handle("/threadcreate", pprof.Handler("threadcreate"))
	profile.Handle("/heap", pprof.Handler("heap"))
	profile.Handle("/block", pprof.Handler("block"))
	profile.Handle("/mutex", pprof.Handler("mutex"))

	httpRouter.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		ktypes.Stat(false, "health", "", "")
		if !lhls.dvrPlaylistMutex.TryLock(10 * time.Second) {
			w.WriteHeader(http.StatusRequestTimeout)
			w.Write([]byte("dvrPlaylistMutex"))
			ktypes.Stat(true, "health", "dvrPlaylistMutex", "")
			return
		}
		lhls.dvrPlaylistMutex.Unlock()

		if !lhls.dvrChunkMutex.TryLock(10 * time.Second) {
			w.WriteHeader(http.StatusRequestTimeout)
			w.Write([]byte("dvrChunkMutex"))
			ktypes.Stat(true, "health", "dvrChunkMutex", "")
			return
		}
		lhls.dvrChunkMutex.Unlock()

		if !lhls.livePlaylistMutex.TryLock(4 * time.Second) {
			w.WriteHeader(http.StatusRequestTimeout)
			w.Write([]byte("livePlaylistMutex"))
			ktypes.Stat(true, "health", "livePlaylistMutex", "")
			return
		}
		lhls.livePlaylistMutex.Unlock()

		if !lhls.liveChunkMutex.TryLock(4 * time.Second) {
			w.WriteHeader(http.StatusRequestTimeout)
			w.Write([]byte("liveChunkMutex"))
			ktypes.Stat(true, "health", "liveChunkMutex", "")
			return
		}
		lhls.liveChunkMutex.Unlock()

		if !lhls.HandleRtmpHealth(10 * time.Second) {
			w.WriteHeader(http.StatusRequestTimeout)
			w.Write([]byte("HandleRtmpHealth"))
			ktypes.Stat(true, "health", "HandleRtmpHealth", "")
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Ok"))
	})

	httpRouter.HandleFunc(lhls.config.HandleLivePlayerUrl(), func(w http.ResponseWriter, r *http.Request) {
		req := &LivePlaylistRequest{}
		err := parseRequest(req, r)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
		}
		player := NewPlayerPage()
		player.Port = config.HttpPort
		player.Application = req.Application
		player.StreamName = req.StreamName
		w.WriteHeader(http.StatusOK)
		player.ComposePlayerPage(w)
	})

	httpServer := &http.Server{
		Addr:         fmt.Sprintf("%s:%d", lhls.config.HttpHost, lhls.config.HttpPort),
		Handler:      httpRouter,
		WriteTimeout: time.Second * 30,
		ReadTimeout:  time.Second * 30,
		IdleTimeout:  time.Second * 30,
	}

	lhls.httpServer = httpServer
	return lhls, nil
}

func (lhls *LiveHls) BuildLiveChunkName(r *LiveChunkRequest) string {
	res := strings.Replace(lhls.config.LiveChunk, "{stream_name_chunk}", r.StreamNameChunk, -1)
	return strings.Replace(res, "{chunk_name}", r.ChunkName, -1)
}

func (lhls *LiveHls) BuildDvrChunkName(r *DvrChunkRequest) string {
	res := strings.Replace(lhls.config.DvrChunk, "{stream_name_chunk}", r.StreamNameChunk, -1)
	return strings.Replace(res, "{chunk_name}", r.ChunkName, -1)
}

func (lhls *LiveHls) BuildDvrChunksPlaylist(r *DvrPlaylistRequest) string {
	res := strings.Replace(lhls.config.DvrPlaylist, "{from:[0-9]+}", strconv.FormatInt(r.From, 10), -1)
	res = strings.Replace(res, "{duration:[0-9]+}", strconv.FormatInt(r.Duration, 10), -1)
	return strings.Replace(res, "{playlist|chunks}", "chunks", -1)
}

//func (lhls *LiveHls) BuildVodManifestChunkName(r *VodChunkRequest) string {
//	res := strings.Replace(lhls.config.VodChunk, "{chunk_name}", r.ChunkName, -1)
//	return res
//}

func (lhls *LiveHls) Listen() error {
	go func() {
		err := lhls.httpServer.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			logrus.Panicf("cannot listen and serve http %+v", err)
		}
	}()
	return nil
}

func (lhls *LiveHls) Serve() error {
	return nil
}

func (lhls *LiveHls) Stop() error {
	return lhls.httpServer.Close()

}
