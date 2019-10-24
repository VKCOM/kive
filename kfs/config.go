package kfs

import "time"

type duration struct {
	time.Duration
}

func (d *duration) UnmarshalText(text []byte) (err error) {
	d.Duration, err = time.ParseDuration(string(text))

	return
}

type KfsConfig struct {
	Basedir       string
	MaxSize       int
	MaxSourceSize int
	RemovalTime   duration
	MaxCacheSize  int64
	WriteTimeout  duration
}

func NewKfsConfig() KfsConfig {
	return KfsConfig{
		Basedir:       "/tmp/kive/",
		MaxSize:       1024 * 1024 * 20,
		MaxSourceSize: 1024 * 1024 * 80,
		RemovalTime:   duration{24*1*time.Hour + 14*time.Hour},
		MaxCacheSize:  5000000,
		WriteTimeout:  duration{1 * time.Second},
	}
}
