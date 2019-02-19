package kfs

import "time"

type KfsConfig struct {
	Basedir       string
	MaxSize       int
	MaxSourceSize int
	RemovalTime   time.Duration
	MaxCacheSize  int64
	WriteTimeout  time.Duration
}

func NewKfsConfig() KfsConfig {
	return KfsConfig{
		Basedir:       "/tmp/kive/",
		MaxSize:       1024 * 1024 * 20,
		MaxSourceSize: 1024 * 1024 * 80,
		RemovalTime:   24*1*time.Hour + 14*time.Hour,
		MaxCacheSize:  5000000,
		WriteTimeout:  1 * time.Second,
	}
}
