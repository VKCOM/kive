package main

import (
	"flag"
	"github.com/sirupsen/logrus"
	"github.com/VKCOM/kive/ktypes"
	"github.com/VKCOM/kive/noop_api"
	"github.com/VKCOM/kive/worker"
	"os"
	"os/signal"
	"syscall"
)

func init() {
	logrus.SetLevel(logrus.DebugLevel)
	logrus.Info("Initializing kive")
	ktypes.ApiInst = &noop_api.NoopApi{}
}

func main() {
	configPath := flag.String("config", "default", "configuration path")
	flag.Parse()

	c := worker.NewConfig(*configPath)

	err := ktypes.ApiInst.Serve()

	if err != nil {
		logrus.Panic("Cannot start api ", err)
	}

	w, err := worker.NewWorker(c)
	if err != nil {
		logrus.Panic("Cannot create worker ", err)
	}

	err = w.Listen()
	if err != nil {
		logrus.Panic("Cannot listen worker ", err)
	}

	err = w.Serve()
	if err != nil {
		logrus.Panic("Cannot serve worker ", err)
	}

	sigch := make(chan os.Signal)
	signal.Notify(sigch, syscall.SIGINT, syscall.SIGTERM)
	logrus.Info(<-sigch)
	w.Stop()
}
