package hls_server

import (
	"github.com/VKCOM/kive/kassets"
	"github.com/sirupsen/logrus"
	"html/template"
	"io"
)

type PlayerPage struct {
	StreamName  string
	Application string
	Port        int
}

func NewPlayerPage() *PlayerPage {
	return &PlayerPage{}
}

func (p *PlayerPage) ComposePlayerPage(writer io.Writer) (string, error) {
	b, err := kassets.Asset("assets/index.html")
	if err != nil {
		logrus.Error("Player asset not found")
	}

	t, err := template.New("HLSPlayer").Parse(string(b))
	if err != nil {
		return "", err
	}
	t.Execute(writer, struct {
		Title       string
		Port        int
		Application string
		StreamName  string
	}{Title: "test", Port: p.Port, Application: p.Application, StreamName: p.StreamName})

	return "", nil
}
