package rtmp_server

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParsePublishUrl(t *testing.T) {
	s := RtmpServer{
		config: NewRtmpServerConfig(),
	}

	tests := []struct {
		url      string
		expected PublishRequest
	}{
		{
			url: "/source?publishsign=kek/gopro",
			expected: PublishRequest{
				Application:        "source",
				IncomingStreamName: "gopro",
				Params: map[string]string{
					"app":                  "source",
					"publishsign":          "kek",
					"incoming_stream_name": "gopro",
				},
			},
		},
		{
			url: "/live?publishsign=aWQ9aHE3dnR3dGJaQmVKR2dRYiZzaWduPWRNMG02d0YvZVM3Uy9YYkZ0YjNNRGc9PQ==/hq7vtwtbZBeJGgQb",
			expected: PublishRequest{
				Application:        "live",
				IncomingStreamName: "hq7vtwtbZBeJGgQb",
				Params: map[string]string{
					"app": "live",
					"incoming_stream_name": "hq7vtwtbZBeJGgQb",
					"publishsign":          "aWQ9aHE3dnR3dGJaQmVKR2dRYiZzaWduPWRNMG02d0YvZVM3Uy9YYkZ0YjNNRGc9PQ==",
				},
			},
		},
	}

	failingTests := []string{
		"awful_url",
		"/123?noStreamName=value",
	}

	for _, test := range tests {
		r, err := s.parsePublishRequest(test.url)
		assert.NoError(t, err)
		assert.Equal(t, test.expected, *r)
	}

	for _, test := range failingTests {
		_, err := s.parsePublishRequest(test)
		assert.Error(t, err)
	}
}
