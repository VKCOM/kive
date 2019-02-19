package media

import (
	"fmt"
	"github.com/VKCOM/joy4/format/flv"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestSplitMuxing(t *testing.T) {
	_, filename, _, _ := runtime.Caller(0)

	dirname := filepath.Dir(filename)
	rpath := dirname + "/../../test_data/t2.flv"
	rpathAnother := dirname + "/../../test_data/l1.flv"

	rfile, err := os.Open(rpath)
	logrus.Info(rpath, err)

	flvReader := flv.NewDemuxer(rfile)
	chunker := NewSegmentDemuxer(flvReader, 3100*time.Millisecond)

	path, _ := ioutil.TempDir("", "kive_chunker")
	os.MkdirAll(filepath.Dir(path), os.ModePerm)

	filelist := make([]string, 0)
	ts := []int{4939000000, 5436000000, 5383000000, 4279000000}
	for i := 0; ; i += 1 {
		wpath := fmt.Sprintf("%s/%d.ts", path, i)
		logrus.Info(wpath)
		wfile, _ := os.Create(wpath)
		filelist = append(filelist, fmt.Sprintf("%d.ts", i))
		si, err := chunker.WriteNext(wfile)
		require.Equal(t, ts[i], int(si.Duration))

		assert.Equal(t, 1280, si.Width)
		assert.Equal(t, 720, si.Height)
		wfile.Close()
		if err == io.EOF {
			break
		} else if err != nil {
			logrus.Infof("%+v", err)
			break
		}
	}

	list := ""
	for _, str := range filelist {
		list += "file '" + str + "'\n"
	}

	lfile := fmt.Sprintf("%s/list.txt", path)

	fullOutfile := fmt.Sprintf("%s/full.mp4", path)
	ioutil.WriteFile(lfile, []byte(list), os.ModePerm)
	logrus.Info(fullOutfile)

	cmd, err := exec.Command("ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", lfile, "-c", "copy", fullOutfile).CombinedOutput()
	if err != nil {
		logrus.Errorf("%s %+v", cmd, err)
	}

	cmpExec := dirname + "/../../test_data/framemd5cmp"
	cmd, err = exec.Command(cmpExec, fullOutfile, rpath, path).CombinedOutput()

	assert.NoError(t, err)

	b, err := ioutil.ReadFile(path + "/full_vs_t2_diff.txt")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(b))

	cmd, err = exec.Command(cmpExec, fullOutfile, rpathAnother, path).CombinedOutput()
	b, err = ioutil.ReadFile(path + "/full_vs_l1_diff.txt")

	assert.NoError(t, err)
	assert.NotEqual(t, 0, len(b))

}
