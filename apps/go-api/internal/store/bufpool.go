package store

import (
	"bytes"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"
)

var bufPool = sync.Pool{
	New: func() any {
		return bytes.NewBuffer(make([]byte, 0, 16<<10))
	},
}

func getBuf() *bytes.Buffer {
	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	return buf
}

func putBuf(buf *bytes.Buffer) {
	if buf.Cap() > 1<<20 {
		return
	}
	bufPool.Put(buf)
}

func newWriteHTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			Proxy:               http.ProxyFromEnvironment,
			MaxIdleConns:        64,
			MaxIdleConnsPerHost: 64,
			MaxConnsPerHost:     64,
			IdleConnTimeout:     90 * time.Second,
			DisableCompression:  true,
			WriteBufferSize:     64 << 10,
			ReadBufferSize:      8 << 10,
		},
	}
}

func closeHTTP(resp *http.Response) {
	if resp == nil {
		return
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
}

func appendUint(buf *bytes.Buffer, v uint64) {
	var tmp [20]byte
	buf.Write(strconv.AppendUint(tmp[:0], v, 10))
}

func appendInt(buf *bytes.Buffer, v int64) {
	var tmp [20]byte
	buf.Write(strconv.AppendInt(tmp[:0], v, 10))
}

// ILP floats need a decimal (`1.0`, not `1`).
func appendILPFloat(buf *bytes.Buffer, v float64) {
	var tmp [32]byte
	b := strconv.AppendFloat(tmp[:0], v, 'g', -1, 64)
	buf.Write(b)
	for _, c := range b {
		if c == '.' || c == 'e' || c == 'E' {
			return
		}
	}
	buf.WriteString(".0")
}
