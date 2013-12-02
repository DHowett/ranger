package partial

import (
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"strings"
	"sync"
)

const DefaultBlockSize int = 128 * 1024

type PartialHTTPReader struct {
	URL       *url.URL
	BlockSize int

	length             int64
	client             http.Client
	blocks             map[int][]byte
	mutex              sync.RWMutex
	initialized        bool
	etag, lastModified string

	off int64
}

type requestByteRange struct {
	block      int
	start, end int64
}

func (r requestByteRange) String() string {
	return fmt.Sprintf("%d-%d", r.start, r.end)
}

func (r *PartialHTTPReader) downloadRanges(ranges []requestByteRange) {
	if len(ranges) > 0 {
		rs := make([]string, len(ranges))
		for i, rng := range ranges {
			rs[i] = rng.String()
		}
		rangeString := strings.Join(rs, ",")

		req, _ := http.NewRequest("GET", r.URL.String(), nil)
		req.Header.Set("Range", fmt.Sprintf("bytes=%s", rangeString))
		if r.etag != "" {
			req.Header.Set("If-Range", r.etag)
		} else if r.lastModified != "" {
			req.Header.Set("If-Range", r.lastModified)
		}

		resp, _ := r.client.Do(req)
		typ, params, _ := mime.ParseMediaType(resp.Header.Get("Content-Type"))
		defer resp.Body.Close()

		if typ == "multipart/byteranges" {
			multipart := multipart.NewReader(resp.Body, params["boundary"])
			r.mutex.Lock()
			i := 0
			for {
				if part, err := multipart.NextPart(); err == nil {
					rng := ranges[i]
					bn := rng.block
					blocklen := (rng.end - rng.start) + 1
					r.blocks[bn] = make([]byte, blocklen)
					io.ReadFull(part, r.blocks[bn])
					i++
				} else {
					break
				}
			}
			r.mutex.Unlock()
		} else {
			r.mutex.Lock()
			bn := 0
			if resp.StatusCode == http.StatusPartialContent {
				// If we've received 206 Partial Content but no multipart parts,
				// we received a contiguous section starting at the first requested block.
				bn = ranges[0].block
			}
			body := make([]byte, r.length)
			io.ReadFull(resp.Body, body)
			for i := r.length; i > 0; i -= int64(r.BlockSize) {
				bs := i
				if bs > int64(r.BlockSize) {
					bs = int64(r.BlockSize)
				}

				r.blocks[bn] = make([]byte, bs)
				copy(r.blocks[bn], body[bn*r.BlockSize:bn*r.BlockSize+int(bs)])

				bn++
			}
			r.mutex.Unlock()
		}
	}
}

func (r *PartialHTTPReader) ReadAt(p []byte, off int64) (int, error) {
	if !r.initialized {
		err := r.init()
		if err != nil {
			return 0, err
		}
	}

	l := len(p)

	if off+int64(l) > r.length {
		return 0, errors.New("read beyond end of file")
	}

	block := int(off / int64(r.BlockSize))
	endBlock := int((off + int64(l)) / int64(r.BlockSize))
	endBlockOff := (off + int64(l)) % int64(r.BlockSize)
	nblocks := endBlock - block
	if endBlockOff > 0 {
		nblocks++
	}

	ranges := make([]requestByteRange, nblocks)
	nreq := 0
	r.mutex.RLock()
	for i := 0; i < nblocks; i++ {
		bn := block + i
		if _, ok := r.blocks[bn]; ok {
			continue
		}
		ranges[i] = requestByteRange{
			bn,
			int64(bn * r.BlockSize),
			int64(((bn + 1) * r.BlockSize) - 1),
		}
		if ranges[i].end > r.length {
			ranges[i].end = r.length
		}

		nreq++
	}
	r.mutex.RUnlock()
	ranges = ranges[:nreq]

	r.downloadRanges(ranges)
	return r.copyRangeToBuffer(p, off)
}

func (r *PartialHTTPReader) copyRangeToBuffer(p []byte, off int64) (int, error) {
	remaining := len(p)
	block := int(off / int64(r.BlockSize))
	startOffset := off % int64(r.BlockSize)
	ncopied := 0

	r.mutex.RLock()
	defer r.mutex.RUnlock()

	for remaining > 0 {
		copylen := r.BlockSize
		if copylen > remaining {
			copylen = remaining
		}

		// if we need to copy more bytes than exist in this block
		if startOffset+int64(copylen) > int64(r.BlockSize) {
			copylen = int(int64(r.BlockSize) - startOffset)
		}

		if _, ok := r.blocks[block]; !ok {
			return 0, errors.New("fu?")
		}
		copy(p[ncopied:ncopied+copylen], r.blocks[block][startOffset:])

		remaining -= copylen
		ncopied += copylen

		block++
		startOffset = 0
	}

	return ncopied, nil
}

func (r *PartialHTTPReader) Length() int64 {
	if !r.initialized {
		r.init()
	}
	return r.length
}

func (r *PartialHTTPReader) Read(p []byte) (int, error) {
	nread, err := r.ReadAt(p, r.off)
	r.off += int64(nread)
	return nread, err
}

func (r *PartialHTTPReader) Seek(off int64, whence int) (int64, error) {
	if off < 0 {
		return 0, errors.New("seek to negative offset!")
	}

	switch whence {
	case 0:
		if off > r.Length() {
			return 0, errors.New("seek beyond end of file")
		}
		r.off = off
	case 1:
		off = r.off + off
		if off > r.Length() {
			return 0, errors.New("seek beyond end of file")
		}
		r.off = off
	case 2:
		off = r.Length() - off
		if off < 0 {
			return 0, errors.New("seek beyond beginning of file")
		}
		r.off = off
	}
	return r.off, nil
}

func (r *PartialHTTPReader) init() error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	r.initialized = true
	r.blocks = make(map[int][]byte)
	if r.BlockSize == 0 {
		r.BlockSize = DefaultBlockSize
	}

	resp, _ := http.Head(r.URL.String())
	if resp.StatusCode == http.StatusNotFound {
		return errors.New("404")
	}

	if !strings.Contains(resp.Header.Get("Accept-Ranges"), "bytes") {
		return errors.New(r.URL.Host + " does not support byte-ranged requests.")
	}

	r.etag = resp.Header.Get("ETag")
	r.lastModified = resp.Header.Get("Last-Modified")
	r.length = resp.ContentLength
	return nil
}

func NewPartialHTTPReader(u *url.URL) (*PartialHTTPReader, error) {
	r := &PartialHTTPReader{
		URL: u,
	}
	err := r.init()
	if err != nil {
		return nil, err
	}
	return r, nil
}
