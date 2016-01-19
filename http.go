package ranger

import (
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"strings"
)

// HTTPRanger is a RangeFetcher that uses the HTTP Range: header to fetch blocks.
//
// HTTPRanger first makes a HEAD request and then between 0 and Length()/BlockSize GET requests, attempting
// whenever possible to optimize for a lower number of requests.
//
// No network requests are made until the first I/O-related function call.
type HTTPRanger struct {
	URL                *url.URL
	Client             http.Client
	etag, lastModified string
	length             int64
	blockSize          int
}

// Initialize implements the Initialize function from the RangeFetcher interface.
// It performs a HEAD request to retrieve the required information from the server.
func (r *HTTPRanger) Initialize(bs int) error {
	resp, _ := r.Client.Head(r.URL.String())
	if resp.StatusCode == http.StatusNotFound {
		return errors.New("404")
	}

	if !strings.Contains(resp.Header.Get("Accept-Ranges"), "bytes") {
		return errors.New(r.URL.Host + " does not support byte-ranged requests.")
	}

	r.blockSize = bs
	r.etag = resp.Header.Get("ETag")
	r.lastModified = resp.Header.Get("Last-Modified")
	r.length = resp.ContentLength
	return nil
}

// Length returns the length, in bytes, of the ranged-over file.
func (r *HTTPRanger) Length() int64 {
	return r.length
}

// FetchBlocks requests blocks from the HTTP server.
func (r *HTTPRanger) FetchBlocks(ranges []BlockByteRange) ([]Block, error) {
	blox := make([]Block, len(ranges))
	if len(ranges) > 0 {
		rs := make([]string, len(ranges))
		for i, rng := range ranges {
			rs[i] = fmt.Sprintf("%d-%d", rng.Start, rng.End)
		}
		rangeString := strings.Join(rs, ",")

		req, _ := http.NewRequest("GET", r.URL.String(), nil)
		req.Header.Set("Range", fmt.Sprintf("bytes=%s", rangeString))
		if r.etag != "" {
			req.Header.Set("If-Range", r.etag)
		} else if r.lastModified != "" {
			req.Header.Set("If-Range", r.lastModified)
		}

		resp, _ := r.Client.Do(req)
		typ, params, _ := mime.ParseMediaType(resp.Header.Get("Content-Type"))
		defer resp.Body.Close()

		if resp.StatusCode >= 400 {
			return nil, errors.New("unexpected response from " + r.URL.Host)
		}

		if typ == "multipart/byteranges" {
			multipart := multipart.NewReader(resp.Body, params["boundary"])
			i := 0
			for {
				if part, err := multipart.NextPart(); err == nil {
					rng := ranges[i]
					bn := rng.Number
					blocklen := (rng.End - rng.Start) + 1
					blox[i] = Block{Number: bn, Data: make([]byte, blocklen)}
					io.ReadFull(part, blox[i].Data)
					i++
				} else {
					if err == io.EOF {
						break
					}
					return nil, err
				}
			}
		} else {
			bn := 0
			if resp.StatusCode == http.StatusPartialContent {
				// If we've received 206 Partial Content but no multipart parts,
				// we received a contiguous section starting at the first requested block.
				bn = ranges[0].Number
			}
			body := make([]byte, resp.ContentLength)
			io.ReadFull(resp.Body, body)
			blox = blox[0:0]
			remaining := resp.ContentLength
			ncopied := int64(0)
			for remaining > 0 {
				bs := int64(r.blockSize)
				if bs > remaining {
					bs = remaining
				}

				blk := Block{bn, make([]byte, bs)}
				bodySlice := body[ncopied : ncopied+bs]
				copy(blk.Data, bodySlice)
				blox = append(blox, blk)

				bn++
				ncopied += bs
				remaining -= bs
			}
		}
	}
	return blox, nil
}
