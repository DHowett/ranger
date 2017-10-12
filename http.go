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

// HTTPClient is an interface describing the methods required from net/http.Client
type HTTPClient interface {
	Do(*http.Request) (*http.Response, error)
	Get(string) (*http.Response, error)
	Head(string) (*http.Response, error)
}

// HTTPRanger is a RangeFetcher that uses the HTTP Range: header to fetch blocks.
//
// HTTPRanger first makes a HEAD request and then between 0 and Length()/BlockSize GET requests, attempting
// whenever possible to optimize for a lower number of requests.
//
// No network requests are made until the first I/O-related function call.
type HTTPRanger struct {
	URL    *url.URL
	Client HTTPClient

	validator string
	length    int64
	blockSize int
}

func statusCodeError(status int) error {
	return fmt.Errorf("unexpected response (status %d)", status)
}

func statusIsAcceptable(status int) bool {
	return status >= 200 && status < 300
}

func validatorFromResponse(resp *http.Response) (string, error) {
	etag := resp.Header.Get("ETag")
	if etag != "" && etag[0] == '"' {
		return etag, nil
	}

	modtime := resp.Header.Get("Last-Modified")
	if modtime != "" {
		return modtime, nil
	}

	return "", errors.New("no applicable validator in response")
}

// Initialize implements the Initialize function from the RangeFetcher interface.
// It performs a HEAD request to retrieve the required information from the server.
func (r *HTTPRanger) Initialize(bs int) error {
	if r.Client == nil {
		r.Client = &http.Client{}
	}

	resp, err := r.Client.Head(r.URL.String())
	if err != nil {
		return err
	}

	if !statusIsAcceptable(resp.StatusCode) {
		return statusCodeError(resp.StatusCode)
	}

	if !strings.Contains(resp.Header.Get("Accept-Ranges"), "bytes") {
		return errors.New(r.URL.String() + " does not support byte-ranged requests.")
	}

	validator, err := validatorFromResponse(resp)
	if err != nil {
		return errors.New(r.URL.String() + " did not offer a strong-enough validator for subsequent requests")
	}

	r.blockSize = bs
	r.validator = validator
	r.length = resp.ContentLength
	return nil
}

// Length returns the length, in bytes, of the ranged-over file.
func (r *HTTPRanger) Length() int64 {
	return r.length
}

func makeByteRangeHeader(ranges []BlockByteRange) string {
	if len(ranges) > 0 {
		rs := make([]string, len(ranges))
		for i, rng := range ranges {
			rs[i] = fmt.Sprintf("%d-%d", rng.Start, rng.End)
		}
		return "bytes=" + strings.Join(rs, ",")
	}
	return ""
}

// FetchBlocks requests blocks from the HTTP server.
func (r *HTTPRanger) FetchBlocks(ranges []BlockByteRange) ([]Block, error) {
	blox := make([]Block, len(ranges))
	if len(ranges) > 0 {
		req, err := http.NewRequest("GET", r.URL.String(), nil)
		if err != nil {
			return nil, err
		}

		req.Header.Set("Range", makeByteRangeHeader(ranges))
		req.Header.Set("If-Range", r.validator)

		resp, err := r.Client.Do(req)
		if err != nil {
			return nil, err
		}

		defer resp.Body.Close()

		switch resp.StatusCode {
		case http.StatusPreconditionFailed:
			return nil, ErrResourceChanged
		case http.StatusNotFound:
			return nil, ErrResourceNotFound
		case http.StatusOK:
			newValidator, err := validatorFromResponse(resp)
			if err != nil || newValidator != r.validator {
				return nil, ErrResourceChanged
			}
		default:
			if !statusIsAcceptable(resp.StatusCode) {
				return nil, statusCodeError(resp.StatusCode)
			}
		}

		typ, params, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
		if err != nil {
			return nil, err
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
