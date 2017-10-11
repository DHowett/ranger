package ranger

import (
	"archive/zip"
	"bytes"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"
)

// blockIdentifyingReadSeeker returns a buffer that, when read, produces Count Size-sized blocks
// containing Sentinel + 8-digit-number + "BEGIN" ....... Sentinel + 8-digit-number + "  END"
type blockIdentifyingReadSeeker struct {
	Sentinel [3]byte
	Count    int
	Size     int

	off int64
}

func (b *blockIdentifyingReadSeeker) Read(p []byte) (n int, err error) {
	max := int64(b.Size*b.Count) - b.off
	// process p in chunks - [end of last block] [whole block] [partial start of next block]
	l := int64(len(p))
	if l > max {
		l = max
	}
	if l > 0 {
		start, cnt := blockRange(b.off, int(l), b.Size)
		fakeblocks := make([]byte, cnt*b.Size)
		for i := 0; i < cnt; i++ {
			bstart := i * b.Size
			bend := bstart + b.Size - 16
			copy(fakeblocks[bstart:bstart+3], b.Sentinel[:])
			copy(fakeblocks[bstart+3:bstart+16], []byte(fmt.Sprintf("%8.08dBEGIN", start+i)))

			copy(fakeblocks[bend:bend+3], b.Sentinel[:])
			copy(fakeblocks[bend+3:bend+16], []byte(fmt.Sprintf("%8.08d  END", start+i)))
		}
		copypos := b.off % int64(b.Size)
		copy(p, fakeblocks[copypos:copypos+l])
	}
	b.off += l
	err = nil
	if b.off == int64(b.Size*b.Count) {
		err = io.EOF
	}
	n = int(l)
	return
}

func (b *blockIdentifyingReadSeeker) Seek(offset int64, whence int) (int64, error) {
	max := int64(b.Size * b.Count)
	switch whence {
	case io.SeekStart:
		// nothing
	case io.SeekCurrent:
		offset = b.off + offset
	case io.SeekEnd:
		offset = max + offset
	}
	if offset >= 0 && offset <= int64(max) {
		b.off = offset
		return b.off, nil
	}
	return b.off, errors.New("invalid seek")
}

type cutoverHandler struct {
	first, second http.Handler
	count         int

	hits int
}

func (c *cutoverHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if c.hits < c.count {
		c.first.ServeHTTP(w, r)
	} else {
		c.second.ServeHTTP(w, r)
	}
	c.hits++
}

// NewCutoverHandler returns an http.Handler that responds via one handler for the first count requests then switches over to another
func NewCutoverHandler(count int, first, second http.Handler) http.Handler {
	return &cutoverHandler{
		first:  first,
		second: second,
		count:  count,
	}
}

func newContentHandler(name string, rs io.ReadSeeker, modtime time.Time) http.Handler {
	h := md5.New()
	rs.Seek(0, io.SeekStart)
	io.Copy(h, rs)
	sum := make([]byte, 0, h.Size())
	sum = h.Sum(sum)
	etag := fmt.Sprintf("\"%02x\"", sum)
	rs.Seek(0, io.SeekStart)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", etag)
		http.ServeContent(w, r, name, modtime, rs)
	})
}

func zipHandler() http.Handler {
	buf := &bytes.Buffer{}
	z := zip.NewWriter(buf)
	for i := 0; i < 10; i++ {
		n := fmt.Sprintf("f%2.02d", i)
		w, _ := z.Create(n)
		rs := &blockIdentifyingReadSeeker{Count: 10, Size: 64}
		copy(rs.Sentinel[:], n[0:3])
		io.Copy(w, rs)
	}
	z.Close()
	br := bytes.NewReader(buf.Bytes())
	return newContentHandler("b.zip", br, time.Now().Add(-1*time.Hour))
}

func newFileHandler(name string) http.Handler {
	f, err := os.Open(name)
	if err != nil {
		panic(err)
	}
	fi, err := f.Stat()
	if err != nil {
		panic(err)
	}

	return newContentHandler(name, f, fi.ModTime())
}

func newStatusHandler(status int) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, http.StatusText(status), status)
	})
}

var testServer *httptest.Server

func initTestServer() {
	fileNotFoundHandler := newStatusHandler(http.StatusNotFound)

	serveMux := http.NewServeMux()

	serveMux.Handle("/404", fileNotFoundHandler)

	serveMux.Handle("/faulty", NewCutoverHandler(1, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "1024")
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Etag", "\"abcdef\"")
		w.WriteHeader(http.StatusOK)
	}), newStatusHandler(http.StatusBadRequest)))

	serveMux.Handle("/noranges", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Accept-Ranges", "")
		w.WriteHeader(http.StatusOK)
	}))

	serveMux.Handle("/b.zip", zipHandler())

	serveMux.Handle("/blocks/bl1", newContentHandler("blocks", &blockIdentifyingReadSeeker{
		Sentinel: [3]byte{'B', 'L', '1'},
		Count:    10,
		Size:     512,
	}, time.Now()))

	serveMux.Handle("/blocks/bl2", newContentHandler("blocks", &blockIdentifyingReadSeeker{
		Sentinel: [3]byte{'B', 'L', '2'},
		Count:    100,
		Size:     512,
	}, time.Now()))

	serveMux.Handle("/blocks/bl3", newContentHandler("blocks", &blockIdentifyingReadSeeker{
		Sentinel: [3]byte{'B', 'L', '3'},
		Count:    128 * 10,
		Size:     1024,
	}, time.Now()))

	// 2: one for HEAD, one for first GET
	serveMux.Handle("/blocks/content_changes", NewCutoverHandler(2, newContentHandler("content_changes", &blockIdentifyingReadSeeker{
		Sentinel: [3]byte{'C', 'H', '1'},
		Count:    100,
		Size:     512,
	}, time.Now().Add(-2*time.Hour)), newContentHandler("content_changes", &blockIdentifyingReadSeeker{
		Sentinel: [3]byte{'C', 'H', '2'},
		Count:    100,
		Size:     512,
	}, time.Now().Add(-1*time.Hour))))

	testServer = httptest.NewServer(serveMux)
	fmt.Println(testServer.URL)
}

func TestMain(m *testing.M) {
	initTestServer()
	os.Exit(m.Run())
}

func newReaderBlockSize(u *url.URL, bs int) (*Reader, error) {
	hpr := &Reader{Fetcher: &HTTPRanger{URL: u}, BlockSize: bs}
	err := hpr.init()
	return hpr, err
}

func TestSequentialRead(t *testing.T) {
	cases := []TestCase{
		&SequentialTestCase{1024, "ef6b552aa90cfff64e670088ef0c8535"},
		&SequentialTestCase{1024, "8a4653b85c77f911e9c1f2fdb8d19e87"},
	}

	url, _ := url.Parse(testServer.URL + "/blocks/bl1")
	hpr, err := newReaderBlockSize(url, 512)
	if err != nil {
		t.Fatal(err)
	}

	for i, tc := range cases {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			tc.RunTest(t, hpr)
		})
	}
}

func TestSeekRead(t *testing.T) {
	cases := []TestCase{
		&SeekTestCase{-1024, os.SEEK_END, 1024, "d77bed730ec881159ecc3ddcb9498823"},
		&SeekTestCase{1024, os.SEEK_SET, 1024, "8a4653b85c77f911e9c1f2fdb8d19e87"},
		&SeekTestCase{1024, os.SEEK_CUR, 1024, "bc178fe5761655f5d0bfb0a086c0430f"},
		&SeekTestCase{0, os.SEEK_SET, 5120, "a32f7f07d7a54d59ed310aa4f79a6b93"},
	}
	url, _ := url.Parse(testServer.URL + "/blocks/bl1")
	hpr, err := newReaderBlockSize(url, 512)
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range cases {
		t.Run(tc.Name(), func(t *testing.T) {
			tc.RunTest(t, hpr)
		})
	}
}

func TestAsynchronousRead(t *testing.T) {
	sums := []string{
		"85fcb2d0dddc364935ca7d5117e4f86a",
		"85ae5cab9fdc677cf2c700e31009aa39",
		"2c83817be02d3d8798be06f93df2b616",
		"4facb692eb3a0a8936560c53ecc6fb63",
		"900025c25ec6399a1bb3b7f1f2732230",
		"d4b8651f70425840506447520b6dd2f4",
		"2489e41fd8d9d520a05d3e0598791a94",
	}

	bsize := 128 * 1024
	cases := []TestCase{}
	for i := 1; i <= 7; i++ {
		// Read 1024 bytes across the beginning/end of 7 ranger blocks
		cases = append(cases, &ReadAtTestCase{
			Offset: int64((bsize * i) - 512),
			Size:   512,
			MD5:    sums[i-1],
		})
	}

	url, _ := url.Parse(testServer.URL + "/blocks/bl3")
	hpr, err := newReaderBlockSize(url, bsize)
	if err != nil {
		t.Fatal(err)
	}

	wg := sync.WaitGroup{}
	for i := 1; i <= 7; i++ {
		n := int64(i)
		wg.Add(1)
		go func() {
			cases[n-1].RunTest(t, hpr)
			wg.Done()
		}()
	}
	wg.Wait()
}

func TestOverlappingAsynchronousRead(t *testing.T) {
	sums := []string{
		"4f701cc42d5f238d8b89ac6fe65b2fbc",
		"a649c4dbcfb1958cdc0435ac360dc720",
		"b3ff6390c595a9e97ade4121b71ef4a9",
		"4d75acd36b780d7a80e20a2f33d82820",
		"63695efbe96feaeeb648a3df9761dff8",
		"ea8d787155b54bea99b9d7a529207770",
		"9d466c69f3405dc5ca314390cd734f94",
	}

	bsize := 1024
	cases := []TestCase{}
	for i := 1; i <= 7; i++ {
		// Read 1024 bytes across the beginning/end of 7 ranger blocks
		cases = append(cases, &ReadAtTestCase{
			Offset: int64((bsize * i) - 512),
			Size:   1024,
			MD5:    sums[i-1],
		})
	}

	url, _ := url.Parse(testServer.URL + "/blocks/bl2")
	hpr, err := newReaderBlockSize(url, bsize)
	if err != nil {
		t.Fatal(err)
	}

	wg := sync.WaitGroup{}
	for i := 1; i <= 7; i++ {
		n := int64(i)
		wg.Add(1)
		go func() {
			cases[n-1].RunTest(t, hpr)
			wg.Done()
		}()
	}
	wg.Wait()
}

func TestZipFilePartialRead(t *testing.T) {
	url, _ := url.Parse(testServer.URL + "/b.zip")
	hpr, err := newReaderBlockSize(url, 16)
	if err != nil {
		t.Fatal(err)
	}

	length, err := hpr.Length()
	if err != nil {
		t.Fatal(err)
	}

	zr, err := zip.NewReader(hpr, length)
	if err != nil {
		t.Fatal(err)
	}

	bytes := make([]byte, zr.File[0].UncompressedSize64)
	rc, err := zr.File[0].Open()
	if err != nil {
		t.Fatal(err)
	}

	io.ReadFull(rc, bytes)
	expected := "6b210f6fe0bac9de21e11acbc6bb292b"
	s := md5Sum(bytes)
	if expected != s {
		t.Fatalf("sum mismatch on %s: expected %s, got %s", zr.File[0].Name, expected, s)
	}
	rc.Close()
}

func Test404(t *testing.T) {
	url, _ := url.Parse(testServer.URL + "/404")
	_, err := NewReader(&HTTPRanger{URL: url})
	if err == nil {
		t.Fail()
	} else {
		t.Log(err)
	}
}

func TestNoRanges(t *testing.T) {
	url, _ := url.Parse(testServer.URL + "/noranges")
	_, err := NewReader(&HTTPRanger{URL: url})
	if err == nil {
		t.Fail()
	} else {
		t.Log(err)
	}
}

// Fails after HEAD
func TestLateFailure(t *testing.T) {
	url, _ := url.Parse(testServer.URL + "/faulty")
	hpr, err := NewReader(&HTTPRanger{URL: url})
	if err != nil {
		t.Fatal(err)
	}

	bytes := make([]byte, 1024)
	n, err := hpr.ReadAt(bytes, 0)
	if err == nil {
		t.Fatalf("read %d bytes", n)
	} else {
		t.Log(err)
	}
}

// Initializes on first call to function (here, Length)
func TestLateInit(t *testing.T) {
	url, _ := url.Parse(testServer.URL + "/blocks/bl1")
	hpr := &Reader{Fetcher: &HTTPRanger{URL: url}}
	length, err := hpr.Length()
	if err != nil {
		t.Fatal(err)
	}
	t.Log("Late-Init Length:", length)

	hpr2 := &Reader{Fetcher: &HTTPRanger{URL: url}}
	bytes := make([]byte, 1024)
	n, err := hpr2.ReadAt(bytes, 100)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("Late-init read", n, "bytes")
}

// Makes sure we get EOF when we hit the end of the file
func TestEOF(t *testing.T) {
	url, _ := url.Parse(testServer.URL + "/blocks/bl1")
	hpr, err := NewReader(&HTTPRanger{URL: url})
	if err != nil {
		t.Fatal(err)
	}

	bytes := make([]byte, 1024)
	hpr.Seek(-1024, 2)
	nbytes, err := hpr.Read(bytes)
	t.Logf("Read %d bytes from end of file.", nbytes)
	if err != io.EOF {
		t.Fatal("Expected EOF, got", err)
	}

	nbytes, err = hpr.Read(bytes)
	t.Logf("Read %d bytes past end of file.", nbytes)
	if err != io.EOF {
		t.Error("Expected EOF, got", err)
	} else if nbytes != 0 {
		t.Errorf("read %d bytes (expected 0 at eof!)", nbytes)
	}
}

func TestInvalidConditions(t *testing.T) {
	url, _ := url.Parse(testServer.URL + "/blocks/bl2")
	hpr, err := NewReader(&HTTPRanger{URL: url})
	if err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 1024)

	t.Run("ReadAtNegative", func(t *testing.T) {
		_, err = hpr.ReadAt(b, -1)
		if err == nil {
			t.Fatal("Expected an error (other than EOF) :P")
		} else {
			t.Log(err)
		}
	})

	t.Run("ReadAtEOF", func(t *testing.T) {
		_, err = hpr.ReadAt(b, 1048576)
		if err == nil {
			t.Fatal("Expected an error (other than EOF) :P")
		} else {
			t.Log(err)
		}
	})

	t.Run("SeekToEOF", func(t *testing.T) {
		_, err := hpr.Seek(1048576, 0)
		if err == nil {
			t.Fatal("Expected an error (other than EOF) :P")
		} else {
			t.Log(err)
		}
	})
	t.Run("SeekPastEOF", func(t *testing.T) {
		_, err := hpr.Seek(10, 0)
		if err != nil {
			t.Fatal("Should have been able to seek to absolute off. 10:", err)
		}

		_, err = hpr.Seek(1048576, 1)
		if err == nil {
			t.Fatal("Expected an error")
		} else {
			t.Log(err)
		}
	})
	t.Run("SeekFromEnd", func(t *testing.T) {
		hpr.Seek(-1048576, 2)
		if err == nil {
			t.Fatal("Expected an error")
		} else {
			t.Log(err)
		}
	})
	t.Run("SeekToNegative", func(t *testing.T) {
		hpr.Seek(-100, 0)
		if err == nil {
			t.Fatal("Expected an error")
		} else {
			t.Log(err)
		}
	})
}

func TestFileMutatesBetweenReads(t *testing.T) {
	url, _ := url.Parse(testServer.URL + "/blocks/content_changes")
	hpr, err := newReaderBlockSize(url, 512)
	if err != nil {
		t.Error(err)
		return
	}

	bytes := make([]byte, 512)
	n, err := hpr.Read(bytes)
	if err != nil || n != 512 {
		t.Errorf("encountered error on first read (got %d bytes): %v", n, err)
	}

	n, err = hpr.Read(bytes)
	if err == nil {
		t.Error("expected to receive mutation error; got data back!")
	} else {
		t.Log(err)
	}
}
