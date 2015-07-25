package ranger

import (
	"net/url"
	"net/http"
	"net/http/httptest"
	"testing"
	"bytes"
	"os"
	"errors"
	"time"
	"io/ioutil"
)

var FixedBlob []byte

// Define a basic in-memory file system.
type memFileSystem struct {}

func (m *memFileSystem) Open(path string) (http.File, error) {
	if path == "/blob" {
		return &memFile{bytes.NewReader(FixedBlob)}, nil
	} else {
		return nil, errors.New("not found")
	}
}

// Define a basic in-memory file.
type memFile struct {
	*bytes.Reader
}

func (m memFile) Readdir(count int) ([]os.FileInfo, error) {
	return nil, errors.New("unsupported")
}

func (m memFile) Stat() (os.FileInfo, error) { return m, nil }
func (m memFile) Size() int64 { return int64(len(FixedBlob)) }
func (m memFile) IsDir() bool { return false }
func (m memFile) ModTime() time.Time { return time.Now() }
func (m memFile) Mode() os.FileMode { return 0 }
func (m memFile) Name() string { return "/blob" }
func (m memFile) Sys() interface{} { return nil }

func (m memFile) Close() error {
	return nil
}

// Initialize the blob as a part of the test.
func init() {
	FixedBlob = make([]byte, 10 * 1024)

	for i := 0; i < len(FixedBlob); i++ {
		FixedBlob[i] = byte(i)
	}
}

func TestReadAndSeek(t *testing.T) {
	server := httptest.NewServer(http.FileServer(&memFileSystem{}))
	defer server.Close()
	
	surl, err := url.Parse(server.URL + "/blob")
	if err != nil {
		t.Fatal(err)
	}
	
	rngr := &HTTPRanger{URL: surl}
	rngr.Initialize(73)
	
	reader, err := NewReader(rngr)
	if err != nil {
		t.Fatal(err)
	}
	
	// Validate seek from end
	off, err := reader.Seek(-2, 2)
	if err != nil {
		t.Fatal(err)
	}
	
	if off != int64(len(FixedBlob) - 2) {
		t.Fatalf("wrong offset: %d (should be %d)", off, len(FixedBlob) - 2)
	}
	
	// Validate absolute seek
	off, err = reader.Seek(1, 0)
	if err != nil {
		t.Fatal(err)
	}
	
	if off != 1 {
		t.Fatalf("wrong offset: %d (should be 1)", off)
	}
	
	// Validate relative seek
	off, err = reader.Seek(2, 1)
	if err != nil {
		t.Fatal(err)
	}
	
	if off != 3 {
		t.Fatalf("wrong offset: %d (should be 3)", off)
	}
	
	// Validate contents of read.
	contents, err := ioutil.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	
	for i := range contents {
		if contents[i] != FixedBlob[i + 3] {
			t.Fatalf("blob differs at position %d", i)
		}
	}
	
	if len(contents) != len(FixedBlob) - 3 {
		t.Fatalf("blob was wrong size (was %d, should be %d)",
			len(contents), len(FixedBlob))
	}
}

func TestReadAt(t *testing.T) {
	server := httptest.NewServer(http.FileServer(&memFileSystem{}))
	defer server.Close()
	
	surl, err := url.Parse(server.URL + "/blob")
	if err != nil {
		t.Fatal(err)
	}
	
	rngr := &HTTPRanger{URL: surl}
	rngr.Initialize(73)
	
	reader, err := NewReader(rngr)
	if err != nil {
		t.Fatal(err)
	}
	
	// Ensure that seeking doesn't corrupt ReadAt offset.
	if _, err = reader.Seek(100, 0); err != nil {
		t.Fatal(err)
	}
	
	// Test reading before end of buffer.
	contents := make([]byte, len(FixedBlob) - 5)
	n, err := reader.ReadAt(contents, 4)
	if err != nil {
		t.Fatal(err)
	}
	
	if n != len(FixedBlob) - 5 {
		t.Fatalf("n was %d but should have been %d", n, len(FixedBlob) - 5)
	}
	
	// Validate contents of read.
	for i := range contents {
		if contents[i] != FixedBlob[i + 4] {
			t.Fatalf("blob differs at position %d", i)
		}
	}
	
	// Test reading through the end of the buffer.
	contents = make([]byte, len(FixedBlob))
	n, err = reader.ReadAt(contents, 3)
	if err == nil {
		t.Fatal("should have returned an error for read off end of file")
	}
	
	if n != len(FixedBlob) - 3 {
		t.Fatalf("n was %d but should have been %d", n, len(FixedBlob) - 3)
	}
	
	// Validate contents of read.
	for i := range contents[:len(contents) - 3] {
		if contents[i] != FixedBlob[i + 3] {
			t.Fatalf("blob differs at position %d", i)
		}
	}
}