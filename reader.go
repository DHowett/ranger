// Package ranger implements an io.ReaderAt and io.ReadSeeker-compliant
// implementation of a caching HTTP range request client.
package ranger

import (
	"errors"
	"io"
	"sync"
)

// RangeFetcher is the interface that wraps the FetchBlocks method.
//
// FetchBlocks fetches the specified block ranges and returns any errors encountered in doing so.
//
// Length returns the length, in bytes, of the ranged-over source.
//
// Initialize, called once and passed the Reader's block size, performs any necessary setup tasks for the RangeFetcher
type RangeFetcher interface {
	FetchBlocks([]BlockByteRange) ([]Block, error)
	Length() int64
	Initialize(int) error
}

// DefaultBlockSize is the default size for the blocks that are downloaded from the server and cached.
const DefaultBlockSize int = 128 * 1024

// Reader is an io.ReaderAt and io.ReadSeeker backed by a partial block store.
type Reader struct {
	// the range fetcher with which to download blocks
	Fetcher RangeFetcher

	// size of the blocks fetched from the source and cached; lower values translate to lower memory usage, but typically require more requests
	BlockSize int

	blocks      map[int][]byte
	mutex       sync.RWMutex
	initialized bool

	off int64
}

// Block is represents a block by its number and its associated data.
type Block struct {
	Number int
	Data   []byte
}

// BlockByteRange represents a not-yet-fetched block and the encompassed byte range.
type BlockByteRange struct {
	Number     int
	Start, End int64
}

func blockRange(off int64, length int, blockSize int) (int, int) {
	startBlock := int(off / int64(blockSize))
	endBlock := int((off + int64(length)) / int64(blockSize))
	endBlockOff := (off + int64(length)) % int64(blockSize)
	nblocks := endBlock - startBlock
	if endBlockOff > 0 {
		nblocks++
	}
	return startBlock, nblocks
}

// ReadAt reads len(p) bytes from the ranged-over source.
// It returns the number of bytes read and the error, if any.
// ReadAt always returns a non-nil error when n < len(b). At end of file, that error is io.EOF.
func (r *Reader) ReadAt(p []byte, off int64) (int, error) {
	if !r.initialized {
		err := r.init()
		if err != nil {
			return 0, err
		}
	}

	l := len(p)

	if off < 0 {
		return 0, errors.New("read before beginning of file")
	}

	if off+int64(l) > r.Length() {
		return 0, errors.New("read beyond end of file")
	}

	startBlock, nblocks := blockRange(off, l, r.BlockSize)
	ranges := make([]BlockByteRange, nblocks)
	nreq := 0
	r.mutex.RLock()
	for i := 0; i < nblocks; i++ {
		bn := startBlock + i
		if _, ok := r.blocks[bn]; ok {
			continue
		}
		ranges[nreq] = BlockByteRange{
			bn,
			int64(bn * r.BlockSize),
			int64(((bn + 1) * r.BlockSize) - 1),
		}
		if ranges[nreq].End > r.Length() {
			ranges[nreq].End = r.Length()
		}

		nreq++
	}
	r.mutex.RUnlock()
	ranges = ranges[:nreq]

	// Lock here so that we don't end up dispatching
	// multiple requests for the same blocks.
	r.mutex.Lock()
	blox, err := r.Fetcher.FetchBlocks(ranges)
	if err != nil {
		return 0, err
	}
	for _, v := range blox {
		r.blocks[v.Number] = v.Data
	}
	r.mutex.Unlock()

	return r.copyRangeToBuffer(p, off)
}

func (r *Reader) copyRangeToBuffer(p []byte, off int64) (int, error) {
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
			return 0, errors.New("lies: we were told we had blocks to copy")
		}
		copy(p[ncopied:ncopied+copylen], r.blocks[block][startOffset:])

		remaining -= copylen
		ncopied += copylen

		block++
		startOffset = 0
	}

	var err error
	if off+int64(len(p)) == r.Length() {
		err = io.EOF
	}

	return ncopied, err
}

// Length returns the length of the ranged-over source.
func (r *Reader) Length() int64 {
	if !r.initialized {
		r.init()
	}
	return r.Fetcher.Length()
}

// Read reads len(p) bytes from ranged-over source.
// It returns the number of bytes read and the error, if any.
// EOF is signaled by a zero count with err set to io.EOF.
func (r *Reader) Read(p []byte) (int, error) {
	if r.off == r.Length() {
		return 0, io.EOF
	}

	nread, err := r.ReadAt(p, r.off)
	r.off += int64(nread)
	return nread, err
}

// Seek sets the offset for the next Read to offset, interpreted
// according to whence: 0 means relative to the origin of the file, 1 means relative
// to the current offset, and 2 means relative to the end. It returns the new offset
// and an error, if any.
func (r *Reader) Seek(off int64, whence int) (int64, error) {
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

func (r *Reader) init() error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	r.initialized = true
	r.blocks = make(map[int][]byte)
	if r.BlockSize == 0 {
		r.BlockSize = DefaultBlockSize
	}

	err := r.Fetcher.Initialize(r.BlockSize)
	if err != nil {
		return err
	}
	return nil
}

// NewReader returns a newly-initialized Reader,
// which also initializes its provided RangeFetcher.
// It returns the new reader and an error, if any.
func NewReader(fetcher RangeFetcher) (*Reader, error) {
	r := &Reader{
		Fetcher: fetcher,
	}
	err := r.init()
	if err != nil {
		return nil, err
	}
	return r, nil
}
