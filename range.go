package ranger

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

// blockRange returns the starting block and number of full blocks covered by a byte range at the given block size
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
