# partial - io.ReaderAt with HTTP range requests!
## INSTALL
	$ go get github.com/DHowett/partial

## OVERVIEW
Package partial provides an implementation of io.ReaderAt and io.ReadSeeker which makes
HTTP partial document requests.

## USE
	package main

	import (
		"archive/zip"
		"io"
		"github.com/DHowett/partial"
		"net/url"
		"os"
	)

	func main() {
		url, _ := url.Parse("http://example.com/example.zip")

		reader, _ := NewRangeReader(url)
		zipreader, _ := zip.NewReader(reader, reader.Length())

		data := make([]byte, zipreader.File[0].UncompressedSize64)
		rc, _ := zipreader.File[0].Open()
		io.ReadFull(rc, data)
		rc.Close()
	}
