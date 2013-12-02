# ranger - io.ReaderAt with range requests!
## INSTALL
	$ go get github.com/DHowett/ranger

## OVERVIEW
Package ranger provides an implementation of io.ReaderAt and io.ReadSeeker which makes
partial document requests. Ranger ships with a range fetcher that operates on an HTTP resource
using the Range: header.

## USE
	package main

	import (
		"archive/zip"
		"io"
		"github.com/DHowett/ranger"
		"net/url"
		"os"
	)

	func main() {
		url, _ := url.Parse("http://example.com/example.zip")

		reader, _ := ranger.NewReader(&ranger.HTTPRanger{URL: url})
		zipreader, _ := zip.NewReader(reader, reader.Length())

		data := make([]byte, zipreader.File[0].UncompressedSize64)
		rc, _ := zipreader.File[0].Open()
		io.ReadFull(rc, data)
		rc.Close()
	}
