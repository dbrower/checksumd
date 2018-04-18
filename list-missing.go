package main

// scan a tree of files and compare against a TSV file where the first
// column is a MD5 sum as hex. List any files that are not found in the list,
// where found means having the same md5 sum.

import (
	"crypto/md5"
	"encoding/csv"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

var (
	totals struct {
		byteCount int
		fileCount int
		found     int
		missing   int
		errors    int
	}
)

const (
	// Constants used by updateStats
	ChkError = iota
	ChkFound
	ChkMissing
)

// Update the global stats. Expects to be called once per file.
// The fileStatus is one of the Chk* constants.
func updateStats(update <-chan int) {
	for fileStatus := range update {
		totals.fileCount += 1
		switch fileStatus {
		case ChkFound:
			totals.found += 1
		case ChkMissing:
			totals.missing += 1
		case ChkError:
			totals.errors += 1
		}
	}
}

// Checksum a file by name.
// Will open the file and return the md5 sum
// of the file, serialized as a hexadecimal ascii string.
// An error is returned if there are problems opening or reading the file.
func checksumFile(name string) (string, error) {
	f, err := os.Open(name)
	if err != nil {
		return "", err
	}
	defer f.Close()

	var result string
	h := md5.New()
	_, err = io.Copy(h, f)
	if err == nil {
		result = hex.EncodeToString(h.Sum(nil))
	}

	return result, err
}

// validateFiles will pull file names from source and checksum them.
// The checksum is compared with an internal list. If it is missing, the
// file name is printed to stdout.
func validateFiles(source <-chan string, hashes map[string]bool, stats chan<- int) {
	for path := range source {
		s, err := checksumFile(path)
		if err != nil {
			fmt.Println(err)
			stats <- ChkError
			continue
		}

		if hashes[s] {
			stats <- ChkFound
		} else {
			fmt.Println(path)
			stats <- ChkMissing
		}
	}
}

func readchecksumfile(name string) (map[string]bool, error) {
	result := make(map[string]bool)
	f, err := os.Open(name)
	if err != nil {
		return result, err
	}
	defer f.Close()
	r := csv.NewReader(f)
	r.Comma = '\t'
	r.ReuseRecord = true

	rec, err := r.Read()
	for err == nil {
		if len(rec) > 0 {
			result[rec[0]] = true
		}
		rec, err = r.Read()
	}
	if err == io.EOF {
		err = nil
	}
	return result, err
}

func main() {
	var (
		fileList = make(chan string, 10)
		statchan = make(chan int, 100)
		n        int
		wg       sync.WaitGroup
		statswg  sync.WaitGroup
	)

	flag.IntVar(&n, "n", 10, "Size of worker pool")
	flag.Parse()

	hashes, err := readchecksumfile(flag.Arg(0))
	if err != nil {
		fmt.Println(err)
		return
	}

	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			validateFiles(fileList, hashes, statchan)
			wg.Done()
		}()
	}
	statswg.Add(1)
	go func() {
		updateStats(statchan)
		statswg.Done()
	}()

	err = filepath.Walk(".", func(path string, info os.FileInfo, err error) error {
		if err == nil && info.Mode().IsRegular() {
			fileList <- path
		}
		return nil
	})

	if err != nil {
		fmt.Println(err)
	}
	close(fileList)
	wg.Wait()
	close(statchan)
	statswg.Wait()
	fmt.Printf("%d files (%d bytes) scanned: ", totals.fileCount, totals.byteCount)
	fmt.Printf("%d found, %d missing, %d errors\n",
		totals.found,
		totals.missing,
		totals.errors)
}
