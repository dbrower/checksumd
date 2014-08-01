package main

/* 171s for about 4.1 GB, cold cache, no checksum files
 *  31s for about 4.1 GB, warmed cache, checksum files present
 */

import (
	"crypto/md5"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
)

type stats struct {
}

var (
	fileList chan string = make(chan string, 10)
	totals   struct {
		m         sync.Mutex
		byteCount int
		fileCount int
		matches   int
		added     int
		conflicts int
		errors    int
	}
)

const (
	// Constants used by updateStats
	ChkMatch = iota
	ChkAdd
	ChkConflict
	ChkError
)

// Update the global stats. Expects to be called once per file.
// The fileStatus is one of the Chk* constants.
func updateStats(fileStatus int) {
	totals.m.Lock()
	defer totals.m.Unlock()
	totals.fileCount += 1
	switch fileStatus {
	case ChkMatch:
		totals.matches += 1
	case ChkAdd:
		totals.added += 1
	case ChkConflict:
		totals.conflicts += 1
	case ChkError:
		totals.errors += 1
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
		result = fmt.Sprintf("%x", h.Sum(nil))
	}

	return result, err
}

// validateFiles will pull file names from source and checksum them.
// The checksums are stored in the file name with an ".md5" suffix.
// If there is already a file with that name, then the calculated
// checksum is compared with the contents of the file. Mismatches
// are displayed to stdout.
func validateFiles(source <-chan string, wg *sync.WaitGroup) {
	defer wg.Done()
	for path := range source {
		s, err := checksumFile(path)
		if err != nil {
			fmt.Println(err)
			updateStats(ChkError)
			continue
		}

		csFilename := path + ".md5"
		_, err = os.Stat(csFilename)
		if err != nil {
			// create a new checksum file
			fmt.Printf("A %s\t%s\n", s, path)
			ioutil.WriteFile(csFilename, []byte(s), 0444)
			updateStats(ChkAdd)
			continue
		}
		storedChecksum, err := ioutil.ReadFile(csFilename)
		switch {
		case err != nil:
			fmt.Printf("  Error: %s: %s\n", err.Error(), path)
			updateStats(ChkError)
		case s != string(storedChecksum):
			// Checksum mismatch.
			fmt.Printf("C %s\t%s\t%s\n", s, storedChecksum, path)
			updateStats(ChkConflict)
		default:
			updateStats(ChkMatch)
		}
	}
}

func checksumFileWalk(path string, info os.FileInfo, err error) error {
	if err == nil &&
		info.Mode().IsRegular() &&
		filepath.Ext(path) != ".md5" {
		fileList <- path
	}
	return nil
}

func main() {
	var n int
	var wg sync.WaitGroup

	flag.IntVar(&n, "n", 10, "Size of worker pool")
	flag.Parse()

	for i := 0; i < n; i++ {
		wg.Add(1)
		go validateFiles(fileList, &wg)
	}

	err := filepath.Walk(".", checksumFileWalk)
	if err != nil {
		fmt.Println(err)
	}
	close(fileList)
	wg.Wait()
	fmt.Printf("%d files (%d bytes) scanned: ", totals.fileCount, totals.byteCount)
	fmt.Printf("%d matches, %d added, %d conflicts, %d errors\n", totals.matches, totals.added, totals.conflicts, totals.errors)
}
