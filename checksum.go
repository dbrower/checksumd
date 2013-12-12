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
	ChkMatch    = 1
	ChkAdd      = 2
	ChkConflict = 3
	ChkError    = 4
)

func updateStats(what int) {
	totals.m.Lock()
	defer totals.m.Unlock()
	totals.fileCount += 1
	switch what {
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
		if err != nil {
			fmt.Printf("  Error: %s: %s\n", err.Error(), path)
			updateStats(ChkError)
		} else if s != string(storedChecksum) {
			// Checksum mismatch.
			fmt.Printf("C %s\t%s\t%s\n", s, storedChecksum, path)
			updateStats(ChkConflict)
		} else {
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
	var numproc int
	var wg sync.WaitGroup

	flag.IntVar(&numproc, "n", 10, "Size of worker pool")
	flag.Parse()

	for i := 0; i < numproc; i++ {
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
