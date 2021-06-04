package main

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/reclamegraag/go-functional/functions"

	"github.com/alexcesaro/log/stdlog"
)

var logger = stdlog.GetFromFlags()
var wg sync.WaitGroup

func main() {
	start := time.Now()
	var rows int
	file := os.Args[1]
	rows = 100000
	if len(os.Args) > 2 {
		rows, _ = strconv.Atoi(os.Args[2])
	}

	splitFile(file, rows)

	logger.Infof(fmt.Sprintf("This file \"%s\" is split per %d rows into multiple files at the same folder.", file, rows))
	logger.Infof(fmt.Sprintf("The file splitting took this long: %v.", time.Since(start)))
}

func splitFile(filePath string, rows int) {
	path := createFilePath(filePath)
	var readLines functions.StringSlice
	var firstLine string

	file, err := os.Open(path)
	LogFatalError(err)
	defer closeFile(file)

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)
	var rowCounter int = 1
	var fileCount int = 1
	for scanner.Scan() {
		line := scanner.Text()
		if rowCounter == 1 {
			firstLine = line
		}
		if math.Mod(float64(rowCounter), float64(rows)) == 0 {
			wg.Add(1)
			go writeFile(path, readLines, fileCount)
			fileCount++
			readLines = nil
			readLines = append(readLines, firstLine)
		}
		readLines = append(readLines, line)
		rowCounter++
	}
	wg.Wait()
}

func writeFile(path string, lines functions.StringSlice, fileCount int) {
	defer wg.Done()
	fileName, fileType := separateFileAndFileType(path)
	newFileName := fmt.Sprintf("%s%s.%s", fileName, fmt.Sprintf("%05d", fileCount), fileType)
	file, err := os.OpenFile(newFileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
	LogFatalError(err)
	defer closeFile(file)
	lines.Foreach(func(line string) {
		_, err := file.WriteString(fmt.Sprintf("%s\n", line))
		LogError(err)
	})
}

func separateFileAndFileType(file string) (string, string) {
	var words functions.StringSlice = strings.Split(file, ".")
	if len(words) > 1 {
		var remainingWords functions.StringSlice = words[:len(words)-1]
		var fileType string = words[len(words)-1]
		return remainingWords.MkString("."), fileType
	} else {
		return words.MkString(""), ""
	}
}

func closeFile(file *os.File) {
	err := file.Close()
	LogFatalError(err)
}

func createFilePath(filePath string) string {
	var path string = filePath
	workingDirectory, err := os.Getwd()
	LogError(err)
	isPath := strings.Contains(filePath, string(os.PathSeparator))
	if !isPath {
		path = fmt.Sprintf("%s%s", workingDirectory, filePath)
	}
	return path
}

func LogError(err error) {
	if err != nil {
		logger.Warningf(fmt.Sprintf("This non fatal error occurred: %s", err.Error()))
	}
}

func LogFatalError(err error) {
	if err != nil {
		logger.Errorf(fmt.Sprintf("This fatal error occurred: %s", err.Error()))
		os.Exit(1)
	}
}
