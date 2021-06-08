package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/reclamegraag/go-functional/functions"
)

var wg sync.WaitGroup

func main() {
	start := time.Now()

	splitterFlags := collectFlags()
	files, rows := splitFile(splitterFlags.file, splitterFlags.rows, splitterFlags.headers)

	log.Printf(fmt.Sprintf("INFO This file \"%s\" with %d rows is split per %d rows into %d files at the same folder.", splitterFlags.file, rows, splitterFlags.rows, files))
	log.Printf(fmt.Sprintf("INFO The file splitting took this long: %v.", time.Since(start)))
}

type SplitterFlags struct {
	headers bool
	rows    int
	file    string
}

func collectFlags() SplitterFlags {
	headersFlag := flag.Bool("copy-headers", false, "\nWhen this option is added the headers from the first file will also be available for all other files as well after the split\n\nExample: splitter -copy-headers my-file.csv\n")
	rowsFlag := flag.Int("rows", 10000, "\nWhen this option is added followed by a number, each time after this number of rows is reached a new split file will be created\n\nExample: splitter my-file.csv -rows 500\n\n")
	fileFlag := flag.String("file", "", "\nWith this option you add one of the below:\n\t1. The file name + file type (e.g. my-file.csv) in the folder of the splitter tool\n\t2. The complete folder path + file name + file type\n\nExample splitter C:\\Users\\Piet.Puk\\Downloads\\my-file.csv\n")
	flag.Parse()

	if len(os.Args) == 1 {
		println("Info\n\n\tThis tool splits files into multiple files and uses this format, where between [] means optional\n\tFormat\t\tsplitter [-copy-headers] -file file [-rows rows]\n\tExample\t\tsplitter -copy-headers C:\\Users\\Piet.Puk\\Downloads\\my-file.csv -rows 5000\n\nOptions\n")
		flag.PrintDefaults()
	}

	if *fileFlag == "" {
		LogError(errors.New("the file has not been given in this format: -file file"))
		println("Info\n\n\tThis tool splits files into multiple files and uses this format, where between [] means optional\n\tFormat\t\tsplitter [-copy-headers] -file file [-rows rows]\n\tExample\t\tsplitter -copy-headers C:\\Users\\Piet.Puk\\Downloads\\my-file.csv -rows 5000\n\nOptions\n")
		flag.PrintDefaults()
		LogFatalError(errors.New("please try again"))
	}

	return SplitterFlags{
		headers: *headersFlag,
		rows:    *rowsFlag,
		file:    *fileFlag,
	}
}

func splitFile(filePath string, rows int, copyHeaders bool) (int64, int64) {
	path := createFilePath(filePath)
	var readLines functions.StringSlice
	var firstLine string

	file, err := os.Open(path)

	LogFatalError(err)
	defer closeFile(file)

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)
	var rowCounter = 1
	var fileCount = 0
	for scanner.Scan() {
		line := scanner.Text()
		if rowCounter == 1 {
			firstLine = line
		}
		if math.Mod(float64(rowCounter), float64(rows)) == 0 {
			fileCount++
			wg.Add(1)
			go writeFile(path, readLines, fileCount)
			readLines = nil
			if copyHeaders {
				readLines = append(readLines, firstLine)
			}
		}
		readLines = append(readLines, line)
		rowCounter++
	}
	wg.Wait()
	return int64(fileCount), int64(rowCounter)
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
		var remainingWords = words[:len(words)-1]
		var fileType = words[len(words)-1]
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
	var path = filePath
	workingDirectory, err := os.Getwd()
	LogError(err)
	isPath := strings.Contains(filePath, string(os.PathSeparator))
	if !isPath {
		path = fmt.Sprintf("%s%s%s", workingDirectory, string(os.PathSeparator), filePath)
	}
	return path
}

func LogError(err error) {
	if err != nil {
		log.Printf("WARNING %s", err.Error())
	}
}

func LogFatalError(err error) {
	if err != nil {
		log.Printf("ERROR %s", err.Error())
		os.Exit(1)
	}
}
