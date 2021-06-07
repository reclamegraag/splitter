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

	println(CreateHelpMessage(createSplitterHelpContent()))
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

func parseArgs() {
	//TODO finish this function by real case outcomes
	var arguments functions.StringSlice = os.Args
	var firstArgument string
	switch numberOfArguments := len(arguments); numberOfArguments {
	case 1:
		println(CreateHelpMessage(createSplitterHelpContent()))
	case 2:
		firstArgument = os.Args[1]
		println(firstArgument)
	default:
		arguments.Map(func(value string) string {
			var isInt bool = true
			intValue, err := strconv.Atoi(value)
			if err != nil {
				isInt = false
			}
			if value == "--help" || value == "-h" {
				return "help docs"
			} else if value == "--copy-headers" || value == "-c" {
				return "set boolean"
			} else if isInt {
				return strconv.Itoa(intValue)
			} else {
				return value
			}
		})
	}
}

type HelpContents []HelpContent

type HelpContent struct {
	Text        string
	ShortOption string
	LongOption  string
}

func createSplitterHelpContent() []HelpContent {
	helpContents := []HelpContent{
		{
			Text: `This tool splits files into multiple files. 
It uses this format, where between [] means optional: 

	splitter [-c|--copy-headers] {file} [-r|--rows {number}]

	Example: splitter -c C:\Users\Piet.Puk\Downloads\my-file.csv -r 5000

Options and values:`,
			ShortOption: "",
			LongOption:  "",
		},
		{
			Text:        `When either the short or the long - non mandatory - option is added the headers from the first file will also be available for all other files as well after the split:`,
			ShortOption: "-c",
			LongOption:  "--copy-headers",
		},
		{
			Text: `{file} is one of the below:
	1. The file name + file type (e.g. my-file.csv) in the folder of the splitter tool
	2. The complete folder path + file name + file type (e.g. C:\Users\Piet.Puk\Downloads\my-file.csv)`,
			ShortOption: "",
			LongOption:  "",
		},
		{
			Text:        `When either the short or the long - non mandatory - option is added followed by a number, each time after the number of rows (default number is 10,000) is reached a new split file will be created (e.g. file 1 with 500 rows, file 2 with 500 rows, file 3 with 350 rows):`,
			ShortOption: "-r",
			LongOption:  "--rows",
		},
	}
	return helpContents
}

func CreateHelpMessage(inputs []HelpContent) string {
	var contents functions.StringSlice
	for i, content := range inputs {
		if i == 0 {
			contents = append(contents, fmt.Sprintf("%s\n", content.Text))
		} else if content.LongOption == "" && content.ShortOption == "" {
			contents = append(contents, fmt.Sprintf("\t%s\n", content.Text))
		} else {
			contents = append(contents, fmt.Sprintf("\t%s", content.Text))
			contents = append(contents, fmt.Sprintf("\t%3s", content.ShortOption))
			contents = append(contents, fmt.Sprintf("\t%s\n", content.LongOption))
		}
	}
	return contents.MkString("\n")
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
