package helper

import (
	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"syscall"

	"github.com/greenplum-db/gpbackup/utils"
)

var (
	content      *int
	dataFile     *string
	logger       *utils.Logger
	oid          *uint
	oidFile      *string
	pipeFile     *string
	printVersion *bool
	restoreAgent *bool
	tocFile      *string
	version      string
)

/*
 * Shared functions
 */

func DoHelper() {
	InitializeGlobals()
	if *restoreAgent {
		doRestoreAgent()
	} else {
		doBackupHelper()
	}
}

func InitializeGlobals() {
	content = flag.Int("content", -2, "Content ID of the corresponding segment")
	dataFile = flag.String("data-file", "", "Absolute path to the data file")
	logger = utils.InitializeLogging("gpbackup_helper", "")
	oid = flag.Uint("oid", 0, "Oid of the table being processed")
	oidFile = flag.String("oid-file", "", "Absolute path to the file containing a list of oids to restore")
	pipeFile = flag.String("pipe-file", "", "Absolute path to the pipe file")
	printVersion = flag.Bool("version", false, "Print version number and exit")
	restoreAgent = flag.Bool("restore-agent", false, "Use gpbackup_helper as an agent for restore")
	tocFile = flag.String("toc-file", "", "Absolute path to the table of contents file")
	flag.Parse()
	if *printVersion {
		fmt.Printf("gpbackup_helper %s\n", version)
		os.Exit(0)
	}
	utils.InitializeSystemFunctions()
}

func SetContent(id int) {
	content = &id
}

func SetFilename(name string) {
	tocFile = &name
}

func SetLogger(log *utils.Logger) {
	logger = log
}

func SetOid(newoid uint) {
	oid = &newoid
}

/*
 * Backup helper functions
 */

func doBackupHelper() {
	toc, lastRead := ReadOrCreateTOC()
	numBytes := ReadAndCountBytes()
	lastProcessed := lastRead + numBytes
	toc.AddSegmentDataEntry(*oid, lastRead, lastProcessed)
	toc.LastByteRead = lastProcessed
	toc.WriteToFile(*tocFile)
}

func ReadOrCreateTOC() (*utils.SegmentTOC, uint64) {
	var toc *utils.SegmentTOC
	var lastRead uint64
	if utils.FileExistsAndIsReadable(*tocFile) {
		toc = utils.NewSegmentTOC(*tocFile)
		lastRead = toc.LastByteRead
	} else {
		toc = &utils.SegmentTOC{}
		toc.DataEntries = make(map[uint]utils.SegmentDataEntry, 1)
		lastRead = 0
	}
	return toc, lastRead
}

func ReadAndCountBytes() uint64 {
	reader := bufio.NewReader(utils.System.Stdin)
	numBytes, _ := io.Copy(utils.System.Stdout, reader)
	return uint64(numBytes)
}

/*
 * Restore helper functions
 */

func getOidListFromFile() []int {
	oidStr, err := utils.System.ReadFile(*oidFile)
	utils.CheckError(err)
	oidStrList := strings.Split(strings.TrimSpace(fmt.Sprintf("%s", oidStr)), "\n")
	oidList := make([]int, len(oidStrList))
	for i, oid := range oidStrList {
		num, _ := strconv.ParseInt(oid, 10, 32)
		oidList[i] = int(num)
	}
	sort.Ints(oidList)
	return oidList
}

func doRestoreAgent() {
	tocEntries := utils.NewSegmentTOC(*tocFile).DataEntries
	lastByte := uint64(0)
	oidList := getOidListFromFile()
	reader := getPipeReader()

	lastPipe := ""
	currentPipe := ""
	nextPipe := fmt.Sprintf("%s_%d", *pipeFile, oidList[0])
	err := syscall.Mkfifo(nextPipe, 0777)
	for i, oid := range oidList {
		log(fmt.Sprintf("Restoring table with oid %d", oid))
		lastPipe = currentPipe
		currentPipe = nextPipe
		if i < len(oidList)-1 {
			nextPipe = fmt.Sprintf("%s_%d", *pipeFile, oidList[i+1])
			err := syscall.Mkfifo(nextPipe, 0777)
			utils.CheckError(err)
		} else {
			nextPipe = ""
		}
		if fileExists(lastPipe) {
			err = os.Remove(lastPipe)
			utils.CheckError(err)
		}

		writer, writeHandle := getPipeWriter(currentPipe)
		start := tocEntries[uint(oid)].StartByte
		end := tocEntries[uint(oid)].EndByte
		log(fmt.Sprintf("Start Byte: %d; End Byte: %d; Last Byte: %d", start, end, lastByte))
		reader.Discard(int(start - lastByte))
		log(fmt.Sprintf("Discarded %d bytes", start-lastByte))
		bytesRead, err := io.CopyN(writer, reader, int64(end-start))
		log(fmt.Sprintf("Read %d bytes", bytesRead))
		utils.CheckError(err)
		err = writer.Flush()
		utils.CheckError(err)
		closePipe(writeHandle)
		lastByte = end
	}
	if fileExists(currentPipe) {
		err = os.Remove(currentPipe)
		utils.CheckError(err)
	}
}

func getPipeReader() *bufio.Reader {
	readHandle, err := os.Open(*dataFile)
	utils.CheckError(err)
	var reader *bufio.Reader
	if strings.HasSuffix(*dataFile, ".gz") {
		gzipReader, err := gzip.NewReader(readHandle)
		utils.CheckError(err)
		reader = bufio.NewReader(gzipReader)
	} else {
		reader = bufio.NewReader(readHandle)
	}
	return reader
}

func getPipeWriter(currentPipe string) (*bufio.Writer, *os.File) {
	log(fmt.Sprintf("Opening pipe for oid %d", oid))
	writeHandle, err := os.OpenFile(currentPipe, os.O_WRONLY, os.ModeNamedPipe)
	utils.CheckError(err)
	writer := bufio.NewWriter(writeHandle)
	return writer, writeHandle
}

func closePipe(writeHandle *os.File) {
	log(fmt.Sprintf("Closing pipe for oid %d", oid))
	err := writeHandle.Close()
	utils.CheckError(err)
}

func fileExists(filename string) bool {
	_, err := utils.System.Stat(filename)
	return err == nil
}

/*
 * Shared helper functions
 */

func log(s string, v ...interface{}) {
	s = fmt.Sprintf("Segment %d: %s", *content, s)
	logger.Verbose(s, v...)
}
