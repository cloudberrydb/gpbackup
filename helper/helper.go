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
	"time"

	"github.com/greenplum-db/gpbackup/utils"
)

var (
	restoreAgent *bool
	content      *int
	dataFile     *string
	logger       *utils.Logger
	oid          *uint
	pipeFile     *string
	tocFile      *string
	oidFile      *string
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
	restoreAgent = flag.Bool("restore-agent", false, "Use gpbackup_helper as an agent for restore")
	content = flag.Int("content", -2, "Content ID of the corresponding segment")
	logger = utils.InitializeLogging("gpbackup_helper", "")
	oid = flag.Uint("oid", 0, "Oid of the table being processed")
	tocFile = flag.String("toc-file", "", "Absolute path to the table of contents file")
	oidFile = flag.String("oid-file", "", "Absolute path to the file containing a list of oids to restore")
	pipeFile = flag.String("pipe-file", "", "Absolute path to the pipe file")
	dataFile = flag.String("data-file", "", "Absolute path to the data file")
	flag.Parse()
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
 * Backup helper functions
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

	for _, oid := range oidList {
		log(fmt.Sprintf("Restoring table with oid %d", oid))
		writeHandle, err := os.OpenFile(*pipeFile, os.O_WRONLY, os.ModeNamedPipe)
		utils.CheckError(err)
		writer := bufio.NewWriter(writeHandle)

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
		err = writeHandle.Close()
		utils.CheckError(err)
		lastByte = end
		/* We sleep for 100 milliseconds to let COPY disconnect before reopening the pipe
		 * so each table gets only one table worth of data
		 */
		time.Sleep(100 * time.Millisecond)
	}
}

/*
 * Shared helper functions
 */

func log(s string, v ...interface{}) {
	s = fmt.Sprintf("Segment %d: %s", *content, s)
	logger.Verbose(s, v...)
}
