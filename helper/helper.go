package helper

import (
	"bufio"
	"flag"
	"io"

	"github.com/greenplum-db/gpbackup/utils"
)

var (
	index   *uint
	logger  *utils.Logger
	oid     *uint
	restore *bool
	tocFile *string
)

/*
 * Shared functions
 */

func DoHelper() {
	InitializeGlobals()
	if *restore {
		doRestoreHelper()
	} else {
		doBackupHelper()
	}
}

func InitializeGlobals() {
	logger = utils.InitializeLogging("gpbackup_helper", "")
	index = flag.Uint("index", 0, "Index of the table to be restored in the TOC file")
	oid = flag.Uint("oid", 0, "Oid of the table being processed")
	restore = flag.Bool("restore", false, "Read in table according to offset in table of contents file")
	tocFile = flag.String("toc-file", "", "Absolute path to the table of contents file")
	flag.Parse()
	utils.InitializeSystemFunctions()
}

func SetFilename(name string) {
	tocFile = &name
}

func SetLogger(log *utils.Logger) {
	logger = log
}

func SetIndex(tableIndex uint) {
	index = &tableIndex
}

/*
 * Backup helper functions
 */

func doBackupHelper() {
	toc, lastRead := ReadOrCreateTOC()
	numBytes := ReadAndCountBytes()
	toc.AddSegmentDataEntry(*oid, lastRead, lastRead+numBytes)
	toc.WriteToFile(*tocFile)
}

func ReadOrCreateTOC() (*utils.TOC, uint64) {
	var toc *utils.TOC
	var lastRead uint64
	if utils.FileExistsAndIsReadable(*tocFile) {
		toc = utils.NewTOC(*tocFile)
		// We always expect the TOC file to contain at least 1 segment data entry
		lastRead = toc.SegmentDataEntries[len(toc.SegmentDataEntries)-1].EndByte
	} else {
		toc = &utils.TOC{}
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

func doRestoreHelper() {
	toc := utils.NewTOC(*tocFile)
	startByte, endByte := GetBoundsForTable(toc)
	CopyByteRange(startByte, endByte)
}

func GetBoundsForTable(toc *utils.TOC) (int64, int64) {
	segmentDataEntry := toc.SegmentDataEntries[*index]
	startByte := int64(segmentDataEntry.StartByte)
	endByte := int64(segmentDataEntry.EndByte)
	return startByte, endByte
}

func CopyByteRange(startByte int64, endByte int64) {
	reader := bufio.NewReader(utils.System.Stdin)
	reader.Discard(int(startByte))
	io.CopyN(utils.System.Stdout, reader, endByte-startByte)
}
