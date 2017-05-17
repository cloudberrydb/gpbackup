package utils

/*
 * This file contains structs and functions related to interacting with files
 * and directories, both locally and remotely over SSH.
 */

import (
	"fmt"
	"io"
	"strings"
)

const DefaultSegmentDir = "<SEG_DATA_DIR>"

var (
	BaseDumpDir = DefaultSegmentDir

	/*
	 * The following two maps map a segment's content id to its host and segment
	 * data directory, respectively.  They're set in the CreateDumpDirectories
	 * function for use throughout the rest of the dump.
	 */
)

/*
 * Generic file/directory manipulation functions
 */

func DirectoryMustExist(dirname string) {
	_, statErr := System.Stat(dirname)
	if statErr != nil {
		logger.Fatal("Cannot stat directory %s: %s", dirname, statErr)
	}
}

func MustOpenFile(filename string) io.Writer {
	logFileHandle, err := System.Create(filename)
	if err != nil {
		logger.Fatal("Unable to create or open file %s: %s", filename, err)
	}
	return logFileHandle
}

func GetUserAndHostInfo() (string, string, string) {
	currentUser, _ := System.CurrentUser()
	userName := currentUser.Username
	userDir := currentUser.HomeDir
	hostname, _ := System.Hostname()
	return userName, userDir, hostname
}

/*
 * Backup-specific file/directory manipulation functions
 */

// TODO: Handle multi-node clusters
func CreateDumpDirs(segConfigMaps SegConfigMaps) {
	for segId, dumpPath := range segConfigMaps.DirMap {
		logger.Verbose("Creating directory %s", dumpPath)
		err := System.MkdirAll(dumpPath, 0700)
		if err != nil {
			logger.Fatal("Cannot create directory %s on host %s: %s", dumpPath, segConfigMaps.HostMap[segId], err.Error())
		}
		CheckError(err)
	}
}

var segConfigMaps SegConfigMaps

type SegConfigMaps struct {
	DirMap  map[int]string
	HostMap map[int]string
}

type QuerySegConfig struct {
	Content  int
	Hostname string
	DataDir  string
}

func GetSegmentConfiguration(connection *DBConn) SegConfigMaps {
	query := `SELECT
content,
hostname,
fselocation as datadir
FROM pg_catalog.gp_segment_configuration
JOIN pg_catalog.pg_filespace_entry
ON (dbid = fsedbid)
WHERE role = 'p'
ORDER BY content;`

	results := make([]QuerySegConfig, 0)
	err := connection.Select(&results, query)
	CheckError(err)

	segConfigMaps.DirMap = make(map[int]string, 0)
	segConfigMaps.HostMap = make(map[int]string, 0)
	dumpPathFmtStr := fmt.Sprintf("%s/backups/%s/%s", BaseDumpDir, DumpTimestamp[0:8], DumpTimestamp)
	for _, seg := range results {
		dumpPath := strings.Replace(dumpPathFmtStr, DefaultSegmentDir, seg.DataDir, -1)
		segConfigMaps.DirMap[seg.Content] = dumpPath
		segConfigMaps.HostMap[seg.Content] = seg.Hostname
	}
	return segConfigMaps
}
