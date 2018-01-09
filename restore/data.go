package restore

/*
 * This file contains structs and functions related to backing up data on the segments.
 */

import (
	"fmt"

	"github.com/greenplum-db/gpbackup/utils"
)

var (
	tableDelim = ","
)

func CopyTableIn(connection *utils.DBConn, tableName string, tableAttributes string, backupFile string, singleDataFile bool, whichConn int) {
	whichConn = connection.ValidateConnNum(whichConn)
	usingCompression, compressionProgram := utils.GetCompressionParameters()
	copyCommand := ""
	if singleDataFile {
		copyCommand = fmt.Sprintf("PROGRAM 'cat %s'", backupFile)
	} else if usingCompression && !singleDataFile {
		copyCommand = fmt.Sprintf("PROGRAM '%s < %s'", compressionProgram.DecompressCommand, backupFile)
	} else {
		copyCommand = fmt.Sprintf("'%s'", backupFile)
	}
	query := fmt.Sprintf("COPY %s%s FROM %s WITH CSV DELIMITER '%s' ON SEGMENT;", tableName, tableAttributes, copyCommand, tableDelim)
	_, err := connection.Exec(query, whichConn)
	if err != nil {
		logger.Fatal(err, "Error loading data into table %s", tableName)
	}
}
