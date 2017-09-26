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

func GetTableDataEntriesFromTOC() map[uint32]utils.DataEntry {
	tableMap := make(map[uint32]utils.DataEntry, 0)
	for _, entry := range globalTOC.DataEntries {
		tableMap[entry.Oid] = entry
	}
	return tableMap
}

func CopyTableIn(connection *utils.DBConn, tableName string, tableAttributes string, backupFile string) {
	usingCompression, compressionProgram := utils.GetCompressionParameters()
	copyCmdStr := ""
	if usingCompression {
		copyCmdStr = fmt.Sprintf("PROGRAM '%s < %s'", compressionProgram.DecompressCommand, backupFile)
	} else {
		copyCmdStr = fmt.Sprintf("'%s'", backupFile)
	}
	query := fmt.Sprintf("COPY %s%s FROM %s WITH CSV DELIMITER '%s' ON SEGMENT;", tableName, tableAttributes, copyCmdStr, tableDelim)
	_, err := connection.Exec(query)
	utils.CheckError(err)
}
