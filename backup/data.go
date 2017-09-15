package backup

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

func WriteTableMapFile(tableMapFilePath string, tables []Relation, tableDefs map[uint32]TableDefinition) {
	logger.Verbose("Writing table map file to %s", globalCluster.GetTableMapFilePath())
	tableMapFile := utils.MustOpenFileForWriting(tableMapFilePath)
	for _, table := range tables {
		if !tableDefs[table.RelationOid].IsExternal {
			utils.MustPrintf(tableMapFile, "%s: %d\n", table.ToString(), table.RelationOid)
		}
	}
	tableMapFile.Close()
}

func CopyTableOut(connection *utils.DBConn, table Relation, backupFile string) {
	usingCompression, compressionProgram := utils.GetCompressionParameters()
	copyCmdStr := ""
	if usingCompression {
		copyCmdStr = fmt.Sprintf("PROGRAM '%s > %s'", compressionProgram.CompressCommand, backupFile)
	} else {
		copyCmdStr = fmt.Sprintf("'%s'", backupFile)
	}
	query := fmt.Sprintf("COPY %s TO %s WITH CSV DELIMITER '%s' ON SEGMENT;", table.ToString(), copyCmdStr, tableDelim)
	_, err := connection.Exec(query)
	utils.CheckError(err)
}
