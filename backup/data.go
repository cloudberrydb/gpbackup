package backup

/*
 * This file contains structs and functions related to dumping data on the segments.
 */

import (
	"fmt"

	"github.com/greenplum-db/gpbackup/utils"
)

var (
	tableDelim = ","
)

func WriteTableMapFile(tables []Relation, extTableMap map[string]bool) {
	tableMapFile := utils.MustOpenFileForWriting(utils.GetTableMapFilePath())
	for _, table := range tables {
		isExternal := extTableMap[table.ToString()]
		if !isExternal {
			utils.MustPrintf(tableMapFile, "%s: %d\n", table.ToString(), table.RelationOid)
		}
	}
}

func CopyTableOut(connection *utils.DBConn, table Relation, dumpFile string) {
	query := fmt.Sprintf("COPY %s TO '%s' WITH CSV DELIMITER '%s' ON SEGMENT;", table.ToString(), dumpFile, tableDelim)
	_, err := connection.Exec(query)
	utils.CheckError(err)
}
