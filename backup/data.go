package backup

/*
 * This file contains structs and functions related to dumping data on the segments.
 */

import (
	"fmt"
	"gpbackup/utils"
)

var (
	tableDelim = ","
)

func WriteTableMapFile(tables []utils.Relation, masterDir string) {

	filename := fmt.Sprintf("%s/gpbackup_%s_table_map", masterDir, utils.DumpTimestamp)
	tableMapFile := utils.MustOpenFile(filename)
	for _, table := range tables {
		fmt.Fprintf(tableMapFile, "%s: %d\n", table.ToString(), table.RelationOid)
	}
}

func CreateTableDumpPath(table utils.Relation) string {
	dumpPathFmtStr := fmt.Sprintf("%s/backups/%s/%s", utils.BaseDumpDir, utils.DumpTimestamp[0:8], utils.DumpTimestamp)
	dumpFile := fmt.Sprintf("%s/gpbackup_<SEGID>_%s_%d", dumpPathFmtStr, utils.DumpTimestamp, table.RelationOid)
	return dumpFile
}

func CopyTableOut(connection *utils.DBConn, table utils.Relation, dumpFile string) {
	query := fmt.Sprintf("COPY %s TO '%s' WITH CSV DELIMITER '%s' ON SEGMENT;", table.ToString(), dumpFile, tableDelim)
	_, err := connection.Exec(query)
	utils.CheckError(err)
}
