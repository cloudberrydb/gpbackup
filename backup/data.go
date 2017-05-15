package backup

/*
 * This file contains structs and functions related to dumping data on the segments.
 */

import (
	"backup_restore/utils"
	"fmt"
)

var (
	tableDelim = ","
)

func WriteTableMapFile(tables []utils.Relation) {
	filename := fmt.Sprintf("%s/gpbackup_%s_table_map", utils.SegDirMap[-1], utils.DumpTimestamp)
	tableMapFile := utils.MustOpenFile(filename)
	for _, table := range tables {
		fmt.Fprintf(tableMapFile, "%s: %d\n", table.ToString(), table.RelationOid)
	}
}

func CopyTableOut(connection *utils.DBConn, table utils.Relation) {
	dumpFile := fmt.Sprintf("%s/gpbackup_<SEGID>_%s_%d", utils.DumpPathFmtStr, utils.DumpTimestamp, table.RelationOid)
	query := fmt.Sprintf("COPY %s TO '%s' WITH CSV DELIMITER '%s' ON SEGMENT;", table.ToString(), dumpFile, tableDelim)
	_, err := connection.Exec(query)
	utils.CheckError(err)
}
