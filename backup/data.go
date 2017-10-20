package backup

/*
 * This file contains structs and functions related to backing up data on the segments.
 */

import (
	"fmt"
	"strings"

	"github.com/greenplum-db/gpbackup/utils"
)

var (
	tableDelim = ","
)

func ConstructTableAttributesList(columnDefs []ColumnDefinition) string {
	names := make([]string, 0)
	for _, col := range columnDefs {
		if !col.IsDropped {
			names = append(names, col.Name)
		}
	}
	if len(names) > 0 {
		return fmt.Sprintf("(%s)", strings.Join(names, ","))
	}
	return ""
}

func AddTableDataEntriesToTOC(tables []Relation, tableDefs map[uint32]TableDefinition) {
	for _, table := range tables {
		if !tableDefs[table.Oid].IsExternal {
			attributes := ConstructTableAttributesList(tableDefs[table.Oid].ColumnDefs)
			globalTOC.AddDataEntry(table.Schema, table.Name, table.Oid, attributes)
		}
	}
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
