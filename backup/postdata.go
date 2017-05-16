package backup

/*
 * This file contains structs and functions related to dumping "post-data" metadata
 * on the master, which is any metadata that needs to be restored after data is
 * restored, such as indexes and rules.
 */

import (
	"gpbackup/utils"
	"fmt"
	"io"
	"sort"
)

func GetIndexesForAllTables(connection *utils.DBConn, tables []utils.Relation) []string {
	indexes := make([]string, 0)
	for _, table := range tables {
		indexList := GetIndexMetadata(connection, table.RelationOid)
		for _, index := range indexList {
			indexStr := fmt.Sprintf("\n\n%s;\n", index.Def)
			if index.Comment.Valid {
				indexStr += fmt.Sprintf("\nCOMMENT ON INDEX %s IS '%s';", index.Name, index.Comment.String)
			}
			indexes = append(indexes, indexStr)
		}
	}
	return indexes
}

func PrintCreateIndexStatements(postdataFile io.Writer, indexes []string) {
	sort.Strings(indexes)
	for _, index := range indexes {
		fmt.Fprintln(postdataFile, index)
	}
}
