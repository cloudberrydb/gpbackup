package restore

/*
 * This file contains structs and functions related to dumping data on the segments.
 */

import (
	"bufio"
	"fmt"
	"strconv"
	"strings"

	"github.com/greenplum-db/gpbackup/utils"
)

var (
	tableDelim = ","
)

func ReadTableMapFile(tableMapFilePath string) map[string]uint32 {
	tableMapFile := utils.MustOpenFileForReading(tableMapFilePath)
	tableMap := make(map[string]uint32, 0)
	scanner := bufio.NewScanner(tableMapFile)
	for scanner.Scan() {
		tokens := strings.Split(scanner.Text(), ": ")
		if len(tokens) > 2 { // A table name may contain ": ", so if we split on that, put the table name back together
			tokens = []string{strings.Join(tokens[0:len(tokens)-1], ": "), tokens[len(tokens)-1]}
		}
		tablename, oidStr := tokens[0], tokens[1]
		oid, err := strconv.ParseUint(oidStr, 10, 32)
		utils.CheckError(err)
		tableMap[tablename] = uint32(oid)
	}
	utils.CheckError(scanner.Err())
	return tableMap
}

func CopyTableIn(connection *utils.DBConn, tableName string, dumpFile string) {
	query := fmt.Sprintf("COPY %s FROM '%s' WITH CSV DELIMITER '%s' ON SEGMENT;", tableName, dumpFile, tableDelim)
	_, err := connection.Exec(query)
	utils.CheckError(err)
}

func GetDatabaseNameFromPredataFile(predataFilename string) string {
	predataFile := utils.MustOpenFileForReading(predataFilename)
	scanner := bufio.NewScanner(predataFile)
	scanner.Scan() // We're looking for the "\c [database name]" which will always be the first line in the file
	utils.CheckError(scanner.Err())
	return scanner.Text()[3:]
}
