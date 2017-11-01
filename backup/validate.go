package backup

import (
	"fmt"

	"github.com/greenplum-db/gpbackup/utils"
)

/*
 * This file contains functions related to validating user input.
 */

func ValidateFilterSchemas(connection *utils.DBConn, schemaList utils.ArrayFlags) {
	if len(schemaList) > 0 {
		quotedSchemasStr := utils.SliceToQuotedString(schemaList)
		query := fmt.Sprintf("SELECT nspname AS string FROM pg_namespace WHERE nspname IN (%s)", quotedSchemasStr)
		resultSchemas := SelectStringSlice(connection, query)
		if len(resultSchemas) < len(schemaList) {
			schemaMap := make(map[string]bool)
			for _, schema := range resultSchemas {
				schemaMap[schema] = true
			}

			for _, schema := range schemaList {
				if _, ok := schemaMap[schema]; !ok {
					logger.Fatal(nil, "Schema %s does not exist", schema)
				}
			}
		}
	}
}

func ValidateFilterTables(connection *utils.DBConn, tableList utils.ArrayFlags) {
	if len(tableList) > 0 {
		quotedTablesStr := utils.SliceToQuotedString(tableList)
		query := fmt.Sprintf(`
SELECT
	c.oid,
	n.nspname || '.' || c.relname AS name
FROM pg_namespace n
JOIN pg_class c ON n.oid = c.relnamespace
WHERE quote_ident(n.nspname) || '.' || quote_ident(c.relname) IN (%s)`, quotedTablesStr)
		resultTables := make([]struct {
			Oid  uint32
			Name string
		}, 0)
		err := connection.Select(&resultTables, query)
		utils.CheckError(err)
		tableMap := make(map[string]uint32)
		for _, table := range resultTables {
			tableMap[table.Name] = table.Oid
		}

		partTableMap := GetPartitionTableMap(connection)
		for _, table := range tableList {
			tableOid := tableMap[table]
			if tableOid == 0 {
				logger.Fatal(nil, "Table %s does not exist", table)
			}
			partType, inMap := partTableMap[tableOid]
			if partType == "i" {
				logger.Fatal(nil, "Cannot filter on %s, as it is an intermediate partition table.  Only parent partition tables and leaf partition tables may be specified.", table)
			}
			if !*leafPartitionData && inMap && partType != "p" {
				logger.Fatal(nil, "Cannot filter on %s, as it is a partition table.  Either filter on its parent table or set the --leaf-partition-data flag.", table)
			}
		}
	}
}

func ValidateFlagCombinations() {
	utils.CheckMandatoryFlags("dbname")

	utils.CheckExclusiveFlags("debug", "quiet", "verbose")
	utils.CheckExclusiveFlags("data-only", "metadata-only")
	utils.CheckExclusiveFlags("include-schema", "include-table-file")
	utils.CheckExclusiveFlags("exclude-schema", "include-schema")
	utils.CheckExclusiveFlags("exclude-schema", "exclude-table-file", "include-table-file")
}
