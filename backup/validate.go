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
	n.nspname || '.' || c.relname AS string
FROM pg_namespace n
JOIN pg_class c ON n.oid = c.relnamespace
WHERE quote_ident(n.nspname) || '.' || quote_ident(c.relname) IN (%s)`, quotedTablesStr)
		resultTables := SelectStringSlice(connection, query)
		if len(resultTables) < len(tableList) {
			tableMap := make(map[string]bool)
			for _, table := range resultTables {
				tableMap[table] = true
			}

			for _, table := range tableList {
				if _, ok := tableMap[table]; !ok {
					logger.Fatal(nil, "Table %s does not exist", table)
				}
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
