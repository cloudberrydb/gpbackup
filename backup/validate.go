package backup

import (
	"fmt"

	"github.com/greenplum-db/gpbackup/utils"
)

/*
 * This file contains functions related to validating user input.
 */

func ValidateIncludeSchemas(connection *utils.DBConn) {
	if len(includeSchemas) > 0 {
		quotedSchemasStr := utils.SliceToQuotedString(includeSchemas)
		query := fmt.Sprintf("SELECT nspname AS string FROM pg_namespace WHERE nspname IN (%s)", quotedSchemasStr)
		resultSchemas := SelectStringSlice(connection, query)
		if len(resultSchemas) < len(includeSchemas) {
			schemaMap := make(map[string]bool)
			for _, schema := range resultSchemas {
				schemaMap[schema] = true
			}

			for _, schema := range includeSchemas {
				if _, ok := schemaMap[schema]; !ok {
					logger.Fatal(nil, "Schema %s does not exist", schema)
				}
			}
		}
	}
}

func ValidateFlagCombinations() {
	utils.CheckMandatoryFlags("dbname")
	utils.CheckExclusiveFlags("debug", "quiet", "verbose")
	utils.CheckExclusiveFlags("data-only", "metadata-only")
}
