package backup

import (
	"fmt"
	"regexp"

	"github.com/greenplum-db/gpbackup/utils"
	"github.com/pkg/errors"
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
		ValidateFQNs(tableList)
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
			if partTableMap[tableOid] == "i" {
				logger.Fatal(nil, "Cannot filter on %s, as it is an intermediate partition table.  Only parent partition tables and leaf partition tables may be specified.", table)
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
	utils.CheckExclusiveFlags("exclude-table-file", "leaf-partition-data")
	utils.CheckExclusiveFlags("metadata-only", "leaf-partition-data")
	utils.CheckExclusiveFlags("metadata-only", "single-data-file")
	utils.CheckExclusiveFlags("no-compression", "compression-level")
}

func ValidateFQNs(fqns []string) {
	unquotedIdentString := "[a-z_][a-z0-9_]*"
	validIdentString := fmt.Sprintf("(?:\"(.*)\"|(%s))", unquotedIdentString)
	validFormat := regexp.MustCompile(fmt.Sprintf(`^%s\.%s$`, validIdentString, validIdentString))
	var matches []string
	for _, fqn := range fqns {
		if matches = validFormat.FindStringSubmatch(fqn); len(matches) == 0 {
			logger.Fatal(errors.Errorf(`Table %s is not correctly fully-qualified.  Please ensure that it is in the format schema.table, it is quoted appropriately, and it has no preceding or trailing whitespace.`, fqn), "")
		}
	}
}

func ValidateCompressionLevel(compressionLevel int) {
	//We treat 0 as a default value and so assume the flag is not set if it is 0
	if compressionLevel < 0 || compressionLevel > 9 {
		logger.Fatal(errors.Errorf("Compression level must be between 1 and 9"), "")
	}
}

func ValidateFlagValues() {
	utils.ValidateBackupDir(*backupDir)
	ValidateCompressionLevel(*compressionLevel)
}
