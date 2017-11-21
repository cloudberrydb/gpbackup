package restore

import (
	"fmt"
	"strings"

	"github.com/greenplum-db/gpbackup/utils"
	"github.com/pkg/errors"
)

/*
 * This file contains functions related to validating user input.
 */

func ValidateFilterSchemas(connection *utils.DBConn, schemaList utils.ArrayFlags) {
	ValidateFilterSchemasInRestoreDatabase(connection, schemaList)
	ValidateFilterSchemasInBackupSet(schemaList)
}

func ValidateFilterSchemasInRestoreDatabase(connection *utils.DBConn, schemaList utils.ArrayFlags) {
	if len(schemaList) > 0 {
		quotedSchemasStr := utils.SliceToQuotedString(schemaList)
		query := fmt.Sprintf("SELECT nspname AS string FROM pg_namespace WHERE nspname IN (%s)", quotedSchemasStr)
		resultSchemas := utils.SelectStringSlice(connection, query)
		if len(resultSchemas) > 0 {
			logger.Fatal(nil, "Schema %s already exists", resultSchemas[0])
		}
	}
}

func ValidateFilterSchemasInBackupSet(schemaList utils.ArrayFlags) {
	schemaMap := make(map[string]bool, len(schemaList))
	for _, schema := range schemaList {
		schemaMap[schema] = true
	}
	if len(schemaList) > 0 {
		if !backupConfig.DataOnly {
			for _, entry := range globalTOC.PredataEntries {
				if _, ok := schemaMap[entry.Schema]; ok {
					delete(schemaMap, entry.Schema)
				}
				if len(schemaMap) == 0 {
					return
				}
			}
		} else {
			for _, entry := range globalTOC.MasterDataEntries {
				if _, ok := schemaMap[entry.Schema]; ok {
					delete(schemaMap, entry.Schema)
				}
				if len(schemaMap) == 0 {
					return
				}
			}
		}
	}
	keys := make([]string, len(schemaMap))

	i := 0
	for k := range schemaMap {
		keys[i] = k
		i++
	}
	if len(schemaList) > 0 {
		logger.Fatal(errors.Errorf("Could not find the following schema(s) in the backup set: %s", strings.Join(keys, ", ")), "")
	}
}

func ValidateBackupFlagCombinations() {
	if backupConfig.SingleDataFile && *numJobs != 1 {
		logger.Fatal(errors.Errorf("Cannot use jobs flag when restoring backups with a single data file per segment."), "")
	}
	if backupConfig.SingleDataFile && len(includeSchemas) > 0 {
		logger.Fatal(errors.Errorf("Cannot use include-schema flag when restoring backups with a single data file per segment."), "")
	}
}

func ValidateFlagCombinations() {
	utils.CheckMandatoryFlags("timestamp")
	utils.CheckExclusiveFlags("debug", "quiet", "verbose")
}
