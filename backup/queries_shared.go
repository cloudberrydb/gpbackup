package backup

/*
 * This file contains structs and functions related to executing specific
 * queries to gather metadata for the objects handled in predata_shared.go.
 */

import (
	"fmt"

	"github.com/greenplum-db/gpbackup/utils"

	"github.com/pkg/errors"
)

/*
 * All queries in the various queries_*.go files come from one of three sources:
 * - Copied from pg_dump largely unmodified
 * - Derived from the output of a psql flag like \d+ or \df
 * - Constructed from scratch
 * In the former two cases, a reference to the query source is provided for
 * further reference.
 *
 * All structs in these file whose names begin with "Query" are intended only
 * for use with the functions immediately following them.  Structs in the utils
 * package (especially Table and Schema) are intended for more general use.
 */

var (
	// A list of schemas we don't ever want to dump, formatted for use in a WHERE clause
	nonUserSchemaFilterClause = `nspname NOT LIKE 'pg_temp_%'
AND nspname NOT LIKE 'pg_toast%'
AND nspname NOT IN ('gp_toolkit', 'information_schema', 'pg_aoseg', 'pg_bitmapindex', 'pg_catalog')`
)

type QueryOid struct {
	Oid uint32
}

func OidFromObjectName(dbconn *utils.DBConn, name string, nameField string, catalogTable string) uint32 {
	query := fmt.Sprintf("SELECT oid FROM %s WHERE %s ='%s'", catalogTable, nameField, name)
	result := QueryOid{}
	err := dbconn.Get(&result, query)
	utils.CheckError(err)
	return result.Oid
}

type QueryObjectMetadata struct {
	Oid        uint32
	Privileges string
	Owner      string
	Comment    string
}

func GetMetadataForObjectType(connection *utils.DBConn, schemaField string, aclField string, ownerField string, catalogTable string) MetadataMap {
	schemaStr := ""
	if schemaField != "" {
		schemaStr = fmt.Sprintf(`JOIN pg_namespace n ON o.%s = n.oid
WHERE %s`, schemaField, nonUserSchemaFilterClause)
	}
	aclStr := ""
	if aclField != "" {
		aclStr = fmt.Sprintf(`CASE
		WHEN o.%s IS NULL THEN ''
		WHEN array_length(o.%s, 1) = 0 THEN 'GRANTEE=/GRANTOR'
		ELSE unnest(o.%s)::text
	END
`, aclField, aclField, aclField)
	} else {
		aclStr = "''"
	}
	sh := ""
	if catalogTable == "pg_database" {
		sh = "sh"
	}
	query := fmt.Sprintf(`
SELECT
	o.oid,
	%s AS privileges,
	pg_get_userbyid(o.%s) AS owner,
	coalesce(%sobj_description(o.oid, '%s'), '') AS comment
FROM %s o
%s
ORDER BY o.oid, privileges;
`, aclStr, ownerField, sh, catalogTable, catalogTable, schemaStr)

	results := make([]QueryObjectMetadata, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)

	metadataMap := make(MetadataMap)
	var metadata ObjectMetadata
	if len(results) > 0 {
		currentOid := uint32(0)
		// Collect all entries for the same object into one ObjectMetadata
		for _, result := range results {
			if result.Oid != currentOid {
				if currentOid != 0 {
					metadataMap[currentOid] = metadata
				}
				currentOid = result.Oid
				metadata = ObjectMetadata{}
				metadata.Privileges = make([]ACL, 0)
				metadata.Owner = result.Owner
				metadata.Comment = result.Comment
			}
			privileges := ParseACL(result.Privileges)
			if privileges != nil {
				metadata.Privileges = append(metadata.Privileges, *privileges)
			}
		}
		metadataMap[currentOid] = metadata
	}
	return metadataMap
}

func GetCommentsForObjectType(connection *utils.DBConn, schemaField string, oidField string, commentTable string, catalogTable string) MetadataMap {
	schemaStr := ""
	if schemaField != "" {
		schemaStr = fmt.Sprintf(`JOIN pg_namespace n ON o.%s = n.oid
WHERE %s`, schemaField, nonUserSchemaFilterClause)
	}
	query := fmt.Sprintf(`
SELECT
	o.%s AS oid,
	coalesce(obj_description(o.%s, '%s'), '') AS comment
FROM %s o
%s;`, oidField, oidField, commentTable, catalogTable, schemaStr)

	results := make([]QueryObjectMetadata, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)

	metadataMap := make(MetadataMap)
	if len(results) > 0 {
		for _, result := range results {
			metadataMap[result.Oid] = ObjectMetadata{[]ACL{}, result.Owner, result.Comment}
		}
	}
	return metadataMap
}

/*
 * Helper functions
 */

type QuerySingleString struct {
	String string
}

/*
 * This is a convenience function for Select() when we're selecting single string
 * that may be NULL or not exist.  We can't use Get() because that expects exactly
 * one string and will panic if no rows are returned, even if using a sql.NullString.
 */
func SelectString(connection *utils.DBConn, query string) string {
	results := make([]QuerySingleString, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	if len(results) == 1 {
		return results[0].String
	} else if len(results) > 1 {
		logger.Fatal(errors.Errorf("Too many rows returned from query: got %d rows, expected 1 row", len(results)), "")
	}
	return ""
}

// This is a convenience function for Select() when we're selecting single strings.
func SelectStringSlice(connection *utils.DBConn, query string) []string {
	results := make([]QuerySingleString, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	retval := make([]string, 0)
	for _, str := range results {
		if str.String != "" {
			retval = append(retval, str.String)
		}
	}
	return retval
}
