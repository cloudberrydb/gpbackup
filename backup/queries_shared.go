package backup

/*
 * This file contains structs and functions related to executing specific
 * queries to gather metadata for the objects handled in predata_shared.go.
 */

import (
	"fmt"
	"sort"

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

type MetadataQueryParams struct {
	NameField    string
	SchemaField  string
	OidField     string
	ACLField     string
	OwnerField   string
	OidTable     string
	CommentTable string
	CatalogTable string
	Shared       bool
}

var (
	// A list of schemas we don't ever want to dump, formatted for use in a WHERE clause
	nonUserSchemaFilterClause = `nspname NOT LIKE 'pg_temp_%' AND nspname NOT LIKE 'pg_toast%' AND nspname NOT IN ('gp_toolkit', 'information_schema', 'pg_aoseg', 'pg_bitmapindex', 'pg_catalog')`

	RoleParams      = MetadataQueryParams{NameField: "rolname", OidField: "oid", CatalogTable: "pg_authid", Shared: true}
	DatabaseParams  = MetadataQueryParams{NameField: "datname", ACLField: "datacl", OwnerField: "datdba", CatalogTable: "pg_database", Shared: true}
	SchemaParams    = MetadataQueryParams{NameField: "nspname", ACLField: "nspacl", OwnerField: "nspowner", CatalogTable: "pg_namespace"}
	TypeParams      = MetadataQueryParams{NameField: "typname", SchemaField: "typnamespace", OwnerField: "typowner", CatalogTable: "pg_type"}
	ProcLangParams  = MetadataQueryParams{NameField: "lanname", ACLField: "lanacl", OwnerField: "lanowner", CatalogTable: "pg_language"}
	FunctionParams  = MetadataQueryParams{NameField: "proname", SchemaField: "pronamespace", ACLField: "proacl", OwnerField: "proowner", CatalogTable: "pg_proc"}
	ProtocolParams  = MetadataQueryParams{NameField: "ptcname", ACLField: "ptcacl", OwnerField: "ptcowner", CatalogTable: "pg_extprotocol"}
	AggregateParams = MetadataQueryParams{NameField: "proname", OwnerField: "proowner", CatalogTable: "pg_proc"}
	RelationParams  = MetadataQueryParams{NameField: "relname", SchemaField: "relnamespace", ACLField: "relacl", OwnerField: "relowner", CatalogTable: "pg_class"}
	ResQueueParams  = MetadataQueryParams{NameField: "rsqname", OidField: "oid", CatalogTable: "pg_resqueue", Shared: true}
	CastParams      = MetadataQueryParams{NameField: "typname", OidField: "oid", OidTable: "pg_type", CatalogTable: "pg_cast"}
	ConParams       = MetadataQueryParams{NameField: "conname", OidField: "oid", CatalogTable: "pg_constraint"}
	IndexParams     = MetadataQueryParams{NameField: "relname", OidField: "indexrelid", OidTable: "pg_class", CommentTable: "pg_class", CatalogTable: "pg_index"}
	RuleParams      = MetadataQueryParams{NameField: "rulename", OidField: "oid", CatalogTable: "pg_rewrite"}
	TriggerParams   = MetadataQueryParams{NameField: "tgname", OidField: "oid", CatalogTable: "pg_trigger"}
)

type QueryOid struct {
	Oid uint32
}

func OidFromObjectName(dbconn *utils.DBConn, name string, params MetadataQueryParams) uint32 {
	catalogTable := params.CatalogTable
	if params.OidTable != "" {
		catalogTable = params.OidTable
	}
	query := fmt.Sprintf("SELECT oid FROM %s WHERE %s ='%s'", catalogTable, params.NameField, name)
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

func GetMetadataForObjectType(connection *utils.DBConn, params MetadataQueryParams) MetadataMap {
	schemaStr := ""
	if params.SchemaField != "" {
		schemaStr = fmt.Sprintf(`JOIN pg_namespace n ON o.%s = n.oid
WHERE %s`, params.SchemaField, nonUserSchemaFilterClause)
	}
	aclStr := ""
	if params.ACLField != "" {
		aclStr = fmt.Sprintf(`CASE
		WHEN o.%s IS NULL THEN ''
		WHEN array_length(o.%s, 1) = 0 THEN 'GRANTEE=/GRANTOR'
		ELSE unnest(o.%s)::text
	END
`, params.ACLField, params.ACLField, params.ACLField)
	} else {
		aclStr = "''"
	}
	descFunc := "obj_description"
	if params.Shared {
		descFunc = "shobj_description"
	}
	query := fmt.Sprintf(`
SELECT
	o.oid,
	%s AS privileges,
	pg_get_userbyid(o.%s) AS owner,
	coalesce(%s(o.oid, '%s'), '') AS comment
FROM %s o
%s
ORDER BY o.oid;
`, aclStr, params.OwnerField, descFunc, params.CatalogTable, params.CatalogTable, schemaStr)

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
					metadataMap[currentOid] = sortACLs(metadata)
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
		metadataMap[currentOid] = sortACLs(metadata)
	}
	return metadataMap
}

func sortACLs(metadata ObjectMetadata) ObjectMetadata {
	sort.Slice(metadata.Privileges, func(i, j int) bool {
		return metadata.Privileges[i].Grantee < metadata.Privileges[j].Grantee
	})
	return metadata
}

func GetCommentsForObjectType(connection *utils.DBConn, params MetadataQueryParams) MetadataMap {
	descFunc := "obj_description"
	if params.Shared {
		descFunc = "shobj_description"
	}
	commentTable := params.CatalogTable
	if params.CommentTable != "" {
		commentTable = params.CommentTable
	}
	query := fmt.Sprintf(`
SELECT
	o.%s AS oid,
	coalesce(%s(o.%s, '%s'), '') AS comment
FROM %s o;
`, params.OidField, descFunc, params.OidField, commentTable, params.CatalogTable)

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
