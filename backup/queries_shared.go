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

func GetAllUserSchemas(connection *utils.DBConn) []Schema {
	/*
	 * This query is constructed from scratch, but the list of schemas to exclude
	 * is copied from gpcrondump so that gpbackup exhibits similar behavior regarding
	 * which schemas are backed up.
	 */
	query := fmt.Sprintf(`
SELECT
	oid,
	nspname AS name
FROM pg_namespace n
WHERE %s
ORDER BY name;`, SchemaFilterClause("n"))

	results := make([]Schema, 0)

	err := connection.Select(&results, query)
	utils.CheckError(err)
	return results
}

type Constraint struct {
	Oid                uint32
	ConName            string
	ConType            string
	ConDef             string
	OwningObject       string
	IsDomainConstraint bool
	IsPartitionParent  bool
}

func GetConstraints(connection *utils.DBConn) []Constraint {
	// This query is adapted from the queries underlying \d in psql.
	query := fmt.Sprintf(`
SELECT
	c.oid,
	conname,
	contype,
	pg_get_constraintdef(c.oid, TRUE) AS condef,
	quote_ident(n.nspname) || '.' || quote_ident(r.relname) AS owningobject,
	'f' AS isdomainconstraint,
	CASE
		WHEN pt.parrelid IS NULL THEN 'f'
		ELSE 't'
	END AS ispartitionparent
FROM pg_constraint c
LEFT JOIN pg_class r ON c.conrelid = r.oid
LEFT JOIN pg_partition pt ON c.conrelid = pt.parrelid
JOIN pg_namespace n ON n.oid = c.connamespace
WHERE %s
AND r.relname IS NOT NULL
AND conrelid NOT IN (SELECT parchildrelid FROM pg_partition_rule)
AND (conrelid, conname) NOT IN (SELECT i.inhrelid, c.conname FROM pg_inherits i JOIN pg_constraint c ON i.inhrelid = c.conrelid JOIN pg_constraint p ON i.inhparent = p.conrelid WHERE c.conname = p.conname)
GROUP BY c.oid, conname, contype, r.relname, n.nspname, pt.parrelid
UNION
SELECT
	c.oid,
	conname,
	contype,
	pg_get_constraintdef(c.oid, TRUE) AS condef,
	quote_ident(n.nspname) || '.' || quote_ident(t.typname) AS owningobject,
	't' AS isdomainconstraint,
	'f' AS ispartitionparent
FROM pg_constraint c
LEFT JOIN pg_type t ON c.contypid = t.oid
JOIN pg_namespace n ON n.oid = c.connamespace
WHERE %s
AND t.typname IS NOT NULL
GROUP BY c.oid, conname, contype, n.nspname, t.typname
ORDER BY conname;`, SchemaFilterClause("n"), SchemaFilterClause("n"))

	results := make([]Constraint, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	return results
}

/*
 * Structs and functions relating to generic metadata handling.
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
	TYPE_AGGREGATE       = MetadataQueryParams{NameField: "proname", SchemaField: "pronamespace", OwnerField: "proowner", CatalogTable: "pg_proc"}
	TYPE_CAST            = MetadataQueryParams{NameField: "typname", OidField: "oid", OidTable: "pg_type", CatalogTable: "pg_cast"}
	TYPE_CONSTRAINT      = MetadataQueryParams{NameField: "conname", SchemaField: "connamespace", OidField: "oid", CatalogTable: "pg_constraint"}
	TYPE_CONVERSION      = MetadataQueryParams{NameField: "conname", OidField: "oid", SchemaField: "connamespace", OwnerField: "conowner", CatalogTable: "pg_conversion"}
	TYPE_DATABASE        = MetadataQueryParams{NameField: "datname", ACLField: "datacl", OwnerField: "datdba", CatalogTable: "pg_database", Shared: true}
	TYPE_FUNCTION        = MetadataQueryParams{NameField: "proname", SchemaField: "pronamespace", ACLField: "proacl", OwnerField: "proowner", CatalogTable: "pg_proc"}
	TYPE_INDEX           = MetadataQueryParams{NameField: "relname", OidField: "indexrelid", OidTable: "pg_class", CommentTable: "pg_class", CatalogTable: "pg_index"}
	TYPE_PROCLANGUAGE    = MetadataQueryParams{NameField: "lanname", ACLField: "lanacl", OwnerField: "lanowner", CatalogTable: "pg_language"}
	TYPE_OPERATOR        = MetadataQueryParams{NameField: "oprname", SchemaField: "oprnamespace", OidField: "oid", OwnerField: "oprowner", CatalogTable: "pg_operator"}
	TYPE_OPERATORCLASS   = MetadataQueryParams{NameField: "opcname", SchemaField: "opcnamespace", OidField: "oid", OwnerField: "opcowner", CatalogTable: "pg_opclass"}
	TYPE_OPERATORFAMILY  = MetadataQueryParams{NameField: "opfname", SchemaField: "opfnamespace", OidField: "oid", OwnerField: "opfowner", CatalogTable: "pg_opfamily"}
	TYPE_PROTOCOL        = MetadataQueryParams{NameField: "ptcname", ACLField: "ptcacl", OwnerField: "ptcowner", CatalogTable: "pg_extprotocol"}
	TYPE_RELATION        = MetadataQueryParams{NameField: "relname", SchemaField: "relnamespace", ACLField: "relacl", OwnerField: "relowner", CatalogTable: "pg_class"}
	TYPE_RESOURCEQUEUE   = MetadataQueryParams{NameField: "rsqname", OidField: "oid", CatalogTable: "pg_resqueue", Shared: true}
	TYPE_ROLE            = MetadataQueryParams{NameField: "rolname", OidField: "oid", CatalogTable: "pg_authid", Shared: true}
	TYPE_RULE            = MetadataQueryParams{NameField: "rulename", OidField: "oid", CatalogTable: "pg_rewrite"}
	TYPE_SCHEMA          = MetadataQueryParams{NameField: "nspname", ACLField: "nspacl", OwnerField: "nspowner", CatalogTable: "pg_namespace"}
	TYPE_TABLESPACE      = MetadataQueryParams{NameField: "spcname", ACLField: "spcacl", OwnerField: "spcowner", CatalogTable: "pg_tablespace", Shared: true}
	TYPE_TSCONFIGURATION = MetadataQueryParams{NameField: "cfgname", OidField: "oid", SchemaField: "cfgnamespace", OwnerField: "cfgowner", CatalogTable: "pg_ts_config"}
	TYPE_TSDICTIONARY    = MetadataQueryParams{NameField: "dictname", OidField: "oid", SchemaField: "dictnamespace", OwnerField: "dictowner", CatalogTable: "pg_ts_dict"}
	TYPE_TSPARSER        = MetadataQueryParams{NameField: "prsname", OidField: "oid", SchemaField: "prsnamespace", CatalogTable: "pg_ts_parser"}
	TYPE_TSTEMPLATE      = MetadataQueryParams{NameField: "tmplname", OidField: "oid", SchemaField: "tmplnamespace", CatalogTable: "pg_ts_template"}
	TYPE_TRIGGER         = MetadataQueryParams{NameField: "tgname", OidField: "oid", CatalogTable: "pg_trigger"}
	TYPE_TYPE            = MetadataQueryParams{NameField: "typname", SchemaField: "typnamespace", OwnerField: "typowner", CatalogTable: "pg_type"}
)

// A list of schemas we don't want to back up, formatted for use in a WHERE clause
func SchemaFilterClause(namespace string) string {
	includeSchemaFilterClauseStr := ""
	if len(includeSchemas) > 0 {
		includeSchemaFilterClauseStr = fmt.Sprintf("AND %s.nspname IN (%s)", namespace, utils.SliceToQuotedString(includeSchemas))
	}
	return fmt.Sprintf(`%s.nspname NOT LIKE 'pg_temp_%%' AND %s.nspname NOT LIKE 'pg_toast%%' AND %s.nspname NOT IN ('gp_toolkit', 'information_schema', 'pg_aoseg', 'pg_bitmapindex', 'pg_catalog') %s`, namespace, namespace, namespace, includeSchemaFilterClauseStr)
}

func GetMetadataForObjectType(connection *utils.DBConn, params MetadataQueryParams) MetadataMap {
	schemaStr := ""
	if params.SchemaField != "" {
		schemaStr = fmt.Sprintf(`JOIN pg_namespace n ON o.%s = n.oid
WHERE %s`, params.SchemaField, SchemaFilterClause("n"))
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

	results := make([]struct {
		Oid        uint32
		Privileges string
		Owner      string
		Comment    string
	}, 0)
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
	schemaStr := ""
	if params.SchemaField != "" {
		schemaStr = fmt.Sprintf(` JOIN pg_namespace n ON o.%s = n.oid
	 WHERE %s`, params.SchemaField, SchemaFilterClause("n"))
	}
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
FROM %s o%s;
`, params.OidField, descFunc, params.OidField, commentTable, params.CatalogTable, schemaStr)

	results := make([]struct {
		Oid     uint32
		Comment string
	}, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)

	metadataMap := make(MetadataMap)
	if len(results) > 0 {
		for _, result := range results {
			metadataMap[result.Oid] = ObjectMetadata{[]ACL{}, "", result.Comment}
		}
	}
	return metadataMap
}

/*
 * Helper functions
 */

/*
 * This is a convenience function for Select() when we're selecting single string
 * that may be NULL or not exist.  We can't use Get() because that expects exactly
 * one string and will panic if no rows are returned, even if using a sql.NullString.
 */
func SelectString(connection *utils.DBConn, query string) string {
	results := make([]struct{ String string }, 0)
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
	results := make([]struct{ String string }, 0)
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
