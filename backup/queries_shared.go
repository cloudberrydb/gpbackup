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

/*
 * Generic functions and structs relating to schemas and relations.
 */
type Schema struct {
	Oid  uint32
	Name string
}

func (s Schema) ToString() string {
	return utils.QuoteIdent(s.Name)
}

func SchemaFromString(name string) Schema {
	var schema string
	var matches []string
	if matches = utils.QuotedIdentifier.FindStringSubmatch(name); len(matches) != 0 {
		schema = utils.ReplacerUnescape.Replace(matches[1])
	} else if matches = utils.UnquotedIdentifier.FindStringSubmatch(name); len(matches) != 0 {
		schema = utils.ReplacerUnescape.Replace(matches[1])
	} else {
		logger.Fatal(errors.Errorf("\"%s\" is not a valid identifier", name), "")
	}
	return Schema{0, schema}
}

type Relation struct {
	SchemaOid    uint32
	RelationOid  uint32
	SchemaName   string
	RelationName string
	DependsUpon  []string
}

/*
 * This function prints a table in fully-qualified schema.table format, with
 * everything quoted and escaped appropriately.
 */
func (t Relation) ToString() string {
	return MakeFQN(t.SchemaName, t.RelationName)
}

/* Parse an appropriately-escaped schema.table string into a Relation.  The Relation's
 * Oid fields are left at 0, and will need to be filled in with the real values
 * if the Relation is to be used in any Get[Something]() function in queries.go.
 */
func RelationFromString(name string) Relation {
	var schema, table string
	var matches []string
	if matches = utils.QuotedOrUnquotedString.FindStringSubmatch(name); len(matches) != 0 {
		if matches[1] != "" { // schema was quoted
			schema = utils.ReplacerUnescape.Replace(matches[1])
		} else { // schema wasn't quoted
			schema = utils.ReplacerUnescape.Replace(matches[2])
		}
		if matches[3] != "" { // table was quoted
			table = utils.ReplacerUnescape.Replace(matches[3])
		} else { // table wasn't quoted
			table = utils.ReplacerUnescape.Replace(matches[4])
		}
	} else {
		logger.Fatal(errors.Errorf("\"%s\" is not a valid fully-qualified table expression", name), "")
	}
	return BasicRelation(schema, table)
}

func BasicRelation(schema string, relation string) Relation {
	return Relation{
		SchemaOid:    0,
		SchemaName:   schema,
		RelationOid:  0,
		RelationName: relation,
	}
}

type Schemas []Schema

func (slice Schemas) Len() int {
	return len(slice)
}

func (slice Schemas) Less(i int, j int) bool {
	return slice[i].Name < slice[j].Name
}

func (slice Schemas) Swap(i int, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}

func SortSchemas(objects Schemas) {
	sort.Sort(objects)
}

/*
 * Given a list of Relations, this function returns a sorted list of their Schemas.
 * It assumes that the Relation list is sorted by schema and then by table, so it
 * doesn't need to do any sorting itself.
 */
func GetUniqueSchemas(schemas []Schema, tables []Relation) []Schema {
	currentSchemaOid := uint32(0)
	uniqueSchemas := make([]Schema, 0)
	schemaMap := make(map[uint32]Schema, 0)
	for _, schema := range schemas {
		schemaMap[schema.Oid] = schema
	}
	for _, table := range tables {
		if table.SchemaOid != currentSchemaOid {
			currentSchemaOid = table.SchemaOid
			uniqueSchemas = append(uniqueSchemas, schemaMap[currentSchemaOid])
		}
	}
	return uniqueSchemas
}

type Relations []Relation

func (slice Relations) Len() int {
	return len(slice)
}

/*
 * Dependencies are sorted before tables that depend on them.  We return false
 * for all comparisons and true by default to ensure that the entire list is
 * traversed.
 */
func (slice Relations) Less(i int, j int) bool {
	for _, dependencyFQN := range slice[i].DependsUpon {
		if slice[j].ToString() == dependencyFQN {
			return false
		}
	}
	return true
}

func (slice Relations) Swap(i int, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}

func SortRelations(tables Relations) {
	sort.Sort(tables)
}

func SortViews(views Views) {
	sort.Sort(views)
}

type Views []QueryViewDefinition

func (d Views) Len() int {
	return len(d)
}

func (d Views) Swap(i, j int) {
	d[i], d[j] = d[j], d[i]
}

func (d Views) Less(i, j int) bool {
	for _, dependencyFQN := range d[i].DependsUpon {
		if d[j].ToString() == dependencyFQN {
			return false
		}
	}
	return true
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
	AggregateParams      = MetadataQueryParams{NameField: "proname", OwnerField: "proowner", CatalogTable: "pg_proc"}
	CastParams           = MetadataQueryParams{NameField: "typname", OidField: "oid", OidTable: "pg_type", CatalogTable: "pg_cast"}
	ConParams            = MetadataQueryParams{NameField: "conname", OidField: "oid", CatalogTable: "pg_constraint"}
	DatabaseParams       = MetadataQueryParams{NameField: "datname", ACLField: "datacl", OwnerField: "datdba", CatalogTable: "pg_database", Shared: true}
	FunctionParams       = MetadataQueryParams{NameField: "proname", SchemaField: "pronamespace", ACLField: "proacl", OwnerField: "proowner", CatalogTable: "pg_proc"}
	IndexParams          = MetadataQueryParams{NameField: "relname", OidField: "indexrelid", OidTable: "pg_class", CommentTable: "pg_class", CatalogTable: "pg_index"}
	ProcLangParams       = MetadataQueryParams{NameField: "lanname", ACLField: "lanacl", OwnerField: "lanowner", CatalogTable: "pg_language"}
	OperatorParams       = MetadataQueryParams{NameField: "oprname", OidField: "oid", OwnerField: "oprowner", CatalogTable: "pg_operator"}
	OperatorClassParams  = MetadataQueryParams{NameField: "opcname", SchemaField: "opcnamespace", OidField: "oid", OwnerField: "opcowner", CatalogTable: "pg_opclass"}
	OperatorFamilyParams = MetadataQueryParams{NameField: "opfname", SchemaField: "opfnamespace", OidField: "oid", OwnerField: "opfowner", CatalogTable: "pg_opfamily"}
	ProtocolParams       = MetadataQueryParams{NameField: "ptcname", ACLField: "ptcacl", OwnerField: "ptcowner", CatalogTable: "pg_extprotocol"}
	RelationParams       = MetadataQueryParams{NameField: "relname", SchemaField: "relnamespace", ACLField: "relacl", OwnerField: "relowner", CatalogTable: "pg_class"}
	ResQueueParams       = MetadataQueryParams{NameField: "rsqname", OidField: "oid", CatalogTable: "pg_resqueue", Shared: true}
	RoleParams           = MetadataQueryParams{NameField: "rolname", OidField: "oid", CatalogTable: "pg_authid", Shared: true}
	RuleParams           = MetadataQueryParams{NameField: "rulename", OidField: "oid", CatalogTable: "pg_rewrite"}
	SchemaParams         = MetadataQueryParams{NameField: "nspname", ACLField: "nspacl", OwnerField: "nspowner", CatalogTable: "pg_namespace"}
	TablespaceParams     = MetadataQueryParams{NameField: "spcname", ACLField: "spcacl", OwnerField: "spcowner", CatalogTable: "pg_tablespace", Shared: true}
	TriggerParams        = MetadataQueryParams{NameField: "tgname", OidField: "oid", CatalogTable: "pg_trigger"}
	TypeParams           = MetadataQueryParams{NameField: "typname", SchemaField: "typnamespace", OwnerField: "typowner", CatalogTable: "pg_type"}
)

// A list of schemas we don't ever want to dump, formatted for use in a WHERE clause
func NonUserSchemaFilterClause(namespace string) string {
	return fmt.Sprintf(`%s.nspname NOT LIKE 'pg_temp_%%' AND %s.nspname NOT LIKE 'pg_toast%%' AND %s.nspname NOT IN ('gp_toolkit', 'information_schema', 'pg_aoseg', 'pg_bitmapindex', 'pg_catalog')`, namespace, namespace, namespace)
}

type QueryOid struct {
	Oid uint32
}

func OidFromObjectName(dbconn *utils.DBConn, schemaName string, objectName string, params MetadataQueryParams) uint32 {
	catalogTable := params.CatalogTable
	if params.OidTable != "" {
		catalogTable = params.OidTable
	}
	schemaStr := ""
	if schemaName != "" {
		schemaStr = fmt.Sprintf(" AND %s = (SELECT oid FROM pg_namespace WHERE nspname = '%s')", params.SchemaField, schemaName)
	}
	query := fmt.Sprintf("SELECT oid FROM %s WHERE %s ='%s'%s", catalogTable, params.NameField, objectName, schemaStr)
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
WHERE %s`, params.SchemaField, NonUserSchemaFilterClause("n"))
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
