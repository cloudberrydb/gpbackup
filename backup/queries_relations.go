package backup

/*
 * This file contains structs and functions related to executing specific
 * queries to gather metadata for the objects handled in predata_relations.go.
 */

import (
	"fmt"
	"strings"

	"github.com/greenplum-db/gpbackup/utils"
)

func GetAllUserTables(connection *utils.DBConn) []Relation {
	// This query is adapted from the getTables() function in pg_dump.c.
	query := `
SELECT
	n.oid AS schemaoid,
	c.oid AS relationoid,
	n.nspname AS schemaname,
	c.relname AS relationname
FROM pg_class c
LEFT JOIN pg_partition_rule pr
	ON c.oid = pr.parchildrelid
LEFT JOIN pg_partition p
	ON pr.paroid = p.oid
LEFT JOIN pg_namespace n
	ON c.relnamespace = n.oid
WHERE relkind = 'r'
AND c.oid NOT IN (SELECT
	p.parchildrelid
FROM pg_partition_rule p
LEFT
JOIN pg_exttable e
	ON p.parchildrelid = e.reloid
WHERE e.reloid IS NULL)
AND (c.relnamespace > 16384
OR n.nspname = 'public')
ORDER BY schemaname, relationname;`

	results := make([]Relation, 0)

	err := connection.Select(&results, query)
	utils.CheckError(err)
	return results
}

type QueryTableAttributes struct {
	AttNum     int
	Name       string `db:"attname"`
	NotNull    bool   `db:"attnotnull"`
	HasDefault bool   `db:"atthasdefault"`
	IsDropped  bool   `db:"attisdropped"`
	TypeName   string `db:"atttypname"`
	Encoding   string `db:"attencoding"`
	Comment    string `db:"attcomment"`
}

func GetTableAttributes(connection *utils.DBConn, oid uint32) []QueryTableAttributes {
	// This query is adapted from the getTableAttrs() function in pg_dump.c.
	query := fmt.Sprintf(`
SELECT a.attnum,
	a.attname,
	a.attnotnull,
	a.atthasdef AS atthasdefault,
	a.attisdropped,
	pg_catalog.format_type(t.oid,a.atttypmod) AS atttypname,
	coalesce(pg_catalog.array_to_string(e.attoptions, ','), '') AS attencoding,
	coalesce(pg_catalog.col_description(a.attrelid, a.attnum), '') AS attcomment
FROM pg_catalog.pg_attribute a
	LEFT JOIN pg_catalog.pg_type t ON a.atttypid = t.oid
	LEFT OUTER JOIN pg_catalog.pg_attribute_encoding e ON e.attrelid = a.attrelid
	AND e.attnum = a.attnum
WHERE a.attrelid = %d
	AND a.attnum > 0::pg_catalog.int2
	AND a.attisdropped = 'f'
ORDER BY a.attrelid,
	a.attnum;`, oid)

	results := make([]QueryTableAttributes, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	return results
}

type QueryTableDefault struct {
	AdNum      int
	DefaultVal string
}

func GetTableDefaults(connection *utils.DBConn, oid uint32) []QueryTableDefault {
	// This query is adapted from the hasdefaults == true case of the getTableAttrs() function in pg_dump.c.
	query := fmt.Sprintf(`
SELECT adnum,
	pg_catalog.pg_get_expr(adbin, adrelid) AS defaultval
FROM pg_catalog.pg_attrdef
WHERE adrelid = %d
ORDER BY adrelid,
	adnum;`, oid)

	results := make([]QueryTableDefault, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	return results
}

func GetDistributionPolicy(connection *utils.DBConn, oid uint32) string {
	// This query is adapted from the addDistributedBy() function in pg_dump.c.
	query := fmt.Sprintf(`
SELECT a.attname as string
FROM pg_attribute a
JOIN (
	SELECT
		unnest(attrnums) AS attnum,
		localoid
	FROM gp_distribution_policy
) p
ON (p.localoid,p.attnum) = (a.attrelid,a.attnum)
WHERE a.attrelid = %d;`, oid)

	results := SelectStringSlice(connection, query)
	if len(results) == 0 {
		return "DISTRIBUTED RANDOMLY"
	}
	distCols := make([]string, 0)
	for _, dist := range results {
		distCols = append(distCols, utils.QuoteIdent(dist))
	}
	return fmt.Sprintf("DISTRIBUTED BY (%s)", strings.Join(distCols, ", "))
}

func GetPartitionDefinition(connection *utils.DBConn, oid uint32) string {
	/* This query is adapted from the gp_partitioning_available == true case of the dumpTableSchema
	 * function in pg_dump.c.
	 */
	query := fmt.Sprintf("SELECT * FROM pg_get_partition_def(%d, true, true) AS string WHERE string IS NOT NULL", oid)
	return SelectString(connection, query)
}

func GetPartitionTemplateDefinition(connection *utils.DBConn, oid uint32) string {
	/* This query is adapted from the isTemplatesSupported == true case of the dumpTableSchema
	 * function in pg_dump.c.
	 */
	query := fmt.Sprintf("SELECT * FROM pg_get_partition_template_def(%d, true, true) AS string WHERE string IS NOT NULL", oid)
	return SelectString(connection, query)
}

func GetStorageOptions(connection *utils.DBConn, oid uint32) string {
	query := fmt.Sprintf(`
SELECT array_to_string(reloptions, ', ') as string
FROM pg_class
WHERE oid = %d AND reloptions IS NOT NULL;`, oid)
	return SelectString(connection, query)
}

func GetTablespaceName(connection *utils.DBConn, oid uint32) string {
	query := fmt.Sprintf(`
SELECT ts.spcname AS string
FROM pg_class c
JOIN pg_tablespace ts
ON ts.oid = c.reltablespace
WHERE c.oid = %d;`, oid)
	return SelectString(connection, query)
}

type QueryDependency struct {
	Object           uint32
	ReferencedObject string
}

func ConstructTableDependencies(connection *utils.DBConn, tables []Relation) []Relation {
	query := `
SELECT
	objid AS object,
	quote_ident(n.nspname) || '.' || quote_ident(p.relname) AS referencedobject 
FROM pg_depend d
JOIN pg_class p
	ON d.refobjid = p.oid AND p.relkind = 'r'
JOIN pg_namespace n
	ON p.relnamespace = n.oid
JOIN pg_class c
	ON d.objid = c.oid AND c.relkind = 'r';`

	results := make([]QueryDependency, 0)
	dependencyMap := make(map[uint32][]string, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	for _, dependency := range results {
		dependencyMap[dependency.Object] = append(dependencyMap[dependency.Object], dependency.ReferencedObject)
	}
	for i := 0; i < len(tables); i++ {
		tables[i].DependsUpon = dependencyMap[tables[i].RelationOid]
	}
	return tables
}

type QueryViewDefinition struct {
	Oid        uint32
	SchemaName string
	ViewName   string
	Definition string
}

func GetViewDefinitions(connection *utils.DBConn) []QueryViewDefinition {
	results := make([]QueryViewDefinition, 0)

	query := fmt.Sprintf(`
SELECT
	c.oid,
	n.nspname AS schemaname,
	c.relname AS viewname,
	pg_get_viewdef(c.oid) AS definition
FROM pg_class c
LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'v'::"char" AND %s;`, nonUserSchemaFilterClause)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	return results
}
