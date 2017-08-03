package backup

/*
 * This file contains structs and functions related to executing specific
 * queries to gather metadata for the objects handled in predata_relations.go.
 */

import (
	"fmt"

	"github.com/greenplum-db/gpbackup/utils"
)

func GetAllUserTables(connection *utils.DBConn) []Relation {
	// This query is adapted from the getTables() function in pg_dump.c.
	query := fmt.Sprintf(`
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
WHERE %s
AND relkind = 'r'
AND c.oid NOT IN (SELECT
	p.parchildrelid
FROM pg_partition_rule p
LEFT
JOIN pg_exttable e
	ON p.parchildrelid = e.reloid
WHERE e.reloid IS NULL)
ORDER BY schemaname, relationname;`, NonUserSchemaFilterClause("n"))

	results := make([]Relation, 0)

	err := connection.Select(&results, query)
	utils.CheckError(err)
	return results
}

type ColumnDefinition struct {
	Oid        uint32 `db:"attrelid"`
	Num        int    `db:"attnum"`
	Name       string `db:"attname"`
	NotNull    bool   `db:"attnotnull"`
	HasDefault bool   `db:"atthasdef"`
	IsDropped  bool   `db:"attisdropped"`
	TypeName   string
	Encoding   string
	StatTarget int `db:"attstattarget"`
	DefaultVal string
	Comment    string
}

func GetColumnDefinitions(connection *utils.DBConn) map[uint32][]ColumnDefinition {
	// This query is adapted from the getTableAttrs() function in pg_dump.c.
	query := fmt.Sprintf(`
SELECT
	a.attrelid,
	a.attnum,
	a.attname,
	a.attnotnull,
	a.atthasdef,
	a.attisdropped,
	pg_catalog.format_type(t.oid,a.atttypmod) AS typename,
	coalesce(pg_catalog.array_to_string(e.attoptions, ','), '') AS encoding,
	a.attstattarget,
	coalesce(pg_catalog.pg_get_expr(ad.adbin, ad.adrelid), '') AS defaultval,
	coalesce(pg_catalog.col_description(a.attrelid, a.attnum), '') AS comment
FROM pg_catalog.pg_attribute a
JOIN pg_class c ON a.attrelid = c.oid
JOIN pg_namespace n ON c.relnamespace = n.oid
LEFT JOIN pg_catalog.pg_attrdef ad ON (a.attrelid = ad.adrelid AND a.attnum = ad.adnum)
LEFT JOIN pg_catalog.pg_type t ON a.atttypid = t.oid
LEFT OUTER JOIN pg_catalog.pg_attribute_encoding e ON e.attrelid = a.attrelid AND e.attnum = a.attnum
WHERE %s
AND a.attnum > 0::pg_catalog.int2
AND a.attisdropped = 'f'
ORDER BY a.attrelid, a.attnum;`, NonUserSchemaFilterClause("n"))

	results := make([]ColumnDefinition, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	resultMap := make(map[uint32][]ColumnDefinition, 0)
	for _, result := range results {
		resultMap[result.Oid] = append(resultMap[result.Oid], result)
	}
	return resultMap
}

type DistributionPolicy struct {
	Oid    uint32
	Policy string
}

func SelectAsOidToStringMap(connection *utils.DBConn, query string) map[uint32]string {
	var results []struct {
		Oid   uint32
		Value string
	}
	err := connection.Select(&results, query)
	utils.CheckError(err)
	resultMap := make(map[uint32]string, 0)
	for _, result := range results {
		resultMap[result.Oid] = result.Value
	}
	return resultMap
}

func GetDistributionPolicies(connection *utils.DBConn, tables []Relation) map[uint32]string {
	// This query is adapted from the addDistributedBy() function in pg_dump.c.
	query := `
SELECT
	a.attrelid AS oid,
	'(' || array_to_string(array_agg(a.attname), ', ') || ')' AS value
FROM pg_attribute a
JOIN (
	SELECT
		unnest(attrnums) AS attnum,
		localoid
	FROM gp_distribution_policy
) p
ON (p.localoid,p.attnum) = (a.attrelid,a.attnum)
GROUP BY a.attrelid ORDER BY a.attrelid;`

	resultMap := SelectAsOidToStringMap(connection, query)
	for _, table := range tables {
		if resultMap[table.RelationOid] != "" {
			resultMap[table.RelationOid] = fmt.Sprintf("DISTRIBUTED BY %s", resultMap[table.RelationOid])
		} else {
			resultMap[table.RelationOid] = "DISTRIBUTED RANDOMLY"
		}
	}
	return resultMap
}

func GetPartitionDefinitions(connection *utils.DBConn) map[uint32]string {
	query := `SELECT parrelid AS oid, pg_get_partition_def(parrelid, true, true) AS value FROM pg_partition`
	return SelectAsOidToStringMap(connection, query)
}

func GetPartitionTemplates(connection *utils.DBConn) map[uint32]string {
	query := fmt.Sprintf("SELECT parrelid AS oid, pg_get_partition_template_def(parrelid, true, true) AS value FROM pg_partition")
	return SelectAsOidToStringMap(connection, query)
}

func GetStorageOptions(connection *utils.DBConn) map[uint32]string {
	query := ` SELECT oid, array_to_string(reloptions, ', ') AS value FROM pg_class WHERE reloptions IS NOT NULL;`
	return SelectAsOidToStringMap(connection, query)
}

func GetTablespaceNames(connection *utils.DBConn) map[uint32]string {
	query := `SELECT c.oid, t.spcname AS value FROM pg_class c JOIN pg_tablespace t ON t.oid = c.reltablespace`
	return SelectAsOidToStringMap(connection, query)
}

type Dependency struct {
	Oid              uint32
	ReferencedObject string
}

func ConstructTableDependencies(connection *utils.DBConn, tables []Relation) []Relation {
	query := `
SELECT
	objid AS oid,
	quote_ident(n.nspname) || '.' || quote_ident(p.relname) AS referencedobject
FROM pg_depend d
JOIN pg_class p ON d.refobjid = p.oid AND p.relkind = 'r'
JOIN pg_namespace n ON p.relnamespace = n.oid
JOIN pg_class c ON d.objid = c.oid AND c.relkind = 'r';`

	results := make([]Dependency, 0)
	dependencyMap := make(map[uint32][]string, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	for _, dependency := range results {
		dependencyMap[dependency.Oid] = append(dependencyMap[dependency.Oid], dependency.ReferencedObject)
	}
	for i := 0; i < len(tables); i++ {
		tables[i].DependsUpon = dependencyMap[tables[i].RelationOid]
	}
	return tables
}

type View struct {
	Oid         uint32
	SchemaName  string
	ViewName    string
	Definition  string
	DependsUpon []string
}

func (v View) ToString() string {
	return MakeFQN(v.SchemaName, v.ViewName)
}

func GetViews(connection *utils.DBConn) []View {
	results := make([]View, 0)

	query := fmt.Sprintf(`
SELECT
	c.oid,
	n.nspname AS schemaname,
	c.relname AS viewname,
	pg_get_viewdef(c.oid) AS definition
FROM pg_class c
LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'v'::"char" AND %s;`, NonUserSchemaFilterClause("n"))
	err := connection.Select(&results, query)
	utils.CheckError(err)
	return results
}

func ConstructViewDependencies(connection *utils.DBConn, views []View) []View {
	query := fmt.Sprintf(`
SELECT DISTINCT
	v2.oid,
	quote_ident(n.nspname) || '.' || quote_ident(v1.relname) AS referencedobject
FROM pg_class v1
JOIN pg_depend d ON d.refobjid = v1.oid
JOIN pg_rewrite rw ON rw.oid = d.objid
JOIN pg_class v2 ON rw.ev_class = v2.oid
JOIN pg_namespace n ON v1.relnamespace = n.oid
WHERE d.classid = 'pg_rewrite'::regclass::oid
	AND v1.oid != v2.oid
	AND v1.relkind = 'v'
	AND %s
ORDER BY v2.oid, referencedobject;`, NonUserSchemaFilterClause("n"))

	results := make([]Dependency, 0)
	dependencyMap := make(map[uint32][]string, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	for _, dependency := range results {
		dependencyMap[dependency.Oid] = append(dependencyMap[dependency.Oid], dependency.ReferencedObject)
	}
	for i := 0; i < len(views); i++ {
		views[i].DependsUpon = dependencyMap[views[i].Oid]
	}
	return views
}
