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

func tableAndSchemaFilterClause() string {
	filterClause := SchemaFilterClause("n")
	if len(excludeTables) > 0 {
		filterClause += fmt.Sprintf("\nAND quote_ident(n.nspname) || '.' || quote_ident(c.relname) NOT IN (%s)", utils.SliceToQuotedString(excludeTables))
	}
	if len(includeTables) > 0 {
		filterClause += fmt.Sprintf("\nAND quote_ident(n.nspname) || '.' || quote_ident(c.relname) IN (%s)", utils.SliceToQuotedString(includeTables))
	}
	return filterClause
}

func GetAllUserTables(connection *utils.DBConn) []Relation {
	if *leafPartitionData && len(includeTables) > 0 {
		return GetUserTablesWithLeafPartitionsAndIncludeFiltering(connection)
	} else if *leafPartitionData {
		return GetUserTablesWithLeafPartitions(connection)
	} else if len(includeTables) > 0 {
		return GetUserTablesWithIncludeFiltering(connection)
	}
	return GetUserTables(connection)
}

func GetUserTables(connection *utils.DBConn) []Relation {
	query := fmt.Sprintf(`
SELECT
	n.oid AS schemaoid,
	c.oid AS oid,
	quote_ident(n.nspname) AS schema,
	quote_ident(c.relname) AS name
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
ORDER BY n.nspname, c.relname;`, tableAndSchemaFilterClause())

	results := make([]Relation, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	return results
}

func GetUserTablesWithIncludeFiltering(connection *utils.DBConn) []Relation {
	includeList := utils.SliceToQuotedString(includeTables)
	query := fmt.Sprintf(`
SELECT
	n.oid AS schemaoid,
	c.oid AS oid,
	quote_ident(n.nspname) AS schema,
	quote_ident(c.relname) AS name
FROM pg_class c
JOIN pg_namespace n
	ON c.relnamespace = n.oid
WHERE %s
AND (
	-- Get tables in the include list
	quote_ident(n.nspname) || '.' || quote_ident(c.relname) IN (%s)
	-- Get partition tables whose children are in the include list
	OR (n.nspname, c.relname) IN (
		SELECT
			n.nspname AS parentschema,
			c.relname AS parentname
		FROM pg_partition p
		JOIN pg_partition_rule r ON p.oid = r.paroid
		JOIN pg_class c ON p.parrelid = c.oid
		JOIN pg_class c2 ON c2.oid = r.parchildrelid
		JOIN pg_namespace n2 ON c2.relnamespace = n2.oid
		WHERE p.paristemplate = false
		AND quote_ident(n2.nspname) || '.' || quote_ident(c2.relname) IN (%s)
	)
	-- Get partition tables whose parents are in the include list
	OR (n.nspname, c.relname) IN (
		SELECT
			n2.nspname AS childschema,
			c2.relname AS childname
		FROM pg_partition p
		JOIN pg_partition_rule r ON p.oid = r.paroid
		JOIN pg_class c ON p.parrelid = c.oid
		JOIN pg_class c2 ON c2.oid = r.parchildrelid
		JOIN pg_namespace n2 ON c2.relnamespace = n2.oid
		JOIN pg_exttable e ON r.parchildrelid = e.reloid
		WHERE p.paristemplate = false
		AND e.reloid IS NOT NULL
		AND quote_ident(n.nspname) || '.' || quote_ident(c.relname) IN (%s)
	)
	-- Get external partition tables whose siblings are in the include list
	OR (n.nspname, c.relname) IN (
		SELECT
			n2.nspname AS childschema,
			c2.relname AS childname
		FROM pg_partition_rule r
		JOIN pg_class c2 ON c2.oid = r.parchildrelid
		JOIN pg_namespace n2 ON c2.relnamespace = n2.oid
		JOIN pg_exttable e ON r.parchildrelid = e.reloid
		WHERE r.paroid IN (
			SELECT
				p.paroid
			FROM pg_partition_rule p
			JOIN pg_class c ON p.parchildrelid = c.oid
			JOIN pg_namespace n ON c.relnamespace = n.oid
			WHERE quote_ident(n.nspname) || '.' || quote_ident(c.relname) IN (%s)
		)
	)
)
AND relkind = 'r'
ORDER BY n.nspname, c.relname;`, SchemaFilterClause("n"), includeList, includeList, includeList, includeList)

	results := make([]Relation, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	return results
}

func GetUserTablesWithLeafPartitions(connection *utils.DBConn) []Relation {
	query := fmt.Sprintf(`
SELECT
	n.oid AS schemaoid,
	c.oid AS oid,
	quote_ident(n.nspname) AS schema,
	quote_ident(c.relname) AS name
FROM pg_class c
JOIN pg_namespace n
	ON c.relnamespace = n.oid
WHERE %s
AND relkind = 'r'
ORDER BY n.nspname, c.relname;`, tableAndSchemaFilterClause())

	results := make([]Relation, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	return results
}

func GetUserTablesWithLeafPartitionsAndIncludeFiltering(connection *utils.DBConn) []Relation {
	/*
	 * In this case, we need to retrieve the parent tables of any partition tables in
	 * the filter, so that we can print the metadata for the parent tables, so we include
	 * tables in the list of include tables OR parent tables of tables in the list.
	 */
	includeList := utils.SliceToQuotedString(includeTables)
	query := fmt.Sprintf(`
SELECT
	n.oid AS schemaoid,
	c.oid AS oid,
	quote_ident(n.nspname) AS schema,
	quote_ident(c.relname) AS name
FROM pg_class c
JOIN pg_namespace n
	ON c.relnamespace = n.oid
WHERE %s
AND (
	-- Get tables in the include list
	quote_ident(n.nspname) || '.' || quote_ident(c.relname) IN (%s)
	-- Get partition tables whose children are in the include list
	OR (n.nspname, c.relname) IN (
		SELECT
			n.nspname AS parentschema,
			c.relname AS parentname
		FROM pg_partition p
		JOIN pg_partition_rule r ON p.oid = r.paroid
		JOIN pg_class c ON p.parrelid = c.oid
		JOIN pg_class c2 ON c2.oid = r.parchildrelid
		JOIN pg_namespace n2  ON c2.relnamespace = n2.oid
		WHERE p.paristemplate = false
		AND quote_ident(n2.nspname) || '.' || quote_ident(c2.relname) IN (%s)
	)
	-- Get partition tables whose parents are in the include list
	OR (n.nspname, c.relname) IN (
		SELECT
			n2.nspname AS childschema,
			c2.relname AS childname
		FROM pg_partition p
		JOIN pg_partition_rule r ON p.oid = r.paroid
		JOIN pg_class c ON p.parrelid = c.oid
		JOIN pg_class c2 ON c2.oid = r.parchildrelid
		JOIN pg_namespace n2  ON c2.relnamespace = n2.oid
		WHERE p.paristemplate = false
		AND quote_ident(n.nspname) || '.' || quote_ident(c.relname) IN (%s)
	)
)
AND relkind = 'r'
ORDER BY n.nspname, c.relname;`, SchemaFilterClause("n"), includeList, includeList, includeList)

	results := make([]Relation, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	return results
}

/*
 * This returns a map of all parent partition tables and leaf partition tables;
 * "p" indicates a parent table, "l" indicates a leaf table, and "i" indicates
 * an intermediate table.
 */
func GetPartitionTableMap(connection *utils.DBConn) map[uint32]string {
	query := `
SELECT
	pc.oid AS oid,
	'p' AS value
FROM pg_partition p
JOIN pg_class pc
	ON p.parrelid = pc.oid
UNION
SELECT
	cc.oid AS oid,
	CASE WHEN p.parlevel = levels.pl THEN 'l' ELSE 'i' END AS value
FROM pg_partition p
JOIN pg_partition_rule r
	ON p.oid = r.paroid
JOIN pg_class cc
	ON r.parchildrelid = cc.oid
JOIN (
	SELECT
		parrelid AS relid,
		max(parlevel) AS pl
	FROM pg_partition
	GROUP BY parrelid
) AS levels
	ON p.parrelid = levels.relid;
`
	return SelectAsOidToStringMap(connection, query)
}

type ColumnDefinition struct {
	Oid         uint32 `db:"attrelid"`
	Num         int    `db:"attnum"`
	Name        string
	NotNull     bool `db:"attnotnull"`
	HasDefault  bool `db:"atthasdef"`
	Type        string
	Encoding    string
	StatTarget  int `db:"attstattarget"`
	StorageType string
	DefaultVal  string
	Comment     string
}

var storageTypeCodes = map[string]string{
	"e": "EXTERNAL",
	"m": "MAIN",
	"p": "PLAIN",
	"x": "EXTENDED",
}

func GetColumnDefinitions(connection *utils.DBConn) map[uint32][]ColumnDefinition {
	// This query is adapted from the getTableAttrs() function in pg_dump.c.
	query := fmt.Sprintf(`
SELECT
	a.attrelid,
	a.attnum,
	quote_ident(a.attname) AS name,
	a.attnotnull,
	a.atthasdef,
	pg_catalog.format_type(t.oid,a.atttypmod) AS type,
	coalesce(pg_catalog.array_to_string(e.attoptions, ','), '') AS encoding,
	a.attstattarget,
	CASE WHEN a.attstorage != t.typstorage THEN a.attstorage ELSE '' END AS storagetype,
	coalesce(pg_catalog.pg_get_expr(ad.adbin, ad.adrelid), '') AS defaultval,
	coalesce(d.description,'') AS comment
FROM pg_catalog.pg_attribute a
JOIN pg_class c ON a.attrelid = c.oid
JOIN pg_namespace n ON c.relnamespace = n.oid
LEFT JOIN pg_catalog.pg_attrdef ad ON (a.attrelid = ad.adrelid AND a.attnum = ad.adnum)
LEFT JOIN pg_catalog.pg_type t ON a.atttypid = t.oid
LEFT JOIN pg_catalog.pg_attribute_encoding e ON e.attrelid = a.attrelid AND e.attnum = a.attnum
LEFT JOIN pg_description d ON (d.objoid = a.attrelid AND d.classoid = 'pg_class'::regclass AND d.objsubid = a.attnum)
WHERE %s
AND a.attnum > 0::pg_catalog.int2
AND a.attisdropped = 'f'
ORDER BY a.attrelid, a.attnum;`, tableAndSchemaFilterClause())

	results := make([]ColumnDefinition, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	resultMap := make(map[uint32][]ColumnDefinition, 0)
	for _, result := range results {
		result.StorageType = storageTypeCodes[result.StorageType]
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
	'(' || array_to_string(array_agg(quote_ident(a.attname)), ', ') || ')' AS value
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
		if resultMap[table.Oid] != "" {
			resultMap[table.Oid] = fmt.Sprintf("DISTRIBUTED BY %s", resultMap[table.Oid])
		} else {
			resultMap[table.Oid] = "DISTRIBUTED RANDOMLY"
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
	query := `SELECT c.oid, quote_ident(t.spcname) AS value FROM pg_class c JOIN pg_tablespace t ON t.oid = c.reltablespace`
	return SelectAsOidToStringMap(connection, query)
}

type Dependency struct {
	Oid              uint32
	ReferencedObject string
}

func ConstructTableDependencies(connection *utils.DBConn, tables []Relation, tableDefs map[uint32]TableDefinition, isTableFiltered bool) []Relation {
	var tableNameMap map[string]bool
	var tableOidList []string
	if isTableFiltered {
		tableNameMap = make(map[string]bool, len(tables))
		tableOidList = make([]string, len(tables))
		for i, table := range tables {
			tableNameMap[table.ToString()] = true
			tableOidList[i] = fmt.Sprintf("%d", table.Oid)
		}
	}
	nonTableQuery := fmt.Sprintf(`
SELECT
	objid AS oid,
	quote_ident(n.nspname) || '.' || quote_ident(p.typname) AS referencedobject,
	'f' AS istable
FROM pg_depend d
JOIN pg_type p ON d.refobjid = p.oid
JOIN pg_namespace n ON p.typnamespace = n.oid
JOIN pg_class c ON d.objid = c.oid AND c.relkind = 'r'
WHERE %s `, SchemaFilterClause("n"))
	tableQuery := `
SELECT
	objid AS oid,
	quote_ident(n.nspname) || '.' || quote_ident(p.relname) AS referencedobject,
	't' AS istable
FROM pg_depend d
JOIN pg_class p ON d.refobjid = p.oid AND p.relkind = 'r'
JOIN pg_namespace n ON p.relnamespace = n.oid
JOIN pg_class c ON d.objid = c.oid AND c.relkind = 'r'`

	query := ""
	// If we are filtering on tables, we only want to record dependencies on other tables in the list
	if isTableFiltered {
		query = fmt.Sprintf("%s\nWHERE objid IN (%s);", tableQuery, strings.Join(tableOidList, ","))
	} else {
		query = fmt.Sprintf("%s\nUNION\n%s;", nonTableQuery, tableQuery)
	}
	results := make([]struct {
		Oid              uint32
		ReferencedObject string
		IsTable          bool
	}, 0)
	dependencyMap := make(map[uint32][]string, 0)
	inheritanceMap := make(map[uint32][]string, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	for _, dependency := range results {
		if tableDefs[dependency.Oid].IsExternal && tableDefs[dependency.Oid].PartitionType == "l" {
			continue
		}
		if dependency.IsTable {
			inheritanceMap[dependency.Oid] = append(inheritanceMap[dependency.Oid], dependency.ReferencedObject)
		}
		if isTableFiltered && !tableNameMap[dependency.ReferencedObject] {
			continue
		}
		dependencyMap[dependency.Oid] = append(dependencyMap[dependency.Oid], dependency.ReferencedObject)
	}
	for i := 0; i < len(tables); i++ {
		tables[i].DependsUpon = dependencyMap[tables[i].Oid]
		tables[i].Inherits = inheritanceMap[tables[i].Oid]
	}
	return tables
}

func GetAllSequenceRelations(connection *utils.DBConn) []Relation {
	query := fmt.Sprintf(`SELECT
	n.oid AS schemaoid,
	c.oid AS oid,
	quote_ident(n.nspname) AS schema,
	quote_ident(c.relname) AS name
FROM pg_class c
LEFT JOIN pg_namespace n
	ON c.relnamespace = n.oid
WHERE relkind = 'S'
AND %s
ORDER BY n.nspname, c.relname;`, SchemaFilterClause("n"))

	results := make([]Relation, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	return results
}

type SequenceDefinition struct {
	Name      string `db:"sequence_name"`
	LastVal   int64  `db:"last_value"`
	StartVal  int64  `db:"start_value"`
	Increment int64  `db:"increment_by"`
	MaxVal    int64  `db:"max_value"`
	MinVal    int64  `db:"min_value"`
	CacheVal  int64  `db:"cache_value"`
	LogCnt    int64  `db:"log_cnt"`
	IsCycled  bool   `db:"is_cycled"`
	IsCalled  bool   `db:"is_called"`
}

func GetSequenceDefinition(connection *utils.DBConn, seqName string) SequenceDefinition {
	query := fmt.Sprintf("SELECT * FROM %s", seqName)
	result := SequenceDefinition{}
	err := connection.Get(&result, query)
	utils.CheckError(err)
	return result
}

func GetSequenceColumnOwnerMap(connection *utils.DBConn) map[string]string {
	query := `SELECT
	quote_ident(n.nspname) AS schema,
	quote_ident(s.relname) AS name,
	quote_ident(t.relname) AS tablename,
	quote_ident(a.attname) AS columnname
FROM pg_depend d
JOIN pg_attribute a
	ON a.attrelid = d.refobjid AND a.attnum = d.refobjsubid
JOIN pg_class s
	ON s.oid = d.objid
JOIN pg_class t
	ON t.oid = d.refobjid
JOIN pg_namespace n
	ON n.oid = s.relnamespace
WHERE s.relkind = 'S';`

	results := make([]struct {
		Schema     string
		Name       string
		TableName  string
		ColumnName string
	}, 0)
	sequenceOwners := make(map[string]string, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	for _, seqOwner := range results {
		seqFQN := utils.MakeFQN(seqOwner.Schema, seqOwner.Name)
		columnFQN := fmt.Sprintf("%s.%s.%s", seqOwner.Schema, seqOwner.TableName, seqOwner.ColumnName)
		sequenceOwners[seqFQN] = columnFQN
	}
	return sequenceOwners
}

type View struct {
	Oid         uint32
	Schema      string
	Name        string
	Definition  string
	DependsUpon []string
}

func (v View) ToString() string {
	return utils.MakeFQN(v.Schema, v.Name)
}

func GetViews(connection *utils.DBConn) []View {
	results := make([]View, 0)

	query := fmt.Sprintf(`
SELECT
	c.oid,
	quote_ident(n.nspname) AS schema,
	quote_ident(c.relname) AS name,
	pg_get_viewdef(c.oid) AS definition
FROM pg_class c
LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'v'::"char" AND %s;`, SchemaFilterClause("n"))
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
ORDER BY v2.oid, referencedobject;`, SchemaFilterClause("n"))

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

func LockTables(connection *utils.DBConn, tables []Relation) {
	logger.Info("Acquiring ACCESS SHARE locks on tables")
	for _, table := range tables {
		_, err := connection.Exec(fmt.Sprintf("LOCK TABLE %s IN ACCESS SHARE MODE", table.ToString()))
		utils.CheckError(err)
	}
	logger.Info("Locks acquired")
}
