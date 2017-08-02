package backup

/*
 * This file contains structs and functions related to executing specific
 * queries to gather metadata for the objects handled in predata_types.go.
 */

import (
	"fmt"

	"github.com/greenplum-db/gpbackup/utils"
)

type CompositeTypeAttribute struct {
	AttName string
	AttType string
}

type TypeDefinition struct {
	Oid             uint32
	TypeSchema      string `db:"nspname"`
	TypeName        string `db:"typname"`
	Type            string `db:"typtype"`
	AttName         string `db:"attname"`
	AttType         string
	Input           string `db:"typinput"`
	Output          string `db:"typoutput"`
	Receive         string
	Send            string
	ModIn           string
	ModOut          string
	InternalLength  int  `db:"typlen"`
	IsPassedByValue bool `db:"typbyval"`
	Alignment       string
	Storage         string `db:"typstorage"`
	DefaultVal      string
	Element         string
	Delimiter       string `db:"typdelim"`
	EnumLabels      string
	BaseType        string
	NotNull         bool `db:"typnotnull"`
	CompositeAtts   []CompositeTypeAttribute
	DependsUpon     []string
}

func GetTypeDefinitions(connection *utils.DBConn) []TypeDefinition {
	/*
	 * To get all user-defined types, this query needs to filter out automatically-
	 * defined types created for tables (e.g. if the user creates table public.foo,
	 * the base type public._foo and the composite type public.foo will also be
	 * created).  However, a join on pg_class is very expensive, so instead it
	 * compares schemaname.typename from pg_type to schemaname.tablename and
	 * schemaname._tablename from pg_tables and schemaname._typename from pg_type
	 * to filter those out.
	 *
	 * Several fields use "-" in place of NULL, so we filter those to empty strings
	 * for ease of processing later.
	 */
	query := fmt.Sprintf(`
SELECT
	t.oid,
	n.nspname,
	t.typname,
	t.typtype,
	coalesce(a.attname, '') AS attname,
	coalesce(pg_catalog.format_type(a.atttypid, NULL), '') AS atttype,
	t.typinput,
	t.typoutput,
	CASE WHEN t.typreceive = 0 THEN '' ELSE t.typreceive::regproc::text END AS receive,
	CASE WHEN t.typsend = 0 THEN '' ELSE t.typsend::regproc::text END AS send,
	CASE WHEN t.typmodin = 0 THEN '' ELSE t.typmodin::regproc::text END AS modin,
	CASE WHEN t.typmodout = 0 THEN '' ELSE t.typmodout::regproc::text END AS modout,
	t.typlen,
	t.typbyval,
	CASE WHEN t.typalign = '-' THEN '' ELSE t.typalign END AS alignment,
	t.typstorage,
	coalesce(t.typdefault, '') AS defaultval,
	CASE WHEN t.typelem != 0::regproc THEN pg_catalog.format_type(t.typelem, NULL) ELSE '' END AS element,
	t.typdelim,
	coalesce(enumlabels, '') AS enumlabels,
	coalesce(b.typname, '') AS basetype,
	t.typnotnull
FROM pg_type t
LEFT JOIN pg_attribute a ON t.typrelid = a.attrelid
LEFT JOIN pg_namespace n ON t.typnamespace = n.oid
LEFT JOIN (
	  SELECT enumtypid,string_agg(quote_literal(enumlabel), E',\n\t') AS enumlabels FROM pg_enum GROUP BY enumtypid
	) e ON t.oid = e.enumtypid
LEFT JOIN pg_type b ON t.typbasetype = b.oid
WHERE %s
AND NOT (t.typname[0] = '_' AND t.typelem != 0)
AND (n.nspname || '.' || t.typname) NOT IN (SELECT nspname || '.' || relname FROM pg_namespace n join pg_class c ON n.oid = c.relnamespace WHERE c.relkind = 'r' OR c.relkind = 'S' OR c.relkind = 'v')
ORDER BY n.nspname, t.typname, a.attname;`, NonUserSchemaFilterClause("n"))

	results := make([]TypeDefinition, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	return results
}

/*
 * We already have the functions on which a base type depends in the base type's
 * TypeDefinition, but we need to query pg_proc to determine whether one of those
 * functions is a built-in function (and therefore should not be considered a
 * dependency for dependency sorting purposes).
 */
func ConstructBaseTypeDependencies(connection *utils.DBConn, types []TypeDefinition) []TypeDefinition {
	query := fmt.Sprintf(`
SELECT DISTINCT
    t.oid,
    quote_ident(n.nspname) || '.' || quote_ident(p.proname) AS referencedobject
FROM pg_depend d
JOIN pg_proc p ON (d.refobjid = p.oid AND p.pronamespace != (SELECT oid FROM pg_namespace WHERE nspname = 'pg_catalog'))
JOIN pg_type t ON (d.objid = t.oid AND t.typtype = 'b')
JOIN pg_namespace n ON n.oid = t.typnamespace
WHERE %s
AND d.refclassid = 'pg_proc'::regclass
AND d.deptype = 'n';`, NonUserSchemaFilterClause("n"))

	results := make([]QueryDependency, 0)
	dependencyMap := make(map[uint32][]string, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	for _, dependency := range results {
		dependencyMap[dependency.Oid] = append(dependencyMap[dependency.Oid], dependency.ReferencedObject)
	}
	for i := 0; i < len(types); i++ {
		if types[i].Type == "b" {
			types[i].DependsUpon = dependencyMap[types[i].Oid]
		}
	}
	return types
}

/*
 * We already have the base type of a domain in the domain's TypeDefinition, but
 * we need to query pg_type to determine whether the base type is built in (and
 * therefore should not be considered a dependency for dependency sorting purposes).
 */
func ConstructDomainDependencies(connection *utils.DBConn, types []TypeDefinition) []TypeDefinition {
	query := fmt.Sprintf(`
SELECT
	t.oid,
	quote_ident(n.nspname) || '.' || quote_ident(bt.typname) AS referencedobject
FROM pg_type t
JOIN pg_namespace n ON t.typnamespace = n.oid
JOIN pg_type bt ON t.typbasetype = bt.oid
WHERE %s
AND bt.typnamespace != (
	SELECT
		bn.oid
	FROM pg_namespace bn
	WHERE bn.nspname = 'pg_catalog'
);`, NonUserSchemaFilterClause("n"))

	results := make([]QueryDependency, 0)
	dependencyMap := make(map[uint32][]string, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	for _, dependency := range results {
		dependencyMap[dependency.Oid] = append(dependencyMap[dependency.Oid], dependency.ReferencedObject)
	}
	for i := 0; i < len(types); i++ {
		if types[i].Type == "d" {
			types[i].DependsUpon = dependencyMap[types[i].Oid]
		}
	}
	return types
}

func ConstructCompositeTypeDependencies(connection *utils.DBConn, types []TypeDefinition) []TypeDefinition {
	query := fmt.Sprintf(`
SELECT DISTINCT
	tc.oid,
	quote_ident(n.nspname) || '.' || quote_ident(t.typname) AS referencedobject
FROM pg_depend d
JOIN pg_type t ON (d.refobjid = t.oid AND t.typtype != 'p' AND t.typtype != 'e' AND t.typnamespace != (SELECT oid FROM pg_namespace WHERE nspname = 'pg_catalog'))
JOIN pg_class c ON (d.objid = c.oid AND c.relkind = 'c')
JOIN pg_type tc ON (tc.typrelid = c.oid AND tc.typtype = 'c')
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE %s
AND d.refclassid = 'pg_type'::regclass
AND c.reltype != t.oid
AND d.deptype = 'n';`, NonUserSchemaFilterClause("n"))

	results := make([]QueryDependency, 0)
	dependencyMap := make(map[uint32][]string, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	for _, dependency := range results {
		dependencyMap[dependency.Oid] = append(dependencyMap[dependency.Oid], dependency.ReferencedObject)
	}
	for i := 0; i < len(types); i++ {
		if types[i].Type == "c" {
			types[i].DependsUpon = dependencyMap[types[i].Oid]
		}
	}
	return types
}
