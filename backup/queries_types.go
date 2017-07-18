package backup

/*
 * This file contains structs and functions related to executing specific
 * queries to gather metadata for the objects handled in predata_types.go.
 */

import (
	"fmt"

	"github.com/greenplum-db/gpbackup/utils"
)

type TypeDefinition struct {
	Oid             uint32
	TypeSchema      string `db:"nspname"`
	TypeName        string `db:"typname"`
	Type            string `db:"typtype"`
	AttName         string `db:"attname"`
	AttType         string
	Input           string `db:"typinput"`
	Output          string `db:"typoutput"`
	Receive         string `db:"typreceive"`
	Send            string `db:"typsend"`
	ModIn           string `db:"typmodin"`
	ModOut          string `db:"typmodout"`
	InternalLength  int    `db:"typlen"`
	IsPassedByValue bool   `db:"typbyval"`
	Alignment       string `db:"typalign"`
	Storage         string `db:"typstorage"`
	DefaultVal      string
	Element         string
	Delimiter       string `db:"typdelim"`
	EnumLabels      string
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
	t.typreceive,
	t.typsend,
	t.typmodin,
	t.typmodout,
	t.typlen,
	t.typbyval,
	t.typalign,
	t.typstorage,
	coalesce(t.typdefault, '') AS defaultval,
	coalesce(pg_catalog.format_type(t.typelem, NULL), '') AS element,
	t.typdelim,
	coalesce(enumlabels, '') as enumlabels
FROM pg_type t
LEFT JOIN pg_attribute a ON t.typrelid = a.attrelid
LEFT JOIN pg_namespace n ON t.typnamespace = n.oid
LEFT JOIN (
	  SELECT enumtypid,string_agg(quote_literal(enumlabel), E',\n\t') AS enumlabels FROM pg_enum GROUP BY enumtypid
	) e ON t.oid = e.enumtypid
WHERE %s
AND (t.typtype = 'c' OR t.typtype = 'b' OR t.typtype='e' OR t.typtype='p')
AND (n.nspname || '.' || t.typname) NOT IN (SELECT nspname || '._' || relname FROM pg_namespace n join pg_class c ON n.oid = c.relnamespace WHERE c.relkind = 'r' OR c.relkind = 'S' OR c.relkind = 'v')
AND (n.nspname || '.' || t.typname) NOT IN (SELECT nspname || '.' || relname FROM pg_namespace n join pg_class c ON n.oid = c.relnamespace WHERE c.relkind = 'r' OR c.relkind = 'S' OR c.relkind = 'v')
AND (n.nspname || '.' || t.typname) NOT IN (SELECT nspname || '._' || typname FROM pg_namespace n join pg_type t ON n.oid = t.typnamespace)
ORDER BY n.nspname, t.typname, a.attname;`, nonUserSchemaFilterClause)

	results := make([]TypeDefinition, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	return results
}
