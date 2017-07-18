package backup

/*
 * This file contains structs and functions related to executing specific
 * queries to gather metadata for the objects handled in predata_externals.go.
 */

import (
	"fmt"

	"github.com/greenplum-db/gpbackup/utils"

	"github.com/pkg/errors"
)

func GetExternalTablesMap(connection *utils.DBConn) map[string]bool {
	extTableMap := make(map[string]bool)
	query := `
SELECT
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
AND relstorage = 'x' AND (c.relnamespace > 16384
OR n.nspname = 'public')
ORDER BY schemaname, relationname;`

	results := make([]utils.Relation, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	for _, table := range results {
		extTableMap[table.ToString()] = true
	}
	return extTableMap
}

func GetExternalTableDefinition(connection *utils.DBConn, oid uint32) ExternalTableDefinition {
	query := fmt.Sprintf(`
SELECT
	coalesce(array_to_string(urilocation, ','), '') AS location,
	array_to_string(execlocation, ',') AS execlocation,
	fmttype AS formattype,
	fmtopts AS formatopts,
	(
		array_to_string(ARRAY(SELECT pg_catalog.quote_ident(option_name) || ' ' || pg_catalog.quote_literal(option_value)
		FROM pg_options_to_table(options)
		ORDER BY option_name), E'\n\t')
	) AS options,
	coalesce(command, '') AS command,
	coalesce(rejectlimit, 0) AS rejectlimit,
	coalesce(rejectlimittype, '') AS rejectlimittype,
	coalesce((SELECT relname FROM pg_class WHERE oid = fmterrtbl), '') AS errtable,
	pg_encoding_to_char(encoding) AS encoding,
	writable
FROM pg_exttable
WHERE reloid = '%d';`, oid)

	results := make([]ExternalTableDefinition, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	if len(results) == 1 {
		return results[0]
	} else if len(results) > 1 {
		logger.Fatal(errors.Errorf("Too many rows returned from query: got %d rows, expected 1 row", len(results)), "")
	}
	return ExternalTableDefinition{}
}
