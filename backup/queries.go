package backup

/*
 * This file contains structs and functions related to executing specific
 * queries that gather object metadata and database information that will
 * be backed up during a dump.
 */

import (
	"fmt"
	"gpbackup/utils"
	"strings"

	"github.com/pkg/errors"
)

/*
 * All queries in this file come from one of three sources:
 * - Copied from pg_dump largely unmodified
 * - Derived from the output of a psql flag like \d+ or \df
 * - Constructed from scratch
 * In the former two cases, a reference to the query source is provided for
 * further reference.
 *
 * All structs in this file whose names begin with "Query" are intended only
 * for use with the functions immediately following them.  Structs in the utils
 * package (especially Table and Schema) are intended for more general use.
 */

var (
	// A list of schemas we don't ever want to dump, formatted for use in a WHERE clause
	nonUserSchemaFilterClause = `nspname NOT LIKE 'pg_temp_%'
AND nspname NOT LIKE 'pg_toast%'
AND nspname NOT IN ('gp_toolkit', 'information_schema', 'pg_aoseg', 'pg_bitmapindex', 'pg_catalog')`
)

/*
 * Queries requiring their own structs
 */

func GetAllUserSchemas(connection *utils.DBConn) []utils.Schema {
	/* This query is constructed from scratch, but the list of schemas to exclude
	 * is copied from gpcrondump so that gpbackup exhibits similar behavior regarding
	 * which schemas are dumped.
	 */
	query := fmt.Sprintf(`
SELECT
	oid AS schemaoid,
	nspname AS schemaname,
	coalesce(obj_description(oid, 'pg_namespace'), '') AS comment,
	pg_get_userbyid(nspowner) AS owner
FROM pg_namespace
WHERE %s
ORDER BY schemaname;`, nonUserSchemaFilterClause)
	results := make([]utils.Schema, 0)

	err := connection.Select(&results, query)
	utils.CheckError(err)
	return results
}

func GetAllUserTables(connection *utils.DBConn) []utils.Relation {
	// This query is adapted from the getTables() function in pg_dump.c.
	query := `
SELECT
	n.oid AS schemaoid,
	c.oid AS relationoid,
n.nspname AS schemaname,
	c.relname AS relationname,
	coalesce(obj_description(c.oid, 'pg_class'), '') AS comment,
	pg_get_userbyid(c.relowner) AS owner
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

	results := make([]utils.Relation, 0)

	err := connection.Select(&results, query)
	utils.CheckError(err)
	return results
}

type QueryTableAtts struct {
	AttNum        int
	AttName       string
	AttNotNull    bool
	AttHasDefault bool
	AttIsDropped  bool
	AttTypName    string
	AttEncoding   string
	AttComment    string
}

func GetTableAttributes(connection *utils.DBConn, oid uint32) []QueryTableAtts {
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
ORDER BY a.attrelid,
	a.attnum;`, oid)

	results := make([]QueryTableAtts, 0)
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

type QueryConstraint struct {
	ConName    string
	ConType    string
	ConDef     string
	ConComment string
}

func GetConstraints(connection *utils.DBConn, oid uint32) []QueryConstraint {
	// This query is adapted from the queries underlying \d in psql.
	query := fmt.Sprintf(`
SELECT
	conname,
	contype,
	pg_catalog.pg_get_constraintdef(oid, TRUE) AS condef,
	coalesce(obj_description(oid, 'pg_constraint'), '') AS concomment
FROM pg_catalog.pg_constraint
WHERE conrelid = %d;
`, oid)

	results := make([]QueryConstraint, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	return results
}

func GetAllSequences(connection *utils.DBConn) []utils.Relation {
	query := `SELECT
	n.oid AS schemaoid,
	c.oid AS relationoid,
	n.nspname AS schemaname,
	c.relname AS relationname,
	coalesce(obj_description(c.oid, 'pg_class'), '') AS comment,
	pg_get_userbyid(c.relowner) AS owner
FROM pg_class c
LEFT JOIN pg_namespace n
	ON c.relnamespace = n.oid
WHERE relkind = 'S'
ORDER BY schemaname, relationname;`
	results := make([]utils.Relation, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	return results
}

type QuerySequence struct {
	Name      string `db:"sequence_name"`
	LastVal   int64  `db:"last_value"`
	Increment int64  `db:"increment_by"`
	MaxVal    int64  `db:"max_value"`
	MinVal    int64  `db:"min_value"`
	CacheVal  int64  `db:"cache_value"`
	LogCnt    int64  `db:"log_cnt"`
	IsCycled  bool   `db:"is_cycled"`
	IsCalled  bool   `db:"is_called"`
}

func GetSequence(connection *utils.DBConn, seqName string) QuerySequence {
	query := fmt.Sprintf("SELECT * FROM %s", seqName)
	result := QuerySequence{}
	err := connection.Get(&result, query)
	utils.CheckError(err)
	return result
}

type QuerySessionGUCs struct {
	ClientEncoding       string `db:"client_encoding"`
	StdConformingStrings string `db:"standard_conforming_strings"`
	DefaultWithOids      string `db:"default_with_oids"`
}

func GetSessionGUCs(connection *utils.DBConn) QuerySessionGUCs {
	result := QuerySessionGUCs{}
	query := "SHOW client_encoding;"
	err := connection.Get(&result, query)
	query = "SHOW standard_conforming_strings;"
	err = connection.Get(&result, query)
	query = "SHOW default_with_oids;"
	err = connection.Get(&result, query)
	utils.CheckError(err)
	return result
}

type QueryIndexMetadata struct {
	Name    string
	Def     string
	Comment string
}

func GetIndexMetadata(connection *utils.DBConn, oid uint32) []QueryIndexMetadata {
	query := fmt.Sprintf(`
SELECT
	t.relname AS name,
	pg_get_indexdef(i.indexrelid) AS def,
	coalesce(obj_description(t.oid, 'pg_class'), '') AS comment
FROM pg_index i
JOIN pg_class t
	ON (t.oid = i.indexrelid)
WHERE i.indrelid = %d
ORDER BY name;`, oid)

	results := make([]QueryIndexMetadata, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	return results
}

type QueryFunctionDefinition struct {
	SchemaName        string `db:"nspname"`
	FunctionName      string `db:"proname"`
	ReturnsSet        bool   `db:"proretset"`
	FunctionBody      string
	BinaryPath        string
	Arguments         string
	IdentArgs         string
	ResultType        string
	Volatility        string  `db:"provolatile"`
	IsStrict          bool    `db:"proisstrict"`
	IsSecurityDefiner bool    `db:"prosecdef"`
	Config            string  `db:"proconfig"`
	Cost              float32 `db:"procost"`
	NumRows           float32 `db:"prorows"`
	SqlUsage          string  `db:"prodataaccess"`
	Language          string
	Comment           string
	Owner             string
}

func GetFunctionDefinitions(connection *utils.DBConn) []QueryFunctionDefinition {
	/*
	 * This query is copied from the dumpFunc() function in pg_dump.c, modified
	 * slightly to also retrieve the function's schema, name, and comment.
	 */
	query := fmt.Sprintf(`
SELECT
	nspname,
	proname,
	proretset,
	coalesce(prosrc, '') AS functionbody,
	coalesce(probin, '') AS binarypath,
	pg_catalog.pg_get_function_arguments(p.oid) AS arguments,
	pg_catalog.pg_get_function_identity_arguments(p.oid) AS identargs,
	pg_catalog.pg_get_function_result(p.oid) AS resulttype,
	provolatile,
	proisstrict,
	prosecdef,
	(
		coalesce(array_to_string(ARRAY(SELECT 'SET ' || option_name || ' TO ' || option_value
		FROM pg_options_to_table(proconfig)), ' '), '')
	) AS proconfig,
	procost,
	prorows,
	prodataaccess,
	(SELECT lanname FROM pg_catalog.pg_language WHERE oid = prolang) AS language,
	coalesce(obj_description(p.oid, 'pg_proc'), '') AS comment,
	pg_get_userbyid(proowner) AS owner
FROM pg_proc p
LEFT JOIN pg_namespace n
ON p.pronamespace = n.oid
WHERE %s;`, nonUserSchemaFilterClause)

	results := make([]QueryFunctionDefinition, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	return results
}

/*
 * Queries using generic structs defined below or structs defined elsewhere
 */

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
	} else {
		distCols := make([]string, 0)
		for _, dist := range results {
			distCols = append(distCols, utils.QuoteIdent(dist))
		}
		return fmt.Sprintf("DISTRIBUTED BY (%s)", strings.Join(distCols, ", "))
	}
}

func GetDatabaseGUCs(connection *utils.DBConn) []string {
	query := fmt.Sprintf(`SELECT unnest(datconfig) AS string
FROM pg_database
WHERE datname = '%s';`, connection.DBName)
	return SelectStringSlice(connection, query)
}

func GetDatabaseOwner(connection *utils.DBConn) string {
	query := fmt.Sprintf(`SELECT pg_catalog.pg_get_userbyid(datdba) AS string
FROM pg_database
WHERE datname = '%s';`, connection.DBName)
	return SelectString(connection, query)
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

func GetDatabaseComment(connection *utils.DBConn) string {
	query := fmt.Sprintf(`SELECT description AS string FROM pg_shdescription
JOIN pg_database ON objoid = pg_database.oid
WHERE datname = '%s';`, connection.DBName)
	return SelectString(connection, query)
}

type QueryProceduralLanguage struct {
	Name      string `db:"lanname"`
	Owner     string
	IsPl      bool `db:"lanispl"`
	PlTrusted bool `db:"lanpltrusted"`
	Handler   string
	Inline    string
	Validator string
	Access    string `db:"lanacl"`
	Comment   string
}

func GetProceduralLanguages(connection *utils.DBConn) []QueryProceduralLanguage {
	results := make([]QueryProceduralLanguage, 0)
	query := `
SELECT l.lanname,
	pg_get_userbyid(l.lanowner) as owner,
	l.lanispl,
	l.lanpltrusted,
	coalesce(p_hand.oid::pg_catalog.regprocedure::text, '') AS handler,
	coalesce(p_in.oid::pg_catalog.regprocedure::text, '') AS inline,
	coalesce(p_val.oid::pg_catalog.regprocedure::text, '') AS validator,
	coalesce(pg_catalog.array_to_string(l.lanacl, ','), '') as lanacl,
	coalesce(obj_description(l.oid, 'pg_language'), '') AS comment
FROM pg_language l
LEFT JOIN pg_proc p_hand ON l.lanplcallfoid = p_hand.oid
LEFT JOIN pg_proc p_in ON l.laninline = p_in.oid
LEFT JOIN pg_proc p_val ON l.lanvalidator = p_val.oid
WHERE l.lanispl='t';`
	err := connection.Select(&results, query)
	utils.CheckError(err)
	return results
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
		retval = append(retval, str.String)
	}
	return retval
}
