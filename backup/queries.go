package backup

/*
 * This file contains structs and functions related to executing specific
 * queries that gather object metadata and database information that will
 * be backed up during a dump.
 */

import (
	"fmt"
	"strings"

	"github.com/greenplum-db/gpbackup/utils"

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
	/*
	 * This query is constructed from scratch, but the list of schemas to exclude
	 * is copied from gpcrondump so that gpbackup exhibits similar behavior regarding
	 * which schemas are dumped.
	 */
	query := fmt.Sprintf(`
SELECT
	oid,
	nspname AS name
FROM pg_namespace
WHERE %s
ORDER BY name;`, nonUserSchemaFilterClause)
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
	AND a.attisdropped = 'f'
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
	Oid         uint32
	ConName     string
	ConType     string
	ConDef      string
	OwningTable string
}

func GetConstraints(connection *utils.DBConn) []QueryConstraint {
	// This query is adapted from the queries underlying \d in psql.
	query := fmt.Sprintf(`
SELECT
	c.oid,
	conname,
	contype,
	pg_get_constraintdef(c.oid, TRUE) AS condef,
	quote_ident(n.nspname) || '.' || quote_ident(t.relname) AS owningtable
FROM pg_constraint c
JOIN pg_class t
	ON c.conrelid = t.oid
JOIN pg_namespace n
	ON n.oid = t.relnamespace
WHERE %s
ORDER BY conname;`, nonUserSchemaFilterClause)

	results := make([]QueryConstraint, 0)
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

func GetAllSequenceRelations(connection *utils.DBConn) []utils.Relation {
	query := `SELECT
	n.oid AS schemaoid,
	c.oid AS relationoid,
	n.nspname AS schemaname,
	c.relname AS relationname
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

type QuerySequenceDefinition struct {
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

func GetSequenceDefinition(connection *utils.DBConn, seqName string) QuerySequenceDefinition {
	query := fmt.Sprintf("SELECT * FROM %s", seqName)
	result := QuerySequenceDefinition{}
	err := connection.Get(&result, query)
	utils.CheckError(err)
	return result
}

type QuerySequenceOwner struct {
	SchemaName   string `db:"nspname"`
	SequenceName string
	TableName    string
	ColumnName   string `db:"attname"`
}

func GetSequenceColumnOwnerMap(connection *utils.DBConn) map[string]string {
	query := `SELECT
	n.nspname,
	s.relname AS sequencename,
	t.relname AS tablename,
	a.attname
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

	results := make([]QuerySequenceOwner, 0)
	sequenceOwners := make(map[string]string, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	for _, seqOwner := range results {
		seqFQN := utils.MakeFQN(seqOwner.SchemaName, seqOwner.SequenceName)
		columnFQN := utils.MakeFQN(seqOwner.TableName, seqOwner.ColumnName)
		sequenceOwners[seqFQN] = columnFQN
	}
	return sequenceOwners
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

/*
 * This function constructs the names of implicit indexes created by
 * unique constraints on tables, so they can be filtered out of the
 * index list.
 *
 * Primary key indexes can only be created implicitly by a primary
 * key constraint, so they can be filtered out directly in the query
 * to get indexes, but multiple unique indexes can be created on the
 * same column so we only want to filter out the implicit ones.
 */
func ConstructImplicitIndexNames(connection *utils.DBConn) map[string]bool {
	query := `
SELECT DISTINCT
	n.nspname || '.' || t.relname || '_' || a.attname || '_key' AS string
FROM pg_constraint c
JOIN pg_class t
	ON c.conrelid = t.oid
JOIN pg_namespace n
	ON (t.relnamespace = n.oid)
JOIN pg_attribute a
	ON c.conrelid = a.attrelid
JOIN pg_index i
	ON i.indrelid = c.conrelid
WHERE a.attnum > 0
AND i.indisunique = 't'
AND i.indisprimary = 'f';
`
	indexNameMap := make(map[string]bool, 0)
	indexNames := SelectStringSlice(connection, query)
	for _, name := range indexNames {
		indexNameMap[name] = true
	}
	return indexNameMap
}

/*
 * This struct is for objects that only have a definition, like indexes
 * (pg_get_indexdef) and rules (pg_get_ruledef), and no owners or the like.
 * We get the owning table for the object because COMMENT ON [object type]
 * statements can require it.
 */
type QuerySimpleDefinition struct {
	Oid          uint32
	Name         string
	OwningSchema string
	OwningTable  string
	Def          string
}

func GetIndexDefinitions(connection *utils.DBConn, indexNameMap map[string]bool) []QuerySimpleDefinition {
	query := fmt.Sprintf(`
SELECT
	i.indexrelid AS oid,
	t.relname AS name,
	n.nspname AS owningschema,
	c.relname AS owningtable,
	pg_get_indexdef(i.indexrelid) AS def
FROM pg_index i
JOIN pg_class c
	ON (c.oid = i.indrelid)
JOIN pg_namespace n
	ON (c.relnamespace = n.oid)
JOIN pg_class t
	ON (t.oid = i.indexrelid)
WHERE %s
AND i.indisprimary = 'f'
ORDER BY name;`, nonUserSchemaFilterClause)

	results := make([]QuerySimpleDefinition, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	filteredIndexes := make([]QuerySimpleDefinition, 0)
	for _, index := range results {
		// We don't want to quote the index name to use it as a map key, just prepend the schema
		indexFQN := fmt.Sprintf("%s.%s", index.OwningSchema, index.Name)
		if !indexNameMap[indexFQN] {
			filteredIndexes = append(filteredIndexes, index)
		}
	}
	return filteredIndexes
}

/*
 * Rules named "_RETURN", "pg_settings_n", and "pg_settings_u" are
 * built-in rules and we don't want to dump them.
 */
func GetRuleDefinitions(connection *utils.DBConn) []QuerySimpleDefinition {
	query := `
SELECT
	r.oid,
	r.rulename AS name,
	n.nspname AS owningschema,
	c.relname AS owningtable,
	pg_get_ruledef(r.oid) AS def
FROM pg_rewrite r
JOIN pg_class c
	ON (c.oid = r.ev_class)
JOIN pg_namespace n
	ON (c.relnamespace = n.oid)
WHERE rulename NOT LIKE '%RETURN'
AND rulename NOT LIKE 'pg_%'
ORDER BY rulename;`

	results := make([]QuerySimpleDefinition, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	return results
}

func GetTriggerDefinitions(connection *utils.DBConn) []QuerySimpleDefinition {
	query := `
SELECT
	t.oid,
	t.tgname AS name,
	n.nspname AS owningschema,
	c.relname AS owningtable,
	pg_get_triggerdef(t.oid) AS def
FROM pg_trigger t
JOIN pg_class c
	ON (c.oid = t.tgrelid)
JOIN pg_namespace n
	ON (c.relnamespace = n.oid)
WHERE tgname NOT LIKE 'pg_%'
AND tgisconstraint = 'f'
ORDER BY tgname;`

	results := make([]QuerySimpleDefinition, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	return results
}

type QueryFunctionDefinition struct {
	Oid               uint32
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
	DataAccess        string  `db:"prodataaccess"`
	Language          string
}

func GetFunctionDefinitions(connection *utils.DBConn) []QueryFunctionDefinition {
	/*
	 * This query is copied from the dumpFunc() function in pg_dump.c, modified
	 * slightly to also retrieve the function's schema, name, and comment.
	 */
	query := fmt.Sprintf(`
SELECT
	p.oid,
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
	(SELECT lanname FROM pg_catalog.pg_language WHERE oid = prolang) AS language
FROM pg_proc p
LEFT JOIN pg_namespace n
	ON p.pronamespace = n.oid
WHERE %s
AND proisagg = 'f'
ORDER BY nspname, proname, identargs;`, nonUserSchemaFilterClause)

	results := make([]QueryFunctionDefinition, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	return results
}

type QueryAggregateDefinition struct {
	Oid                 uint32
	SchemaName          string `db:"nspname"`
	AggregateName       string `db:"proname"`
	Arguments           string
	IdentArgs           string
	TransitionFunction  uint32 `db:"aggtransfn"`
	PreliminaryFunction uint32 `db:"aggprelimfn"`
	FinalFunction       uint32 `db:"aggfinalfn"`
	SortOperator        uint32 `db:"aggsortop"`
	TransitionDataType  string
	InitialValue        string
	IsOrdered           bool `db:"aggordered"`
}

func GetAggregateDefinitions(connection *utils.DBConn) []QueryAggregateDefinition {
	query := fmt.Sprintf(`
SELECT
	p.oid,
	n.nspname,
	p.proname,
	pg_catalog.pg_get_function_arguments(p.oid) AS arguments,
	pg_catalog.pg_get_function_identity_arguments(p.oid) AS identargs,
	a.aggtransfn::regproc::oid,
	a.aggprelimfn::regproc::oid,
	a.aggfinalfn::regproc::oid,
	a.aggsortop::regproc::oid,
	t.typname as transitiondatatype,
	coalesce(a.agginitval, '') AS initialvalue,
	a.aggordered
FROM pg_aggregate a
LEFT JOIN pg_proc p ON a.aggfnoid = p.oid
LEFT JOIN pg_type t ON a.aggtranstype = t.oid
LEFT JOIN pg_namespace n ON p.pronamespace = n.oid
WHERE %s;`, nonUserSchemaFilterClause)

	results := make([]QueryAggregateDefinition, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	return results
}

type QueryFunction struct {
	Oid            uint32
	FunctionSchema string `db:"nspname"`
	FunctionName   string `db:"proname"`
	Arguments      string
}

type FunctionInfo struct {
	QualifiedName string
	Arguments     string
	IsInternal    bool
}

func GetFunctionOidToInfoMap(connection *utils.DBConn) map[uint32]FunctionInfo {
	query := `
SELECT
	p.oid,
	n.nspname,
	p.proname,
	pg_catalog.pg_get_function_arguments(p.oid) AS arguments
FROM pg_proc p
LEFT JOIN pg_namespace n ON p.pronamespace = n.oid;
`

	results := make([]QueryFunction, 0)
	funcMap := make(map[uint32]FunctionInfo, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	for _, function := range results {
		fqn := utils.MakeFQN(function.FunctionSchema, function.FunctionName)

		isInternal := false
		if function.FunctionSchema == "pg_catalog" {
			isInternal = true
		}
		funcInfo := FunctionInfo{QualifiedName: fqn, Arguments: function.Arguments, IsInternal: isInternal}
		funcMap[function.Oid] = funcInfo
	}
	return funcMap
}

type QueryCastDefinition struct {
	Oid            uint32
	SourceType     string
	TargetType     string
	FunctionSchema string
	FunctionName   string
	FunctionArgs   string
	CastContext    string
}

func GetCastDefinitions(connection *utils.DBConn) []QueryCastDefinition {
	query := fmt.Sprintf(`
SELECT
	c.oid,
	pg_catalog.format_type(c.castsource, NULL) AS sourcetype,
	pg_catalog.format_type(c.casttarget, NULL) AS targettype,
	coalesce(n.nspname, '') AS functionschema,
	coalesce(p.proname, '') AS functionname,
	pg_get_function_arguments(p.oid) AS functionargs,
	c.castcontext
FROM pg_cast c
LEFT JOIN pg_proc p ON c.castfunc = p.oid
LEFT JOIN pg_description d ON c.oid = d.objoid
JOIN pg_namespace n ON p.pronamespace = n.oid
WHERE %s
ORDER BY 1, 2;`, nonUserSchemaFilterClause)

	results := make([]QueryCastDefinition, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	return results
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

/*
 * Queries using generic structs defined below or structs defined elsewhere
 */
type QueryOid struct {
	Oid uint32
}

func OidFromObjectName(dbconn *utils.DBConn, name string, nameField string, catalogTable string) uint32 {
	query := fmt.Sprintf("SELECT oid FROM %s WHERE %s ='%s'", catalogTable, nameField, name)
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

func GetMetadataForObjectType(connection *utils.DBConn, schemaField string, aclField string, ownerField string, catalogTable string) utils.MetadataMap {
	schemaStr := ""
	if schemaField != "" {
		schemaStr = fmt.Sprintf(`JOIN pg_namespace n ON o.%s = n.oid
WHERE %s`, schemaField, nonUserSchemaFilterClause)
	}
	aclStr := ""
	if aclField != "" {
		aclStr = fmt.Sprintf(`CASE
		WHEN o.%s IS NULL THEN ''
		WHEN array_length(o.%s, 1) = 0 THEN 'GRANTEE=/GRANTOR'
		ELSE unnest(o.%s)::text
	END
`, aclField, aclField, aclField)
	} else {
		aclStr = "''"
	}
	sh := ""
	if catalogTable == "pg_database" {
		sh = "sh"
	}
	query := fmt.Sprintf(`
SELECT
	o.oid,
	%s AS privileges,
	pg_get_userbyid(o.%s) AS owner,
	coalesce(%sobj_description(o.oid, '%s'), '') AS comment
FROM %s o
%s
ORDER BY o.oid, privileges;
`, aclStr, ownerField, sh, catalogTable, catalogTable, schemaStr)

	results := make([]QueryObjectMetadata, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)

	metadataMap := make(utils.MetadataMap)
	var metadata utils.ObjectMetadata
	if len(results) > 0 {
		currentOid := uint32(0)
		// Collect all entries for the same object into one ObjectMetadata
		for _, result := range results {
			if result.Oid != currentOid {
				if currentOid != 0 {
					metadataMap[currentOid] = metadata
				}
				currentOid = result.Oid
				metadata = utils.ObjectMetadata{}
				metadata.Privileges = make([]utils.ACL, 0)
				metadata.Owner = result.Owner
				metadata.Comment = result.Comment
			}
			privileges := utils.ParseACL(result.Privileges)
			if privileges != nil {
				metadata.Privileges = append(metadata.Privileges, *privileges)
			}
		}
		metadataMap[currentOid] = metadata
	}
	return metadataMap
}

func GetCommentsForObjectType(connection *utils.DBConn, schemaField string, oidField string, commentTable string, catalogTable string) utils.MetadataMap {
	schemaStr := ""
	if schemaField != "" {
		schemaStr = fmt.Sprintf(`JOIN pg_namespace n ON o.%s = n.oid
WHERE %s`, schemaField, nonUserSchemaFilterClause)
	}
	query := fmt.Sprintf(`
SELECT
	o.%s AS oid,
	coalesce(obj_description(o.%s, '%s'), '') AS comment
FROM %s o
%s;`, oidField, oidField, commentTable, catalogTable, schemaStr)

	results := make([]QueryObjectMetadata, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)

	metadataMap := make(utils.MetadataMap)
	if len(results) > 0 {
		for _, result := range results {
			metadataMap[result.Oid] = utils.ObjectMetadata{[]utils.ACL{}, result.Owner, result.Comment}
		}
	}
	return metadataMap
}

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

func GetDatabaseGUCs(connection *utils.DBConn) []string {
	query := fmt.Sprintf(`
SELECT ('SET ' || option_name || ' TO ' || option_value) AS string
FROM pg_options_to_table(
	(SELECT datconfig FROM pg_database WHERE datname = '%s')
);`, connection.DBName)
	return SelectStringSlice(connection, query)
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

type QueryProceduralLanguage struct {
	Oid       uint32
	Name      string `db:"lanname"`
	Owner     string
	IsPl      bool   `db:"lanispl"`
	PlTrusted bool   `db:"lanpltrusted"`
	Handler   uint32 `db:"lanplcallfoid"`
	Inline    uint32 `db:"laninline"`
	Validator uint32 `db:"lanvalidator"`
}

func GetProceduralLanguages(connection *utils.DBConn) []QueryProceduralLanguage {
	results := make([]QueryProceduralLanguage, 0)
	query := `
SELECT
	oid,
	l.lanname,
	pg_get_userbyid(l.lanowner) as owner,
	l.lanispl,
	l.lanpltrusted,
	l.lanplcallfoid::regprocedure::oid,
	l.laninline::regprocedure::oid,
	l.lanvalidator::regprocedure::oid
FROM pg_language l
WHERE l.lanispl='t';
`
	err := connection.Select(&results, query)
	utils.CheckError(err)
	return results
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

type QueryExtProtocol struct {
	Oid           uint32
	Name          string `db:"ptcname"`
	Owner         string
	Trusted       bool   `db:"ptctrusted"`
	ReadFunction  uint32 `db:"ptcreadfn"`
	WriteFunction uint32 `db:"ptcwritefn"`
	Validator     uint32 `db:"ptcvalidatorfn"`
}

func GetExternalProtocols(connection *utils.DBConn) []QueryExtProtocol {
	results := make([]QueryExtProtocol, 0)
	query := `
SELECT
	p.oid,
	p.ptcname,
	pg_get_userbyid(p.ptcowner) as owner,
	p.ptctrusted,
	p.ptcreadfn,
	p.ptcwritefn,
	p.ptcvalidatorfn
FROM pg_extprotocol p;
`
	err := connection.Select(&results, query)
	utils.CheckError(err)
	return results
}

type QueryResourceQueue struct {
	Name             string
	ActiveStatements int
	MaxCost          string
	CostOvercommit   bool
	MinCost          string
	Priority         string
	MemoryLimit      string
	Comment          string
}

func GetResourceQueues(connection *utils.DBConn) []QueryResourceQueue {
	results := make([]QueryResourceQueue, 0)
	/*
	 * maxcost and mincost are represented as real types in the database, but we round to two decimals
	 * and cast them as text for more consistent formatting. pg_dumpall does this as well.
	 */
	query := `
SELECT
	rsqname AS name,
	rsqcountlimit AS activestatements,
	ROUND(rsqcostlimit::numeric, 2)::text AS maxcost,
	rsqovercommit AS costovercommit,
	ROUND(rsqignorecostlimit::numeric, 2)::text AS mincost,
	priority_capability.ressetting::text AS priority,
	memory_capability.ressetting::text AS memorylimit,
	coalesce(descriptions.description, '') AS comment
FROM
	pg_resqueue r
		JOIN
		(SELECT resqueueid, ressetting FROM pg_resqueuecapability WHERE restypid = 5) priority_capability
		ON r.oid = priority_capability.resqueueid
	JOIN
		(SELECT resqueueid, ressetting FROM pg_resqueuecapability WHERE restypid = 6) memory_capability
		ON r.oid = memory_capability.resqueueid
	LEFT JOIN pg_shdescription descriptions ON r.oid = descriptions.objoid;
`
	err := connection.Select(&results, query)
	utils.CheckError(err)
	return results
}

type TimeConstraint struct {
	Oid       uint32
	StartDay  int
	StartTime string
	EndDay    int
	EndTime   string
}

type QueryRole struct {
	Oid             uint32
	Name            string
	Super           bool
	Inherit         bool
	CreateRole      bool
	CreateDB        bool
	CanLogin        bool
	ConnectionLimit int
	Password        string
	ValidUntil      string
	Comment         string
	ResQueue        string
	Createrextgpfd  bool
	Createrexthttp  bool
	Createwextgpfd  bool
	Createrexthdfs  bool
	Createwexthdfs  bool
	TimeConstraints []TimeConstraint
}

/*
 * We convert rolvaliduntil to UTC and then append '-00' so that
 * we standardize times to UTC but do not lose time zone information
 * in the timestamp.
 */
func GetRoles(connection *utils.DBConn) []QueryRole {
	roles := make([]QueryRole, 0)
	query := `
SELECT
	oid AS oid,
	rolname AS name,
	rolsuper AS super,
	rolinherit AS inherit,
	rolcreaterole AS createrole,
	rolcreatedb AS createdb,
	rolcanlogin AS  canlogin,
	rolconnlimit AS connectionlimit,
	coalesce(rolpassword, '') AS password,
	coalesce(timezone('UTC', rolvaliduntil) || '-00', '') AS validuntil,
	coalesce(shobj_description(oid, 'pg_authid'), '') AS comment,
	(SELECT rsqname FROM pg_resqueue WHERE pg_resqueue.oid = rolresqueue) AS resqueue,
	rolcreaterexthttp AS createrexthttp,
	rolcreaterextgpfd AS createrextgpfd,
	rolcreatewextgpfd AS createwextgpfd,
	rolcreaterexthdfs AS createrexthdfs,
	rolcreatewexthdfs AS createwexthdfs
FROM
	pg_authid`
	err := connection.Select(&roles, query)
	utils.CheckError(err)

	constraintsByRole := getTimeConstraintsByRole(connection)

	for idx, role := range roles {
		roles[idx].TimeConstraints = constraintsByRole[role.Oid]
	}

	return roles
}

func getTimeConstraintsByRole(connection *utils.DBConn) map[uint32][]TimeConstraint {
	timeConstraints := make([]TimeConstraint, 0)
	query := `
SELECT
	authid AS oid,
	start_day AS startday,
	start_time::text AS starttime,
	end_day AS endday,
	end_time::text AS endtime
FROM
	pg_auth_time_constraint
	`

	err := connection.Select(&timeConstraints, query)
	utils.CheckError(err)

	constraintsByRole := make(map[uint32][]TimeConstraint, 0)
	for _, constraint := range timeConstraints {
		roleConstraints, ok := constraintsByRole[constraint.Oid]
		if !ok {
			roleConstraints = make([]TimeConstraint, 0)
		}
		constraintsByRole[constraint.Oid] = append(roleConstraints, constraint)
	}

	return constraintsByRole
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
