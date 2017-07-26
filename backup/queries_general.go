package backup

/*
 * This file contains structs and functions related to executing specific
 * queries to gather metadata for the objects handled in predata_general.go.
 */

import (
	"fmt"

	"github.com/greenplum-db/gpbackup/utils"
)

/*
 * Queries requiring their own structs
 */

func GetAllUserSchemas(connection *utils.DBConn) []Schema {
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
	results := make([]Schema, 0)

	err := connection.Select(&results, query)
	utils.CheckError(err)
	return results
}

type QueryConstraint struct {
	Oid                uint32
	ConName            string
	ConType            string
	ConDef             string
	OwningObject       string
	IsDomainConstraint bool
}

func GetConstraints(connection *utils.DBConn) []QueryConstraint {
	// This query is adapted from the queries underlying \d in psql.
	query := fmt.Sprintf(`
SELECT
	c.oid,
	conname,
	contype,
	pg_get_constraintdef(c.oid, TRUE) AS condef,
	CASE
		WHEN r.relname IS NULL THEN quote_ident(n.nspname) || '.' ||quote_ident(t.typname)
		ELSE  quote_ident(n.nspname) || '.' || quote_ident(r.relname)
	END AS owningobject,
	CASE
		WHEN r.relname IS NULL THEN 't'
		ELSE 'f'
	END AS isdomainconstraint
FROM pg_constraint c
LEFT JOIN pg_class r
	ON c.conrelid = r.oid
LEFT JOIN pg_type t
	ON c.contypid = t.oid
JOIN pg_namespace n
	ON n.oid = c.connamespace
WHERE %s
ORDER BY conname;`, nonUserSchemaFilterClause)

	results := make([]QueryConstraint, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	return results
}

func GetAllSequenceRelations(connection *utils.DBConn) []Relation {
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

	results := make([]Relation, 0)
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
		seqFQN := MakeFQN(seqOwner.SchemaName, seqOwner.SequenceName)
		columnFQN := MakeFQN(seqOwner.TableName, seqOwner.ColumnName)
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

type QueryOperator struct {
	Oid              uint32
	SchemaName       string
	Name             string
	ProcedureName    string
	LeftArgType      string
	RightArgType     string
	CommutatorOp     string
	NegatorOp        string
	RestrictFunction string
	JoinFunction     string
	CanHash          bool
	CanMerge         bool
}

func GetOperators(connection *utils.DBConn) []QueryOperator {
	results := make([]QueryOperator, 0)
	query := fmt.Sprintf(`
SELECT
	o.oid,
	n.nspname AS schemaname,
	oprname AS name,
	oprcode AS procedurename,
	oprleft::regtype AS leftargtype,
	oprright::regtype AS rightargtype,
	oprcom::regoper AS commutatorop,
	oprnegate::regoper AS negatorop,
	oprrest AS restrictfunction,
	oprjoin AS joinfunction,
	oprcanmerge AS canmerge,
	oprcanhash AS canhash
FROM pg_operator o
JOIN pg_namespace n on n.oid = o.oprnamespace
WHERE %s AND oprcode != 0`, nonUserSchemaFilterClause)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	return results
}
