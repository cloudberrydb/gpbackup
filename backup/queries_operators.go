package backup

/*
 * This file contains structs and functions related to executing specific
 * queries to gather metadata for the objects handled in predata_general.go.
 */

import (
	"fmt"

	"github.com/greenplum-db/gpbackup/utils"
)

type Operator struct {
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

func GetOperators(connection *utils.DBConn) []Operator {
	results := make([]Operator, 0)
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
WHERE %s AND oprcode != 0`, SchemaFilterClause("n"))
	err := connection.Select(&results, query)
	utils.CheckError(err)
	return results
}

type OperatorFamily struct {
	Oid         uint32
	SchemaName  string
	Name        string
	IndexMethod string
}

func GetOperatorFamilies(connection *utils.DBConn) []OperatorFamily {
	results := make([]OperatorFamily, 0)
	query := fmt.Sprintf(`
SELECT
	o.oid,
	n.nspname AS schemaname,
	opfname AS name,
	(SELECT amname FROM pg_am WHERE oid = opfmethod) AS indexMethod
FROM pg_opfamily o
JOIN pg_namespace n on n.oid = o.opfnamespace
WHERE %s`, SchemaFilterClause("n"))
	err := connection.Select(&results, query)
	utils.CheckError(err)
	return results
}

type OperatorClass struct {
	Oid          uint32
	ClassSchema  string
	ClassName    string
	FamilySchema string
	FamilyName   string
	IndexMethod  string
	Type         string
	Default      bool
	StorageType  string
	Operators    []OperatorClassOperator
	Functions    []OperatorClassFunction
}

func GetOperatorClasses(connection *utils.DBConn) []OperatorClass {
	results := make([]OperatorClass, 0)
	query := fmt.Sprintf(`
SELECT
	c.oid,
	cls_ns.nspname AS classschema,
	opcname AS classname,
	fam_ns.nspname AS familyschema,
	opfname AS familyname,
	(SELECT amname FROM pg_catalog.pg_am WHERE oid = opcmethod) AS indexmethod,
	opcintype::pg_catalog.regtype AS type,
	opcdefault AS default,
	opckeytype::pg_catalog.regtype AS storagetype
FROM pg_catalog.pg_opclass c
LEFT JOIN pg_catalog.pg_opfamily f ON f.oid = opcfamily
JOIN pg_catalog.pg_namespace cls_ns ON cls_ns.oid = opcnamespace
JOIN pg_catalog.pg_namespace fam_ns ON fam_ns.oid = opfnamespace
WHERE %s`, SchemaFilterClause("cls_ns"))
	err := connection.Select(&results, query)
	utils.CheckError(err)

	operators := GetOperatorClassOperators(connection)
	for i := range results {
		results[i].Operators = operators[results[i].Oid]
	}
	functions := GetOperatorClassFunctions(connection)
	for i := range results {
		results[i].Functions = functions[results[i].Oid]
	}

	return results
}

type OperatorClassOperator struct {
	ClassOid       uint32
	StrategyNumber int
	Operator       string
	Recheck        bool
}

func GetOperatorClassOperators(connection *utils.DBConn) map[uint32][]OperatorClassOperator {
	results := make([]OperatorClassOperator, 0)
	query := fmt.Sprintf(`
SELECT
	refobjid AS classoid,
	amopstrategy AS strategynumber,
	amopopr::pg_catalog.regoperator AS operator,
	amopreqcheck AS recheck
FROM pg_catalog.pg_amop ao, pg_catalog.pg_depend
WHERE refclassid = 'pg_catalog.pg_opclass'::pg_catalog.regclass
AND classid = 'pg_catalog.pg_amop'::pg_catalog.regclass
AND objid = ao.oid
ORDER BY amopstrategy
`)
	err := connection.Select(&results, query)
	utils.CheckError(err)

	operators := make(map[uint32][]OperatorClassOperator, 0)
	for _, result := range results {
		operators[result.ClassOid] = append(operators[result.ClassOid], result)
	}
	return operators
}

type OperatorClassFunction struct {
	ClassOid      uint32
	SupportNumber int
	FunctionName  string
}

func GetOperatorClassFunctions(connection *utils.DBConn) map[uint32][]OperatorClassFunction {
	results := make([]OperatorClassFunction, 0)
	query := fmt.Sprintf(`
SELECT
	refobjid AS classoid,
	amprocnum AS supportnumber,
	amproc::pg_catalog.regprocedure AS functionname
FROM pg_catalog.pg_amproc ap, pg_catalog.pg_depend
WHERE refclassid = 'pg_catalog.pg_opclass'::pg_catalog.regclass
AND classid = 'pg_catalog.pg_amproc'::pg_catalog.regclass
AND objid = ap.oid
ORDER BY amprocnum
`)
	err := connection.Select(&results, query)
	utils.CheckError(err)

	functions := make(map[uint32][]OperatorClassFunction, 0)
	for _, result := range results {
		functions[result.ClassOid] = append(functions[result.ClassOid], result)
	}
	return functions
}
