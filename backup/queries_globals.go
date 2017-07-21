package backup

/*
 * This file contains structs and functions related to executing specific
 * queries to gather metadata for the objects handled in metadata_globals.go.
 */

import (
	"fmt"

	"github.com/greenplum-db/gpbackup/utils"
)

type QueryDatabaseName struct {
	Oid  uint32
	Name string `db:"datname"`
}

func GetDatabaseNames(connection *utils.DBConn) []QueryDatabaseName {
	query := `
SELECT
	oid,
	datname
FROM pg_database;`

	results := make([]QueryDatabaseName, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	return results
}
func GetDatabaseGUCs(connection *utils.DBConn) []string {
	query := fmt.Sprintf(`
SELECT ('SET ' || option_name || ' TO ' || option_value) AS string
FROM pg_options_to_table(
	(SELECT datconfig FROM pg_database WHERE datname = '%s')
);`, connection.DBName)
	return SelectStringSlice(connection, query)
}

type QueryResourceQueue struct {
	Oid              uint32
	Name             string
	ActiveStatements int
	MaxCost          string
	CostOvercommit   bool
	MinCost          string
	Priority         string
	MemoryLimit      string
}

func GetResourceQueues(connection *utils.DBConn) []QueryResourceQueue {
	/*
	 * maxcost and mincost are represented as real types in the database, but we round to two decimals
	 * and cast them as text for more consistent formatting. pg_dumpall does this as well.
	 */
	query := `
SELECT
	r.oid,
	rsqname AS name,
	rsqcountlimit AS activestatements,
	ROUND(rsqcostlimit::numeric, 2)::text AS maxcost,
	rsqovercommit AS costovercommit,
	ROUND(rsqignorecostlimit::numeric, 2)::text AS mincost,
	priority_capability.ressetting::text AS priority,
	memory_capability.ressetting::text AS memorylimit
FROM
	pg_resqueue r
		JOIN
		(SELECT resqueueid, ressetting FROM pg_resqueuecapability WHERE restypid = 5) priority_capability
		ON r.oid = priority_capability.resqueueid
	JOIN
		(SELECT resqueueid, ressetting FROM pg_resqueuecapability WHERE restypid = 6) memory_capability
		ON r.oid = memory_capability.resqueueid;
`
	results := make([]QueryResourceQueue, 0)
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
