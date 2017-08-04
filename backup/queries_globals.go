package backup

/*
 * This file contains structs and functions related to executing specific
 * queries to gather metadata for the objects handled in metadata_globals.go.
 */

import (
	"fmt"

	"github.com/greenplum-db/gpbackup/utils"
)

type SessionGUCs struct {
	ClientEncoding       string `db:"client_encoding"`
	StdConformingStrings string `db:"standard_conforming_strings"`
	DefaultWithOids      string `db:"default_with_oids"`
}

func GetSessionGUCs(connection *utils.DBConn) SessionGUCs {
	result := SessionGUCs{}
	query := "SHOW client_encoding;"
	err := connection.Get(&result, query)
	query = "SHOW standard_conforming_strings;"
	err = connection.Get(&result, query)
	query = "SHOW default_with_oids;"
	err = connection.Get(&result, query)
	utils.CheckError(err)
	return result
}

type DatabaseName struct {
	Oid            uint32
	DatabaseName   string `db:"datname"`
	TablespaceName string `db:"spcname"`
}

func GetDatabaseNames(connection *utils.DBConn) []DatabaseName {
	query := `
SELECT
	d.oid,
	d.datname,
	t.spcname
FROM pg_database d
JOIN pg_tablespace t
ON d.dattablespace = t.oid;`

	results := make([]DatabaseName, 0)
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

type ResourceQueue struct {
	Oid              uint32
	Name             string
	ActiveStatements int
	MaxCost          string
	CostOvercommit   bool
	MinCost          string
	Priority         string
	MemoryLimit      string
}

func GetResourceQueues(connection *utils.DBConn) []ResourceQueue {
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
	results := make([]ResourceQueue, 0)
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

type Role struct {
	Oid             uint32
	Name            string `db:"rolname"`
	Super           bool   `db:"rolsuper"`
	Inherit         bool   `db:"rolinherit"`
	CreateRole      bool   `db:"rolcreaterole"`
	CreateDB        bool   `db:"rolcreatedb"`
	CanLogin        bool   `db:"rolcanlogin"`
	ConnectionLimit int    `db:"rolconnlimit"`
	Password        string
	ValidUntil      string
	ResQueue        string
	Createrextgpfd  bool `db:"rolcreaterexthttp"`
	Createrexthttp  bool `db:"rolcreaterextgpfd"`
	Createwextgpfd  bool `db:"rolcreatewextgpfd"`
	Createrexthdfs  bool `db:"rolcreaterexthdfs"`
	Createwexthdfs  bool `db:"rolcreatewexthdfs"`
	TimeConstraints []TimeConstraint
}

/*
 * We convert rolvaliduntil to UTC and then append '-00' so that
 * we standardize times to UTC but do not lose time zone information
 * in the timestamp.
 */
func GetRoles(connection *utils.DBConn) []Role {
	query := `
SELECT
	oid,
	rolname,
	rolsuper,
	rolinherit,
	rolcreaterole,
	rolcreatedb,
	rolcanlogin,
	rolconnlimit,
	coalesce(rolpassword, '') AS password,
	coalesce(timezone('UTC', rolvaliduntil) || '-00', '') AS validuntil,
	(SELECT rsqname FROM pg_resqueue WHERE pg_resqueue.oid = rolresqueue) AS resqueue,
	rolcreaterexthttp,
	rolcreaterextgpfd,
	rolcreatewextgpfd,
	rolcreaterexthdfs,
	rolcreatewexthdfs
FROM
	pg_authid`

	roles := make([]Role, 0)
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

type RoleMember struct {
	Role    string
	Member  string
	Grantor string
	IsAdmin bool
}

func GetRoleMembers(connection *utils.DBConn) []RoleMember {
	query := `
SELECT
	pg_get_userbyid(roleid) AS role,
	pg_get_userbyid(member) AS member,
	pg_get_userbyid(grantor) AS grantor,
	admin_option as isadmin
FROM pg_auth_members
ORDER BY roleid, member;`

	results := make([]RoleMember, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	return results
}

type Tablespace struct {
	Oid        uint32
	Tablespace string
	Filespace  string
}

func GetTablespaces(connection *utils.DBConn) []Tablespace {
	query := `
SELECT
	t.oid,
	quote_ident(spcname) AS tablespace,
	quote_ident(fsname) AS filespace
FROM pg_tablespace t
JOIN pg_filespace f
ON t.spcfsoid = f.oid
WHERE fsname != 'pg_system';`

	results := make([]Tablespace, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	return results
}
