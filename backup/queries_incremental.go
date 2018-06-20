package backup

import (
	"fmt"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpbackup/utils"
)

func GetAOIncrementalMetadata(connection *dbconn.DBConn) map[string]utils.AOEntry {
	var modCounts = getAllModCounts(connection)
	var lastDDLTimestamps = getLastDDLTimestamps(connection)
	aoTableEntries := make(map[string]utils.AOEntry)
	for aoTableFQN := range modCounts {
		aoTableEntries[aoTableFQN] = utils.AOEntry{
			Modcount:         modCounts[aoTableFQN],
			LastDDLTimestamp: lastDDLTimestamps[aoTableFQN],
		}
	}

	return aoTableEntries
}

func getAllModCounts(connection *dbconn.DBConn) map[string]int64 {
	var segTableFQNs = getAOSegTableFQNs(connection)
	modCounts := make(map[string]int64)
	for aoTableFQN, segTableFQN := range segTableFQNs {
		modCounts[aoTableFQN] = getModCount(connection, segTableFQN)
	}
	return modCounts
}

func getAOSegTableFQNs(connection *dbconn.DBConn) map[string]string {
	query := `
	SELECT
		seg.aotablefqn,
		'pg_aoseg.' || quote_ident(c1.relname) AS aosegtablefqn
	FROM
		pg_class c1
	JOIN
		(
			SELECT
				pg_ao.relid AS aooid,
				pg_ao.segrelid,
				aotables.aotablefqn
			FROM
				pg_appendonly pg_ao
				JOIN
				(
					SELECT
						c2.oid,
						quote_ident(n.nspname)|| '.' || quote_ident(c2.relname) AS aotablefqn
					FROM
						pg_class c2
						JOIN
						pg_namespace n
						ON c2.relnamespace = n.oid
					WHERE
						relstorage IN ( 'ao', 'co' )
				) aotables
				ON
					pg_ao.relid = aotables.oid
		) seg
	ON
		c1.oid = seg.segrelid
`
	results := make([]struct {
		AOTableFQN    string
		AOSegTableFQN string
	}, 0)
	err := connection.Select(&results, query)
	gplog.FatalOnError(err)
	resultMap := make(map[string]string)
	for _, result := range results {
		resultMap[result.AOTableFQN] = result.AOSegTableFQN
	}
	return resultMap
}

func getModCount(connection *dbconn.DBConn, aosegtablefqn string) int64 {
	query := fmt.Sprintf(`
	SELECT modcount FROM %s
`, aosegtablefqn)

	var results []struct {
		Modcount int64
	}
	err := connection.Select(&results, query)
	gplog.FatalOnError(err)

	if len(results) == 0 {
		return 0
	}
	return results[0].Modcount
}

func getLastDDLTimestamps(connection *dbconn.DBConn) map[string]string {
	query := `
	SELECT
		quote_ident(n.nspname) || '.' || quote_ident(aorelname) as aotablefqn,
		lastddltimestamp
	FROM
		(
			SELECT
				oid AS aooid,
				relname AS aorelname,
				relnamespace
			FROM
				pg_class
			WHERE
				relstorage IN ('ao', 'co')
		) aotables
	JOIN
		(
			SELECT
				objid,
				MAX(statime) AS lastddltimestamp
			FROM
				pg_stat_last_operation
			WHERE
				staactionname IN ('CREATE', 'ALTER', 'TRUNCATE')
			GROUP BY
				objid
		) lastop
	ON
		aotables.aooid = lastop.objid
	JOIN 
		pg_namespace n
	ON
		aotables.relnamespace = n.oid
`

	var results []struct {
		AOTableFQN       string
		LastDDLTimestamp string
	}
	err := connection.Select(&results, query)
	gplog.FatalOnError(err)
	resultMap := make(map[string]string)
	for _, result := range results {
		resultMap[result.AOTableFQN] = result.LastDDLTimestamp
	}
	return resultMap
}
