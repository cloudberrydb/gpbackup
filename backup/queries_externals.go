package backup

/*
 * This file contains structs and functions related to executing specific
 * queries to gather metadata for the objects handled in predata_externals.go.
 */

import (
	"github.com/greenplum-db/gpbackup/utils"
)

func GetExternalTableDefinitions(connection *utils.DBConn) map[uint32]ExternalTableDefinition {
	version4query := `
SELECT
	reloid AS oid,
	CASE WHEN location[1] NOT IN ('ALL_SEGMENTS', 'HOST', 'MASTER_ONLY', 'PER_HOST', 'SEGMENT_ID', 'TOTAL_SEGS') THEN unnest(location) ELSE '' END AS location,
	CASE WHEN location[1] IN ('ALL_SEGMENTS', 'HOST', 'MASTER_ONLY', 'PER_HOST', 'SEGMENT_ID', 'TOTAL_SEGS') THEN unnest(location) ELSE 'ALL_SEGMENTS' END AS execlocation,
	fmttype AS formattype,
	fmtopts AS formatopts,
	'' AS options,
	coalesce(command, '') AS command,
	coalesce(rejectlimit, 0) AS rejectlimit,
	coalesce(rejectlimittype, '') AS rejectlimittype,
	coalesce((SELECT relname FROM pg_class WHERE oid = fmterrtbl), '') AS errtable,
	pg_encoding_to_char(encoding) AS encoding,
	writable
FROM pg_exttable;`

	query := `
SELECT
	reloid AS oid,
	CASE WHEN urilocation IS NOT NULL THEN unnest(urilocation) ELSE '' END AS location,
	array_to_string(execlocation, ',') AS execlocation,
	fmttype AS formattype,
	fmtopts AS formatopts,
	(
		array_to_string(ARRAY(SELECT pg_catalog.quote_ident(option_name) || ' ' || pg_catalog.quote_literal(option_value)
		FROM pg_options_to_table(options)
		ORDER BY option_name), E',\n\t')
	) AS options,
	coalesce(command, '') AS command,
	coalesce(rejectlimit, 0) AS rejectlimit,
	coalesce(rejectlimittype, '') AS rejectlimittype,
	coalesce((SELECT relname FROM pg_class WHERE oid = fmterrtbl), '') AS errtable,
	pg_encoding_to_char(encoding) AS encoding,
	writable
FROM pg_exttable;`

	results := make([]ExternalTableDefinition, 0)
	var err error
	if connection.Version.Before("5") {
		err = connection.Select(&results, version4query)
	} else {
		err = connection.Select(&results, query)
	}
	utils.CheckError(err)
	resultMap := make(map[uint32]ExternalTableDefinition)
	var extTableDef ExternalTableDefinition
	for _, result := range results {
		if resultMap[result.Oid].Oid != 0 {
			extTableDef = resultMap[result.Oid]
		} else {
			extTableDef = result
		}
		if result.Location != "" {
			extTableDef.URIs = append(extTableDef.URIs, result.Location)
		}
		resultMap[result.Oid] = extTableDef
	}
	return resultMap
}

type ExternalProtocol struct {
	Oid           uint32
	Name          string `db:"ptcname"`
	Owner         string
	Trusted       bool   `db:"ptctrusted"`
	ReadFunction  uint32 `db:"ptcreadfn"`
	WriteFunction uint32 `db:"ptcwritefn"`
	Validator     uint32 `db:"ptcvalidatorfn"`
}

func GetExternalProtocols(connection *utils.DBConn) []ExternalProtocol {
	results := make([]ExternalProtocol, 0)
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
