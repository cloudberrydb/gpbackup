package backup

/*
 * This file contains structs and functions related to executing specific
 * queries to gather metadata for the objects handled in predata_textsearch.go.
 */

import (
	"fmt"

	"github.com/greenplum-db/gpbackup/utils"
)

type TextSearchParser struct {
	Oid          uint32
	Schema       string `db:"nspname"`
	Name         string `db:"prsname"`
	StartFunc    string `db:"prsstart"`
	TokenFunc    string `db:"prstoken"`
	EndFunc      string `db:"prsend"`
	LexTypesFunc string `db:"prslextype"`
	HeadlineFunc string
}

func GetTextSearchParsers(connection *utils.DBConn) []TextSearchParser {
	query := fmt.Sprintf(`
SELECT
	p.oid,
	nspname,
	prsname,
	prsstart,
	prstoken,
	prsend,
	prslextype,
	CASE WHEN prsheadline::regproc::text = '-' THEN '' ELSE prsheadline::regproc::text END AS headlinefunc 
FROM pg_ts_parser p
JOIN pg_namespace n ON n.oid = p.prsnamespace
WHERE prsname != 'default'
ORDER BY prsname;`)

	results := make([]TextSearchParser, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	return results
}
