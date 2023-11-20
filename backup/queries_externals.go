package backup

/*
 * This file contains structs and functions related to executing specific
 * queries to gather metadata for the objects handled in predata_externals.go.
 */

import (
	"github.com/cloudberrydb/gp-common-go-libs/dbconn"
	"github.com/cloudberrydb/gp-common-go-libs/gplog"
	"github.com/cloudberrydb/gpbackup/toc"
)

func GetExternalTableDefinitions(connectionPool *dbconn.DBConn) map[uint32]ExternalTableDefinition {
	gplog.Verbose("Retrieving external table information")
	// Cannot use unnest() in CASE statements anymore in GPDB 7+ so convert
	// it to a LEFT JOIN LATERAL. We do not use LEFT JOIN LATERAL for GPDB 6
	// because the CASE unnest() logic is more performant.
	atLeast7Query := `
	SELECT e.reloid AS oid,
		ljl_unnest AS location,
		array_to_string(e.execlocation, ',') AS execlocation,
		e.fmttype AS formattype,
		e.fmtopts AS formatopts,
		coalesce(e.command, '') AS command,
		coalesce(e.rejectlimit, 0) AS rejectlimit,
		coalesce(e.rejectlimittype, '') AS rejectlimittype,
		e.logerrors,
		coalesce('log_errors=p' = any(ft.ftoptions), false) AS logerrpersist,
		pg_encoding_to_char(e.encoding) AS encoding,
		e.writable
	FROM pg_exttable e
		LEFT JOIN pg_foreign_table ft ON e.reloid = ft.ftrelid
		LEFT JOIN LATERAL unnest(urilocation) ljl_unnest ON urilocation IS NOT NULL`

	var query string
	query = atLeast7Query

	results := make([]ExternalTableDefinition, 0)
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
	resultMap := make(map[uint32]ExternalTableDefinition)
	var extTableDef ExternalTableDefinition
	for _, result := range results {
		if resultMap[result.Oid].Oid != 0 {
			extTableDef = resultMap[result.Oid]
		} else {
			extTableDef = result
		}
		if result.Location.Valid && result.Location.String != "" {
			extTableDef.URIs = append(extTableDef.URIs, result.Location.String)
		}
		resultMap[result.Oid] = extTableDef
	}
	return resultMap
}

type ExternalProtocol struct {
	Oid           uint32
	Name          string
	Owner         string
	Trusted       bool   `db:"ptctrusted"`
	ReadFunction  uint32 `db:"ptcreadfn"`
	WriteFunction uint32 `db:"ptcwritefn"`
	Validator     uint32 `db:"ptcvalidatorfn"`
}

func (p ExternalProtocol) GetMetadataEntry() (string, toc.MetadataEntry) {
	return "predata",
		toc.MetadataEntry{
			Schema:          "",
			Name:            p.Name,
			ObjectType:      "PROTOCOL",
			ReferenceObject: "",
			StartByte:       0,
			EndByte:         0,
		}
}

func (p ExternalProtocol) GetUniqueID() UniqueID {
	return UniqueID{ClassID: PG_EXTPROTOCOL_OID, Oid: p.Oid}
}

func (p ExternalProtocol) FQN() string {
	return p.Name
}

func GetExternalProtocols(connectionPool *dbconn.DBConn) []ExternalProtocol {
	results := make([]ExternalProtocol, 0)
	query := `
	SELECT p.oid,
		quote_ident(p.ptcname) AS name,
		pg_get_userbyid(p.ptcowner) AS owner,
		p.ptctrusted,
		p.ptcreadfn,
		p.ptcwritefn,
		p.ptcvalidatorfn
	FROM pg_extprotocol p`
	err := connectionPool.Select(&results, query)
	gplog.FatalOnError(err)
	return results
}

type PartitionInfo struct {
	PartitionRuleOid       uint32
	PartitionParentRuleOid uint32
	ParentRelationOid      uint32
	ParentSchema           string
	ParentRelationName     string
	RelationOid            uint32
	PartitionName          string
	PartitionRank          int
	IsExternal             bool
}

func (pi PartitionInfo) GetMetadataEntry() (string, toc.MetadataEntry) {
	return "predata",
		toc.MetadataEntry{
			Schema:          pi.ParentSchema,
			Name:            pi.ParentRelationName,
			ObjectType:      "EXCHANGE PARTITION",
			ReferenceObject: "",
			StartByte:       0,
			EndByte:         0,
		}
}

func GetExternalPartitionInfo(connectionPool *dbconn.DBConn) ([]PartitionInfo, map[uint32]PartitionInfo) {
	// For GPDB 7+, external partitions will have their own ATTACH PARTITION DDL command
	// instead of a complicated EXCHANGE PARTITION command.
	return []PartitionInfo{}, make(map[uint32]PartitionInfo, 0)
}
