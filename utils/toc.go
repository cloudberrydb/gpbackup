package utils

import (
	"fmt"
	"io"
	"regexp"

	yaml "gopkg.in/yaml.v2"
)

type TOC struct {
	metadataEntryMap   map[string]*[]MetadataEntry
	GlobalEntries      []MetadataEntry
	PredataEntries     []MetadataEntry
	PostdataEntries    []MetadataEntry
	StatisticsEntries  []MetadataEntry
	MasterDataEntries  []MasterDataEntry
	SegmentDataEntries []SegmentDataEntry
}

type MetadataEntry struct {
	Schema     string
	Name       string
	ObjectType string
	StartByte  uint64
	EndByte    uint64
}

type MasterDataEntry struct {
	Schema          string
	Name            string
	Oid             uint32
	AttributeString string
}

type SegmentDataEntry struct {
	Oid       uint // We use uint since the flags package does not have a uint32 flag
	StartByte uint64
	EndByte   uint64
}

func NewTOC(filename string) *TOC {
	toc := &TOC{}
	contents, err := System.ReadFile(filename)
	CheckError(err)
	err = yaml.Unmarshal(contents, toc)
	CheckError(err)
	return toc
}

func (toc *TOC) WriteToFileAndMakeReadOnly(filename string) {
	defer System.Chmod(filename, 0444)
	toc.WriteToFile(filename)
}

func (toc *TOC) WriteToFile(filename string) {
	tocFile := MustOpenFileForWriting(filename)
	tocContents, _ := yaml.Marshal(toc)
	MustPrintBytes(tocFile, tocContents)
}

type StatementWithType struct {
	ObjectType string
	Statement  string
}

func (toc *TOC) GetSQLStatementForObjectTypes(filename string, metadataFile io.ReaderAt, objectTypes []string, includeSchemas []string, includeTables []string) []StatementWithType {
	entries := *toc.metadataEntryMap[filename]
	objectSet := NewIncludeSet(objectTypes)
	schemaSet := NewIncludeSet(includeSchemas)
	tableSet := NewIncludeSet(includeTables)
	statements := make([]StatementWithType, 0)
	for _, entry := range entries {
		shouldIncludeObject := objectSet.MatchesFilter(entry.ObjectType)
		shouldIncludeSchema := schemaSet.MatchesFilter(entry.Schema)
		tableFQN := MakeFQN(entry.Schema, entry.Name)
		shouldIncludeTable := len(includeTables) == 0 || (entry.ObjectType == "TABLE" && tableSet.MatchesFilter(tableFQN))
		if shouldIncludeObject && shouldIncludeSchema && shouldIncludeTable {
			contents := make([]byte, entry.EndByte-entry.StartByte)
			_, err := metadataFile.ReadAt(contents, int64(entry.StartByte))
			CheckError(err)
			statements = append(statements, StatementWithType{ObjectType: entry.ObjectType, Statement: string(contents)})
		}
	}
	return statements
}

func (toc *TOC) GetAllSQLStatements(filename string, metadataFile io.ReaderAt) []StatementWithType {
	entries := *toc.metadataEntryMap[filename]
	statements := make([]StatementWithType, 0)
	for _, entry := range entries {
		contents := make([]byte, entry.EndByte-entry.StartByte)
		_, err := metadataFile.ReadAt(contents, int64(entry.StartByte))
		CheckError(err)
		statements = append(statements, StatementWithType{ObjectType: entry.ObjectType, Statement: string(contents)})
	}
	return statements
}

func (toc *TOC) GetDataEntriesMatching(includeSchemas []string, includeTables []string) []MasterDataEntry {
	restoreAllSchemas := len(includeSchemas) == 0
	var schemaHashes map[string]bool
	if !restoreAllSchemas {
		schemaHashes = make(map[string]bool, len(includeSchemas))
		for _, schema := range includeSchemas {
			schemaHashes[schema] = true
		}
	}
	restoreAllTables := len(includeTables) == 0
	var tableHashes map[string]bool
	if !restoreAllTables {
		tableHashes = make(map[string]bool, len(includeTables))
		for _, table := range includeTables {
			tableHashes[table] = true
		}
	}
	matchingEntries := make([]MasterDataEntry, 0)
	for _, entry := range toc.MasterDataEntries {
		_, validSchema := schemaHashes[entry.Schema]
		validSchema = restoreAllSchemas || validSchema
		tableFQN := MakeFQN(entry.Schema, entry.Name)
		_, validTable := tableHashes[tableFQN]
		validTable = restoreAllTables || validTable
		if validSchema && validTable {
			matchingEntries = append(matchingEntries, entry)
		}
	}
	return matchingEntries
}

func SubstituteRedirectDatabaseInStatements(statements []StatementWithType, oldName string, newName string) []StatementWithType {
	shouldReplace := map[string]bool{"DATABASE GUC": true, "DATABASE": true, "DATABASE METADATA": true}
	originalDatabase := regexp.QuoteMeta(oldName)
	newDatabase := newName
	pattern := regexp.MustCompile(fmt.Sprintf("DATABASE %s(;| OWNER| SET| TABLESPACE| TO| FROM| IS)", originalDatabase))
	for i := range statements {
		if shouldReplace[statements[i].ObjectType] {
			statements[i].Statement = pattern.ReplaceAllString(statements[i].Statement, fmt.Sprintf("DATABASE %s$1", newDatabase))
		}
	}
	return statements
}

func (toc *TOC) InitializeEntryMap(global string, predata string, postdata string, statistics string) {
	toc.metadataEntryMap = make(map[string]*[]MetadataEntry, 4)
	toc.metadataEntryMap[global] = &toc.GlobalEntries
	toc.metadataEntryMap[predata] = &toc.PredataEntries
	toc.metadataEntryMap[postdata] = &toc.PostdataEntries
	toc.metadataEntryMap[statistics] = &toc.StatisticsEntries
}

func (toc *TOC) InitializeEntryMapFromCluster(cluster Cluster) {
	globalFilename := cluster.GetGlobalFilePath()
	predataFilename := cluster.GetPredataFilePath()
	postdataFilename := cluster.GetPostdataFilePath()
	statisticsFilename := cluster.GetStatisticsFilePath()
	toc.InitializeEntryMap(globalFilename, predataFilename, postdataFilename, statisticsFilename)
}

func (toc *TOC) AddMetadataEntry(schema string, name string, objectType string, start uint64, file *FileWithByteCount) {
	*toc.metadataEntryMap[file.Filename] = append(*toc.metadataEntryMap[file.Filename], MetadataEntry{schema, name, objectType, start, file.ByteCount})
}

func (toc *TOC) AddMasterDataEntry(schema string, name string, oid uint32, attributeString string) {
	toc.MasterDataEntries = append(toc.MasterDataEntries, MasterDataEntry{schema, name, oid, attributeString})
}

func (toc *TOC) AddSegmentDataEntry(oid uint, startByte uint64, endByte uint64) {
	toc.SegmentDataEntries = append(toc.SegmentDataEntries, SegmentDataEntry{oid, startByte, endByte})
}
