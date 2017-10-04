package utils

import (
	"fmt"
	"io"
	"io/ioutil"
	"regexp"

	yaml "gopkg.in/yaml.v2"
)

type TOC struct {
	GlobalEntries     []MetadataEntry
	PredataEntries    []MetadataEntry
	PostdataEntries   []MetadataEntry
	StatisticsEntries []MetadataEntry
	DataEntries       []DataEntry
}

type MetadataEntry struct {
	Schema     string
	Name       string
	ObjectType string
	StartByte  uint64
	EndByte    uint64
}

type DataEntry struct {
	Schema          string
	Name            string
	Oid             uint32
	AttributeString string
}

func NewTOC(filename string) *TOC {
	toc := &TOC{}
	contents, err := ioutil.ReadFile(filename)
	CheckError(err)
	err = yaml.Unmarshal(contents, toc)
	CheckError(err)
	return toc
}

func (toc *TOC) WriteToFile(filename string) {
	defer System.Chmod(filename, 0444)
	tocFile := MustOpenFileForWriting(filename)
	tocContents, _ := yaml.Marshal(toc)
	MustPrintBytes(tocFile, tocContents)
}

type StatementWithType struct {
	ObjectType string
	Statement  string
}

func (toc *TOC) GetSQLStatementForObjectTypes(entries []MetadataEntry, metadataFile io.ReaderAt, objectTypes ...string) []StatementWithType {
	objectHashes := make(map[string]bool, len(objectTypes))
	for _, objectType := range objectTypes {
		objectHashes[objectType] = true
	}
	statements := make([]StatementWithType, 0)
	for _, entry := range entries {
		if _, ok := objectHashes[entry.ObjectType]; ok {
			contents := make([]byte, entry.EndByte-entry.StartByte)
			_, err := metadataFile.ReadAt(contents, int64(entry.StartByte))
			CheckError(err)
			statements = append(statements, StatementWithType{ObjectType: entry.ObjectType, Statement: string(contents)})
		}
	}
	return statements
}

func (toc *TOC) GetAllSQLStatements(entries []MetadataEntry, metadataFile io.ReaderAt) []StatementWithType {
	statements := make([]StatementWithType, 0)
	for _, entry := range entries {
		contents := make([]byte, entry.EndByte-entry.StartByte)
		_, err := metadataFile.ReadAt(contents, int64(entry.StartByte))
		CheckError(err)
		statements = append(statements, StatementWithType{ObjectType: entry.ObjectType, Statement: string(contents)})
	}
	return statements
}

func SubstituteRedirectDatabaseInStatements(statements []StatementWithType, oldName string, newName string) []StatementWithType {
	shouldReplace := map[string]bool{"DATABASE GUC": true, "DATABASE": true, "DATABASE METADATA": true}
	originalDatabase := regexp.QuoteMeta(QuoteIdent(oldName))
	newDatabase := QuoteIdent(newName)
	pattern := regexp.MustCompile(fmt.Sprintf("DATABASE %s(;| OWNER| SET)", originalDatabase))
	for i := range statements {
		if shouldReplace[statements[i].ObjectType] {
			statements[i].Statement = pattern.ReplaceAllString(statements[i].Statement, fmt.Sprintf("DATABASE %s$1", newDatabase))
		}
	}
	return statements
}

func (toc *TOC) AddPredataEntry(schema string, name string, objectType string, start uint64, end uint64) {
	toc.PredataEntries = append(toc.PredataEntries, MetadataEntry{schema, name, objectType, start, end})
}

func (toc *TOC) AddPostdataEntry(schema string, name string, objectType string, start uint64, end uint64) {
	toc.PostdataEntries = append(toc.PostdataEntries, MetadataEntry{schema, name, objectType, start, end})
}

func (toc *TOC) AddGlobalEntry(schema string, name string, objectType string, start uint64, end uint64) {
	toc.GlobalEntries = append(toc.GlobalEntries, MetadataEntry{schema, name, objectType, start, end})
}

func (toc *TOC) AddDataEntry(schema string, name string, oid uint32, attributeString string) {
	toc.DataEntries = append(toc.DataEntries, DataEntry{schema, name, oid, attributeString})
}

func (toc *TOC) AddStatisticsEntry(schema string, name string, objectType string, start uint64, end uint64) {
	toc.StatisticsEntries = append(toc.StatisticsEntries, MetadataEntry{schema, name, objectType, start, end})
}
