package utils

import (
	"io"
	"io/ioutil"

	yaml "gopkg.in/yaml.v2"
)

type TOC struct {
	GlobalEntries   []Entry
	PredataEntries  []Entry
	PostdataEntries []Entry
}

type Entry struct {
	Name       string
	Schema     string
	ObjectType string
	StartByte  uint64
	EndByte    uint64
}

func NewTOC(filename string) *TOC {
	toc := &TOC{}
	contents, err := ioutil.ReadFile(filename)
	CheckError(err)
	err = yaml.Unmarshal(contents, toc)
	CheckError(err)
	return toc
}

func (toc *TOC) GetSQLStatementForObjectTypes(entries []Entry, metadataFile io.ReaderAt, objectTypes ...string) []string {
	objectHashes := make(map[string]bool, len(objectTypes))
	for _, objectType := range objectTypes {
		objectHashes[objectType] = true
	}
	statements := []string{}
	for _, entry := range entries {
		if _, ok := objectHashes[entry.ObjectType]; ok {
			contents := make([]byte, entry.EndByte-entry.StartByte)
			_, err := metadataFile.ReadAt(contents, int64(entry.StartByte))
			CheckError(err)
			statements = append(statements, string(contents))
		}
	}
	return statements
}

func (toc *TOC) AddPredataEntry(schema string, name string, objectType string, start uint64, end uint64) {
	toc.PredataEntries = append(toc.PredataEntries, Entry{name, schema, objectType, start, end})
}

func (toc *TOC) AddPostdataEntry(schema string, name string, objectType string, start uint64, end uint64) {
	toc.PostdataEntries = append(toc.PostdataEntries, Entry{name, schema, objectType, start, end})
}

func (toc *TOC) AddGlobalEntry(schema string, name string, objectType string, start uint64, end uint64) {
	toc.GlobalEntries = append(toc.GlobalEntries, Entry{name, schema, objectType, start, end})
}
