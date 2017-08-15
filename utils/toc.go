package utils

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

func (toc *TOC) AddPredataEntry(schema string, name string, objectType string, start uint64, end uint64) {
	toc.PredataEntries = append(toc.PredataEntries, Entry{name, schema, objectType, start, end})
}

func (toc *TOC) AddPostdataEntry(schema string, name string, objectType string, start uint64, end uint64) {
	toc.PostdataEntries = append(toc.PostdataEntries, Entry{name, schema, objectType, start, end})
}

func (toc *TOC) AddGlobalEntry(schema string, name string, objectType string, start uint64, end uint64) {
	toc.GlobalEntries = append(toc.GlobalEntries, Entry{name, schema, objectType, start, end})
}
