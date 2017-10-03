package backup_test

import (
	"database/sql/driver"

	"github.com/blang/semver"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"
	sqlmock "gopkg.in/DATA-DOG/go-sqlmock.v1"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup/dependencies tests", func() {
	var (
		function1 backup.Function
		function2 backup.Function
		function3 backup.Function
		relation1 backup.Relation
		relation2 backup.Relation
		relation3 backup.Relation
		type1     backup.Type
		type2     backup.Type
		type3     backup.Type
		view1     backup.View
		view2     backup.View
		view3     backup.View
	)

	BeforeEach(func() {
		function1 = backup.Function{SchemaName: "public", FunctionName: "function1", Arguments: "integer, integer", DependsUpon: []string{}}
		function2 = backup.Function{SchemaName: "public", FunctionName: "function1", Arguments: "numeric, text", DependsUpon: []string{}}
		function3 = backup.Function{SchemaName: "public", FunctionName: "function2", Arguments: "integer, integer", DependsUpon: []string{}}
		relation1 = backup.Relation{SchemaName: "public", RelationName: "relation1", DependsUpon: []string{}}
		relation2 = backup.Relation{SchemaName: "public", RelationName: "relation2", DependsUpon: []string{}}
		relation3 = backup.Relation{SchemaName: "public", RelationName: "relation3", DependsUpon: []string{}}
		type1 = backup.Type{TypeSchema: "public", TypeName: "type1", DependsUpon: []string{}}
		type2 = backup.Type{TypeSchema: "public", TypeName: "type2", DependsUpon: []string{}}
		type3 = backup.Type{TypeSchema: "public", TypeName: "type3", DependsUpon: []string{}}
		view1 = backup.View{SchemaName: "public", ViewName: "view1", DependsUpon: []string{}}
		view2 = backup.View{SchemaName: "public", ViewName: "view2", DependsUpon: []string{}}
		view3 = backup.View{SchemaName: "public", ViewName: "view3", DependsUpon: []string{}}
	})
	Describe("TopologicalSort", func() {
		It("returns the original slice if there are no dependencies among objects", func() {
			relations := []backup.Sortable{relation1, relation2, relation3}

			relations = backup.TopologicalSort(relations)

			Expect(relations[0].Name()).To(Equal("public.relation1"))
			Expect(relations[1].Name()).To(Equal("public.relation2"))
			Expect(relations[2].Name()).To(Equal("public.relation3"))
		})
		It("sorts the slice correctly if there is an object dependent on one other object", func() {
			relation1.DependsUpon = []string{"public.relation3"}
			relations := []backup.Sortable{relation1, relation2, relation3}

			relations = backup.TopologicalSort(relations)

			Expect(relations[0].Name()).To(Equal("public.relation2"))
			Expect(relations[1].Name()).To(Equal("public.relation3"))
			Expect(relations[2].Name()).To(Equal("public.relation1"))
		})
		It("sorts the slice correctly if there are two objects dependent on one other object", func() {
			view1.DependsUpon = []string{"public.view2"}
			view3.DependsUpon = []string{"public.view2"}
			views := []backup.Sortable{view1, view2, view3}

			views = backup.TopologicalSort(views)

			Expect(views[0].Name()).To(Equal("public.view2"))
			Expect(views[1].Name()).To(Equal("public.view1"))
			Expect(views[2].Name()).To(Equal("public.view3"))
		})
		It("sorts the slice correctly if there is one object dependent on two other objects", func() {
			type2.DependsUpon = []string{"public.type1", "public.type3"}
			types := []backup.Sortable{type1, type2, type3}

			types = backup.TopologicalSort(types)

			Expect(types[0].Name()).To(Equal("public.type1"))
			Expect(types[1].Name()).To(Equal("public.type3"))
			Expect(types[2].Name()).To(Equal("public.type2"))
		})
		It("sorts the slice correctly if there are complex dependencies", func() {
			type2.DependsUpon = []string{"public.type1", "public.function2(integer, integer)"}
			function3.DependsUpon = []string{"public.type1"}
			sortable := []backup.Sortable{type1, type2, function3}

			sortable = backup.TopologicalSort(sortable)

			Expect(sortable[0].Name()).To(Equal("public.type1"))
			Expect(sortable[1].Name()).To(Equal("public.function2(integer, integer)"))
			Expect(sortable[2].Name()).To(Equal("public.type2"))
		})
		It("aborts if dependency loop (this shouldn't be possible)", func() {
			type1.DependsUpon = []string{"public.type3"}
			type2.DependsUpon = []string{"public.type1"}
			type3.DependsUpon = []string{"public.type2"}
			sortable := []backup.Sortable{type1, type2, type3}

			defer testutils.ShouldPanicWithMessage("Dependency resolution failed. This is a bug, please report.")
			sortable = backup.TopologicalSort(sortable)
		})
		It("aborts if dependencies are not met", func() {
			type1.DependsUpon = []string{"missing_thing", "public.type2"}
			sortable := []backup.Sortable{type1, type2}

			defer testutils.ShouldPanicWithMessage("Dependency resolution failed. This is a bug, please report.")
			sortable = backup.TopologicalSort(sortable)
		})
	})
	Describe("SortFunctionsAndTypesAndTablesInDependencyOrder", func() {
		It("returns a slice of unsorted functions followed by unsorted types followed by unsorted tables if there are no dependencies among objects", func() {
			functions := []backup.Function{function1, function2, function3}
			types := []backup.Type{type1, type2, type3}
			relations := []backup.Relation{relation1, relation2, relation3}
			results := backup.SortFunctionsAndTypesAndTablesInDependencyOrder(functions, types, relations)
			expected := []backup.Sortable{function1, function2, function3, type1, type2, type3, relation1, relation2, relation3}
			Expect(results).To(Equal(expected))
		})
		It("returns a slice of sorted functions, types, and relations if there are dependencies among objects of the same type", func() {
			function2.DependsUpon = []string{"public.function2(integer, integer)"}
			type2.DependsUpon = []string{"public.type3"}
			relation2.DependsUpon = []string{"public.relation3"}
			functions := []backup.Function{function1, function2, function3}
			types := []backup.Type{type1, type2, type3}
			relations := []backup.Relation{relation1, relation2, relation3}
			results := backup.SortFunctionsAndTypesAndTablesInDependencyOrder(functions, types, relations)
			expected := []backup.Sortable{function1, function3, type1, type3, relation1, relation3, function2, type2, relation2}
			Expect(results).To(Equal(expected))
		})
		It("returns a slice of sorted functions, types, and relations if there are dependencies among objects of different types", func() {
			function2.DependsUpon = []string{"public.type3"}
			type2.DependsUpon = []string{"public.relation3"}
			relation2.DependsUpon = []string{"public.type1"}
			functions := []backup.Function{function1, function2, function3}
			types := []backup.Type{type1, type2, type3}
			relations := []backup.Relation{relation1, relation2, relation3}
			results := backup.SortFunctionsAndTypesAndTablesInDependencyOrder(functions, types, relations)
			expected := []backup.Sortable{function1, function3, type1, type3, relation1, relation3, relation2, function2, type2}
			Expect(results).To(Equal(expected))
		})
	})
	Describe("ConstructFunctionAndTypeDependencyLists", func() {
		var oldVersion utils.GPDBVersion
		BeforeEach(func() {
			oldVersion = connection.Version
		})
		AfterEach(func() {
			connection.Version = oldVersion
		})
		It("queries dependencies for functions and types in GPDB 5", func() {
			connection.Version = utils.GPDBVersion{VersionString: "5.0.0", SemVer: semver.MustParse("5.0.0")}
			funcInfoMap := map[uint32]backup.FunctionInfo{}
			header := []string{"oid", "referencedobject"}
			functionRows := sqlmock.NewRows(header).AddRow([]driver.Value{"1", "public.type"}...)
			baseTypeRows := sqlmock.NewRows(header).AddRow([]driver.Value{"2", "public.func(integer, integer)"}...)
			compTypeRows := sqlmock.NewRows(header).AddRow([]driver.Value{"3", "public.othertype"}...)
			domainRows := sqlmock.NewRows(header).AddRow([]driver.Value{"4", "public.builtin"}...)

			function1.Oid = 1
			type1.Oid = 2
			type1.Type = "b"
			type2.Oid = 3
			type2.Type = "c"
			type3.Oid = 4
			type3.Type = "d"

			functions := []backup.Function{function1}
			types := []backup.Type{type1, type2, type3}

			mock.ExpectQuery(`SELECT (.*)`).WillReturnRows(functionRows)
			mock.ExpectQuery(`SELECT (.*)`).WillReturnRows(baseTypeRows)
			mock.ExpectQuery(`SELECT (.*)`).WillReturnRows(domainRows)
			mock.ExpectQuery(`SELECT (.*)`).WillReturnRows(compTypeRows)

			functions, types = backup.ConstructFunctionAndTypeDependencyLists(connection, functions, types, funcInfoMap)

			Expect(functions[0].DependsUpon).To(Equal([]string{"public.type"}))
			Expect(types[0].DependsUpon).To(Equal([]string{"public.func(integer, integer)"}))
			Expect(types[1].DependsUpon).To(Equal([]string{"public.othertype"}))
			Expect(types[2].DependsUpon).To(Equal([]string{"public.builtin"}))
		})
		It("queries dependencies for functions and types in GPDB 4.3", func() {
			connection.Version = utils.GPDBVersion{VersionString: "4.3.0", SemVer: semver.MustParse("4.3.0")}
			funcInfoMap := map[uint32]backup.FunctionInfo{
				5: {QualifiedName: "public.func", Arguments: "integer, integer"},
			}
			header := []string{"oid", "referencedobject"}
			functionRows := sqlmock.NewRows(header).AddRow([]driver.Value{"1", "public.type"}...)
			baseTypeHeader := []string{"oid", "referencedoid"}
			baseTypeRows := sqlmock.NewRows(baseTypeHeader).AddRow([]driver.Value{"2", "5"}...)
			compTypeRows := sqlmock.NewRows(header).AddRow([]driver.Value{"3", "public.othertype"}...)
			domainRows := sqlmock.NewRows(header).AddRow([]driver.Value{"4", "public.builtin"}...)

			function1.Oid = 1
			type1.Oid = 2
			type1.Type = "b"
			type2.Oid = 3
			type2.Type = "c"
			type3.Oid = 4
			type3.Type = "d"

			functions := []backup.Function{function1}
			types := []backup.Type{type1, type2, type3}

			mock.ExpectQuery(`SELECT (.*)`).WillReturnRows(functionRows)
			mock.ExpectQuery(`SELECT (.*)`).WillReturnRows(baseTypeRows)
			mock.ExpectQuery(`SELECT (.*)`).WillReturnRows(domainRows)
			mock.ExpectQuery(`SELECT (.*)`).WillReturnRows(compTypeRows)

			functions, types = backup.ConstructFunctionAndTypeDependencyLists(connection, functions, types, funcInfoMap)

			Expect(functions[0].DependsUpon).To(Equal([]string{"public.type"}))
			Expect(types[0].DependsUpon).To(Equal([]string{"public.func(integer, integer)"}))
			Expect(types[1].DependsUpon).To(Equal([]string{"public.othertype"}))
			Expect(types[2].DependsUpon).To(Equal([]string{"public.builtin"}))
		})
	})
	Describe("ConstructFunctionAndTypeAndTableMetadataMap", func() {
		It("composes metadata maps for functions, types, and tables into one map", func() {
			funcMap := backup.MetadataMap{1: backup.ObjectMetadata{Comment: "function"}}
			typeMap := backup.MetadataMap{2: backup.ObjectMetadata{Comment: "type"}}
			tableMap := backup.MetadataMap{3: backup.ObjectMetadata{Comment: "relation"}}
			result := backup.ConstructFunctionAndTypeAndTableMetadataMap(funcMap, typeMap, tableMap)
			expected := backup.MetadataMap{
				1: backup.ObjectMetadata{Comment: "function"},
				2: backup.ObjectMetadata{Comment: "type"},
				3: backup.ObjectMetadata{Comment: "relation"},
			}
			Expect(result).To(Equal(expected))
		})
	})
	Describe("SortViews", func() {
		It("sorts the slice correctly if there are two objects dependent on one other object", func() {
			view1.DependsUpon = []string{"public.view2"}
			view3.DependsUpon = []string{"public.view2"}
			views := []backup.View{view1, view2, view3}

			views = backup.SortViews(views)

			Expect(views[0].Name()).To(Equal("public.view2"))
			Expect(views[1].Name()).To(Equal("public.view1"))
			Expect(views[2].Name()).To(Equal("public.view3"))
		})
	})
})
