package backup_test

import (
	"github.com/greenplum-db/gpbackup/backup"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
)

var _ = Describe("backup/dependencies tests", func() {
	buffer := gbytes.NewBuffer()

	BeforeEach(func() {
		buffer = gbytes.BufferWithBytes([]byte(""))
	})
	Describe("TopologicalSort", func() {
		It("returns the original slice if there are no dependencies among objects", func() {
			relations := []backup.Sortable{
				backup.Relation{SchemaName: "public", RelationName: "relation1", DependsUpon: []string{}},
				backup.Relation{SchemaName: "public", RelationName: "relation2", DependsUpon: []string{}},
				backup.Relation{SchemaName: "public", RelationName: "relation3", DependsUpon: []string{}},
			}

			relations = backup.TopologicalSort(relations)

			Expect(relations[0].Name()).To(Equal("public.relation1"))
			Expect(relations[1].Name()).To(Equal("public.relation2"))
			Expect(relations[2].Name()).To(Equal("public.relation3"))
		})
		It("sorts the slice correctly if there is an object dependent on one other object", func() {
			relations := []backup.Sortable{
				backup.Relation{SchemaName: "public", RelationName: "relation1", DependsUpon: []string{"public.relation3"}},
				backup.Relation{SchemaName: "public", RelationName: "relation2", DependsUpon: []string{}},
				backup.Relation{SchemaName: "public", RelationName: "relation3", DependsUpon: []string{}},
			}

			relations = backup.TopologicalSort(relations)

			Expect(relations[0].Name()).To(Equal("public.relation2"))
			Expect(relations[1].Name()).To(Equal("public.relation3"))
			Expect(relations[2].Name()).To(Equal("public.relation1"))
		})
		It("sorts the slice correctly if there are two objects dependent on one other object", func() {
			views := []backup.Sortable{
				backup.View{SchemaName: "public", ViewName: "view1", DependsUpon: []string{"public.view2"}},
				backup.View{SchemaName: "public", ViewName: "view2", DependsUpon: []string{}},
				backup.View{SchemaName: "public", ViewName: "view3", DependsUpon: []string{"public.view2"}},
			}

			views = backup.TopologicalSort(views)

			Expect(views[0].Name()).To(Equal("public.view2"))
			Expect(views[1].Name()).To(Equal("public.view1"))
			Expect(views[2].Name()).To(Equal("public.view3"))
		})
		It("sorts the slice correctly if there is one object dependent on two other objects", func() {
			types := []backup.Sortable{
				backup.Type{TypeSchema: "public", TypeName: "type1", DependsUpon: []string{}},
				backup.Type{TypeSchema: "public", TypeName: "type2", DependsUpon: []string{"public.type1", "public.type3"}},
				backup.Type{TypeSchema: "public", TypeName: "type3", DependsUpon: []string{}},
			}

			types = backup.TopologicalSort(types)

			Expect(types[0].Name()).To(Equal("public.type1"))
			Expect(types[1].Name()).To(Equal("public.type3"))
			Expect(types[2].Name()).To(Equal("public.type2"))
		})
		It("sorts the slice correctly if there are complex dependencies", func() {
			sortable := []backup.Sortable{
				backup.Type{TypeSchema: "public", TypeName: "type1", DependsUpon: []string{}},
				backup.Type{TypeSchema: "public", TypeName: "type2", DependsUpon: []string{"public.type1", "public.function3"}},
				backup.Function{SchemaName: "public", FunctionName: "function3", DependsUpon: []string{"public.type1"}},
			}

			sortable = backup.TopologicalSort(sortable)

			Expect(sortable[0].Name()).To(Equal("public.type1"))
			Expect(sortable[1].Name()).To(Equal("public.function3"))
			Expect(sortable[2].Name()).To(Equal("public.type2"))
		})
	})
})
