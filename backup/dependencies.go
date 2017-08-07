package backup

import (
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/pkg/errors"
)

func SortFunctionsAndTypesInDependencyOrder(types []Type, functions []Function) []Sortable {
	objects := make([]Sortable, 0)
	for _, typ := range types {
		if typ.Type != "e" && typ.Type != "p" {
			objects = append(objects, typ)
		}
	}
	for _, function := range functions {
		objects = append(objects, function)
	}
	sorted := TopologicalSort(objects)
	return sorted
}

func ConstructDependencyLists(connection *utils.DBConn, types []Type, functions []Function) ([]Type, []Function) {
	types = CoalesceCompositeTypes(types)
	types = ConstructBaseTypeDependencies(connection, types)
	types = ConstructDomainDependencies(connection, types)
	types = ConstructCompositeTypeDependencies(connection, types)
	functions = ConstructFunctionDependencies(connection, functions)
	return types, functions
}

func ConstructFunctionAndTypeMetadataMap(types MetadataMap, functions MetadataMap) MetadataMap {
	metadataMap := make(MetadataMap, 0)
	for k, v := range types {
		metadataMap[k] = v
	}
	for k, v := range functions {
		metadataMap[k] = v
	}
	return metadataMap
}

/*
 * Structs and functions for topological sort
 */

type Sortable interface {
	Name() string
	Dependencies() []string
}

func (r Relation) Name() string {
	return r.ToString()
}

func (v View) Name() string {
	return MakeFQN(v.SchemaName, v.ViewName)
}

func (f Function) Name() string {
	return MakeFQN(f.SchemaName, f.FunctionName)
}

func (t Type) Name() string {
	return MakeFQN(t.TypeSchema, t.TypeName)
}

func (r Relation) Dependencies() []string {
	return r.DependsUpon
}

func (v View) Dependencies() []string {
	return v.DependsUpon
}

func (f Function) Dependencies() []string {
	return f.DependsUpon
}

func (t Type) Dependencies() []string {
	return t.DependsUpon
}

func SortRelations(relations []Relation) []Relation {
	sortable := make([]Sortable, len(relations))
	for i := range relations {
		sortable[i] = relations[i]
	}
	sortable = TopologicalSort(sortable)
	for i := range sortable {
		relations[i] = sortable[i].(Relation)
	}
	return relations
}

func SortViews(views []View) []View {
	sortable := make([]Sortable, len(views))
	for i := range views {
		sortable[i] = views[i]
	}
	sortable = TopologicalSort(sortable)
	for i := range views {
		views[i] = sortable[i].(View)
	}
	return views
}

func TopologicalSort(slice []Sortable) []Sortable {
	inDegrees := make(map[string]int, 0)
	dependencyIndexes := make(map[string]int, 0)
	isDependentOn := make(map[string][]string, 0)
	queue := make([]Sortable, 0)
	sorted := make([]Sortable, 0)
	notVisited := make(map[string]bool)
	for i, item := range slice {
		name := item.Name()
		deps := item.Dependencies()
		notVisited[name] = true
		inDegrees[name] = len(deps)
		for _, dep := range deps {
			isDependentOn[dep] = append(isDependentOn[dep], name)
		}
		dependencyIndexes[name] = i
		if len(deps) == 0 {
			queue = append(queue, item)
		}
	}
	for len(queue) > 0 {
		item := queue[0]
		queue = queue[1:]
		sorted = append(sorted, item)
		delete(notVisited, item.Name())
		for _, dep := range isDependentOn[item.Name()] {
			inDegrees[dep]--
			if inDegrees[dep] == 0 {
				queue = append(queue, slice[dependencyIndexes[dep]])
			}
		}
	}
	if len(slice) != len(sorted) {
		logger.Verbose("Failed to sort %v", slice)
		logger.Fatal(errors.Errorf("Dependency resolution failed. This is a bug, please report."), "")
	}
	return sorted
}
