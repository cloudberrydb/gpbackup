package backup

import (
	"fmt"

	"github.com/greenplum-db/gpbackup/utils"
	"github.com/pkg/errors"
)

func SortFunctionsAndTypesAndTablesInDependencyOrder(functions []Function, types []Type, tables []Relation) []Sortable {
	objects := make([]Sortable, 0)
	for _, function := range functions {
		objects = append(objects, function)
	}
	for _, typ := range types {
		if typ.Type != "e" && typ.Type != "p" {
			objects = append(objects, typ)
		}
	}
	for _, table := range tables {
		objects = append(objects, table)
	}
	sorted := TopologicalSort(objects)
	return sorted
}

func ConstructFunctionAndTypeDependencyLists(connection *utils.DBConn, functions []Function, types []Type, funcInfoMap map[uint32]FunctionInfo) ([]Function, []Type) {
	functions = ConstructFunctionDependencies(connection, functions)
	types = CoalesceCompositeTypes(types)
	/*
	 * Each of the Construct[...]TypeDependencies functions adds dependency information
	 * to the appropriate types in-place, without modifying the slice or the order of
	 * elements, so we re-use the same slice for each call and after all three calls
	 * all of the types will have their dependencies set.
	 */
	if connection.Version.Before("5") {
		types = ConstructBaseTypeDependencies4(connection, types, funcInfoMap)
	} else {
		types = ConstructBaseTypeDependencies5(connection, types)
	}
	types = ConstructDomainDependencies(connection, types)
	types = ConstructCompositeTypeDependencies(connection, types)
	return functions, types
}

func ConstructFunctionAndTypeAndTableMetadataMap(functions MetadataMap, types MetadataMap, tables MetadataMap) MetadataMap {
	metadataMap := make(MetadataMap, 0)
	for k, v := range functions {
		metadataMap[k] = v
	}
	for k, v := range types {
		metadataMap[k] = v
	}
	for k, v := range tables {
		metadataMap[k] = v
	}
	return metadataMap
}

/*
 * Structs and functions for topological sort
 */

type Sortable interface {
	FQN() string
	Dependencies() []string
}

func (r Relation) FQN() string {
	return r.ToString()
}

func (v View) FQN() string {
	return utils.MakeFQN(v.Schema, v.Name)
}

func (f Function) FQN() string {
	/*
	 * We need to include arguments to differentiate functions with the same name;
	 * we don't use IdentArgs because we already have Arguments in the funcInfoMap.
	 */
	return fmt.Sprintf("%s(%s)", utils.MakeFQN(f.Schema, f.Name), f.Arguments)
}

func (t Type) FQN() string {
	return utils.MakeFQN(t.Schema, t.Name)
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
		name := item.FQN()
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
		delete(notVisited, item.FQN())
		for _, dep := range isDependentOn[item.FQN()] {
			inDegrees[dep]--
			if inDegrees[dep] == 0 {
				queue = append(queue, slice[dependencyIndexes[dep]])
			}
		}
	}
	if len(slice) != len(sorted) {
		logger.Verbose("Failed to sort dependencies.")
		logger.Verbose("Not yet visited:")
		for _, item := range slice {
			if notVisited[item.FQN()] {
				logger.Verbose("Object: %s; Dependencies: %s", item.FQN(), item.Dependencies())
			}
		}
		logger.Fatal(errors.Errorf("Dependency resolution failed; see log file %s for details. This is a bug, please report.", logger.GetLogFilePath()), "")
	}
	return sorted
}
