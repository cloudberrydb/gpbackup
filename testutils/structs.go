package testutils

/*
 * This file contains test structs and functions used in unit tests via dependency injection.
 */

import (
	"fmt"

	"reflect"

	"strings"

	"github.com/jmoiron/sqlx"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type TestDriver struct {
	DBExists   bool
	RoleExists bool
	DB         *sqlx.DB
	DBName     string
	User       string
}

func (driver TestDriver) Connect(driverName string, dataSourceName string) (*sqlx.DB, error) {
	if driver.DBExists && driver.RoleExists {
		return driver.DB, nil
	} else if driver.DBExists {
		return nil, fmt.Errorf("pq: role \"%s\" does not exist", driver.User)
	}
	return nil, fmt.Errorf("pq: database \"%s\" does not exist", driver.DBName)
}

type TestResult struct {
	Rows int64
}

func (result TestResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (result TestResult) RowsAffected() (int64, error) {
	return result.Rows, nil
}

/*
 * If fields are to be filtered in or out, set shouldFilter to true; filterInclude is true to
 * include fields or false to exclude fields, and filterFields contains the field names to filter on.
 * To filter on a field "fieldname" in struct "structname", pass in "fieldname".
 * To filter on a field "fieldname" in a nested struct under field "structfield", pass in "structfield.fieldname".
 * This function assumes structs will only ever be nested one level deep.
 */
func StructMatcher(expected interface{}, actual interface{}, shouldFilter bool, filterInclude bool, filterFields ...string) []string {
	// Add field names for the top-level struct to a filter map, and split off nested field names to pass down to nested structs
	filterMap := make(map[string]bool)
	nestedFilterFields := make([]string, 0)
	for i := 0; i < len(filterFields); i++ {
		fieldNames := strings.Split(filterFields[i], ".")
		if len(fieldNames) == 2 {
			nestedFilterFields = append(nestedFilterFields, fieldNames[1])
			// If we include a nested struct field, we also need to include the nested struct
			if filterInclude {
				filterMap[fieldNames[0]] = true
			}
		} else {
			filterMap[filterFields[i]] = true
		}
	}

	expectedStruct := reflect.Indirect(reflect.ValueOf(expected))
	actualStruct := reflect.Indirect(reflect.ValueOf(actual))
	mismatches := []string{}
	mismatches = append(mismatches, InterceptGomegaFailures(func() {
		for i := 0; i < expectedStruct.NumField(); i++ {
			expectedField := expectedStruct.Field(i)
			actualField := actualStruct.Field(i)
			fieldName := actualStruct.Type().Field(i).Name
			// If we're including, skip this field if the name doesn't match; if we're excluding, skip if it does match
			if shouldFilter && ((filterInclude && !filterMap[fieldName]) || (!filterInclude && filterMap[fieldName])) {
				continue
			}
			fieldIsStruct := actualField.Kind() == reflect.Struct && !actualField.IsNil()
			actualFieldIsNonemptySlice := actualField.Kind() == reflect.Slice && !actualField.IsNil() && actualField.Len() > 0
			expectedFieldIsNonemptySlice := expectedField.Kind() == reflect.Slice && !expectedField.IsNil() && expectedField.Len() > 0
			fieldIsStructSlice := actualFieldIsNonemptySlice && expectedFieldIsNonemptySlice && actualField.Len() == expectedField.Len() && actualField.Index(0).Kind() == reflect.Struct
			if fieldIsStruct {
				expectedStructField := expectedStruct.Field(i).Interface()
				actualStructField := actualStruct.Field(i).Interface()
				mismatches = append(mismatches, StructMatcher(expectedStructField, actualStructField, shouldFilter, filterInclude, nestedFilterFields...)...)
			} else if fieldIsStructSlice {
				for j := 0; j < actualField.Len(); j++ {
					expectedStructField := expectedStruct.Field(i).Index(j).Interface()
					actualStructField := actualStruct.Field(i).Index(j).Interface()
					mismatches = append(mismatches, StructMatcher(expectedStructField, actualStructField, shouldFilter, filterInclude, nestedFilterFields...)...)
				}
			} else {
				expectedValue := expectedStruct.Field(i).Interface()
				actualValue := actualStruct.Field(i).Interface()
				Expect(expectedValue).To(Equal(actualValue), "Mismatch on field %s", fieldName)
			}
		}
	})...)
	return mismatches
}

func ExpectStructsToMatch(expected interface{}, actual interface{}) {
	mismatches := StructMatcher(expected, actual, false, false)
	if len(mismatches) > 0 {
		Fail(strings.Join(mismatches, "\n"))
	}
}

func ExpectStructsToMatchExcluding(expected interface{}, actual interface{}, excludeFields ...string) {
	mismatches := StructMatcher(expected, actual, true, false, excludeFields...)
	if len(mismatches) > 0 {
		Fail(strings.Join(mismatches, "\n"))
	}
}

func ExpectStructsToMatchIncluding(expected interface{}, actual interface{}, includeFields ...string) {
	mismatches := StructMatcher(expected, actual, true, true, includeFields...)
	if len(mismatches) > 0 {
		Fail(strings.Join(mismatches, "\n"))
	}
}
