package testutils

/*
 * This file contains test structs and functions used in unit tests via dependency injection.
 */

import (
	"errors"
	"fmt"

	"reflect"

	"strings"

	"github.com/jmoiron/sqlx"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type TestDriver struct {
	DBExists bool
	DB       *sqlx.DB
	DBName   string
}

func (driver TestDriver) Connect(driverName string, dataSourceName string) (*sqlx.DB, error) {
	if driver.DBExists {
		return driver.DB, nil
	} else {
		return nil, errors.New(fmt.Sprintf("Database %s does not exist", driver.DBName))
	}
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

func StructMatcher(expected interface{}, actual interface{}) []string {
	expectedStruct := reflect.Indirect(reflect.ValueOf(expected))
	actualStruct := reflect.Indirect(reflect.ValueOf(actual))

	mismatches := InterceptGomegaFailures(func() {
		for i := 0; i < expectedStruct.NumField(); i++ {
			actualFieldName := actualStruct.Type().Field(i).Name
			expectedValue := expectedStruct.Field(i).Interface()
			actualValue := actualStruct.Field(i).Interface()
			Expect(expectedValue).To(Equal(actualValue), "Mismatch on field %s ", actualFieldName)
		}
	})
	return mismatches
}

func StructMatcherExcluding(expected interface{}, actual interface{}, excludeFields []string) []string {

	excludeMap := make(map[string]bool)
	for i := 0; i < len(excludeFields); i += 1 {
		excludeMap[excludeFields[i]] = true
	}

	expectedStruct := reflect.Indirect(reflect.ValueOf(expected))
	actualStruct := reflect.Indirect(reflect.ValueOf(actual))

	mismatches := InterceptGomegaFailures(func() {
		for i := 0; i < expectedStruct.NumField(); i++ {
			actualFieldName := actualStruct.Type().Field(i).Name
			if excludeMap[actualFieldName] {
				continue
			}

			expectedValue := expectedStruct.Field(i).Interface()
			actualValue := actualStruct.Field(i).Interface()
			Expect(expectedValue).To(Equal(actualValue), "Mismatch on field %s ", actualFieldName)
		}
	})
	return mismatches
}

func StructMatcherIncluding(expected interface{}, actual interface{}, includeFields []string) []string {
	includeMap := make(map[string]bool)
	for i := 0; i < len(includeFields); i += 1 {
		includeMap[includeFields[i]] = true
	}

	expectedStruct := reflect.Indirect(reflect.ValueOf(expected))
	actualStruct := reflect.Indirect(reflect.ValueOf(actual))

	mismatches := InterceptGomegaFailures(func() {
		for i := 0; i < expectedStruct.NumField(); i++ {
			actualFieldName := actualStruct.Type().Field(i).Name
			if includeMap[actualFieldName] {
				expectedValue := expectedStruct.Field(i).Interface()
				actualValue := actualStruct.Field(i).Interface()
				Expect(expectedValue).To(Equal(actualValue), "Mismatch on field %s ", actualFieldName)
			}
		}
	})
	return mismatches
}

func ExpectStructsToMatch(expected interface{}, actual interface{}) {
	mismatches := StructMatcher(expected, actual)
	if len(mismatches) > 0 {
		Fail(strings.Join(mismatches, "\n"))
	}
}

func ExpectStructsToMatchExcluding(expected interface{}, actual interface{}, excludeFields []string) {
	mismatches := StructMatcherExcluding(expected, actual, excludeFields)
	if len(mismatches) > 0 {
		Fail(strings.Join(mismatches, "\n"))
	}
}

func ExpectStructsToMatchIncluding(expected interface{}, actual interface{}, includeFields []string) {
	mismatches := StructMatcherIncluding(expected, actual, includeFields)
	if len(mismatches) > 0 {
		Fail(strings.Join(mismatches, "\n"))
	}
}
