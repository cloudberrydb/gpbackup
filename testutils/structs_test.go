package testutils

import (
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/utils"

	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestTestUtils(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "testutils tests")
}

var _ = Describe("StructMatchers", func() {
	Describe("StructMatcher", func() {
		It("returns no failures for the same structs", func() {
			schema1 := utils.Schema{0, "test_schema", "", "testrole"}
			schema2 := utils.Schema{0, "test_schema", "", "testrole"}
			mismatches := StructMatcher(&schema1, &schema2, false, false)
			Expect(mismatches).To(BeEmpty())
		})
		It("returns mismatches with different structs", func() {
			schema1 := utils.Schema{0, "test_schema", "", "testrole"}
			schema2 := utils.Schema{0, "", "", "testrole"}
			mismatches := StructMatcher(&schema1, &schema2, false, false)
			Expect(mismatches).ToNot(BeEmpty())
		})
		It("returns mismatches in nested structs", func() {
			role1 := backup.QueryRole{Oid: 0, Name: "test_role", TimeConstraints: []backup.TimeConstraint{{StartDay: 3}}}
			role2 := backup.QueryRole{Oid: 0, Name: "test_role", TimeConstraints: []backup.TimeConstraint{{StartDay: 4}}}
			mismatches := StructMatcher(&role1, &role2, false, false)
			Expect(len(mismatches)).To(Equal(1))
			Expect(mismatches[0]).To(Equal("Mismatch on field StartDay\nExpected\n    <int>: 3\nto equal\n    <int>: 4"))
		})
		It("returns mismatches including struct fields", func() {
			role1 := backup.QueryRole{Oid: 0, Name: "test_role", TimeConstraints: []backup.TimeConstraint{{StartDay: 3}}}
			role2 := backup.QueryRole{Oid: 0, Name: "test_role2", TimeConstraints: []backup.TimeConstraint{{StartDay: 4}}}
			mismatches := StructMatcher(&role1, &role2, true, true, "Name")
			Expect(len(mismatches)).To(Equal(1))
			Expect(mismatches[0]).To(Equal("Mismatch on field Name\nExpected\n    <string>: test_role\nto equal\n    <string>: test_role2"))
		})
		It("returns mismatches including nested struct fields", func() {
			role1 := backup.QueryRole{Oid: 0, Name: "test_role", TimeConstraints: []backup.TimeConstraint{{StartDay: 3}}}
			role2 := backup.QueryRole{Oid: 0, Name: "test_role2", TimeConstraints: []backup.TimeConstraint{{StartDay: 4}}}
			mismatches := StructMatcher(&role1, &role2, true, true, "TimeConstraints.StartDay")
			Expect(len(mismatches)).To(Equal(1))
			Expect(mismatches[0]).To(Equal("Mismatch on field StartDay\nExpected\n    <int>: 3\nto equal\n    <int>: 4"))
		})
		It("returns mismatches excluding struct fields", func() {
			role1 := backup.QueryRole{Oid: 0, Name: "test_role", TimeConstraints: []backup.TimeConstraint{{StartDay: 3}}}
			role2 := backup.QueryRole{Oid: 0, Name: "test_role2", TimeConstraints: []backup.TimeConstraint{{StartDay: 4}}}
			mismatches := StructMatcher(&role1, &role2, true, false, "Name")
			Expect(len(mismatches)).To(Equal(1))
			Expect(mismatches[0]).To(Equal("Mismatch on field StartDay\nExpected\n    <int>: 3\nto equal\n    <int>: 4"))
		})
		It("returns mismatches excluding nested struct fields", func() {
			role1 := backup.QueryRole{Oid: 0, Name: "test_role", TimeConstraints: []backup.TimeConstraint{{StartDay: 3}}}
			role2 := backup.QueryRole{Oid: 0, Name: "test_role2", TimeConstraints: []backup.TimeConstraint{{StartDay: 4}}}
			mismatches := StructMatcher(&role1, &role2, true, false, "TimeConstraints.StartDay")
			Expect(len(mismatches)).To(Equal(1))
			Expect(mismatches[0]).To(Equal("Mismatch on field Name\nExpected\n    <string>: test_role\nto equal\n    <string>: test_role2"))
		})
		It("formats a nice error message for mismatches", func() {
			schema1 := utils.Schema{0, "test_schema", "", "testrole"}
			schema2 := utils.Schema{0, "another_schema", "", "testrole"}
			mismatches := StructMatcher(&schema1, &schema2, false, false)
			Expect(mismatches).To(Equal([]string{"Mismatch on field SchemaName\nExpected\n    <string>: test_schema\nto equal\n    <string>: another_schema"}))
		})
	})
})
