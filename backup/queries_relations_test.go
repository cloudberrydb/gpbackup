package backup_test

import (
	"github.com/greenplum-db/gpbackup/backup"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Relation object manipulation", func() {

	Describe("Unquoted", func() {
		It("returns unquoted input without changing", func() {
			inRel := backup.Relation{
				SchemaOid: 0,
				Oid:       0,
				Schema:    `foo`,
				Name:      `bar`,
			}
			outRel := inRel.Unquoted()
			Expect(outRel.Schema).To(Equal(`foo`))
			Expect(outRel.Name).To(Equal(`bar`))
		})
		It("removes quotes from Schema attr", func() {
			inRel := backup.Relation{
				SchemaOid: 0,
				Oid:       0,
				Schema:    `"FOO"`,
				Name:      `bar`,
			}
			outRel := inRel.Unquoted()
			Expect(outRel.Schema).To(Equal(`FOO`))
			Expect(outRel.Name).To(Equal(`bar`))
		})
		It("removes quotes from Name attr", func() {
			inRel := backup.Relation{
				SchemaOid: 0,
				Oid:       0,
				Schema:    `foo`,
				Name:      `"BAR"`,
			}
			outRel := inRel.Unquoted()
			Expect(outRel.Schema).To(Equal(`foo`))
			Expect(outRel.Name).To(Equal(`BAR`))
		})
	})
})
