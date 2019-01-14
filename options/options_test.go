package options_test

import (
	"io/ioutil"
	"os"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"

	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/options"
	"github.com/greenplum-db/gpbackup/utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/spf13/pflag"
)

var _ = Describe("options creation", func() {
	var (
		myflags        *pflag.FlagSet
		connectionPool *dbconn.DBConn
	)
	BeforeEach(func() {
		myflags = &pflag.FlagSet{}
		backup.SetFlagDefaults(myflags)

		connectionPool, _, _, _, _ = testhelper.SetupTestEnvironment()
	})

	Describe("included tables", func() {
		It("returns no included tables when none specified", func() {
			subject, err := options.NewOptions(myflags, connectionPool, &SuccessfulValidInDb{})
			Expect(err).To(Not(HaveOccurred()))

			includedTables := subject.GetIncludedTables()
			Expect(includedTables).To(BeEmpty())
		})
		It("returns the include tables when one table in flag", func() {
			err := myflags.Set(utils.INCLUDE_RELATION, "foo.bar")

			if err != nil {
				Fail("cannot set relation")
			}
			subject, err := options.NewOptions(myflags, connectionPool, &SuccessfulValidInDb{})
			Expect(err).To(Not(HaveOccurred()))

			includedTables := subject.GetIncludedTables()
			Expect(len(includedTables)).To(Equal(1))
			Expect(includedTables[0]).To(Equal("foo.bar"))
		})
		It("returns an include with special characters besides quote, dot and comma", func() {
			err := myflags.Set(utils.INCLUDE_RELATION, `foo '~#$%^&*()_-+[]{}><\|;:/?!\t\n.bar`)
			if err != nil {
				Fail("cannot set relation")
			}
			subject, err := options.NewOptions(myflags, connectionPool, &SuccessfulValidInDb{})
			Expect(err).To(Not(HaveOccurred()))

			includedTables := subject.GetIncludedTables()
			Expect(len(includedTables)).To(Equal(1))
			Expect(includedTables[0]).To(Equal(`foo '~#$%^&*()_-+[]{}><\|;:/?!\t\n.bar`))
		})
		// todo handle embedded dots
		PIt("handles dots within schema or tablename", func() {
		})
		// todo handle embedded commas
		PIt("handles commas within schema or tablename", func() {
		})
		// todo handle embedded quotes
		PIt("handles quotes within schema or tablename", func() {
		})

		It("returns all included tables when multiple individual flags provided", func() {
			err := myflags.Set(utils.INCLUDE_RELATION, "foo.bar")
			if err != nil {
				Fail("cannot set relation flag")
			}
			err = myflags.Set(utils.INCLUDE_RELATION, "bar.baz")
			if err != nil {
				Fail("cannot set relation flag")
			}
			subject, err := options.NewOptions(myflags, connectionPool, &SuccessfulValidInDb{})
			Expect(err).To(Not(HaveOccurred()))

			includedTables := subject.GetIncludedTables()
			Expect(len(includedTables)).To(Equal(2))
			Expect(includedTables[0]).To(Equal("foo.bar"))
			Expect(includedTables[1]).To(Equal("bar.baz"))
		})
		It("returns the text-file tables when specified", func() {
			file, err := ioutil.TempFile("/tmp", "gpbackup_test_options*.txt")
			Expect(err).To(Not(HaveOccurred()))
			defer func() {
				_ = os.Remove(file.Name())
			}()
			_, err = file.WriteString("myschema.mytable\n")
			Expect(err).To(Not(HaveOccurred()))
			_, err = file.WriteString("myschema.mytable2\n")
			Expect(err).To(Not(HaveOccurred()))
			err = file.Close()
			Expect(err).To(Not(HaveOccurred()))

			err = myflags.Set(utils.INCLUDE_RELATION_FILE, file.Name())
			if err != nil {
				Fail("cannot set relations file flag")
			}
			subject, err := options.NewOptions(myflags, connectionPool, &SuccessfulValidInDb{})
			Expect(err).To(Not(HaveOccurred()))

			includedTables := subject.GetIncludedTables()
			Expect(len(includedTables)).To(Equal(2))
			Expect(includedTables[0]).To(Equal("myschema.mytable"))
			Expect(includedTables[1]).To(Equal("myschema.mytable2"))
		})
		It("returns an error upon invalid inclusions", func() {
			err := myflags.Set(utils.INCLUDE_RELATION, "foo")
			if err != nil {
				Fail("cannot set relation")
			}
			_, err = options.NewOptions(myflags, connectionPool, &SuccessfulValidInDb{})
			Expect(err).To(HaveOccurred())
		})
	})
})

type SuccessfulValidInDb struct{}

func (d SuccessfulValidInDb) ValidateInDatabase(tableList []string, conn *dbconn.DBConn) {}
