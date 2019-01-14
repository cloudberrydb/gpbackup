package options_test

import (
	. "github.com/onsi/ginkgo"
)

var _ = Describe("options db validator", func() {
	var (
	//connectionPool *dbconn.DBConn
	)
	BeforeEach(func() {
		//connectionPool, _, _, _, _ = testhelper.SetupTestEnvironment()
	})

	Describe("validate in database", func() {

		//It("succeeds when table fqn is found in database", func() {
		//	//mock.ExpectExec(fmt.Sprintf(options.QUERY_TEMPLATE, "foo.bar")).WillReturnResult(sqlmock.NewResult(1, 1))
		//	//mock.ExpectExec(fmt.Sprintf(options.PARTITION_TABLE_MAP_QUERY, "foo.bar")).WillReturnResult(sqlmock.NewResult(1, 1))
		//	err := myflags.Set(utils.INCLUDE_RELATION, "foo.bar")
		//
		//	if err != nil {
		//		Fail("cannot set relation")
		//	}
		//	subject, err := options.NewOptions(myflags, connectionPool, &SuccessfulValidInDb{})
		//	Expect(err).To(Not(HaveOccurred()))
		//
		//	includedTables := subject.GetIncludedTables()
		//	Expect(len(includedTables)).To(Equal(1))
		//	Expect(includedTables[0]).To(Equal("foo.bar"))
		//})
	})
})
