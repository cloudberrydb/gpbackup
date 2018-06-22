package integration

import (
	"fmt"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup integration tests", func() {
	var aoTableFQN = "public.ao_foo"
	var aoCOTableFQN = "public.ao_co_foo"
	var aoPartParentTableFQN = "public.ao_part"
	var aoPartChildTableFQN = "public.ao_part_1_prt_child"
	BeforeEach(func() {
		testhelper.AssertQueryRuns(connection,
			fmt.Sprintf("CREATE TABLE %s (i int) WITH (appendonly=true)", aoTableFQN))
		testhelper.AssertQueryRuns(connection,
			fmt.Sprintf("CREATE TABLE %s (i int) WITH (appendonly=true,orientation='column')", aoCOTableFQN))
		testhelper.AssertQueryRuns(connection,
			fmt.Sprintf(`CREATE TABLE %s (id int, letter char(1)) WITH (appendonly=true)
	DISTRIBUTED BY (id)
	PARTITION BY LIST (letter)
	(
		PARTITION child VALUES ('C')
	);`, aoPartParentTableFQN))
	})
	AfterEach(func() {
		testhelper.AssertQueryRuns(connection, fmt.Sprintf("DROP TABLE %s", aoTableFQN))
		testhelper.AssertQueryRuns(connection, fmt.Sprintf("DROP TABLE %s", aoCOTableFQN))
		testhelper.AssertQueryRuns(connection, fmt.Sprintf("DROP TABLE %s", aoPartParentTableFQN))
	})
	Describe("GetAOIncrementalMetadata", func() {
		Context("AO, AO_CO and AO partition tables are only just created", func() {
			var aoIncrementalMetadata map[string]utils.AOEntry
			BeforeEach(func() {
				aoIncrementalMetadata = backup.GetAOIncrementalMetadata(connection)
			})
			It("should have a modcount of 0", func() {
				Expect(aoIncrementalMetadata[aoTableFQN].Modcount).To(Equal(int64(0)))
				Expect(aoIncrementalMetadata[aoCOTableFQN].Modcount).To(Equal(int64(0)))
				Expect(aoIncrementalMetadata[aoPartParentTableFQN].Modcount).To(Equal(int64(0)))
				Expect(aoIncrementalMetadata[aoPartChildTableFQN].Modcount).To(Equal(int64(0)))
			})
			It("should have a last DDL timestamp", func() {
				Expect(aoIncrementalMetadata[aoTableFQN].LastDDLTimestamp).To(Not(BeEmpty()))
				Expect(aoIncrementalMetadata[aoCOTableFQN].LastDDLTimestamp).To(Not(BeEmpty()))
				Expect(aoIncrementalMetadata[aoPartParentTableFQN].LastDDLTimestamp).To(Not(BeEmpty()))
				Expect(aoIncrementalMetadata[aoPartChildTableFQN].LastDDLTimestamp).To(Not(BeEmpty()))
			})
		})
		Context("AO, AO_CO and AO partition tables have DML changes", func() {
			Context("After an insert(s)", func() {
				var initialAOIncrementalMetadata map[string]utils.AOEntry
				var aoIncrementalMetadata map[string]utils.AOEntry
				BeforeEach(func() {
					initialAOIncrementalMetadata = backup.GetAOIncrementalMetadata(connection)

					testhelper.AssertQueryRuns(connection, fmt.Sprintf("INSERT INTO %s values(10)", aoTableFQN))
					testhelper.AssertQueryRuns(connection, fmt.Sprintf("INSERT INTO %s values(10)", aoCOTableFQN))
					testhelper.AssertQueryRuns(connection, fmt.Sprintf("INSERT INTO %s values(10, 'C')", aoPartParentTableFQN))

					aoIncrementalMetadata = backup.GetAOIncrementalMetadata(connection)
				})
				AfterEach(func() {
					testhelper.AssertQueryRuns(connection, fmt.Sprintf("DELETE FROM %s", aoTableFQN))
					testhelper.AssertQueryRuns(connection, fmt.Sprintf("DELETE FROM %s", aoCOTableFQN))
					testhelper.AssertQueryRuns(connection, fmt.Sprintf("DELETE FROM %s", aoPartParentTableFQN))
				})
				It("should increase modcount for non partition tables", func() {
					Expect(aoIncrementalMetadata[aoTableFQN].Modcount).
						To(BeNumerically(">", initialAOIncrementalMetadata[aoTableFQN].Modcount))
					Expect(aoIncrementalMetadata[aoCOTableFQN].Modcount).
						To(BeNumerically(">", initialAOIncrementalMetadata[aoCOTableFQN].Modcount))
				})
				It("should NOT increase modcount for parent partition tables", func() {
					Expect(aoIncrementalMetadata[aoPartParentTableFQN].Modcount).
						To(Equal(initialAOIncrementalMetadata[aoPartParentTableFQN].Modcount))
				})
				It("should increase modcount for modified child partition tables", func() {
					Expect(aoIncrementalMetadata[aoPartChildTableFQN].Modcount).
						To(BeNumerically(">", initialAOIncrementalMetadata[aoPartChildTableFQN].Modcount))
				})
				It("should have a last DDL timestamp", func() {
					Expect(aoIncrementalMetadata[aoTableFQN].LastDDLTimestamp).To(Not(BeEmpty()))
					Expect(aoIncrementalMetadata[aoCOTableFQN].LastDDLTimestamp).To(Not(BeEmpty()))
					Expect(aoIncrementalMetadata[aoPartParentTableFQN].LastDDLTimestamp).To(Not(BeEmpty()))
					Expect(aoIncrementalMetadata[aoPartChildTableFQN].LastDDLTimestamp).To(Not(BeEmpty()))
				})
			})
			Context("After a delete operation", func() {
				var initialAOIncrementalMetadata map[string]utils.AOEntry
				var aoIncrementalMetadata map[string]utils.AOEntry
				BeforeEach(func() {
					testhelper.AssertQueryRuns(connection, fmt.Sprintf("INSERT INTO %s values(10)", aoTableFQN))
					testhelper.AssertQueryRuns(connection, fmt.Sprintf("INSERT INTO %s values(10)", aoCOTableFQN))
					testhelper.AssertQueryRuns(connection, fmt.Sprintf("INSERT INTO %s values(10, 'C')", aoPartParentTableFQN))

					initialAOIncrementalMetadata = backup.GetAOIncrementalMetadata(connection)

					testhelper.AssertQueryRuns(connection, fmt.Sprintf("DELETE FROM %s", aoTableFQN))
					testhelper.AssertQueryRuns(connection, fmt.Sprintf("DELETE FROM %s", aoCOTableFQN))
					testhelper.AssertQueryRuns(connection, fmt.Sprintf("DELETE FROM %s", aoPartParentTableFQN))

					aoIncrementalMetadata = backup.GetAOIncrementalMetadata(connection)
				})
				It("should increase modcount for non partition tables", func() {
					Expect(aoIncrementalMetadata[aoTableFQN].Modcount).
						To(BeNumerically(">", initialAOIncrementalMetadata[aoTableFQN].Modcount))
					Expect(aoIncrementalMetadata[aoCOTableFQN].Modcount).
						To(BeNumerically(">", initialAOIncrementalMetadata[aoCOTableFQN].Modcount))
				})
				It("should NOT increase modcount for parent partition tables", func() {
					Expect(aoIncrementalMetadata[aoPartParentTableFQN].Modcount).
						To(Equal(initialAOIncrementalMetadata[aoPartParentTableFQN].Modcount))
				})
				It("should increase modcount for modified child partition tables", func() {
					Expect(aoIncrementalMetadata[aoPartChildTableFQN].Modcount).
						To(BeNumerically(">", initialAOIncrementalMetadata[aoPartChildTableFQN].Modcount))
				})
				It("should have a last DDL timestamp", func() {
					Expect(aoIncrementalMetadata[aoTableFQN].LastDDLTimestamp).To(Not(BeEmpty()))
					Expect(aoIncrementalMetadata[aoCOTableFQN].LastDDLTimestamp).To(Not(BeEmpty()))
					Expect(aoIncrementalMetadata[aoPartParentTableFQN].LastDDLTimestamp).To(Not(BeEmpty()))
					Expect(aoIncrementalMetadata[aoPartChildTableFQN].LastDDLTimestamp).To(Not(BeEmpty()))
				})
			})
		})
		Context("AO, AO_CO and AO partition tables have DDL changes", func() {
			var initialAOIncrementalMetadata map[string]utils.AOEntry
			var aoIncrementalMetadata map[string]utils.AOEntry
			Context("After a column add", func() {
				BeforeEach(func() {
					initialAOIncrementalMetadata = backup.GetAOIncrementalMetadata(connection)

					testhelper.AssertQueryRuns(connection, fmt.Sprintf("ALTER TABLE %s ADD COLUMN k int", aoTableFQN))
					testhelper.AssertQueryRuns(connection, fmt.Sprintf("ALTER TABLE %s ADD COLUMN k int", aoCOTableFQN))
					testhelper.AssertQueryRuns(connection, fmt.Sprintf("ALTER TABLE %s ADD COLUMN k int", aoPartParentTableFQN))

					aoIncrementalMetadata = backup.GetAOIncrementalMetadata(connection)
				})
				It("should NOT care about modcount", func() {})
				It("should have a changed last DDL timestamp", func() {
					Expect(aoIncrementalMetadata[aoTableFQN].LastDDLTimestamp).
						To(Not(Equal(initialAOIncrementalMetadata[aoTableFQN].LastDDLTimestamp)))
					Expect(aoIncrementalMetadata[aoCOTableFQN].LastDDLTimestamp).
						To(Not(Equal(initialAOIncrementalMetadata[aoCOTableFQN].LastDDLTimestamp)))
					Expect(aoIncrementalMetadata[aoPartParentTableFQN].LastDDLTimestamp).
						To(Not(Equal(initialAOIncrementalMetadata[aoCOTableFQN].LastDDLTimestamp)))
					Expect(aoIncrementalMetadata[aoPartChildTableFQN].LastDDLTimestamp).
						To(Not(Equal(initialAOIncrementalMetadata[aoCOTableFQN].LastDDLTimestamp)))
				})
			})
			Context("After a column drop", func() {
				BeforeEach(func() {
					testhelper.AssertQueryRuns(connection, fmt.Sprintf("ALTER TABLE %s ADD COLUMN k int", aoTableFQN))
					testhelper.AssertQueryRuns(connection, fmt.Sprintf("ALTER TABLE %s ADD COLUMN k int", aoCOTableFQN))
					testhelper.AssertQueryRuns(connection, fmt.Sprintf("ALTER TABLE %s ADD COLUMN k int", aoPartParentTableFQN))
					initialAOIncrementalMetadata = backup.GetAOIncrementalMetadata(connection)

					testhelper.AssertQueryRuns(connection, fmt.Sprintf("ALTER TABLE %s DROP COLUMN k", aoTableFQN))
					testhelper.AssertQueryRuns(connection, fmt.Sprintf("ALTER TABLE %s DROP COLUMN k", aoCOTableFQN))
					testhelper.AssertQueryRuns(connection, fmt.Sprintf("ALTER TABLE %s DROP COLUMN k", aoPartParentTableFQN))

					aoIncrementalMetadata = backup.GetAOIncrementalMetadata(connection)
				})
				It("should NOT care about modcount", func() {})
				It("should have a changed last DDL timestamp", func() {
					Expect(aoIncrementalMetadata[aoTableFQN].LastDDLTimestamp).
						To(Not(Equal(initialAOIncrementalMetadata[aoTableFQN].LastDDLTimestamp)))
					Expect(aoIncrementalMetadata[aoCOTableFQN].LastDDLTimestamp).
						To(Not(Equal(initialAOIncrementalMetadata[aoCOTableFQN].LastDDLTimestamp)))
					Expect(aoIncrementalMetadata[aoPartParentTableFQN].LastDDLTimestamp).
						To(Not(Equal(initialAOIncrementalMetadata[aoCOTableFQN].LastDDLTimestamp)))
					Expect(aoIncrementalMetadata[aoPartChildTableFQN].LastDDLTimestamp).
						To(Not(Equal(initialAOIncrementalMetadata[aoCOTableFQN].LastDDLTimestamp)))
				})
			})
			Context("After truncating a child partition", func() {
				BeforeEach(func() {
					initialAOIncrementalMetadata = backup.GetAOIncrementalMetadata(connection)

					testhelper.AssertQueryRuns(connection, fmt.Sprintf("TRUNCATE TABLE %s", aoPartChildTableFQN))

					aoIncrementalMetadata = backup.GetAOIncrementalMetadata(connection)
				})
				It("should NOT care about modcount", func() {})
				It("should have a changed last DDL timestamp for the child", func() {
					Expect(aoIncrementalMetadata[aoPartChildTableFQN].LastDDLTimestamp).
						To(Not(Equal(initialAOIncrementalMetadata[aoPartChildTableFQN].LastDDLTimestamp)))
				})
				It("should NOT have a changed last DDL timestamp for the parent", func() {
					Expect(aoIncrementalMetadata[aoPartParentTableFQN].LastDDLTimestamp).
						To(Equal(initialAOIncrementalMetadata[aoPartParentTableFQN].LastDDLTimestamp))
				})
			})
		})
		Context("AO, AO_CO and AO partition tables have DML and DDL changes", func() {
			var initialAOIncrementalMetadata map[string]utils.AOEntry
			var aoIncrementalMetadata map[string]utils.AOEntry
			Context("After an insert followed by an ALTER table", func() {
				BeforeEach(func() {
					initialAOIncrementalMetadata = backup.GetAOIncrementalMetadata(connection)

					testhelper.AssertQueryRuns(connection, fmt.Sprintf("INSERT INTO %s values(10)", aoTableFQN))
					testhelper.AssertQueryRuns(connection, fmt.Sprintf("ALTER TABLE %s ADD COLUMN k int", aoTableFQN))
					testhelper.AssertQueryRuns(connection, fmt.Sprintf("INSERT INTO %s values(10)", aoCOTableFQN))
					testhelper.AssertQueryRuns(connection, fmt.Sprintf("ALTER TABLE %s ADD COLUMN k int", aoCOTableFQN))
					testhelper.AssertQueryRuns(connection, fmt.Sprintf("INSERT INTO %s values(10, 'C')", aoPartParentTableFQN))
					testhelper.AssertQueryRuns(connection, fmt.Sprintf("ALTER TABLE %s ADD COLUMN k int", aoPartParentTableFQN))

					aoIncrementalMetadata = backup.GetAOIncrementalMetadata(connection)
				})
				It("should NOT care about modcount", func() {
					//We don't care about modcount because DDL operations can reset the modcount value
				})
				It("should have a changed last DDL timestamp", func() {
					Expect(aoIncrementalMetadata[aoTableFQN].LastDDLTimestamp).
						To(Not(Equal(initialAOIncrementalMetadata[aoTableFQN].LastDDLTimestamp)))
					Expect(aoIncrementalMetadata[aoCOTableFQN].LastDDLTimestamp).
						To(Not(Equal(initialAOIncrementalMetadata[aoCOTableFQN].LastDDLTimestamp)))
					Expect(aoIncrementalMetadata[aoPartParentTableFQN].LastDDLTimestamp).
						To(Not(Equal(initialAOIncrementalMetadata[aoCOTableFQN].LastDDLTimestamp)))
					Expect(aoIncrementalMetadata[aoPartChildTableFQN].LastDDLTimestamp).
						To(Not(Equal(initialAOIncrementalMetadata[aoCOTableFQN].LastDDLTimestamp)))
				})
			})
		})
	})
})
