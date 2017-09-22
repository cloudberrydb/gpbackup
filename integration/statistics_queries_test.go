package integration

import (
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup integration tests", func() {
	tables := []backup.Relation{backup.BasicRelation("public", "foo")}
	var tableOid uint32
	BeforeEach(func() {
		testutils.AssertQueryRuns(connection, "CREATE TABLE foo(i int, j text, k bool)")
		tableOid = testutils.OidFromObjectName(connection, "public", "foo", backup.TYPE_RELATION)
		testutils.AssertQueryRuns(connection, "INSERT INTO foo VALUES (1, 'a', 't')")
		testutils.AssertQueryRuns(connection, "INSERT INTO foo VALUES (2, 'b', 'f')")
		testutils.AssertQueryRuns(connection, "ANALYZE foo")
	})
	AfterEach(func() {
		testutils.AssertQueryRuns(connection, "DROP TABLE foo")
	})
	Describe("GetAttributeStatistics", func() {
		It("returns attribute statistics for a table", func() {
			attStats := backup.GetAttributeStatistics(connection, tables)
			Expect(len(attStats)).To(Equal(1))
			Expect(len(attStats[tableOid])).To(Equal(3))
			tableAttStatsI := attStats[tableOid][0]
			tableAttStatsJ := attStats[tableOid][1]
			tableAttStatsK := attStats[tableOid][2]

			/*
			 * Attribute statistics will vary by GPDB version, but statistics for a
			 * certain table should always be the same in a particular version given
			 * the same schema and data.
			 */
			expectedStats4I := backup.AttributeStatistic{Oid: tableOid, SchemaName: "public", TableName: "foo", AttName: "i",
				TypeName: "int4", Relid: tableOid, AttNumber: 1, Width: 4, Distinct: -1, Kind1: 1, Kind2: 0, Operator1: 96,
				Operator2: 0, Numbers1: []string{"0.5", "0.5"}, Values1: []string{"1", "2"}}
			expectedStats4J := backup.AttributeStatistic{Oid: tableOid, SchemaName: "public", TableName: "foo", AttName: "j",
				TypeName: "text", Relid: tableOid, AttNumber: 2, Width: 2, Distinct: -1, Kind1: 1, Kind2: 0, Operator1: 98,
				Operator2: 0, Numbers1: []string{"0.5", "0.5"}, Values1: []string{"a", "b"}}
			expectedStats4K := backup.AttributeStatistic{Oid: tableOid, SchemaName: "public", TableName: "foo", AttName: "k",
				TypeName: "bool", Relid: tableOid, AttNumber: 3, Width: 1, Distinct: 2, Kind1: 1, Kind2: 0, Operator1: 91,
				Operator2: 0, Numbers1: []string{"0.5", "0.5"}, Values1: []string{"t", "f"}}
			expectedStats5I := backup.AttributeStatistic{Oid: tableOid, SchemaName: "public", TableName: "foo", AttName: "i",
				TypeName: "int4", Relid: tableOid, AttNumber: 1, Width: 4, Distinct: -1, Kind1: 2, Kind2: 3, Operator1: 97,
				Operator2: 97, Numbers2: []string{"1"}, Values1: []string{"1", "2"}}
			expectedStats5J := backup.AttributeStatistic{Oid: tableOid, SchemaName: "public", TableName: "foo", AttName: "j",
				TypeName: "text", Relid: tableOid, AttNumber: 2, Width: 2, Distinct: -1, Kind1: 2, Kind2: 3, Operator1: 664,
				Operator2: 664, Numbers2: []string{"1"}, Values1: []string{"a", "b"}}
			expectedStats5K := backup.AttributeStatistic{Oid: tableOid, SchemaName: "public", TableName: "foo", AttName: "k",
				TypeName: "bool", Relid: tableOid, AttNumber: 3, Width: 1, Distinct: -1, Kind1: 2, Kind2: 3, Operator1: 58,
				Operator2: 58, Numbers2: []string{"-1"}, Values1: []string{"f", "t"}}

			if connection.Version.Before("5") {
				testutils.ExpectStructsToMatch(&expectedStats4I, &tableAttStatsI)
				testutils.ExpectStructsToMatch(&expectedStats4J, &tableAttStatsJ)
				testutils.ExpectStructsToMatch(&expectedStats4K, &tableAttStatsK)
			} else {
				testutils.ExpectStructsToMatch(&expectedStats5I, &tableAttStatsI)
				testutils.ExpectStructsToMatch(&expectedStats5J, &tableAttStatsJ)
				testutils.ExpectStructsToMatch(&expectedStats5K, &tableAttStatsK)
			}
		})
	})
	Describe("GetTupleStatistics", func() {
		It("returns tuple statistics for a table", func() {
			tupleStats := backup.GetTupleStatistics(connection, tables)
			Expect(len(tupleStats)).To(Equal(1))
			tableTupleStats := tupleStats[tableOid]

			// Tuple statistics will not vary by GPDB version.
			expectedStats := backup.TupleStatistic{Oid: tableOid, SchemaName: "public", TableName: "foo", RelPages: 2, RelTuples: 2}

			testutils.ExpectStructsToMatch(&expectedStats, &tableTupleStats)
		})
	})
})
