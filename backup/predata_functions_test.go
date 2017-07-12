package backup_test

import (
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
)

var _ = Describe("backup/predata tests", func() {
	buffer := gbytes.NewBuffer()

	BeforeEach(func() {
		buffer = gbytes.BufferWithBytes([]byte(""))
	})
	Describe("Functions involved in printing CREATE FUNCTION statements", func() {
		var funcDef backup.QueryFunctionDefinition
		funcDefs := make([]backup.QueryFunctionDefinition, 1)
		funcDefault := backup.QueryFunctionDefinition{1, "public", "func_name", false, "add_two_ints", "", "integer, integer", "integer, integer", "integer",
			"v", false, false, "", float32(1), float32(0), "", "internal"}
		BeforeEach(func() {
			funcDef = funcDefault
			funcDefs[0] = funcDef
		})

		Describe("PrintCreateFunctionStatements", func() {
			var (
				funcMetadataMap utils.MetadataMap
			)
			BeforeEach(func() {
				funcMetadataMap = utils.MetadataMap{}
			})
			It("prints a function definition for an internal function without a binary path", func() {
				backup.PrintCreateFunctionStatements(buffer, funcDefs, funcMetadataMap)
				testutils.ExpectRegexp(buffer, `CREATE FUNCTION public.func_name(integer, integer) RETURNS integer AS
$$add_two_ints$$
LANGUAGE internal;
`)
			})
			It("prints a function definition for a function that returns a set", func() {
				funcDefs[0].ReturnsSet = true
				funcDefs[0].ResultType = "SETOF integer"
				backup.PrintCreateFunctionStatements(buffer, funcDefs, funcMetadataMap)
				testutils.ExpectRegexp(buffer, `CREATE FUNCTION public.func_name(integer, integer) RETURNS SETOF integer AS
$$add_two_ints$$
LANGUAGE internal;
`)
			})
			It("prints a function definition for a function with permissions, an owner, and a comment", func() {
				funcMetadata := utils.ObjectMetadata{[]utils.ACL{utils.DefaultACLForType("testrole", "FUNCTION")}, "testrole", "This is a function comment."}
				funcMetadataMap[1] = funcMetadata
				backup.PrintCreateFunctionStatements(buffer, funcDefs, funcMetadataMap)
				testutils.ExpectRegexp(buffer, `CREATE FUNCTION public.func_name(integer, integer) RETURNS integer AS
$$add_two_ints$$
LANGUAGE internal;


COMMENT ON FUNCTION public.func_name(integer, integer) IS 'This is a function comment.';


ALTER FUNCTION public.func_name(integer, integer) OWNER TO testrole;


REVOKE ALL ON FUNCTION public.func_name(integer, integer) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.func_name(integer, integer) FROM testrole;
GRANT ALL ON FUNCTION public.func_name(integer, integer) TO testrole;`)
			})
		})
		Describe("PrintFunctionBodyOrPath", func() {
			It("prints a function definition for an internal function with 'NULL' binary path using '-'", func() {
				funcDef.BinaryPath = "-"
				backup.PrintFunctionBodyOrPath(buffer, funcDef)
				testutils.ExpectRegexp(buffer, `
$$add_two_ints$$
`)
			})
			It("prints a function definition for an internal function with a binary path", func() {
				funcDef.BinaryPath = "$libdir/binary"
				backup.PrintFunctionBodyOrPath(buffer, funcDef)
				testutils.ExpectRegexp(buffer, `
'$libdir/binary', 'add_two_ints'
`)
			})
			It("prints a function definition for a function with a one-line function definition", func() {
				funcDef.FunctionBody = "SELECT $1+$2"
				funcDef.Language = "sql"
				backup.PrintFunctionBodyOrPath(buffer, funcDef)
				testutils.ExpectRegexp(buffer, `$_$SELECT $1+$2$_$`)
			})
			It("prints a function definition for a function with a multi-line function definition", func() {
				funcDef.FunctionBody = `
BEGIN
	SELECT $1 + $2
END
`
				funcDef.Language = "sql"
				backup.PrintFunctionBodyOrPath(buffer, funcDef)
				testutils.ExpectRegexp(buffer, `$_$
BEGIN
	SELECT $1 + $2
END
$_$`)
			})
		})
		Describe("PrintFunctionModifiers", func() {
			Context("SqlUsage cases", func() {
				It("prints 'c' as CONTAINS SQL", func() {
					funcDef.DataAccess = "c"
					backup.PrintFunctionModifiers(buffer, funcDef)
					testutils.ExpectRegexp(buffer, "CONTAINS SQL")
				})
				It("prints 'm' as MODIFIES SQL DATA", func() {
					funcDef.DataAccess = "m"
					backup.PrintFunctionModifiers(buffer, funcDef)
					testutils.ExpectRegexp(buffer, "MODIFIES SQL DATA")
				})
				It("prints 'n' as NO SQL", func() {
					funcDef.DataAccess = "n"
					backup.PrintFunctionModifiers(buffer, funcDef)
					testutils.ExpectRegexp(buffer, "NO SQL")
				})
				It("prints 'r' as READS SQL DATA", func() {
					funcDef.DataAccess = "r"
					backup.PrintFunctionModifiers(buffer, funcDef)
					testutils.ExpectRegexp(buffer, "READS SQL DATA")
				})
			})
			Context("Volatility cases", func() {
				It("does not print anything for 'v'", func() {
					funcDef.Volatility = "v"
					backup.PrintFunctionModifiers(buffer, funcDef)
					Expect(buffer.Contents()).To(Equal([]byte{}))
				})
				It("prints 's' as STABLE", func() {
					funcDef.Volatility = "s"
					backup.PrintFunctionModifiers(buffer, funcDef)
					testutils.ExpectRegexp(buffer, "STABLE")
				})
				It("prints 'i' as IMMUTABLE", func() {
					funcDef.Volatility = "i"
					backup.PrintFunctionModifiers(buffer, funcDef)
					testutils.ExpectRegexp(buffer, "IMMUTABLE")
				})
			})
			It("prints 'STRICT' if IsStrict is set", func() {
				funcDef.IsStrict = true
				backup.PrintFunctionModifiers(buffer, funcDef)
				testutils.ExpectRegexp(buffer, "STRICT")
			})
			It("prints 'SECURITY DEFINER' if IsSecurityDefiner is set", func() {
				funcDef.IsSecurityDefiner = true
				backup.PrintFunctionModifiers(buffer, funcDef)
				testutils.ExpectRegexp(buffer, "SECURITY DEFINER")
			})
			Context("Cost cases", func() {
				/*
				 * The default COST values are 1 for C and internal functions and
				 * 100 for any other language, so it should not print COST clauses
				 * for those values but print any other COST.
				 */
				It("prints 'COST 5' if Cost is set to 5", func() {
					funcDef.Cost = 5
					backup.PrintFunctionModifiers(buffer, funcDef)
					testutils.ExpectRegexp(buffer, "COST 5")
				})
				It("prints 'COST 1' if Cost is set to 1 and language is not c or internal", func() {
					funcDef.Cost = 1
					funcDef.Language = "sql"
					backup.PrintFunctionModifiers(buffer, funcDef)
					testutils.ExpectRegexp(buffer, "COST 1")
				})
				It("does not print 'COST 1' if Cost is set to 1 and language is c", func() {
					funcDef.Cost = 1
					funcDef.Language = "c"
					backup.PrintFunctionModifiers(buffer, funcDef)
					Expect(buffer.Contents()).To(Equal([]byte{}))
				})
				It("does not print 'COST 1' if Cost is set to 1 and language is internal", func() {
					funcDef.Cost = 1
					funcDef.Language = "internal"
					backup.PrintFunctionModifiers(buffer, funcDef)
					Expect(buffer.Contents()).To(Equal([]byte{}))
				})
				It("prints 'COST 100' if Cost is set to 100 and language is c", func() {
					funcDef.Cost = 100
					funcDef.Language = "c"
					backup.PrintFunctionModifiers(buffer, funcDef)
					testutils.ExpectRegexp(buffer, "COST 100")
				})
				It("prints 'COST 100' if Cost is set to 100 and language is internal", func() {
					funcDef.Cost = 100
					funcDef.Language = "internal"
					backup.PrintFunctionModifiers(buffer, funcDef)
					testutils.ExpectRegexp(buffer, "COST 100")
				})
				It("does not print 'COST 100' if Cost is set to 100 and language is not c or internal", func() {
					funcDef.Cost = 100
					funcDef.Language = "sql"
					backup.PrintFunctionModifiers(buffer, funcDef)
					Expect(buffer.Contents()).To(Equal([]byte{}))
				})
			})
			Context("NumRows cases", func() {
				/*
				 * A ROWS value of 0 means "no estimate" and 1000 means "too high
				 * to estimate", so those should not be printed but any other ROWS
				 * value should be.
				 */
				It("prints 'ROWS 5' if Rows is set to 5", func() {
					funcDef.NumRows = 5
					funcDef.ReturnsSet = true
					backup.PrintFunctionModifiers(buffer, funcDef)
					testutils.ExpectRegexp(buffer, "ROWS 5")
				})
				It("does not print 'ROWS' if Rows is set but ReturnsSet is false", func() {
					funcDef.NumRows = 100
					funcDef.ReturnsSet = false
					backup.PrintFunctionModifiers(buffer, funcDef)
					Expect(buffer.Contents()).To(Equal([]byte{}))
				})
				It("does not print 'ROWS' if Rows is set to 0", func() {
					funcDef.NumRows = 0
					funcDef.ReturnsSet = true
					backup.PrintFunctionModifiers(buffer, funcDef)
					Expect(buffer.Contents()).To(Equal([]byte{}))
				})
				It("does not print 'ROWS' if Rows is set to 1000", func() {
					funcDef.NumRows = 1000
					funcDef.ReturnsSet = true
					backup.PrintFunctionModifiers(buffer, funcDef)
					Expect(buffer.Contents()).To(Equal([]byte{}))
				})
			})
			It("prints config statements if any are set", func() {
				funcDef.Config = "SET client_min_messages TO error"
				backup.PrintFunctionModifiers(buffer, funcDef)
				testutils.ExpectRegexp(buffer, "SET client_min_messages TO error")
			})
		})
	})
	Describe("PrintCreateAggregateStatements", func() {
		aggDefs := make([]backup.QueryAggregateDefinition, 1)
		aggDefault := backup.QueryAggregateDefinition{1, "public", "agg_name", "integer, integer", "integer, integer", 1, 0, 0, 0, "integer", "", false}
		funcInfoMap := map[uint32]backup.FunctionInfo{
			1: {QualifiedName: "public.mysfunc", Arguments: "integer"},
			2: {QualifiedName: "public.mypfunc", Arguments: "numeric, numeric"},
			3: {QualifiedName: "public.myffunc", Arguments: "text"},
			4: {QualifiedName: "public.mysortop", Arguments: "bigint"},
		}
		aggMetadataMap := utils.MetadataMap{}
		BeforeEach(func() {
			aggDefs[0] = aggDefault
			aggMetadataMap = utils.MetadataMap{}
		})

		It("prints an aggregate definition for an unordered aggregate with no optional specifications", func() {
			backup.PrintCreateAggregateStatements(buffer, aggDefs, funcInfoMap, aggMetadataMap)
			testutils.ExpectRegexp(buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer
);`)
		})
		It("prints an aggregate definition for an ordered aggregate with no optional specifications", func() {
			aggDefs[0].IsOrdered = true
			backup.PrintCreateAggregateStatements(buffer, aggDefs, funcInfoMap, aggMetadataMap)
			testutils.ExpectRegexp(buffer, `CREATE ORDERED AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer
);`)
		})
		It("prints an aggregate definition for an unordered aggregate with no arguments", func() {
			aggDefs[0].Arguments = ""
			aggDefs[0].IdentArgs = ""
			backup.PrintCreateAggregateStatements(buffer, aggDefs, funcInfoMap, aggMetadataMap)
			testutils.ExpectRegexp(buffer, `CREATE AGGREGATE public.agg_name(*) (
	SFUNC = public.mysfunc,
	STYPE = integer
);`)
		})
		It("prints an aggregate with a preliminary function", func() {
			aggDefs[0].PreliminaryFunction = 2
			backup.PrintCreateAggregateStatements(buffer, aggDefs, funcInfoMap, aggMetadataMap)
			testutils.ExpectRegexp(buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer,
	PREFUNC = public.mypfunc
);`)
		})
		It("prints an aggregate with a final function", func() {
			aggDefs[0].FinalFunction = 3
			backup.PrintCreateAggregateStatements(buffer, aggDefs, funcInfoMap, aggMetadataMap)
			testutils.ExpectRegexp(buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer,
	FINALFUNC = public.myffunc
);`)
		})
		It("prints an aggregate with an initial condition", func() {
			aggDefs[0].InitialValue = "0"
			backup.PrintCreateAggregateStatements(buffer, aggDefs, funcInfoMap, aggMetadataMap)
			testutils.ExpectRegexp(buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer,
	INITCOND = '0'
);`)
		})
		It("prints an aggregate with a sort operator", func() {
			aggDefs[0].SortOperator = 4
			backup.PrintCreateAggregateStatements(buffer, aggDefs, funcInfoMap, aggMetadataMap)
			testutils.ExpectRegexp(buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer,
	SORTOP = public.mysortop
);`)
		})
		It("prints an aggregate with multiple specifications", func() {
			aggDefs[0].FinalFunction = 3
			aggDefs[0].SortOperator = 4
			backup.PrintCreateAggregateStatements(buffer, aggDefs, funcInfoMap, aggMetadataMap)
			testutils.ExpectRegexp(buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer,
	FINALFUNC = public.myffunc,
	SORTOP = public.mysortop
);`)
		})
		It("prints an aggregate with owner and comment", func() {
			aggMetadataMap[1] = utils.ObjectMetadata{Privileges: []utils.ACL{}, Owner: "testrole", Comment: "This is an aggregate comment."}
			backup.PrintCreateAggregateStatements(buffer, aggDefs, funcInfoMap, aggMetadataMap)
			testutils.ExpectRegexp(buffer, `CREATE AGGREGATE public.agg_name(integer, integer) (
	SFUNC = public.mysfunc,
	STYPE = integer
);


COMMENT ON AGGREGATE public.agg_name(integer, integer) IS 'This is an aggregate comment.';


ALTER AGGREGATE public.agg_name(integer, integer) OWNER TO testrole;`)
		})
		It("prints an aggregate with owner, comment, and no arguments", func() {
			aggDefs[0].Arguments = ""
			aggDefs[0].IdentArgs = ""
			aggMetadataMap[1] = utils.ObjectMetadata{Privileges: []utils.ACL{}, Owner: "testrole", Comment: "This is an aggregate comment."}
			backup.PrintCreateAggregateStatements(buffer, aggDefs, funcInfoMap, aggMetadataMap)
			testutils.ExpectRegexp(buffer, `CREATE AGGREGATE public.agg_name(*) (
	SFUNC = public.mysfunc,
	STYPE = integer
);


COMMENT ON AGGREGATE public.agg_name(*) IS 'This is an aggregate comment.';


ALTER AGGREGATE public.agg_name(*) OWNER TO testrole;`)
		})
	})
	Describe("PrintCreateCastStatements", func() {
		emptyMetadataMap := utils.MetadataMap{}
		It("prints an explicit cast with a function", func() {
			castDef := backup.QueryCastDefinition{1, "src", "dst", "public", "cast_func", "integer, integer", "e"}
			backup.PrintCreateCastStatements(buffer, []backup.QueryCastDefinition{castDef}, emptyMetadataMap)
			testutils.ExpectRegexp(buffer, `CREATE CAST (src AS dst)
	WITH FUNCTION public.cast_func(integer, integer);`)
		})
		It("prints an implicit cast with a function", func() {
			castDef := backup.QueryCastDefinition{1, "src", "dst", "public", "cast_func", "integer, integer", "i"}
			backup.PrintCreateCastStatements(buffer, []backup.QueryCastDefinition{castDef}, emptyMetadataMap)
			testutils.ExpectRegexp(buffer, `CREATE CAST (src AS dst)
	WITH FUNCTION public.cast_func(integer, integer)
AS IMPLICIT;`)
		})
		It("prints an assignment cast with a function", func() {
			castDef := backup.QueryCastDefinition{1, "src", "dst", "public", "cast_func", "integer, integer", "a"}
			backup.PrintCreateCastStatements(buffer, []backup.QueryCastDefinition{castDef}, emptyMetadataMap)
			testutils.ExpectRegexp(buffer, `CREATE CAST (src AS dst)
	WITH FUNCTION public.cast_func(integer, integer)
AS ASSIGNMENT;`)
		})
		It("prints an explicit cast without a function", func() {
			castDef := backup.QueryCastDefinition{1, "src", "dst", "", "", "", "e"}
			backup.PrintCreateCastStatements(buffer, []backup.QueryCastDefinition{castDef}, emptyMetadataMap)
			testutils.ExpectRegexp(buffer, `CREATE CAST (src AS dst)
	WITHOUT FUNCTION;`)
		})
		It("prints an implicit cast without a function", func() {
			castDef := backup.QueryCastDefinition{1, "src", "dst", "", "", "", "i"}
			backup.PrintCreateCastStatements(buffer, []backup.QueryCastDefinition{castDef}, emptyMetadataMap)
			testutils.ExpectRegexp(buffer, `CREATE CAST (src AS dst)
	WITHOUT FUNCTION
AS IMPLICIT;`)
		})
		It("prints an assignment cast without a function", func() {
			castDef := backup.QueryCastDefinition{1, "src", "dst", "", "", "", "a"}
			backup.PrintCreateCastStatements(buffer, []backup.QueryCastDefinition{castDef}, emptyMetadataMap)
			testutils.ExpectRegexp(buffer, `CREATE CAST (src AS dst)
	WITHOUT FUNCTION
AS ASSIGNMENT;`)
		})
		It("prints a cast with a comment", func() {
			castDef := backup.QueryCastDefinition{1, "src", "dst", "", "", "", "e"}
			castMetadataMap := testutils.DefaultMetadataMap("CAST", false, false, true)
			backup.PrintCreateCastStatements(buffer, []backup.QueryCastDefinition{castDef}, castMetadataMap)
			testutils.ExpectRegexp(buffer, `CREATE CAST (src AS dst)
	WITHOUT FUNCTION;

COMMENT ON CAST (src AS dst) IS 'This is a cast comment.';`)
		})
	})
})
