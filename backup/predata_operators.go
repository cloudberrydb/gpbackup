package backup

/*
 * This file contains structs and functions related to dumping metadata on the
 * master for objects that don't fall under any other predata categorization,
 * such as procedural languages and constraints, that needs to be restored
 * before data is restored.
 */

import (
	"fmt"
	"io"
	"strings"

	"github.com/greenplum-db/gpbackup/utils"
)

func PrintCreateOperatorStatements(predataFile io.Writer, operators []Operator, operatorMetadata MetadataMap) {
	for _, operator := range operators {
		// We do not use MakeFQN here as the operator cannot be quoted
		schema := utils.QuoteIdent(operator.SchemaName)
		operatorFQN := fmt.Sprintf("%s.%s", schema, operator.Name)
		optionalFields := make([]string, 0)
		leftArg := "NONE"
		rightArg := "NONE"
		if operator.LeftArgType != "-" {
			leftArg = operator.LeftArgType
			optionalFields = append(optionalFields, fmt.Sprintf("LEFTARG = %s", leftArg))
		}
		if operator.RightArgType != "-" {
			rightArg = operator.RightArgType
			optionalFields = append(optionalFields, fmt.Sprintf("RIGHTARG = %s", rightArg))
		}
		if operator.CommutatorOp != "0" {
			optionalFields = append(optionalFields, fmt.Sprintf("COMMUTATOR = OPERATOR(%s)", operator.CommutatorOp))
		}
		if operator.NegatorOp != "0" {
			optionalFields = append(optionalFields, fmt.Sprintf("NEGATOR = OPERATOR(%s)", operator.NegatorOp))
		}
		if operator.RestrictFunction != "-" {
			optionalFields = append(optionalFields, fmt.Sprintf("RESTRICT = %s", operator.RestrictFunction))
		}
		if operator.JoinFunction != "-" {
			optionalFields = append(optionalFields, fmt.Sprintf("JOIN = %s", operator.JoinFunction))
		}
		if operator.CanHash {
			optionalFields = append(optionalFields, "HASHES")
		}
		if operator.CanMerge {
			optionalFields = append(optionalFields, "MERGES")
		}
		utils.MustPrintf(predataFile, `

CREATE OPERATOR %s (
	PROCEDURE = %s,
	%s
);`, operatorFQN, operator.ProcedureName, strings.Join(optionalFields, ",\n\t"))
		operatorStr := fmt.Sprintf("%s (%s, %s)", operatorFQN, leftArg, rightArg)
		PrintObjectMetadata(predataFile, operatorMetadata[operator.Oid], operatorStr, "OPERATOR")
	}
}
func PrintCreateOperatorFamilyStatements(predataFile io.Writer, operatorFamilies []OperatorFamily, operatorFamilyMetadata MetadataMap) {
	for _, operatorFamily := range operatorFamilies {
		operatorFamilyFQN := MakeFQN(operatorFamily.SchemaName, operatorFamily.Name)
		operatorFamilyStr := fmt.Sprintf("%s USING %s", operatorFamilyFQN, utils.QuoteIdent(operatorFamily.IndexMethod))
		utils.MustPrintf(predataFile, "\n\nCREATE OPERATOR FAMILY %s;", operatorFamilyStr)
		PrintObjectMetadata(predataFile, operatorFamilyMetadata[operatorFamily.Oid], operatorFamilyStr, "OPERATOR FAMILY")
	}
}

func PrintCreateOperatorClassStatements(predataFile io.Writer, operatorClasses []OperatorClass, operatorClassMetadata MetadataMap) {
	for _, operatorClass := range operatorClasses {
		operatorClassFQN := MakeFQN(operatorClass.ClassSchema, operatorClass.ClassName)
		utils.MustPrintf(predataFile, "\n\nCREATE OPERATOR CLASS %s", operatorClassFQN)
		forTypeStr := ""
		if operatorClass.Default {
			forTypeStr += "DEFAULT "
		}
		forTypeStr += fmt.Sprintf("FOR TYPE %s USING %s", operatorClass.Type, operatorClass.IndexMethod)
		if operatorClass.FamilyName != operatorClass.ClassName {
			operatorFamilyFQN := MakeFQN(operatorClass.FamilySchema, operatorClass.FamilyName)
			forTypeStr += fmt.Sprintf(" FAMILY %s", operatorFamilyFQN)
		}
		utils.MustPrintf(predataFile, "\n\t%s", forTypeStr)
		opClassClauses := []string{}
		if len(operatorClass.Operators) != 0 {
			for _, operator := range operatorClass.Operators {
				opStr := fmt.Sprintf("OPERATOR %d %s", operator.StrategyNumber, operator.Operator)
				if operator.Recheck {
					opStr += " RECHECK"
				}
				opClassClauses = append(opClassClauses, opStr)
			}
		}
		if len(operatorClass.Functions) != 0 {
			for _, function := range operatorClass.Functions {
				opClassClauses = append(opClassClauses, fmt.Sprintf("FUNCTION %d %s", function.SupportNumber, function.FunctionName))
			}
		}
		if operatorClass.StorageType != "-" || len(opClassClauses) == 0 {
			storageType := operatorClass.StorageType
			if operatorClass.StorageType == "-" {
				storageType = operatorClass.Type
			}
			opClassClauses = append(opClassClauses, fmt.Sprintf("STORAGE %s", storageType))
		}
		utils.MustPrintf(predataFile, " AS\n\t%s;", strings.Join(opClassClauses, ",\n\t"))

		operatorClassStr := fmt.Sprintf("%s USING %s", operatorClassFQN, utils.QuoteIdent(operatorClass.IndexMethod))
		PrintObjectMetadata(predataFile, operatorClassMetadata[operatorClass.Oid], operatorClassStr, "OPERATOR CLASS")
	}
}
