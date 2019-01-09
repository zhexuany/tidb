package core

import (
	"bytes"
	"fmt"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/zhexuany/parser/mysql"
)

func convertToArithmicOp(str string) string {
	switch str {
	case "sum":
		return "sum"
	case "avg":
		return "avg"
	case "mul":
		return "*"
	case "plus":
		return "+"
	case "div":
		return "/"
	case "minus":
		return "-"
	case "count":
		return "count"
	case "le":
		return "<"
	case "gt":
		return ">"
	case "eq":
		return "="
	default:
		return ""
	}
	return ""
}

var (
	addingParenthesis = false
)

func aggFuncToPartialSQL(args ...expression.Expression) string {
	var buf bytes.Buffer
	if len(args) == 0 {
		return ""
	}
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch v := arg.(type) {
		case *expression.ScalarFunction:
			children := v.GetArgs()
			childrenLen := len(children)
			switch childrenLen {
			case 3:
				panic("ternary expr is not supported")
			case 2:
				if addingParenthesis {
					buf.WriteString(fmt.Sprintf("(%s%s%s)", aggFuncToPartialSQL(children[0]),
						convertToArithmicOp(v.FuncName.L), aggFuncToPartialSQL(children[1])))
				} else {
					addingParenthesis = true
					buf.WriteString(fmt.Sprintf("%s %s %s", aggFuncToPartialSQL(children[0]),
						convertToArithmicOp(v.FuncName.L), aggFuncToPartialSQL(children[1])))
				}
			case 1:
				buf.WriteString(fmt.Sprintf("%s(%s)", convertToArithmicOp(v.FuncName.L),
					aggFuncToPartialSQL(children[0])))
			}
		case *expression.Column:
			return v.ColName.L
		case *expression.Constant:
			switch v.GetType().Tp {
			// TODO adding more cases
			case mysql.TypeDatetime:
				t := v.Value.GetMysqlTime().Time
				dateTime := fmt.Sprintf("%d-%d-%d", t.Year(), t.Month(), t.Day())
				return fmt.Sprintf("'%s'", dateTime)
			default:
				return fmt.Sprintf("%s", v.String())
			}
		case *expression.CorrelatedColumn:
			return v.OrigColName.L
		}
	}
	addingParenthesis = false
	return buf.String()
}

func projToPartisqlSQL(args []expression.Expression) string {
	var buf bytes.Buffer
	length := len(args)
	for i := 0; i < length; i++ {
		buf.WriteString(aggFuncToPartialSQL(args[i]))
		if i < length-1 {
			buf.WriteString(",")
		}
	}
	return buf.String()
}
func aggFuncsToPartialSQL(aggFuncs []*aggregation.AggFuncDesc) string {
	var buf bytes.Buffer
	length := len(aggFuncs)
	for i := 0; i < length; i++ {
		name := convertToArithmicOp(aggFuncs[i].Name)
		if name == "" {
			buf.WriteString(fmt.Sprintf("%s", aggFuncToPartialSQL(aggFuncs[i].Args...)))
		} else {
			buf.WriteString(fmt.Sprintf("%s(%s)", name, aggFuncToPartialSQL(aggFuncs[i].Args...)))
		}
		if i < length-1 {
			buf.WriteString(", ")
		}
	}

	return buf.String()
}

func groupBysToPartialSQL(groupBys []expression.Expression) string {
	var buf bytes.Buffer
	for i := 0; i < len(groupBys); i++ {
		switch by := groupBys[i].(type) {
		case *expression.Column:
			{
				buf.WriteString(by.ColName.L)
				if i < len(groupBys)-1 {
					buf.WriteString(", ")
				}
			}
		}
	}

	return buf.String()
}

// ToPartialSQL implements PhysicalPlan ToPartialSQL interface.
func (p *basePhysicalPlan) ToPartialSQL() (string, error) {
	return "", errors.Errorf("plan %s fails converts to PB", p.basePlan.ExplainID())
}

// ToPartialSQL implements PhysicalPlan ToPartialSQL interface.
func (p *PhysicalHashAgg) ToPartialSQL() (string, string, error) {
	return groupBysToPartialSQL(p.GroupByItems), aggFuncsToPartialSQL(p.AggFuncs), nil
}

// ToPartialSQL implements PhysicalPlan ToPartialSQL interface.
func (p *PhysicalStreamAgg) ToPartialSQL() (string, string, error) {
	return groupBysToPartialSQL(p.GroupByItems), aggFuncsToPartialSQL(p.AggFuncs), nil
}

// ToPartialSQL implements PhysicalPlan ToPartialSQL interface.
func (p *PhysicalProjection) ToPartialSQL() (string, error) {
	return projToPartisqlSQL(p.Exprs), nil
}

// ToPartialSQL implements PhysicalPlan ToPartialSQL interface.
func (p *PhysicalSelection) ToPartialSQL() (string, error) {
	return aggFuncToPartialSQL(p.Conditions...), nil
}

// ToPartialSQL implements PhysicalPlan ToPartialSQL interface.
func (p *PhysicalTopN) ToPartialSQL() (string, error) {
	panic("not supported")
}

// ToPartialSQL implements PhysicalPlan ToPartialSQL interface.
func (p *PhysicalLimit) ToPartialSQL() (string, error) {
	panic("not supported")
}
func (p *PhysicalTableReader) ToPartialSQL() (string, error) {
	var buf bytes.Buffer
	for i := 0; i < len(p.schema.Columns); i++ {
		buf.WriteString(p.schema.Columns[i].ColName.L)
		if i < len(p.schema.Columns)-1 {
			buf.WriteString(",")
		}
	}
	return buf.String(), nil
}

// ToPartialSQL implements PhysicalPlan ToPartialSQL interface.
func (p *PhysicalTableScan) ToPartialSQL() (string, error) {
	return p.Table.Name.L, nil
}

// ToPartialSQL implements PhysicalPlan ToPartialSQL interface.
func (p *PhysicalIndexScan) ToPartialSQL() (string, error) {
	panic("not supported")
}
