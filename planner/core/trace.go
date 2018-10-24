package core

import (
	"github.com/pingcap/tidb/ast"
)

// Trace represents a trace plan.
type Trace struct {
	baseSchemaProducer

	StmtNode ast.StmtNode

	OriginalSql string
}
