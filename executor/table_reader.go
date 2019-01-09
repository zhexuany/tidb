// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/pingcap/tidb/types"
	"github.com/zhexuany/parser/mysql"
	"strings"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/distsql"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tipb/go-tipb"
)

// make sure `TableReaderExecutor` implements `Executor`.
var _ Executor = &TableReaderExecutor{}

// TableReaderExecutor sends DAG request and reads table data from kv layer.
type TableReaderExecutor struct {
	baseExecutor

	table           table.Table
	physicalTableID int64
	keepOrder       bool
	desc            bool
	ranges          []*ranger.Range
	dagPB           *tipb.DAGRequest
	// columns are only required by union scan.
	columns []*model.ColumnInfo

	// resultHandler handles the order of the result. Since (MAXInt64, MAXUint64] stores before [0, MaxInt64] physically
	// for unsigned int.
	resultHandler *tableResultHandler
	streaming     bool
	feedback      *statistics.QueryFeedback

	// corColInFilter tells whether there's correlated column in filter.
	corColInFilter bool
	// corColInAccess tells whether there's correlated column in access conditions.
	corColInAccess bool
	useTiFlash     bool

	tiFlishConn *sql.DB
	flashSQL    string
	rows        *sql.Rows
	dbName      string
	plans       []plannercore.PhysicalPlan
}

// Open initialzes necessary variables for using this executor.
func (e *TableReaderExecutor) Open(ctx context.Context) error {
	if e.useTiFlash {
		var err error
		connectStr := fmt.Sprintf("tcp://127.0.0.1:9000?debug=true&database=%s", "default")
		//connectStr := fmt.Sprintf("tcp://127.0.0.1:9000?debug=true&database=%s", e.dbName)
		e.tiFlishConn, err = sql.Open("clickhouse",
			connectStr)
		if err != nil {
			return errors.Trace(err)
		}

		e.rows, err = e.tiFlishConn.Query(e.flashSQL)
		if err != nil {
			return errors.Trace(err)
		}
		return nil
	}

	var err error
	if e.corColInFilter {
		e.dagPB.Executors, _, err = constructDistExec(e.ctx, e.plans)
		if err != nil {
			return errors.Trace(err)
		}
	}
	if e.corColInAccess {
		ts := e.plans[0].(*plannercore.PhysicalTableScan)
		access := ts.AccessCondition
		pkTP := ts.Table.GetPkColInfo().FieldType
		e.ranges, err = ranger.BuildTableRange(access, e.ctx.GetSessionVars().StmtCtx, &pkTP)
		if err != nil {
			return errors.Trace(err)
		}
	}

	e.resultHandler = &tableResultHandler{}
	firstPartRanges, secondPartRanges := splitRanges(e.ranges, e.keepOrder)
	firstResult, err := e.buildResp(ctx, firstPartRanges)
	if err != nil {
		e.feedback.Invalidate()
		return errors.Trace(err)
	}
	if len(secondPartRanges) == 0 {
		e.resultHandler.open(nil, firstResult)
		return nil
	}
	var secondResult distsql.SelectResult
	secondResult, err = e.buildResp(ctx, secondPartRanges)
	if err != nil {
		e.feedback.Invalidate()
		return errors.Trace(err)
	}
	e.resultHandler.open(firstResult, secondResult)
	return nil
}

func convertValue(name string) interface{} {
	switch name {
	case "Float64":
		v := float64(0)
		return &v
	case "Int32", "UInt64":
		v := uint64(0)
		return &v
	case "Date":
		t := time.Now()
		return &t
	case "String":
		s := ""
		return &s
	}

	if strings.Contains(name, "FixedString") {
		s := ""
		return &s
	}
	return nil
}

func toTiDBTp(name string) byte {
	switch name {
	case "Int32":
		return mysql.TypeLonglong
	case "Float64":
		return mysql.TypeDouble
	case "UInt64":
		return mysql.TypeLonglong
	case "Date":
		return mysql.TypeDate
	case "DateTime":
		return mysql.TypeDatetime
	case "String":
		return mysql.TypeString
	}

	if strings.Contains(name, "FixedString") {
		return mysql.TypeString
	}
	return mysql.TypeNull
}

// Next fills data into the chunk passed by its caller.
// The task was actually done by tableReaderHandler.
func (e *TableReaderExecutor) Next(ctx context.Context, chk *chunk.Chunk) error {
	if e.useTiFlash {
		defer e.rows.Close()
		chk.Reset()

		colTps, _ := e.rows.ColumnTypes()
		results := make([]interface{}, len(colTps))
		for e.rows.Next() {
			for i := 0; i < len(colTps); i++ {
				results[i] = convertValue(colTps[i].DatabaseTypeName())
			}
			if err := e.rows.Scan(results...); err != nil {
				return err
			}

			for i := 0; i < len(results); i++ {
				switch toTiDBTp(colTps[i].DatabaseTypeName()) {
				case mysql.TypeString:
					chk.AppendString(i, *results[i].(*string))
				case mysql.TypeDecimal:
					myDec := &types.MyDecimal{}
					myDec.FromFloat64(*results[i].(*float64))
					chk.AppendMyDecimal(i, myDec)
				case mysql.TypeDouble:
					chk.AppendFloat64(i, *results[i].(*float64))
				case mysql.TypeLonglong, mysql.TypeShort, mysql.TypeLong:
					chk.AppendUint64(i, *results[i].(*uint64))
				case mysql.TypeDate:
					goTime := *results[i].(*time.Time)
					t := types.Time{Time: types.FromGoTime(goTime)}
					chk.AppendTime(i, t)
				default:
					panic(fmt.Sprintf("%s is not supported", colTps[i].DatabaseTypeName()))
				}
			}
		}

		return nil
	}

	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("tableReader.Next", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
	}
	if e.runtimeStats != nil {
		start := time.Now()
		defer func() { e.runtimeStats.Record(time.Now().Sub(start), chk.NumRows()) }()
	}
	if err := e.resultHandler.nextChunk(ctx, chk); err != nil {
		e.feedback.Invalidate()
		return err
	}
	return errors.Trace(nil)
}

// Close implements the Executor Close interface.
func (e *TableReaderExecutor) Close() error {
	if e.useTiFlash {
		return nil
	}
	err := e.resultHandler.Close()
	if e.runtimeStats != nil {
		copStats := e.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.Get(e.plans[0].ExplainID())
		copStats.SetRowNum(e.feedback.Actual())
	}
	e.ctx.StoreQueryFeedback(e.feedback)
	return errors.Trace(err)
}

// buildResp first builds request and sends it to tikv using distsql.Select. It uses SelectResut returned by the callee
// to fetch all results.
func (e *TableReaderExecutor) buildResp(ctx context.Context, ranges []*ranger.Range) (distsql.SelectResult, error) {
	var builder distsql.RequestBuilder
	kvReq, err := builder.SetTableRanges(e.physicalTableID, ranges, e.feedback).
		SetDAGRequest(e.dagPB).
		SetDesc(e.desc).
		SetKeepOrder(e.keepOrder).
		SetStreaming(e.streaming).
		SetFromSessionVars(e.ctx.GetSessionVars()).
		Build()
	if err != nil {
		return nil, errors.Trace(err)
	}
	result, err := distsql.Select(ctx, e.ctx, kvReq, e.retTypes(), e.feedback)
	if err != nil {
		return nil, errors.Trace(err)
	}
	result.Fetch(ctx)
	return result, nil
}

type tableResultHandler struct {
	// If the pk is unsigned and we have KeepOrder=true.
	// optionalResult handles the request whose range is in signed int range.
	// result handles the request whose range is exceed signed int range.
	// Otherwise, we just set optionalFinished true and the result handles the whole ranges.
	optionalResult distsql.SelectResult
	result         distsql.SelectResult

	optionalFinished bool
}

func (tr *tableResultHandler) open(optionalResult, result distsql.SelectResult) {
	if optionalResult == nil {
		tr.optionalFinished = true
		tr.result = result
		return
	}
	tr.optionalResult = optionalResult
	tr.result = result
	tr.optionalFinished = false
}

func (tr *tableResultHandler) nextChunk(ctx context.Context, chk *chunk.Chunk) error {
	if !tr.optionalFinished {
		err := tr.optionalResult.Next(ctx, chk)
		if err != nil {
			return errors.Trace(err)
		}
		if chk.NumRows() > 0 {
			return nil
		}
		tr.optionalFinished = true
	}
	return tr.result.Next(ctx, chk)
}

func (tr *tableResultHandler) nextRaw(ctx context.Context) (data []byte, err error) {
	if !tr.optionalFinished {
		data, err = tr.optionalResult.NextRaw(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if data != nil {
			return data, nil
		}
		tr.optionalFinished = true
	}
	data, err = tr.result.NextRaw(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return data, nil
}

func (tr *tableResultHandler) Close() error {
	err := closeAll(tr.optionalResult, tr.result)
	tr.optionalResult, tr.result = nil, nil
	return errors.Trace(err)
}
