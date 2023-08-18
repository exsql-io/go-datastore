package engine

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/compute"
	"github.com/apache/arrow/go/v13/arrow/compute/exprs"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/exsql-io/go-datastore/common"
	"github.com/exsql-io/go-datastore/services"
	"github.com/substrait-io/substrait-go/expr"
	"github.com/substrait-io/substrait-go/plan"
	"github.com/substrait-io/substrait-go/types"
	"log"
)

type VolcanoOperator interface {
	Open() error
	Next() (common.ColumnarBatch, error)
	Close() error
}

type VolcanoEngine struct {
	allocator memory.Allocator
}

func NewVolcanoEngine() VolcanoEngine {
	return VolcanoEngine{
		allocator: memory.DefaultAllocator,
	}
}

func (engine *VolcanoEngine) Execute(leafs map[string]*services.Leaf, p *plan.Plan) (*common.CloseableIterator, error) {
	operator, err := engine.convertPlanToVolcanoOperator(leafs, p)
	if err != nil {
		return nil, err
	}

	var iterator common.CloseableIterator
	iterator = &volcanoIterator{
		root: operator,
	}

	return &iterator, nil
}

func (engine *VolcanoEngine) convertPlanToVolcanoOperator(leafs map[string]*services.Leaf, p *plan.Plan) (*VolcanoOperator, error) {
	filter, err := extractFilterFromPlan(p)
	if err != nil {
		return nil, err
	}

	project := filter.Input().(*plan.ProjectRel)

	s := leafs[project.Input().(*plan.NamedTableReadRel).Names()[0]].Store
	iterator, err := (*s).Iterator()
	if err != nil {
		return nil, err
	}

	var scanOperator VolcanoOperator
	scanOperator = newVolcanoScan(iterator)

	extensionSet := exprs.NewExtensionSet(p.ExtensionRegistry(), exprs.DefaultExtensionIDRegistry)
	ctx := exprs.WithExtensionIDSet(compute.WithAllocator(context.Background(), memory.DefaultAllocator), extensionSet)

	args := make([]types.FuncArg, len(project.Expressions()))
	for index, e := range project.Expressions() {
		args[index] = e
	}

	var projectOperator VolcanoOperator
	projectOperator = newVolcanoProjection(&scanOperator, func(batch common.ColumnarBatch) common.ColumnarBatch {
		var records arrow.Record
		records = *batch

		fields := make([]arrow.Field, len(project.OutputMapping()))
		columns := make([]arrow.Array, len(project.OutputMapping()))
		for index, id := range project.OutputMapping() {
			fields[index] = records.Schema().Field(int(id))
			columns[index] = records.Column(int(id))
		}

		var projected arrow.Record
		projected = array.NewRecord(arrow.NewSchema(fields, nil), columns, records.NumRows())

		return &projected
	})

	filterExtended := &expr.Extended{
		Extensions: extensionSet.GetSubstraitRegistry().Set,
		ReferredExpr: []expr.ExpressionReference{
			expr.NewExpressionReference([]string{"out"}, filter.Condition()),
		},
		BaseSchema: (*s).NamedStruct(),
	}

	var filterOperator VolcanoOperator
	filterOperator = newVolcanoFilter(&projectOperator, func(batch common.ColumnarBatch) common.ColumnarBatch {
		var records arrow.Record
		records = *batch

		recordDatum := compute.NewDatum(records)
		defer recordDatum.Release()

		selector, err := exprs.ExecuteScalarSubstrait(ctx, filterExtended, recordDatum)
		if err != nil {
			log.Fatalln(err)
		}

		defer selector.Release()

		dtm := selector.(*compute.ArrayDatum)

		filtered, err := compute.FilterRecordBatch(ctx, *batch, dtm.MakeArray(), compute.DefaultFilterOptions())
		if err != nil {
			log.Fatalln(err)
		}

		return &filtered
	})

	return &filterOperator, nil
}

func extractFilterFromPlan(p *plan.Plan) (*plan.FilterRel, error) {
	if len(p.Relations()) != 1 {
		return nil, fmt.Errorf("expecting only one relation part of the plan got: %d", len(p.Relations()))
	}

	relation := p.Relations()[0]
	if !relation.IsRoot() {
		return nil, errors.New("expecting the plan relation to be a root one")
	}

	root := relation.Root()
	rel := root.Input()

	switch r := rel.(type) {
	case *plan.FilterRel:
		return r, nil
	}

	log.Printf("%+v\n", rel)

	return nil, errors.New("not implemented")
}

type volcanoIterator struct {
	root    *VolcanoOperator
	current common.ColumnarBatch
}

func (vi *volcanoIterator) Next() bool {
	batch, err := (*vi.root).Next()
	for err == nil && (*batch).NumRows() == 0 {
		batch, err = (*vi.root).Next()
	}

	if err != nil {
		if err == common.EOB {
			return false
		}

		return false
	}

	vi.current = batch
	return true
}

func (vi *volcanoIterator) Value() common.ColumnarBatch {
	return vi.current
}

func (vi *volcanoIterator) Close() {
	_ = (*vi.root).Close()
}

type VolcanoScan struct {
	source common.CloseableIterator
}

func newVolcanoScan(source *common.CloseableIterator) *VolcanoScan {
	return &VolcanoScan{source: *source}
}

func (scan *VolcanoScan) Open() error {
	return nil
}

func (scan *VolcanoScan) Next() (common.ColumnarBatch, error) {
	if scan.source.Next() {
		return scan.source.Value(), nil
	}

	return nil, common.EOB
}

func (scan *VolcanoScan) Close() error {
	scan.source.Close()
	return nil
}

type VolcanoFilter struct {
	child     VolcanoOperator
	condition func(common.ColumnarBatch) common.ColumnarBatch
}

func newVolcanoFilter(child *VolcanoOperator, condition func(common.ColumnarBatch) common.ColumnarBatch) *VolcanoFilter {
	return &VolcanoFilter{child: *child, condition: condition}
}

func (filter *VolcanoFilter) Open() error {
	return filter.child.Open()
}

func (filter *VolcanoFilter) Next() (common.ColumnarBatch, error) {
	batch, err := filter.child.Next()
	if err != nil {
		if err == common.EOB {
			return nil, common.EOB
		}

		return nil, err
	}

	return filter.condition(batch), nil
}

func (filter *VolcanoFilter) Close() error {
	return filter.child.Close()
}

type VolcanoProjection struct {
	child   VolcanoOperator
	project func(common.ColumnarBatch) common.ColumnarBatch
}

func newVolcanoProjection(child *VolcanoOperator, project func(common.ColumnarBatch) common.ColumnarBatch) *VolcanoProjection {
	return &VolcanoProjection{child: *child, project: project}
}

func (filter *VolcanoProjection) Open() error {
	return filter.child.Open()
}

func (filter *VolcanoProjection) Next() (common.ColumnarBatch, error) {
	batch, err := filter.child.Next()
	if err != nil {
		if err == common.EOB {
			return nil, common.EOB
		}

		return nil, err
	}

	return filter.project(batch), nil
}

func (filter *VolcanoProjection) Close() error {
	return filter.child.Close()
}
