package engine

import (
	"errors"
	"github.com/apache/arrow/go/v13/arrow"
)

type ColumnarBatch *arrow.Record

type CloseableIterator interface {
	Next() bool
	Value() ColumnarBatch
	Close()
}

var EOB = errors.New("EOB")

type VolcanoOperator interface {
	Open() error
	Next() (ColumnarBatch, error)
	Close() error
}

type VolcanoEngine struct {
}

func (engine *VolcanoEngine) Process(operator *VolcanoOperator) (*CloseableIterator, error) {
	var iterator CloseableIterator
	iterator = &volcanoIterator{
		root: operator,
	}

	return &iterator, nil
}

type volcanoIterator struct {
	root    *VolcanoOperator
	current ColumnarBatch
}

func (vi *volcanoIterator) Next() bool {
	batch, err := (*vi.root).Next()
	for err == nil && (*batch).NumRows() == 0 {
		batch, err = (*vi.root).Next()
	}

	if err != nil {
		if err == EOB {
			return false
		}

		return false
	}

	vi.current = batch
	return true
}

func (vi *volcanoIterator) Value() ColumnarBatch {
	return vi.current
}

func (vi *volcanoIterator) Close() {
	_ = (*vi.root).Close()
}

type VolcanoScan struct {
	source CloseableIterator
}

func NewVolcanoScan(source *CloseableIterator) *VolcanoScan {
	return &VolcanoScan{source: *source}
}

func (scan *VolcanoScan) Open() error {
	return nil
}

func (scan *VolcanoScan) Next() (ColumnarBatch, error) {
	if scan.source.Next() {
		return scan.source.Value(), nil
	}

	return nil, EOB
}

func (scan *VolcanoScan) Close() error {
	scan.source.Close()
	return nil
}

type VolcanoFilter struct {
	child     VolcanoOperator
	condition func(ColumnarBatch) ColumnarBatch
}

func NewVolcanoFilter(child *VolcanoOperator, condition func(ColumnarBatch) ColumnarBatch) *VolcanoFilter {
	return &VolcanoFilter{child: *child, condition: condition}
}

func (filter *VolcanoFilter) Open() error {
	return filter.child.Open()
}

func (filter *VolcanoFilter) Next() (ColumnarBatch, error) {
	batch, err := filter.child.Next()
	if err != nil {
		if err == EOB {
			return nil, EOB
		}

		return nil, err
	}

	return filter.condition(batch), nil
}

func (filter *VolcanoFilter) Close() error {
	return filter.child.Close()
}
