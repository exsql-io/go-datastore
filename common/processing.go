package common

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
