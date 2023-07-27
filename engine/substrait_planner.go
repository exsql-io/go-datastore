package engine

import (
	"errors"
	"github.com/exsql-io/go-datastore/services"
	"github.com/substrait-io/substrait-go/expr"
	"github.com/substrait-io/substrait-go/plan"
	"github.com/substrait-io/substrait-go/types"
	"golang.org/x/exp/slices"
	"strconv"
	"vitess.io/vitess/go/vt/sqlparser"
)

func Plan(leafs map[string]*services.Leaf, sql string) (*plan.Plan, error) {
	statement, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, err
	}

	switch stm := statement.(type) {
	case *sqlparser.Select:
		switch condition := stm.Where.Expr.(type) {
		case *sqlparser.ComparisonExpr:
			leaf := leafs[stm.From[0].(*sqlparser.AliasedTableExpr).Expr.(sqlparser.TableName).Name.String()]
			builder := plan.NewBuilderDefault()
			rel, err := buildFilter(leaf, builder, condition)
			if err != nil {
				return nil, err
			}

			rootNames := toColumnNames(stm.SelectExprs)
			p, err := builder.Plan(rel, rootNames)
			if err != nil {
				return nil, err
			}

			return p, nil
		}
	}

	return nil, errors.New("not implemented")
}

func buildFilter(leaf *services.Leaf, builder plan.Builder, comparison *sqlparser.ComparisonExpr) (*plan.FilterRel, error) {
	scan := builder.NamedScan([]string{leaf.Name}, (*leaf.Store).NamedStruct())

	condition, err := toCondition(builder, scan, comparison)
	if err != nil {
		return nil, err
	}

	filter, err := builder.Filter(scan, condition)
	if err != nil {
		return nil, err
	}

	return filter, nil
}

func toCondition(builder plan.Builder, scan *plan.NamedTableReadRel, comparison *sqlparser.ComparisonExpr) (*expr.ScalarFunction, error) {
	left, err := toArg(builder, scan, comparison.Left)
	if err != nil {
		return nil, err
	}

	right, err := toArg(builder, scan, comparison.Right)
	if err != nil {
		return nil, err
	}

	fn, err := builder.ScalarFn(
		"https://github.com/substrait-io/substrait/blob/main/extensions/functions_comparison.yaml",
		"equal",
		nil,
		left,
		right,
	)

	if err != nil {
		return nil, err
	}

	return fn, nil
}

func toArg(builder plan.Builder, scan *plan.NamedTableReadRel, sqlExpr sqlparser.Expr) (types.FuncArg, error) {
	switch value := sqlExpr.(type) {
	case *sqlparser.Literal:
		switch value.Type {
		case sqlparser.StrVal:
			return expr.NewPrimitiveLiteral(value.Val, false), nil
		case sqlparser.IntVal:
			i, err := strconv.ParseInt(value.Val, 10, 32)
			if err != nil {
				return nil, err
			}

			return expr.NewPrimitiveLiteral(int32(i), false), nil
		}
	case *sqlparser.ColName:
		index := slices.Index(scan.BaseSchema().Names, value.Name.String())
		return builder.RootFieldRef(scan, int32(index))
	}

	return nil, errors.New("not implemented")
}

func toColumnNames(selectExprs sqlparser.SelectExprs) []string {
	columns := make([]string, len(selectExprs))
	for index, e := range selectExprs {
		switch column := e.(type) {
		case *sqlparser.AliasedExpr:
			switch literal := column.Expr.(type) {
			case *sqlparser.ColName:
				columns[index] = literal.CompliantName()
				break
			}
			break

		case *sqlparser.Nextval:
			switch literal := column.Expr.(type) {
			case *sqlparser.Literal:
				columns[index] = literal.Val
				break
			}
			break
		}
	}

	return columns
}
