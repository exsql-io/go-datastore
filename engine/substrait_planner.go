package engine

import (
	"errors"
	"github.com/exsql-io/go-datastore/services"
	"github.com/substrait-io/substrait-go/expr"
	"github.com/substrait-io/substrait-go/plan"
	"github.com/substrait-io/substrait-go/types"
	"strconv"
	"vitess.io/vitess/go/vt/sqlparser"
)

type PhysicalRelation struct {
	name   string
	schema types.NamedStruct
}

func (pr *PhysicalRelation) createLookup() map[string]int32 {
	lookup := map[string]int32{}
	for index, name := range pr.schema.Names {
		lookup[name] = int32(index)
	}

	return lookup
}

type PhysicalRelationResolver func(name string) (PhysicalRelation, error)

func Plan(leafs map[string]*services.Leaf, sql string) (*plan.Plan, error) {
	resolver := func(name string) (PhysicalRelation, error) {
		leaf := leafs[name]
		return PhysicalRelation{name: leaf.Name, schema: (*leaf.Store).NamedStruct()}, nil
	}

	statement, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, err
	}

	builder := plan.NewBuilderDefault()

	switch stm := statement.(type) {
	case *sqlparser.Select:
		s, pr, err := handleFrom(builder, resolver, stm.From)
		if err != nil {
			return nil, err
		}

		lookup := pr.createLookup()

		rootNames, proj, err := handleProjection(builder, s, lookup, stm.SelectExprs)

		f, err := handleWhere(builder, proj, lookup, stm.Where)
		if err != nil {
			return nil, err
		}

		p, err := builder.Plan(f, rootNames)
		if err != nil {
			return nil, err
		}

		return p, nil
	}

	return nil, errors.New("not implemented")
}

func handleFrom(builder plan.Builder, resolver PhysicalRelationResolver, from []sqlparser.TableExpr) (*plan.NamedTableReadRel, *PhysicalRelation, error) {
	physicalRelation, err := resolver(from[0].(*sqlparser.AliasedTableExpr).Expr.(sqlparser.TableName).Name.String())
	if err != nil {
		return nil, nil, err
	}

	scan := builder.NamedScan([]string{physicalRelation.name}, physicalRelation.schema)

	return scan, &physicalRelation, nil
}

func handleWhere(builder plan.Builder, input plan.Rel, lookup map[string]int32, where *sqlparser.Where) (*plan.FilterRel, error) {
	switch condition := where.Expr.(type) {
	case *sqlparser.ComparisonExpr:
		f, err := buildFilter(builder, input, lookup, condition)
		if err != nil {
			return nil, err
		}

		return f, nil
	}

	return nil, errors.New("not implemented")
}

func handleProjection(builder plan.Builder, input plan.Rel, lookup map[string]int32, selectExprs sqlparser.SelectExprs) ([]string, *plan.ProjectRel, error) {
	rootNames := toColumnNames(selectExprs)
	expressions, err := toExpressions(builder, input, lookup, selectExprs)
	if err != nil {
		return nil, nil, err
	}

	project, err := builder.ProjectRemap(input, toMapping(rootNames, lookup), expressions...)
	if err != nil {
		return nil, nil, err
	}

	return rootNames, project, nil
}

func buildFilter(builder plan.Builder, input plan.Rel, lookup map[string]int32, comparison *sqlparser.ComparisonExpr) (*plan.FilterRel, error) {
	condition, err := toCondition(builder, input, lookup, comparison)
	if err != nil {
		return nil, err
	}

	mapping := make([]int32, len(input.OutputMapping()))
	for index, _ := range mapping {
		mapping[index] = int32(index)
	}

	filter, err := builder.FilterRemap(input, condition, mapping)
	if err != nil {
		return nil, err
	}

	return filter, nil
}

func toCondition(builder plan.Builder, input plan.Rel, lookup map[string]int32, comparison *sqlparser.ComparisonExpr) (*expr.ScalarFunction, error) {
	left, err := toArg(builder, input, lookup, comparison.Left)
	if err != nil {
		return nil, err
	}

	right, err := toArg(builder, input, lookup, comparison.Right)
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

func toArg(builder plan.Builder, input plan.Rel, lookup map[string]int32, sqlExpr sqlparser.Expr) (types.FuncArg, error) {
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
		return builder.RootFieldRef(input, lookup[value.Name.String()])
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

func toExpressions(builder plan.Builder, input plan.Rel, lookup map[string]int32, selectExprs sqlparser.SelectExprs) ([]expr.Expression, error) {
	expressions := make([]expr.Expression, len(selectExprs))
	for index, e := range selectExprs {
		switch column := e.(type) {
		case *sqlparser.AliasedExpr:
			switch literal := column.Expr.(type) {
			case *sqlparser.ColName:
				expression, err := builder.RootFieldRef(input, lookup[literal.CompliantName()])
				if err != nil {
					return nil, err
				}

				expressions[index] = expression
				break
			}
			break

		case *sqlparser.Nextval:
			switch literal := column.Expr.(type) {
			case *sqlparser.ColName:
				expression, err := builder.RootFieldRef(input, lookup[literal.CompliantName()])
				if err != nil {
					return nil, err
				}

				expressions[index] = expression
				break
			}
			break
		}
	}

	return expressions, nil
}

func toMapping(columns []string, lookup map[string]int32) []int32 {
	mapping := make([]int32, len(columns))
	for index, column := range columns {
		mapping[index] = lookup[column]
	}

	return mapping
}
