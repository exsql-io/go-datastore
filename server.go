package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/arrow/go/v13/arrow/compute"
	"github.com/apache/arrow/go/v13/arrow/compute/exprs"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/exsql-io/go-datastore/services"
	"github.com/exsql-io/go-datastore/store"
	"github.com/goccy/go-json"
	"github.com/labstack/echo/v4"
	"github.com/substrait-io/substrait-go/expr"
	"github.com/substrait-io/substrait-go/plan"
	"github.com/substrait-io/substrait-go/types"
	"golang.org/x/exp/slices"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"vitess.io/vitess/go/vt/sqlparser"
)

func main() {
	configuration, err := LoadConfiguration(os.Getenv("EXSQL_DATASTORE_SERVER_CONFIGURATION_PATH"))
	if err != nil {
		panic(err)
	}

	var tailers = make(map[string]*services.Tailer)
	var leafs = make(map[string]*services.Leaf)

	for _, stream := range configuration.Streams {
		tailer, err := services.NewTailer(configuration.InstanceId, configuration.Brokers, stream.Topic)
		if err != nil {
			panic(err)
		}

		tailer.Start()

		tailers[stream.Topic] = tailer

		schema, inputFormatType := stream.Schema, stream.Format
		leaf, err := services.NewLeaf(stream.Topic, schema, inputFormatType, tailer.Channel)
		if err != nil {
			panic(err)
		}

		leaf.Start()

		leafs[stream.Topic] = leaf
	}

	e := echo.New()
	e.GET("/streams/:name", func(context echo.Context) error { return getStream(leafs, context) })
	e.POST("/streams/sql", func(echoContext echo.Context) error {
		body, err := io.ReadAll(echoContext.Request().Body)
		if err != nil {
			log.Println(err.Error())
			return err
		}

		var message map[string]string
		err = json.Unmarshal(body, &message)
		if err != nil {
			log.Println(err.Error())
			return err
		}

		p, err := planFromSql(leafs, message["query"])
		if err != nil {
			return err
		}

		filter, err := extractFilterFromPlan(p)
		if err != nil {
			return err
		}

		s := leafs[filter.Input().(*plan.NamedTableReadRel).Names()[0]].Store

		extensionSet := exprs.NewExtensionSet(p.ExtensionRegistry(), exprs.DefaultExtensionIDRegistry)
		extended := &expr.Extended{
			Extensions: extensionSet.GetSubstraitRegistry().Set,
			ReferredExpr: []expr.ExpressionReference{
				expr.NewExpressionReference([]string{"out"}, filter.Condition()),
			},
			BaseSchema: (*s).NamedStruct(),
		}

		iterator, err := (*s).Iterator(func(input compute.Datum) (compute.Datum, error) {
			ctx := exprs.WithExtensionIDSet(compute.WithAllocator(context.Background(), memory.DefaultAllocator), extensionSet)
			return exprs.ExecuteScalarSubstrait(ctx, extended, input)
		})

		if err != nil {
			return err
		}

		defer (*iterator).Close()

		return iteratorResponse(iterator, echoContext)
	})

	e.Logger.Fatal(e.Start(":1323"))
}

func getStream(leafs map[string]*services.Leaf, context echo.Context) error {
	name := context.Param("name")
	leaf := leafs[name]

	iterator, err := (*leaf.Store).Iterator()
	if err != nil {
		return err
	}

	defer (*iterator).Close()

	return iteratorResponse(iterator, context)
}

func iteratorResponse(iterator *store.CloseableIterator, context echo.Context) error {
	context.Response().Header().Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	context.Response().WriteHeader(http.StatusOK)

	writer := context.Response()
	for (*iterator).Next() {
		batch := (*iterator).Value()

		count := (*batch).NumRows()
		for i := int64(0); i < count; i++ {
			record := (*batch).NewSlice(i, i+1)
			bytes, err := record.MarshalJSON()
			if err != nil {
				return err
			}

			_, err = writer.Write(bytes[1 : len(bytes)-1])
			if err != nil {
				return err
			}

			writer.Flush()
			record.Release()
		}

		(*batch).Release()
	}

	return nil
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

func planFromSql(leafs map[string]*services.Leaf, sql string) (*plan.Plan, error) {
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

			p, err := builder.Plan(rel, toColumnNames(stm.SelectExprs))
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
	for _, e := range selectExprs {
		switch column := e.(type) {
		case *sqlparser.Nextval:
			switch literal := column.Expr.(type) {
			case *sqlparser.Literal:
				columns = append(columns, literal.Val)
			}
		}
	}

	return columns
}
