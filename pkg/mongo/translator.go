package mongo

import (
	"fmt"
	"reflect"
	"regexp"

	"github.com/fabricatorsltd/go-wormhole/pkg/query"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// BuildFilter translates a provider-neutral query.Node AST into a MongoDB filter document.
func BuildFilter(node query.Node) (bson.D, error) {
	if node == nil {
		return bson.D{}, nil
	}

	switch n := node.(type) {
	case query.Predicate:
		return buildPredicate(n)
	case query.Composite:
		return buildComposite(n)
	default:
		return nil, fmt.Errorf("unsupported query node type %T", node)
	}
}

// BuildFindOptions translates ordering/pagination from query.Query to Mongo find options.
func BuildFindOptions(q query.Query) *options.FindOptions {
	opts := options.Find()

	if len(q.OrderBy) > 0 {
		opts.SetSort(buildSort(q.OrderBy))
	}
	if q.Limit > 0 {
		opts.SetLimit(int64(q.Limit))
	}
	if q.Offset > 0 {
		opts.SetSkip(int64(q.Offset))
	}

	return opts
}

func buildSort(order []query.Sort) bson.D {
	sort := make(bson.D, 0, len(order))
	for _, s := range order {
		dir := int32(1)
		if s.Dir == query.Desc {
			dir = -1
		}
		sort = append(sort, bson.E{Key: s.Field, Value: dir})
	}
	return sort
}

func buildComposite(c query.Composite) (bson.D, error) {
	if len(c.Children) == 0 {
		return bson.D{}, nil
	}

	docs := make(bson.A, 0, len(c.Children))
	for _, child := range c.Children {
		doc, err := BuildFilter(child)
		if err != nil {
			return nil, err
		}
		docs = append(docs, doc)
	}

	op := "$and"
	if c.Logic == query.LogicOr {
		op = "$or"
	}
	return bson.D{{Key: op, Value: docs}}, nil
}

func buildPredicate(p query.Predicate) (bson.D, error) {
	switch p.Op {
	case query.OpEq:
		return bson.D{{Key: p.Field, Value: p.Value}}, nil
	case query.OpNeq:
		return bson.D{{Key: p.Field, Value: bson.D{{Key: "$ne", Value: p.Value}}}}, nil
	case query.OpGt:
		return bson.D{{Key: p.Field, Value: bson.D{{Key: "$gt", Value: p.Value}}}}, nil
	case query.OpGte:
		return bson.D{{Key: p.Field, Value: bson.D{{Key: "$gte", Value: p.Value}}}}, nil
	case query.OpLt:
		return bson.D{{Key: p.Field, Value: bson.D{{Key: "$lt", Value: p.Value}}}}, nil
	case query.OpLte:
		return bson.D{{Key: p.Field, Value: bson.D{{Key: "$lte", Value: p.Value}}}}, nil
	case query.OpIn:
		values, err := toBsonArray(p.Value)
		if err != nil {
			return nil, err
		}
		return bson.D{{Key: p.Field, Value: bson.D{{Key: "$in", Value: values}}}}, nil
	case query.OpLike:
		pattern := ".*" + regexp.QuoteMeta(fmt.Sprintf("%v", p.Value)) + ".*"
		return bson.D{{Key: p.Field, Value: bson.D{{Key: "$regex", Value: pattern}}}}, nil
	case query.OpIsNil:
		return bson.D{{Key: p.Field, Value: nil}}, nil
	case query.OpIsNotNil:
		return bson.D{{Key: p.Field, Value: bson.D{{Key: "$ne", Value: nil}}}}, nil
	default:
		return nil, fmt.Errorf("unsupported predicate op %v", p.Op)
	}
}

func toBsonArray(v any) (bson.A, error) {
	if arr, ok := v.([]any); ok {
		return bson.A(arr), nil
	}

	rv := reflect.ValueOf(v)
	if !rv.IsValid() {
		return nil, fmt.Errorf("IN operator requires a non-nil slice")
	}
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return nil, fmt.Errorf("IN operator requires a slice/array, got %T", v)
	}

	out := make(bson.A, 0, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		out = append(out, rv.Index(i).Interface())
	}
	return out, nil
}
