package mongo

import (
	"reflect"
	"testing"

	"github.com/fabricatorsltd/go-wormhole/pkg/query"
	"go.mongodb.org/mongo-driver/bson"
)

func TestBuildFilter_Predicate(t *testing.T) {
	tests := []struct {
		name string
		in   query.Predicate
		want bson.D
	}{
		{
			name: "eq",
			in:   query.Predicate{Field: "name", Op: query.OpEq, Value: "alice"},
			want: bson.D{{Key: "name", Value: "alice"}},
		},
		{
			name: "gt",
			in:   query.Predicate{Field: "age", Op: query.OpGt, Value: 18},
			want: bson.D{{Key: "age", Value: bson.D{{Key: "$gt", Value: 18}}}},
		},
		{
			name: "in",
			in:   query.Predicate{Field: "age", Op: query.OpIn, Value: []int{18, 19}},
			want: bson.D{{Key: "age", Value: bson.D{{Key: "$in", Value: bson.A{18, 19}}}}},
		},
		{
			name: "is nil",
			in:   query.Predicate{Field: "deleted_at", Op: query.OpIsNil},
			want: bson.D{{Key: "deleted_at", Value: nil}},
		},
		{
			name: "is not nil",
			in:   query.Predicate{Field: "deleted_at", Op: query.OpIsNotNil},
			want: bson.D{{Key: "deleted_at", Value: bson.D{{Key: "$ne", Value: nil}}}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := BuildFilter(tt.in)
			if err != nil {
				t.Fatalf("BuildFilter() error = %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("BuildFilter() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestBuildFilter_Composite(t *testing.T) {
	in := query.Composite{
		Logic: query.LogicAnd,
		Children: []query.Node{
			query.Predicate{Field: "age", Op: query.OpGte, Value: 18},
			query.Predicate{Field: "active", Op: query.OpEq, Value: true},
		},
	}

	got, err := BuildFilter(in)
	if err != nil {
		t.Fatalf("BuildFilter() error = %v", err)
	}

	want := bson.D{{
		Key: "$and",
		Value: bson.A{
			bson.D{{Key: "age", Value: bson.D{{Key: "$gte", Value: 18}}}},
			bson.D{{Key: "active", Value: true}},
		},
	}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("BuildFilter() = %#v, want %#v", got, want)
	}
}

func TestBuildFilter_InRequiresSlice(t *testing.T) {
	_, err := BuildFilter(query.Predicate{Field: "age", Op: query.OpIn, Value: 10})
	if err == nil {
		t.Fatal("expected error for non-slice IN value")
	}
}

func TestBuildFindOptions(t *testing.T) {
	q := query.From("users").
		OrderBy("name", query.Asc).
		OrderBy("age", query.Desc).
		Limit(5).
		Offset(2).
		Build()

	opts := BuildFindOptions(q)

	sort, ok := opts.Sort.(bson.D)
	if !ok {
		t.Fatalf("expected bson.D sort, got %T", opts.Sort)
	}
	wantSort := bson.D{
		{Key: "name", Value: int32(1)},
		{Key: "age", Value: int32(-1)},
	}
	if !reflect.DeepEqual(sort, wantSort) {
		t.Fatalf("sort = %#v, want %#v", sort, wantSort)
	}

	if opts.Limit == nil || *opts.Limit != 5 {
		t.Fatalf("limit = %v, want 5", opts.Limit)
	}
	if opts.Skip == nil || *opts.Skip != 2 {
		t.Fatalf("skip = %v, want 2", opts.Skip)
	}
}
