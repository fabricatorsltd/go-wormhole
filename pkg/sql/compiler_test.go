package sql_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/mirkobrombin/go-wormhole/pkg/model"
	"github.com/mirkobrombin/go-wormhole/pkg/query"
	wsql "github.com/mirkobrombin/go-wormhole/pkg/sql"
)

func testMeta() *model.EntityMeta {
	meta := &model.EntityMeta{
		Name: "users",
		GoType: reflect.TypeOf(struct {
			ID   int
			Name string
			Age  int
		}{}),
		Fields: []model.FieldMeta{
			{FieldName: "ID", Column: "id", PrimaryKey: true, AutoIncr: true},
			{FieldName: "Name", Column: "name"},
			{FieldName: "Age", Column: "age"},
		},
	}
	meta.PrimaryKey = &meta.Fields[0]
	meta.BuildIndex()
	return meta
}

func TestCompileSelect_Simple(t *testing.T) {
	c := &wsql.Compiler{}
	meta := testMeta()

	q := query.From("users").Build()
	out := c.Select(meta, q)

	want := `SELECT "id", "name", "age" FROM "users"`
	if out.SQL != want {
		t.Fatalf("SQL:\n  got:  %s\n  want: %s", out.SQL, want)
	}
	if len(out.Params) != 0 {
		t.Fatalf("params: want 0, got %d", len(out.Params))
	}
}

func TestCompileSelect_WhereEq(t *testing.T) {
	c := &wsql.Compiler{}
	meta := testMeta()

	q := query.From("users").
		Filter(query.Predicate{Field: "age", Op: query.OpEq, Value: 18}).
		Build()
	out := c.Select(meta, q)

	if !strings.Contains(out.SQL, `WHERE "age" = ?`) {
		t.Fatalf("SQL missing WHERE clause: %s", out.SQL)
	}
	if len(out.Params) != 1 || out.Params[0] != 18 {
		t.Fatalf("params: want [18], got %v", out.Params)
	}
}

func TestCompileSelect_CompositeAnd(t *testing.T) {
	c := &wsql.Compiler{}
	meta := testMeta()

	q := query.From("users").
		Filter(
			query.Predicate{Field: "age", Op: query.OpGt, Value: 18},
			query.Predicate{Field: "name", Op: query.OpLike, Value: "%al%"},
		).
		Build()
	out := c.Select(meta, q)

	if !strings.Contains(out.SQL, `"age" > ?`) {
		t.Fatalf("SQL missing age > ?: %s", out.SQL)
	}
	if !strings.Contains(out.SQL, `"name" LIKE ?`) {
		t.Fatalf("SQL missing name LIKE ?: %s", out.SQL)
	}
	if !strings.Contains(out.SQL, " AND ") {
		t.Fatalf("SQL missing AND: %s", out.SQL)
	}
	if len(out.Params) != 2 {
		t.Fatalf("params: want 2, got %d", len(out.Params))
	}
}

func TestCompileSelect_OrderLimitOffset(t *testing.T) {
	c := &wsql.Compiler{}
	meta := testMeta()

	q := query.From("users").
		OrderBy("age", query.Desc).
		Limit(10).
		Offset(20).
		Build()
	out := c.Select(meta, q)

	if !strings.Contains(out.SQL, `ORDER BY "age" DESC`) {
		t.Fatalf("SQL missing ORDER BY: %s", out.SQL)
	}
	if !strings.Contains(out.SQL, "LIMIT 10") {
		t.Fatalf("SQL missing LIMIT: %s", out.SQL)
	}
	if !strings.Contains(out.SQL, "OFFSET 20") {
		t.Fatalf("SQL missing OFFSET: %s", out.SQL)
	}
}

func TestCompileSelect_InOperator(t *testing.T) {
	c := &wsql.Compiler{}
	meta := testMeta()

	q := query.From("users").
		Filter(query.Predicate{Field: "age", Op: query.OpIn, Value: []any{18, 21, 30}}).
		Build()
	out := c.Select(meta, q)

	if !strings.Contains(out.SQL, `"age" IN (?, ?, ?)`) {
		t.Fatalf("SQL missing IN clause: %s", out.SQL)
	}
	if len(out.Params) != 3 {
		t.Fatalf("params: want 3, got %d", len(out.Params))
	}
}

func TestCompileSelect_IsNull(t *testing.T) {
	c := &wsql.Compiler{}
	meta := testMeta()

	q := query.From("users").
		Filter(query.Predicate{Field: "age", Op: query.OpIsNil}).
		Build()
	out := c.Select(meta, q)

	if !strings.Contains(out.SQL, `"age" IS NULL`) {
		t.Fatalf("SQL missing IS NULL: %s", out.SQL)
	}
	if len(out.Params) != 0 {
		t.Fatalf("params: want 0, got %d", len(out.Params))
	}
}

func TestCompileSelect_Numbered(t *testing.T) {
	c := &wsql.Compiler{Numbered: true}
	meta := testMeta()

	q := query.From("users").
		Filter(
			query.Predicate{Field: "age", Op: query.OpGt, Value: 18},
			query.Predicate{Field: "name", Op: query.OpEq, Value: "alice"},
		).
		Build()
	out := c.Select(meta, q)

	if !strings.Contains(out.SQL, "$1") || !strings.Contains(out.SQL, "$2") {
		t.Fatalf("SQL missing $N placeholders: %s", out.SQL)
	}
}

func TestCompileInsert(t *testing.T) {
	c := &wsql.Compiler{}
	meta := testMeta()

	values := map[string]any{"ID": 0, "Name": "alice", "Age": 30}
	out := c.Insert(meta, values)

	// ID is auto_increment, should be skipped
	if strings.Contains(out.SQL, `"id"`) {
		t.Fatalf("SQL should skip auto_increment field: %s", out.SQL)
	}
	if !strings.Contains(out.SQL, `INSERT INTO "users"`) {
		t.Fatalf("SQL missing INSERT INTO: %s", out.SQL)
	}
	if len(out.Params) != 2 {
		t.Fatalf("params: want 2 (name, age), got %d", len(out.Params))
	}
}

func TestCompileUpdate_Partial(t *testing.T) {
	c := &wsql.Compiler{}
	meta := testMeta()

	values := map[string]any{"ID": 1, "Name": "bob", "Age": 25}
	out := c.Update(meta, values, []string{"Age"}, 1)

	// Only Age should appear in SET
	if !strings.Contains(out.SQL, `SET "age" = ?`) {
		t.Fatalf("SQL missing SET age: %s", out.SQL)
	}
	if strings.Contains(out.SQL, `"name" =`) {
		t.Fatalf("SQL should NOT include unchanged name: %s", out.SQL)
	}
	if !strings.Contains(out.SQL, `WHERE "id" = ?`) {
		t.Fatalf("SQL missing WHERE pk: %s", out.SQL)
	}
	// params: age value + pk value
	if len(out.Params) != 2 {
		t.Fatalf("params: want 2, got %d", len(out.Params))
	}
}

func TestCompileDelete(t *testing.T) {
	c := &wsql.Compiler{}
	meta := testMeta()

	out := c.Delete(meta, 42)

	want := `DELETE FROM "users" WHERE "id" = ?`
	if out.SQL != want {
		t.Fatalf("SQL:\n  got:  %s\n  want: %s", out.SQL, want)
	}
	if len(out.Params) != 1 || out.Params[0] != 42 {
		t.Fatalf("params: want [42], got %v", out.Params)
	}
}

func TestCompileSelectWithJoins(t *testing.T) {
	c := &wsql.Compiler{}
	meta := testMeta()

	q := query.From("users").Build()
	joins := []wsql.JoinSpec{
		{Table: "orders", LocalKey: "id", ForeignKey: "user_id"},
	}
	out := c.SelectWithJoins(meta, q, joins)

	if !strings.Contains(out.SQL, `LEFT JOIN "orders"`) {
		t.Fatalf("SQL missing LEFT JOIN: %s", out.SQL)
	}
	if !strings.Contains(out.SQL, `"users"."id" = "orders"."user_id"`) {
		t.Fatalf("SQL missing JOIN ON: %s", out.SQL)
	}
}

func TestCompileOr(t *testing.T) {
	c := &wsql.Compiler{}
	meta := testMeta()

	q := query.From("users").
		Or(func(b *query.Builder) {
			b.Where("age", query.OpEq, 18)
			b.Where("age", query.OpEq, 21)
		}).
		Build()
	out := c.Select(meta, q)

	if !strings.Contains(out.SQL, " OR ") {
		t.Fatalf("SQL missing OR: %s", out.SQL)
	}
	if len(out.Params) != 2 {
		t.Fatalf("params: want 2, got %d", len(out.Params))
	}
}
