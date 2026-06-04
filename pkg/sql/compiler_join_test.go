package sql_test

import (
	"strings"
	"testing"

	"github.com/fabricatorsltd/go-wormhole/pkg/query"
	wsql "github.com/fabricatorsltd/go-wormhole/pkg/sql"
)

// TestCompileSelect_InnerJoin emits FROM main JOIN other ON col = col.
func TestCompileSelect_InnerJoin(t *testing.T) {
	c := &wsql.Compiler{}
	meta := testMeta()

	q := query.From("users").
		Join("post", query.Predicate{
			Field: "user_id", Table: "post",
			Op:    query.OpEq,
			Value: query.ColumnRef{Field: "id", Table: "users"},
		}).
		Build()
	out := c.Select(meta, q)

	want := `FROM "users" JOIN "post" ON "post"."user_id" = "users"."id"`
	if !strings.Contains(out.SQL, want) {
		t.Errorf("SQL = %q\nwant substring %q", out.SQL, want)
	}
	if len(out.Params) != 0 {
		t.Errorf("expected 0 params (column refs), got %v", out.Params)
	}
}

// TestCompileSelect_LeftJoin emits LEFT JOIN.
func TestCompileSelect_LeftJoin(t *testing.T) {
	c := &wsql.Compiler{}
	meta := testMeta()

	q := query.From("users").
		LeftJoin("post", query.Predicate{
			Field: "user_id", Table: "post",
			Op:    query.OpEq,
			Value: query.ColumnRef{Field: "id", Table: "users"},
		}).
		Build()
	out := c.Select(meta, q)

	if !strings.Contains(out.SQL, `LEFT JOIN "post"`) {
		t.Errorf("expected LEFT JOIN: %s", out.SQL)
	}
}

// TestCompileSelect_RightJoin emits RIGHT JOIN.
func TestCompileSelect_RightJoin(t *testing.T) {
	c := &wsql.Compiler{}
	meta := testMeta()
	q := query.From("users").
		RightJoin("post", query.Predicate{
			Field: "user_id", Table: "post",
			Op:    query.OpEq,
			Value: query.ColumnRef{Field: "id", Table: "users"},
		}).
		Build()
	out := c.Select(meta, q)
	if !strings.Contains(out.SQL, `RIGHT JOIN "post"`) {
		t.Errorf("expected RIGHT JOIN: %s", out.SQL)
	}
}

// TestCompileSelect_FullJoin emits FULL JOIN.
func TestCompileSelect_FullJoin(t *testing.T) {
	c := &wsql.Compiler{}
	meta := testMeta()
	q := query.From("users").
		FullJoin("post", query.Predicate{
			Field: "user_id", Table: "post",
			Op:    query.OpEq,
			Value: query.ColumnRef{Field: "id", Table: "users"},
		}).
		Build()
	out := c.Select(meta, q)
	if !strings.Contains(out.SQL, `FULL JOIN "post"`) {
		t.Errorf("expected FULL JOIN: %s", out.SQL)
	}
}

// TestCompileSelect_JoinWithFilteredJoinedColumn verifies a join column used
// in WHERE is emitted with table-qualified syntax.
func TestCompileSelect_JoinWithFilteredJoinedColumn(t *testing.T) {
	c := &wsql.Compiler{}
	meta := testMeta()

	q := query.From("users").
		Join("post", query.Predicate{
			Field: "user_id", Table: "post",
			Op:    query.OpEq,
			Value: query.ColumnRef{Field: "id", Table: "users"},
		}).
		Filter(query.Predicate{Field: "status", Table: "post", Op: query.OpEq, Value: "active"}).
		Build()
	out := c.Select(meta, q)

	if !strings.Contains(out.SQL, `WHERE "post"."status" = `) {
		t.Errorf("expected qualified WHERE: %s", out.SQL)
	}
	if len(out.Params) != 1 || out.Params[0] != "active" {
		t.Errorf("params = %v, want [active]", out.Params)
	}
}

// TestCompileSelect_NoJoinUnqualified verifies single-table queries that use
// direct AST construction (no Table set) remain unqualified for back-compat.
func TestCompileSelect_NoJoinUnqualified(t *testing.T) {
	c := &wsql.Compiler{}
	meta := testMeta()

	q := query.From("users").
		Where("age", query.OpGt, 18).
		Build()
	out := c.Select(meta, q)

	if strings.Contains(out.SQL, `"users"."age"`) {
		t.Errorf("single-table query should not qualify: %s", out.SQL)
	}
	if !strings.Contains(out.SQL, `"age" > `) {
		t.Errorf("expected unqualified WHERE: %s", out.SQL)
	}
}

// TestCompileSelect_TwoJoins verifies chained JOINs.
func TestCompileSelect_TwoJoins(t *testing.T) {
	c := &wsql.Compiler{}
	meta := testMeta()

	q := query.From("job_candidate").
		LeftJoin("job_offer", query.Predicate{
			Field: "id", Table: "job_offer",
			Op:    query.OpEq,
			Value: query.ColumnRef{Field: "offer_id", Table: "job_candidate"},
		}).
		LeftJoin("pos", query.Predicate{
			Field: "id", Table: "pos",
			Op:    query.OpEq,
			Value: query.ColumnRef{Field: "owner_id", Table: "job_offer"},
		}).
		Build()
	out := c.Select(meta, q)

	if strings.Count(out.SQL, "LEFT JOIN") != 2 {
		t.Errorf("expected 2 LEFT JOINs: %s", out.SQL)
	}
}
