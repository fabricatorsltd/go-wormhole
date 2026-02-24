package dsl_test

import (
	"testing"

	"github.com/fabricatorsltd/go-wormhole/pkg/dsl"
	"github.com/fabricatorsltd/go-wormhole/pkg/query"
)

type User struct {
	ID    int    `db:"primary_key; auto_increment"`
	Name  string `db:"type:varchar(100)"`
	Email string `db:"column:email_addr; type:varchar(255)"`
	Age   int    `db:"nullable"`
}

// Score has no db tag — must still be resolvable (Issue 2 fix).
type Profile struct {
	UserID int
	Score  float64
	Bio    string `db:"column:biography"`
}

func init() {
	dsl.Register(User{})
	dsl.Register(Profile{})
}

func TestFieldName(t *testing.T) {
	u := &User{}
	if got := dsl.FieldName(u, &u.Age); got != "Age" {
		t.Fatalf("FieldName: want Age, got %s", got)
	}
}

func TestColumnName(t *testing.T) {
	u := &User{}
	if got := dsl.ColumnName(u, &u.Email); got != "email_addr" {
		t.Fatalf("ColumnName: want email_addr, got %s", got)
	}
	if got := dsl.ColumnName(u, &u.Name); got != "name" {
		t.Fatalf("ColumnName: want name, got %s", got)
	}
}

func TestFieldsWithoutTags(t *testing.T) {
	p := &Profile{}
	// UserID has no db tag at all — should resolve to snake_case
	if got := dsl.ColumnName(p, &p.UserID); got != "user_id" {
		t.Fatalf("ColumnName(UserID): want user_id, got %s", got)
	}
	if got := dsl.ColumnName(p, &p.Score); got != "score" {
		t.Fatalf("ColumnName(Score): want score, got %s", got)
	}
	// Bio has a column override
	if got := dsl.ColumnName(p, &p.Bio); got != "biography" {
		t.Fatalf("ColumnName(Bio): want biography, got %s", got)
	}
}

func TestEq(t *testing.T) {
	u := &User{}
	cond := dsl.Eq(u, &u.Age, 18)
	// Field now holds the column name
	if cond.Field != "age" || cond.Op != query.OpEq || cond.Value != 18 {
		t.Fatalf("Eq: unexpected %+v", cond)
	}
}

func TestGt(t *testing.T) {
	u := &User{}
	cond := dsl.Gt(u, &u.Age, 21)
	if cond.Field != "age" || cond.Op != query.OpGt || cond.Value != 21 {
		t.Fatalf("Gt: unexpected %+v", cond)
	}
}

func TestContains(t *testing.T) {
	u := &User{}
	cond := dsl.Contains(u, &u.Name, "alice")
	if cond.Field != "name" || cond.Op != query.OpLike || cond.Value != "%alice%" {
		t.Fatalf("Contains: unexpected %+v", cond)
	}
}

func TestIn(t *testing.T) {
	u := &User{}
	cond := dsl.In(u, &u.Age, 18, 21, 30)
	if cond.Field != "age" {
		t.Fatalf("In: want field age, got %s", cond.Field)
	}
	items, ok := cond.Value.([]any)
	if !ok || len(items) != 3 {
		t.Fatalf("In: unexpected value %+v", cond.Value)
	}
}

func TestIsNil(t *testing.T) {
	u := &User{}
	cond := dsl.IsNil(u, &u.Age)
	if cond.Field != "age" || cond.Op != query.OpIsNil {
		t.Fatalf("IsNil: unexpected %+v", cond)
	}
}

func TestEqOnUntaggedField(t *testing.T) {
	p := &Profile{}
	cond := dsl.Eq(p, &p.Score, 9.5)
	if cond.Field != "score" || cond.Value != 9.5 {
		t.Fatalf("Eq(untagged): unexpected %+v", cond)
	}
}

func TestBuilderFilter(t *testing.T) {
	u := &User{}
	q := query.From("user").
		Filter(
			dsl.Gt(u, &u.Age, 18),
			dsl.Contains(u, &u.Name, "al"),
		).
		OrderBy("age", query.Desc).
		Limit(10).
		Build()

	if q.EntityName != "user" {
		t.Fatalf("entity: want user, got %s", q.EntityName)
	}
	if q.Limit != 10 {
		t.Fatalf("limit: want 10, got %d", q.Limit)
	}

	comp, ok := q.Where.(query.Composite)
	if !ok {
		t.Fatalf("where: expected Composite, got %T", q.Where)
	}
	if len(comp.Children) != 2 {
		t.Fatalf("where: expected 2 children, got %d", len(comp.Children))
	}
}
