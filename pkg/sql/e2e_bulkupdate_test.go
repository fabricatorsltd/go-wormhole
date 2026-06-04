package sql_test

import (
	"database/sql"
	"strings"
	"testing"

	_ "github.com/glebarez/sqlite"

	wctx "github.com/fabricatorsltd/go-wormhole/pkg/context"
	"github.com/fabricatorsltd/go-wormhole/pkg/dsl"
	"github.com/fabricatorsltd/go-wormhole/pkg/query"
	"github.com/fabricatorsltd/go-wormhole/pkg/schema"
	wsql "github.com/fabricatorsltd/go-wormhole/pkg/sql"
)

func TestCompiler_UpdateWhere(t *testing.T) {
	meta := schema.Parse(&User{})

	q := query.Query{Where: query.Predicate{Field: "age", Op: query.OpLt, Value: 18}}
	sets := []query.Assignment{{Field: "name", Value: "minor"}}

	// Default placeholder style.
	c := &wsql.Compiler{}
	got := c.UpdateWhere(meta, q, sets)
	if !strings.Contains(got.SQL, `UPDATE "user" SET "name" = ?`) || !strings.Contains(got.SQL, `WHERE "age" < ?`) {
		t.Errorf("unexpected SQL: %s", got.SQL)
	}
	if len(got.Params) != 2 || got.Params[0] != "minor" || got.Params[1] != 18 {
		t.Errorf("params: got %v, want [minor 18]", got.Params)
	}

	// Numbered placeholders: SET param is $1, WHERE param is $2.
	cn := &wsql.Compiler{Numbered: true}
	gotN := cn.UpdateWhere(meta, q, sets)
	if !strings.Contains(gotN.SQL, `SET "name" = $1`) || !strings.Contains(gotN.SQL, `WHERE "age" < $2`) {
		t.Errorf("numbered placeholder order wrong: %s", gotN.SQL)
	}
}

type buUser struct {
	ID     int    `db:"column:id;primary_key;auto_increment"`
	Status string `db:"column:status"`
	Age    int    `db:"column:age"`
}

func init() { dsl.Register(buUser{}) }

func openBulkUpdateDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	stmts := []string{
		`CREATE TABLE "bu_user" ("id" INTEGER PRIMARY KEY AUTOINCREMENT, "status" TEXT NOT NULL, "age" INTEGER NOT NULL)`,
		`INSERT INTO "bu_user" ("status","age") VALUES ('pending',20),('pending',30),('active',40)`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

// Exercises the type-safe dsl path end to end, including the table-qualified
// WHERE that dsl.Eq produces.
func TestE2E_BulkUpdate_DSLPath(t *testing.T) {
	db := openBulkUpdateDB(t)
	defer db.Close()
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	u := &buUser{}
	n, err := ctx.Set(&buUser{}).
		Where(dsl.Eq(u, &u.Status, "pending")).
		Update(dsl.Set(u, &u.Status, "active"))
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("rows affected: got %d, want 2", n)
	}

	// All three rows should now be active.
	var active int
	if err := db.QueryRow(`SELECT COUNT(*) FROM "bu_user" WHERE "status" = 'active'`).Scan(&active); err != nil {
		t.Fatal(err)
	}
	if active != 3 {
		t.Errorf("active rows: got %d, want 3", active)
	}
}

func TestE2E_BulkUpdate_NoWhereUpdatesAll(t *testing.T) {
	db := openBulkUpdateDB(t)
	defer db.Close()
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	u := &buUser{}
	n, err := ctx.Set(&buUser{}).Update(dsl.Set(u, &u.Age, 0))
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("rows affected: got %d, want 3", n)
	}
}

type buDoc struct {
	ID   int      `db:"column:id;primary_key;auto_increment"`
	Tags []string `db:"column:tags;json"`
}

func init() { dsl.Register(buDoc{}) }

// A json-tagged column must be serialized on the bulk path the same way Save
// serializes it, and must round-trip back through the ORM reader.
func TestE2E_BulkUpdate_JSONColumn(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`CREATE TABLE "bu_doc" ("id" INTEGER PRIMARY KEY AUTOINCREMENT, "tags" TEXT NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO "bu_doc" ("id","tags") VALUES (1, '[]')`); err != nil {
		t.Fatal(err)
	}

	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	d := &buDoc{}
	n, err := ctx.Set(&buDoc{}).
		Where(dsl.Eq(d, &d.ID, 1)).
		Update(dsl.Set(d, &d.Tags, []string{"a", "b"}))
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("rows affected: got %d, want 1", n)
	}

	var got buDoc
	if err := ctx.Set(&got).Find(1); err != nil {
		t.Fatal(err)
	}
	if len(got.Tags) != 2 || got.Tags[0] != "a" || got.Tags[1] != "b" {
		t.Errorf("round-trip tags: got %#v, want [a b]", got.Tags)
	}
}

func TestE2E_BulkUpdate_NoAssignmentsErrors(t *testing.T) {
	db := openBulkUpdateDB(t)
	defer db.Close()
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	_, err := ctx.Set(&buUser{}).Update()
	if err == nil || !strings.Contains(err.Error(), "at least one assignment") {
		t.Fatalf("want assignment-required error, got %v", err)
	}
}
