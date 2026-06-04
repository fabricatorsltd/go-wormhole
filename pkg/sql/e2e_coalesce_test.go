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

type coalesceUser struct {
	ID       int            `db:"column:id;primary_key"`
	Nickname sql.NullString `db:"column:nickname;nullable"`
	Name     string         `db:"column:name"`
}

type displayRow struct {
	ID      int    `db:"column:id"`
	Display string `db:"column:display"`
}

func init() { dsl.Register(coalesceUser{}) }

func openCoalesceDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	if _, err := db.Exec(`CREATE TABLE "coalesce_user" ("id" INTEGER PRIMARY KEY, "nickname" TEXT, "name" TEXT NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO "coalesce_user" ("id","nickname","name") VALUES (1,'nick1','Name1'),(2,NULL,'Name2')`); err != nil {
		t.Fatal(err)
	}
	return db
}

// COALESCE in all three positions, end to end on SQLite: a projection that
// falls back to name when nickname is NULL, a WHERE on the coalesced value, and
// an ORDER BY on it.
func TestE2E_Coalesce_ProjectionWhereOrderBy(t *testing.T) {
	db := openCoalesceDB(t)
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()
	u := &coalesceUser{}
	fallback := func() query.CoalesceExpr {
		return dsl.Coalesce(dsl.Col(u, &u.Nickname), dsl.Col(u, &u.Name))
	}

	// Projection: nickname or, when NULL, name.
	var rows []displayRow
	if err := ctx.Set(&rows).From("coalesce_user").
		Select("id").
		SelectCoalesce(fallback(), "display").
		OrderBy("id", query.Asc).
		All(); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].Display != "nick1" || rows[1].Display != "Name2" {
		t.Fatalf("projection: got %+v, want [nick1, Name2]", rows)
	}

	// WHERE on the coalesced value: only the row whose fallback is "Name2".
	var found []coalesceUser
	if err := ctx.Set(&found).Where(dsl.CoalesceEq(fallback(), "Name2")).All(); err != nil {
		t.Fatal(err)
	}
	if len(found) != 1 || found[0].ID != 2 {
		t.Fatalf("where: got %+v, want only id 2", found)
	}

	// ORDER BY the coalesced value: "Name2" sorts before "nick1" (uppercase N).
	var ordered []coalesceUser
	if err := ctx.Set(&ordered).OrderByCoalesce(fallback(), query.Asc).All(); err != nil {
		t.Fatal(err)
	}
	if len(ordered) != 2 || ordered[0].ID != 2 || ordered[1].ID != 1 {
		t.Fatalf("order by: got ids %d,%d, want 2,1", ordered[0].ID, ordered[1].ID)
	}
}

// A COALESCE projection cannot be combined with an aggregate: the select body
// emits aggregates xor row projections, so a silent drop is rejected instead.
func TestE2E_Coalesce_RejectedWithAggregate(t *testing.T) {
	db := openCoalesceDB(t)
	p := wsql.New(db)
	u := &coalesceUser{}
	q := query.From("coalesce_user").
		Aggregate(query.AggCount, "*", "cnt").
		SelectCoalesce(dsl.Coalesce(dsl.Col(u, &u.Nickname), dsl.Col(u, &u.Name)), "display").
		Build()

	var out []displayRow
	err := p.Execute(t.Context(), schema.Parse(&displayRow{}), q, &out)
	if err == nil || !strings.Contains(err.Error(), "COALESCE projections cannot be combined with aggregates") {
		t.Fatalf("want aggregate+coalesce rejection, got %v", err)
	}
}
