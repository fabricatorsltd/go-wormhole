package sql_test

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/glebarez/sqlite"

	wctx "github.com/fabricatorsltd/go-wormhole/pkg/context"
	"github.com/fabricatorsltd/go-wormhole/pkg/provider"
	"github.com/fabricatorsltd/go-wormhole/pkg/schema"
	wsql "github.com/fabricatorsltd/go-wormhole/pkg/sql"
)

// Tree mirrors kiara's merkle_tree shape: a []string column persisted as JSON
// text via the `json` tag.
type Tree struct {
	ID     string   `db:"column:id; primary_key"`
	Leaves []string `db:"column:leaves; json"`
}

func openTreeDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`CREATE TABLE "tree" ("id" TEXT PRIMARY KEY, "leaves" TEXT)`); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestE2E_JSONColumn_RoundTrip(t *testing.T) {
	db := openTreeDB(t)
	defer db.Close()
	p := wsql.New(db)
	ctx := context.Background()
	meta := schema.Parse(&Tree{})

	in := &Tree{ID: "t1", Leaves: []string{"0xaa", "0xbb", "0xcc"}}
	if _, err := p.Insert(ctx, meta, in); err != nil {
		t.Fatal(err)
	}

	// Persisted as JSON text.
	var raw string
	if err := db.QueryRow(`SELECT leaves FROM tree WHERE id='t1'`).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if raw != `["0xaa","0xbb","0xcc"]` {
		t.Fatalf("leaves not stored as JSON: %q", raw)
	}

	// Read back through the ORM (scanRow path) and unmarshaled.
	var got Tree
	if err := p.Find(ctx, meta, "t1", &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Leaves) != 3 || got.Leaves[0] != "0xaa" || got.Leaves[2] != "0xcc" {
		t.Fatalf("JSON round-trip failed: %+v", got.Leaves)
	}
}

func TestE2E_JSONColumn_Upsert(t *testing.T) {
	db := openTreeDB(t)
	defer db.Close()
	c := wctx.New(wsql.New(db))
	ctx := context.Background()
	cc := provider.ConflictClause{Columns: []string{"id"}, Update: []string{"leaves"}}

	if err := c.Upsert(ctx, &Tree{ID: "t1", Leaves: []string{"a"}}, cc); err != nil {
		t.Fatal(err)
	}
	if err := c.Upsert(ctx, &Tree{ID: "t1", Leaves: []string{"a", "b"}}, cc); err != nil {
		t.Fatal(err)
	}

	var raw string
	_ = db.QueryRow(`SELECT leaves FROM tree WHERE id='t1'`).Scan(&raw)
	if raw != `["a","b"]` {
		t.Fatalf("upsert JSON: %q", raw)
	}
}
