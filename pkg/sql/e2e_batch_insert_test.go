package sql_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/glebarez/sqlite"

	wctx "github.com/fabricatorsltd/go-wormhole/pkg/context"
	"github.com/fabricatorsltd/go-wormhole/pkg/provider"
	"github.com/fabricatorsltd/go-wormhole/pkg/schema"
	wsql "github.com/fabricatorsltd/go-wormhole/pkg/sql"
)

// bUser has a client-assigned (non-auto-increment) primary key and a
// `default:`-tagged column, so it exercises the column-set grouping in the
// batch path.
type bUser struct {
	ID     string `db:"column:id;primary_key"`
	Name   string `db:"column:name"`
	Status string `db:"column:status;default:'active'"`
}

// bOrder has a belongs-to FK back to bUser, used to check FK fixup across a
// batched parent insert.
type bOrder struct {
	ID     string `db:"column:id;primary_key"`
	UserID string `db:"column:user_id"`
	User   *bUser `db:"ref"` // belongs-to (FK user_id on this table)
}

func openBatchDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	stmts := []string{
		`CREATE TABLE "b_user" (
			"id" TEXT PRIMARY KEY,
			"name" TEXT NOT NULL,
			"status" TEXT NOT NULL DEFAULT 'active')`,
		`CREATE TABLE "b_order" (
			"id" TEXT PRIMARY KEY,
			"user_id" TEXT NOT NULL)`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

// The SQL Tx advertises the batch capability so flush takes the multi-row path.
func TestBatchInserter_Implemented(t *testing.T) {
	db := openBatchDB(t)
	defer db.Close()
	tx, err := wsql.New(db).Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, ok := tx.(provider.BatchInserter); !ok {
		t.Fatal("SQL Tx should implement provider.BatchInserter")
	}
}

// A run of same-type client-PK inserts all land in the database via the batch
// path.
func TestE2E_BatchInsert_AllRowsPersisted(t *testing.T) {
	db := openBatchDB(t)
	defer db.Close()
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	users := []*bUser{
		{ID: "u1", Name: "Alice", Status: "vip"},
		{ID: "u2", Name: "Bob", Status: "vip"},
		{ID: "u3", Name: "Carol", Status: "vip"},
	}
	for _, u := range users {
		ctx.Add(u)
	}
	if err := ctx.Save(); err != nil {
		t.Fatalf("save: %v", err)
	}

	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM "b_user"`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("row count: got %d, want 3", n)
	}
	for _, u := range users {
		var name, status string
		if err := db.QueryRow(`SELECT "name","status" FROM "b_user" WHERE "id"=?`, u.ID).Scan(&name, &status); err != nil {
			t.Fatalf("row %s: %v", u.ID, err)
		}
		if name != u.Name || status != "vip" {
			t.Errorf("row %s: got (%q,%q), want (%q,vip)", u.ID, name, status, u.Name)
		}
	}
}

// The discriminating test: when entities of the same type emit different
// column sets (one sets a `default:` column, the next leaves it zero), the
// batch must split so the first stores its explicit value and the second takes
// the DB default. A naive single-statement batch would corrupt one of them.
func TestE2E_BatchInsert_HeterogeneousDefaults(t *testing.T) {
	db := openBatchDB(t)
	defer db.Close()
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	// u1 sets status explicitly (column emitted); u2 leaves it zero (column
	// omitted, DB default 'active' applies); u3 sets it again.
	ctx.Add(&bUser{ID: "u1", Name: "Alice", Status: "vip"})
	ctx.Add(&bUser{ID: "u2", Name: "Bob"})
	ctx.Add(&bUser{ID: "u3", Name: "Carol", Status: "staff"})
	if err := ctx.Save(); err != nil {
		t.Fatalf("save: %v", err)
	}

	want := map[string]string{"u1": "vip", "u2": "active", "u3": "staff"}
	for id, status := range want {
		var got string
		if err := db.QueryRow(`SELECT "status" FROM "b_user" WHERE "id"=?`, id).Scan(&got); err != nil {
			t.Fatalf("row %s: %v", id, err)
		}
		if got != status {
			t.Errorf("row %s status: got %q, want %q", id, got, status)
		}
	}
}

// FK fixup must run for every entity in a batched run: children inserted after
// a batched parent should carry the parent's PK in their FK column.
func TestE2E_BatchInsert_FixupAcrossBatch(t *testing.T) {
	db := openBatchDB(t)
	defer db.Close()
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	u1 := &bUser{ID: "u1", Name: "Alice", Status: "vip"}
	u2 := &bUser{ID: "u2", Name: "Bob", Status: "vip"}
	ctx.Add(u1)
	ctx.Add(u2)
	// Orders reference users by pointer; the FK column is filled by fixup.
	ctx.Add(&bOrder{ID: "o1", User: u1})
	ctx.Add(&bOrder{ID: "o2", User: u2})
	if err := ctx.Save(); err != nil {
		t.Fatalf("save: %v", err)
	}

	want := map[string]string{"o1": "u1", "o2": "u2"}
	for id, userID := range want {
		var got string
		if err := db.QueryRow(`SELECT "user_id" FROM "b_order" WHERE "id"=?`, id).Scan(&got); err != nil {
			t.Fatalf("order %s: %v", id, err)
		}
		if got != userID {
			t.Errorf("order %s user_id: got %q, want %q", id, got, userID)
		}
	}
}

// bCounter has an auto-increment PK, so it must NOT be batched (each row needs
// its generated key written back).
type bCounter struct {
	ID    int    `db:"column:id;primary_key;auto_increment"`
	Label string `db:"column:label"`
}

// Auto-increment entities skip the batch path and still get their generated PK
// written back onto each in-memory entity.
func TestE2E_BatchInsert_AutoIncrStaysPerRow(t *testing.T) {
	db := openBatchDB(t)
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE "b_counter" ("id" INTEGER PRIMARY KEY AUTOINCREMENT, "label" TEXT NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	c1 := &bCounter{Label: "a"}
	c2 := &bCounter{Label: "b"}
	c3 := &bCounter{Label: "c"}
	ctx.Add(c1)
	ctx.Add(c2)
	ctx.Add(c3)
	if err := ctx.Save(); err != nil {
		t.Fatalf("save: %v", err)
	}
	// Per-row inserts assign distinct auto-increment PKs; a multi-row batch
	// would have left these at zero.
	if c1.ID == 0 || c2.ID == 0 || c3.ID == 0 {
		t.Fatalf("auto-increment PKs not written back: %d %d %d", c1.ID, c2.ID, c3.ID)
	}
	if c1.ID == c2.ID || c2.ID == c3.ID {
		t.Errorf("PKs not distinct: %d %d %d", c1.ID, c2.ID, c3.ID)
	}
}

// bNode is a self-referential tree: the FK is propagated through the PARENT's
// Children nav (1:N), not a belongs-to on the child. This is the direction the
// batch path must not skip: parent and child are the same type and land in one
// run, so the child's FK column has to be filled before the rows are written.
type bNode struct {
	ID       string   `db:"column:id;primary_key"`
	ParentID string   `db:"column:parent_id"`
	Children []*bNode `db:"fk:parent_id"`
}

// Discriminator for the parent-side FK fixup: a same-type parent and child in
// one batch run must both reach the database with the FK set, not just the
// in-memory struct. A batch that fixes child FKs only after INSERT would write
// an empty parent_id here.
func TestE2E_BatchInsert_ParentChildSameTypeRun(t *testing.T) {
	db := openBatchDB(t)
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE "b_node" ("id" TEXT PRIMARY KEY, "parent_id" TEXT NOT NULL DEFAULT '')`); err != nil {
		t.Fatal(err)
	}
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	parent := &bNode{ID: "root"}
	child := &bNode{ID: "c1"}
	parent.Children = []*bNode{child}
	ctx.Add(parent)
	ctx.Add(child)
	if err := ctx.Save(); err != nil {
		t.Fatalf("save: %v", err)
	}

	var got string
	if err := db.QueryRow(`SELECT "parent_id" FROM "b_node" WHERE "id"='c1'`).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != "root" {
		t.Errorf("child parent_id in DB: got %q, want %q", got, "root")
	}
}

// A run larger than the per-statement placeholder budget is split into several
// INSERTs and every row still lands. With 3 columns the cap allows 300 rows per
// statement, so 305 rows must span two statements without loss.
func TestE2E_BatchInsert_ChunksOverParamLimit(t *testing.T) {
	db := openBatchDB(t)
	defer db.Close()
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	const n = 305
	for i := 0; i < n; i++ {
		ctx.Add(&bUser{ID: fmt.Sprintf("u%03d", i), Name: "x", Status: "vip"})
	}
	if err := ctx.Save(); err != nil {
		t.Fatalf("save: %v", err)
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM "b_user"`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != n {
		t.Errorf("row count: got %d, want %d", count, n)
	}
}

// InsertMany builds one statement with a VALUES tuple per row and flattens the
// parameters in row-major order.
func TestCompiler_InsertMany(t *testing.T) {
	meta := schema.Parse(&bUser{})
	c := &wsql.Compiler{}
	rows := []map[string]any{
		{"ID": "u1", "Name": "Alice", "Status": "vip"},
		{"ID": "u2", "Name": "Bob", "Status": "vip"},
	}
	compiled := c.InsertMany(meta, rows)

	wantSQL := `INSERT INTO "b_user" ("id", "name", "status") VALUES (?, ?, ?), (?, ?, ?)`
	if compiled.SQL != wantSQL {
		t.Errorf("SQL:\n got %q\nwant %q", compiled.SQL, wantSQL)
	}
	wantParams := []any{"u1", "Alice", "vip", "u2", "Bob", "vip"}
	if len(compiled.Params) != len(wantParams) {
		t.Fatalf("param count: got %d, want %d", len(compiled.Params), len(wantParams))
	}
	for i := range wantParams {
		if compiled.Params[i] != wantParams[i] {
			t.Errorf("param %d: got %v, want %v", i, compiled.Params[i], wantParams[i])
		}
	}
}

// The Postgres-style numbered placeholders ($1..$n) must run continuously
// across every VALUES tuple, not restart per row.
func TestCompiler_InsertMany_NumberedPlaceholders(t *testing.T) {
	meta := schema.Parse(&bUser{})
	c := &wsql.Compiler{Numbered: true}
	rows := []map[string]any{
		{"ID": "u1", "Name": "Alice", "Status": "vip"},
		{"ID": "u2", "Name": "Bob", "Status": "vip"},
	}
	compiled := c.InsertMany(meta, rows)

	wantSQL := `INSERT INTO "b_user" ("id", "name", "status") VALUES ($1, $2, $3), ($4, $5, $6)`
	if compiled.SQL != wantSQL {
		t.Errorf("SQL:\n got %q\nwant %q", compiled.SQL, wantSQL)
	}
}

// schema.Parse must register the models so relations can resolve.
func init() {
	schema.Parse(&bUser{})
	schema.Parse(&bOrder{})
	schema.Parse(&bNode{})
}
