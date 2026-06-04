package sql_test

import (
	"database/sql"
	stderrors "errors"
	"strings"
	"testing"

	_ "github.com/glebarez/sqlite"

	wctx "github.com/fabricatorsltd/go-wormhole/pkg/context"
	"github.com/fabricatorsltd/go-wormhole/pkg/provider"
	"github.com/fabricatorsltd/go-wormhole/pkg/schema"
	wsql "github.com/fabricatorsltd/go-wormhole/pkg/sql"
)

type vDoc struct {
	ID      int    `db:"column:id;primary_key;auto_increment"`
	Title   string `db:"column:title"`
	Version int    `db:"column:version;version"`
}

// The compiled UPDATE for a versioned entity bumps the version server-side and
// guards the WHERE on the loaded version value.
func TestCompiler_Update_VersionGuard(t *testing.T) {
	meta := schema.Parse(&vDoc{})
	c := &wsql.Compiler{}
	values := map[string]any{"ID": 7, "Title": "new", "Version": 3}

	compiled := c.Update(meta, values, []string{"Title"}, 7)

	if !strings.Contains(compiled.SQL, `"version" = "version" + 1`) {
		t.Errorf("missing version bump in:\n%s", compiled.SQL)
	}
	if !strings.Contains(compiled.SQL, `WHERE "id" = ? AND "version" = ?`) {
		t.Errorf("missing version guard in WHERE:\n%s", compiled.SQL)
	}
	// Last param is the guard value (the loaded version).
	if got := compiled.Params[len(compiled.Params)-1]; got != 3 {
		t.Errorf("guard param: got %v, want 3", got)
	}
}

func openConcurrencyDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`CREATE TABLE "v_doc" (
		"id" INTEGER PRIMARY KEY AUTOINCREMENT,
		"title" TEXT NOT NULL,
		"version" INTEGER NOT NULL DEFAULT 0)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO "v_doc" ("id","title","version") VALUES (1,'original',0)`); err != nil {
		t.Fatal(err)
	}
	return db
}

// Two contexts load the same row; the first save wins and bumps the version,
// the second save sees a stale version and fails with ErrConcurrencyConflict.
func TestE2E_OptimisticConcurrency_Conflict(t *testing.T) {
	db := openConcurrencyDB(t)
	defer db.Close()

	c1 := wctx.New(wsql.New(db))
	defer c1.Close()
	c2 := wctx.New(wsql.New(db))
	defer c2.Close()

	var d1 vDoc
	if err := c1.Set(&d1).Find(1); err != nil {
		t.Fatal(err)
	}
	var d2 vDoc
	if err := c2.Set(&d2).Find(1); err != nil {
		t.Fatal(err)
	}

	// First writer wins.
	d1.Title = "edited by c1"
	if err := c1.Save(); err != nil {
		t.Fatalf("c1 save: %v", err)
	}
	if d1.Version != 1 {
		t.Errorf("c1 version after save: got %d, want 1", d1.Version)
	}

	// Second writer is stale (still version 0) and must be rejected.
	d2.Title = "edited by c2"
	err := c2.Save()
	if err == nil || !stderrors.Is(err, provider.ErrConcurrencyConflict) {
		t.Fatalf("c2 save: want ErrConcurrencyConflict, got %v", err)
	}

	// The winning write is intact in the database.
	var check vDoc
	c3 := wctx.New(wsql.New(db))
	defer c3.Close()
	if err := c3.Set(&check).Find(1); err != nil {
		t.Fatal(err)
	}
	if check.Title != "edited by c1" || check.Version != 1 {
		t.Errorf("db state: got (%q, v%d), want (edited by c1, v1)", check.Title, check.Version)
	}
}

// Regression: when one entity in a multi-entity save conflicts, the whole
// transaction rolls back and the OTHER entity's in-memory version must not be
// left ahead of the database (otherwise it would false-conflict forever).
func TestE2E_OptimisticConcurrency_RollbackKeepsInMemoryVersion(t *testing.T) {
	db := openConcurrencyDB(t)
	defer db.Close()
	if _, err := db.Exec(`INSERT INTO "v_doc" ("id","title","version") VALUES (2,'two',0)`); err != nil {
		t.Fatal(err)
	}

	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()
	var d1, d2 vDoc
	if err := ctx.Set(&d1).Find(1); err != nil {
		t.Fatal(err)
	}
	if err := ctx.Set(&d2).Find(2); err != nil {
		t.Fatal(err)
	}

	// A concurrent writer advances row 2's version, making ctx's d2 stale.
	other := wctx.New(wsql.New(db))
	defer other.Close()
	var o2 vDoc
	if err := other.Set(&o2).Find(2); err != nil {
		t.Fatal(err)
	}
	o2.Title = "two-prime"
	if err := other.Save(); err != nil {
		t.Fatal(err)
	}

	// ctx edits both; row 1 would update cleanly, row 2 conflicts, so the whole
	// save rolls back.
	d1.Title = "one-edited"
	d2.Title = "two-edited"
	err := ctx.Save()
	if err == nil || !stderrors.Is(err, provider.ErrConcurrencyConflict) {
		t.Fatalf("want ErrConcurrencyConflict, got %v", err)
	}

	if d1.Version != 0 {
		t.Errorf("d1.Version after rollback: got %d, want 0 (must match the rolled-back DB)", d1.Version)
	}

	var check vDoc
	c3 := wctx.New(wsql.New(db))
	defer c3.Close()
	if err := c3.Set(&check).Find(1); err != nil {
		t.Fatal(err)
	}
	if check.Title != "original" || check.Version != 0 {
		t.Errorf("row 1 after rollback: got (%q, v%d), want (original, v0)", check.Title, check.Version)
	}
}

// Deleting a versioned row that another transaction has since modified is
// rejected with ErrConcurrencyConflict, and the row is left intact.
func TestE2E_OptimisticConcurrency_DeleteConflict(t *testing.T) {
	db := openConcurrencyDB(t)
	defer db.Close()

	c1 := wctx.New(wsql.New(db))
	defer c1.Close()
	c2 := wctx.New(wsql.New(db))
	defer c2.Close()

	var d1 vDoc
	if err := c1.Set(&d1).Find(1); err != nil {
		t.Fatal(err)
	}
	var d2 vDoc
	if err := c2.Set(&d2).Find(1); err != nil {
		t.Fatal(err)
	}

	// c1 bumps the version.
	d1.Title = "edited"
	if err := c1.Save(); err != nil {
		t.Fatal(err)
	}

	// c2's delete is stale and must be rejected.
	c2.Remove(&d2)
	err := c2.Save()
	if err == nil || !stderrors.Is(err, provider.ErrConcurrencyConflict) {
		t.Fatalf("stale delete: want ErrConcurrencyConflict, got %v", err)
	}

	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM "v_doc" WHERE "id" = 1`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("row should remain after rejected delete, count = %d", n)
	}
}

// Deleting a versioned row that has not changed succeeds.
func TestE2E_OptimisticConcurrency_DeleteSucceeds(t *testing.T) {
	db := openConcurrencyDB(t)
	defer db.Close()
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	var d vDoc
	if err := ctx.Set(&d).Find(1); err != nil {
		t.Fatal(err)
	}
	ctx.Remove(&d)
	if err := ctx.Save(); err != nil {
		t.Fatalf("delete: %v", err)
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM "v_doc" WHERE "id" = 1`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("row should be deleted, count = %d", n)
	}
}

// A second, non-conflicting save after reloading succeeds and bumps again.
func TestE2E_OptimisticConcurrency_SequentialSavesBumpVersion(t *testing.T) {
	db := openConcurrencyDB(t)
	defer db.Close()

	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	var d vDoc
	if err := ctx.Set(&d).Find(1); err != nil {
		t.Fatal(err)
	}
	d.Title = "v1"
	if err := ctx.Save(); err != nil {
		t.Fatal(err)
	}
	d.Title = "v2"
	if err := ctx.Save(); err != nil {
		t.Fatalf("second save: %v", err)
	}
	if d.Version != 2 {
		t.Errorf("version after two saves: got %d, want 2", d.Version)
	}
}
