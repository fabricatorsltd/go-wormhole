package migrations_test

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/glebarez/sqlite"

	"github.com/fabricatorsltd/go-wormhole/pkg/migrations"
	"github.com/fabricatorsltd/go-wormhole/pkg/model"
	"github.com/fabricatorsltd/go-wormhole/pkg/schema"
)

type seedUser struct {
	ID   int    `db:"column:id;primary_key"`
	Name string `db:"column:name"`
}

func openSeedDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	if _, err := db.Exec(`CREATE TABLE "seed_user" ("id" INTEGER PRIMARY KEY, "name" TEXT NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	return db
}

func writeSeed(t *testing.T, dir, table, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, table+".json"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func nameByID(t *testing.T, db *sql.DB, id int) (string, bool) {
	t.Helper()
	var n string
	err := db.QueryRow(`SELECT "name" FROM "seed_user" WHERE "id"=?`, id).Scan(&n)
	if err == sql.ErrNoRows {
		return "", false
	}
	if err != nil {
		t.Fatal(err)
	}
	return n, true
}

// Reconcile inserts seed rows, then on a second run updates changed rows, deletes
// rows removed from the file, adds new ones, and leaves rows it never seeded
// untouched. This is the whole feature in one flow.
func TestReconcileSeeds_UpsertAndDelete(t *testing.T) {
	db := openSeedDB(t)
	dir := t.TempDir()
	models := []*model.EntityMeta{schema.Parse(&seedUser{})}
	ctx := context.Background()

	// First run: insert alice(1), bob(2).
	writeSeed(t, dir, "seed_user", `[{"id":1,"name":"alice"},{"id":2,"name":"bob"}]`)
	if err := migrations.ReconcileSeeds(ctx, db, migrations.DefaultDialect{}, models, dir); err != nil {
		t.Fatal(err)
	}
	if n, ok := nameByID(t, db, 1); !ok || n != "alice" {
		t.Fatalf("after seed: id1 = %q,%v (the FieldName-vs-column mapping would make this NULL)", n, ok)
	}
	if _, ok := nameByID(t, db, 2); !ok {
		t.Fatal("after seed: id2 missing")
	}

	// A row the seeder did not create.
	if _, err := db.Exec(`INSERT INTO "seed_user" ("id","name") VALUES (99,'manual')`); err != nil {
		t.Fatal(err)
	}

	// Second run: id1 renamed, id2 removed, id3 added.
	writeSeed(t, dir, "seed_user", `[{"id":1,"name":"ALICE"},{"id":3,"name":"carol"}]`)
	if err := migrations.ReconcileSeeds(ctx, db, migrations.DefaultDialect{}, models, dir); err != nil {
		t.Fatal(err)
	}

	// id1 updated in place (not duplicated): the integer-PK coercion makes the
	// ON CONFLICT match.
	if n, ok := nameByID(t, db, 1); !ok || n != "ALICE" {
		t.Errorf("id1 should be updated to ALICE, got %q,%v", n, ok)
	}
	var count1 int
	if err := db.QueryRow(`SELECT COUNT(*) FROM "seed_user" WHERE "id"=1`).Scan(&count1); err != nil {
		t.Fatal(err)
	}
	if count1 != 1 {
		t.Errorf("id1 duplicated (coercion trap): count=%d", count1)
	}
	// id2 deleted (it was seeded, now removed).
	if _, ok := nameByID(t, db, 2); ok {
		t.Error("id2 should have been deleted")
	}
	// id3 added.
	if n, ok := nameByID(t, db, 3); !ok || n != "carol" {
		t.Errorf("id3 should be carol, got %q,%v", n, ok)
	}
	// id99 (never seeded) untouched.
	if n, ok := nameByID(t, db, 99); !ok || n != "manual" {
		t.Errorf("non-seed row id99 must be left alone, got %q,%v", n, ok)
	}

	if _, err := os.Stat(filepath.Join(dir, ".seed_snapshot.json")); err != nil {
		t.Errorf("seed snapshot not written: %v", err)
	}
}

type seedAcct struct {
	ID     int    `db:"column:id;primary_key;auto_increment"`
	Name   string `db:"column:name"`
	Active bool   `db:"column:active;default:1"`
}

// Seeding must write explicit auto-increment keys (so ON CONFLICT matches on a
// re-run instead of duplicating) and explicit zero-valued defaulted columns (so
// the authored value wins over the database default). This is the case the
// flush-path column dropping would silently break.
func TestReconcileSeeds_AutoIncrAndDefaults(t *testing.T) {
	db := openSeedDB(t)
	if _, err := db.Exec(`DROP TABLE "seed_user"`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE "seed_acct" (
		"id" INTEGER PRIMARY KEY AUTOINCREMENT,
		"name" TEXT NOT NULL,
		"active" INTEGER NOT NULL DEFAULT 1)`); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	models := []*model.EntityMeta{schema.Parse(&seedAcct{})}
	ctx := context.Background()

	writeSeed(t, dir, "seed_acct", `[{"id":1,"name":"a","active":false},{"id":2,"name":"b","active":true}]`)
	for range 2 { // seed twice: the second run must update, not duplicate
		if err := migrations.ReconcileSeeds(ctx, db, migrations.DefaultDialect{}, models, dir); err != nil {
			t.Fatal(err)
		}
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM "seed_acct"`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("auto-increment PK seeded twice duplicated rows: count=%d, want 2", count)
	}
	var active int
	if err := db.QueryRow(`SELECT "active" FROM "seed_acct" WHERE "id"=1`).Scan(&active); err != nil {
		t.Fatal(err)
	}
	if active != 0 {
		t.Errorf("seeded active=false was dropped for the column default: got %d, want 0", active)
	}
}

// An empty seed directory is a no-op, not an error.
func TestReconcileSeeds_NoFiles(t *testing.T) {
	db := openSeedDB(t)
	models := []*model.EntityMeta{schema.Parse(&seedUser{})}
	if err := migrations.ReconcileSeeds(context.Background(), db, migrations.DefaultDialect{}, models, t.TempDir()); err != nil {
		t.Fatalf("empty seed dir should be a no-op, got %v", err)
	}
}

// A seed file for an unknown table is a clear error, not a silent skip.
func TestReconcileSeeds_UnknownTable(t *testing.T) {
	db := openSeedDB(t)
	dir := t.TempDir()
	writeSeed(t, dir, "nope", `[{"id":1}]`)
	err := migrations.ReconcileSeeds(context.Background(), db, migrations.DefaultDialect{}, []*model.EntityMeta{schema.Parse(&seedUser{})}, dir)
	if err == nil {
		t.Fatal("expected an error for a seed file with no matching model")
	}
}
