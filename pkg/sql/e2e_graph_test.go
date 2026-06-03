package sql_test

import (
	"database/sql"
	"strings"
	"testing"

	_ "github.com/glebarez/sqlite"

	wctx "github.com/fabricatorsltd/go-wormhole/pkg/context"
	"github.com/fabricatorsltd/go-wormhole/pkg/dsl"
	"github.com/fabricatorsltd/go-wormhole/pkg/migrations"
	"github.com/fabricatorsltd/go-wormhole/pkg/model"
	"github.com/fabricatorsltd/go-wormhole/pkg/schema"
	wsql "github.com/fabricatorsltd/go-wormhole/pkg/sql"
)

type gUser struct {
	ID     int       `db:"column:id;primary_key;auto_increment"`
	Name   string    `db:"column:name"`
	Orders []*gOrder `db:"fk:user_id"`
}

type gOrder struct {
	ID     int     `db:"column:id;primary_key;auto_increment"`
	UserID int     `db:"column:user_id"`
	Total  float64 `db:"column:total"`
	User   *gUser  `db:"ref"`
}

func init() {
	dsl.Register(gUser{})
	dsl.Register(gOrder{})
}

func openGraphDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	stmts := []string{
		`CREATE TABLE "g_user" ("id" INTEGER PRIMARY KEY AUTOINCREMENT, "name" TEXT NOT NULL)`,
		`CREATE TABLE "g_order" ("id" INTEGER PRIMARY KEY AUTOINCREMENT, "user_id" INTEGER NOT NULL, "total" REAL NOT NULL DEFAULT 0)`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

// New children added before their new parent still get the parent's
// auto-increment PK written into their FK column, thanks to insert ordering
// plus graph fixup.
func TestE2E_GraphSave_FixupAndOrdering(t *testing.T) {
	db := openGraphDB(t)
	defer db.Close()

	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	user := &gUser{Name: "Alice"}
	o1 := &gOrder{Total: 10, User: user}
	o2 := &gOrder{Total: 20, User: user}
	user.Orders = []*gOrder{o1, o2}

	// Add children first to force the ordering logic to do real work.
	ctx.Add(o1, o2, user)
	if err := ctx.Save(); err != nil {
		t.Fatal(err)
	}

	if user.ID == 0 {
		t.Fatal("user PK not assigned")
	}
	if o1.UserID != user.ID || o2.UserID != user.ID {
		t.Fatalf("FK fixup failed: o1.UserID=%d o2.UserID=%d, want %d", o1.UserID, o2.UserID, user.ID)
	}

	// Confirm the rows landed with the right FK in the database.
	var orders []*gOrder
	if err := ctx.Set(&orders).OrderBy("id", 0).All(); err != nil {
		t.Fatal(err)
	}
	if len(orders) != 2 {
		t.Fatalf("orders: got %d, want 2", len(orders))
	}
	for _, o := range orders {
		if o.UserID != user.ID {
			t.Errorf("persisted order %d: user_id=%d, want %d", o.ID, o.UserID, user.ID)
		}
	}
}

type cycA struct {
	ID int   `db:"column:id;primary_key;auto_increment"`
	BID int  `db:"column:b_id"`
	B  *cycB `db:"ref"` // belongs-to B: B must insert first
}

type cycB struct {
	ID int   `db:"column:id;primary_key;auto_increment"`
	AID int  `db:"column:a_id"`
	A  *cycA `db:"ref"` // belongs-to A: A must insert first
}

// Two new rows that each belong to the other cannot be ordered; SaveChanges
// must report the cycle rather than loop forever.
func TestE2E_GraphSave_CycleErrors(t *testing.T) {
	db := openGraphDB(t)
	defer db.Close()
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	a := &cycA{}
	b := &cycB{}
	a.B = b
	b.A = a

	ctx.Add(a, b)
	err := ctx.Save()
	if err == nil || !strings.Contains(err.Error(), "cyclic") {
		t.Fatalf("want cyclic dependency error, got %v", err)
	}
}

// The foreign keys generated in Phase B are real DDL: with SQLite enforcement
// enabled, an insert that violates a generated FK is rejected.
func TestE2E_GeneratedForeignKey_Enforced(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`PRAGMA foreign_keys = ON`); err != nil {
		t.Fatal(err)
	}

	targets := []*model.EntityMeta{schema.Parse(&gUser{}), schema.Parse(&gOrder{})}
	ops := migrations.ComputeDiff(targets, migrations.DatabaseSchema{})
	script := migrations.GenerateSQLScript(ops, migrations.DefaultDialect{})

	// Drop comment lines so they don't fuse onto the following statement.
	var clean []string
	for _, line := range strings.Split(script, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "--") {
			continue
		}
		clean = append(clean, line)
	}
	for _, stmt := range strings.Split(strings.Join(clean, "\n"), ";") {
		s := strings.TrimSpace(stmt)
		if s == "" {
			continue
		}
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("exec %q: %v", s, err)
		}
	}

	if _, err := db.Exec(`INSERT INTO "g_user" ("name") VALUES ('Alice')`); err != nil {
		t.Fatal(err)
	}
	// user_id 999 does not exist -> FK violation.
	_, err = db.Exec(`INSERT INTO "g_order" ("user_id","total") VALUES (999, 5)`)
	if err == nil {
		t.Fatal("expected FK violation, got nil")
	}
}
