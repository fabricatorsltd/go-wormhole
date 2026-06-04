package sql_test

import (
	"database/sql"
	"strings"
	"testing"

	_ "github.com/glebarez/sqlite"

	wctx "github.com/fabricatorsltd/go-wormhole/pkg/context"
	"github.com/fabricatorsltd/go-wormhole/pkg/query"
	"github.com/fabricatorsltd/go-wormhole/pkg/schema"
	wsql "github.com/fabricatorsltd/go-wormhole/pkg/sql"
)

type tUser struct {
	ID   int    `db:"column:id;primary_key;auto_increment"`
	Name string `db:"column:name"`
}

func openTrackingDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`CREATE TABLE "t_user" ("id" INTEGER PRIMARY KEY AUTOINCREMENT, "name" TEXT NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO "t_user" ("id","name") VALUES (1,'alice'),(2,'bob')`); err != nil {
		t.Fatal(err)
	}
	return db
}

func nameOf(t *testing.T, db *sql.DB, id int) string {
	t.Helper()
	var n string
	if err := db.QueryRow(`SELECT "name" FROM "t_user" WHERE "id"=?`, id).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// A plain Find tracks the result, so a later mutation is persisted by Save.
func TestE2E_Find_TracksByDefault(t *testing.T) {
	db := openTrackingDB(t)
	defer db.Close()
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	var u tUser
	if err := ctx.Set(&u).Find(1); err != nil {
		t.Fatal(err)
	}
	u.Name = "alice2"
	if err := ctx.Save(); err != nil {
		t.Fatalf("save: %v", err)
	}
	if got := nameOf(t, db, 1); got != "alice2" {
		t.Errorf("tracked Find should persist mutation: got %q, want alice2", got)
	}
}

// NoTracking detaches the result: the mutation is not persisted by Save.
func TestE2E_Find_NoTracking(t *testing.T) {
	db := openTrackingDB(t)
	defer db.Close()
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	var u tUser
	if err := ctx.Set(&u).NoTracking().Find(1); err != nil {
		t.Fatal(err)
	}
	u.Name = "ghost"
	if err := ctx.Save(); err != nil {
		t.Fatalf("save: %v", err)
	}
	if got := nameOf(t, db, 1); got != "alice" {
		t.Errorf("NoTracking Find must not persist mutation: got %q, want alice", got)
	}
}

// A context built WithNoTracking defaults Find to detached; AsTracking opts a
// single query back in.
func TestE2E_Find_ContextNoTrackingDefault(t *testing.T) {
	db := openTrackingDB(t)
	defer db.Close()
	ctx := wctx.New(wsql.New(db), wctx.WithNoTracking())
	defer ctx.Close()

	// Default (no-tracking): mutation dropped.
	var u tUser
	if err := ctx.Set(&u).Find(1); err != nil {
		t.Fatal(err)
	}
	u.Name = "ghost"
	if err := ctx.Save(); err != nil {
		t.Fatalf("save: %v", err)
	}
	if got := nameOf(t, db, 1); got != "alice" {
		t.Errorf("context no-tracking default must drop mutation: got %q, want alice", got)
	}

	// AsTracking overrides the context default back to tracked.
	var u2 tUser
	if err := ctx.Set(&u2).AsTracking().Find(2); err != nil {
		t.Fatal(err)
	}
	u2.Name = "bob2"
	if err := ctx.Save(); err != nil {
		t.Fatalf("save: %v", err)
	}
	if got := nameOf(t, db, 2); got != "bob2" {
		t.Errorf("AsTracking should override context default: got %q, want bob2", got)
	}
}

// Collection queries are non-tracking by default, so element mutations are not
// persisted.
func TestE2E_All_UntrackedByDefault(t *testing.T) {
	db := openTrackingDB(t)
	defer db.Close()
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	var users []*tUser
	if err := ctx.Set(&users).All(); err != nil {
		t.Fatal(err)
	}
	for _, u := range users {
		u.Name = u.Name + "-x"
	}
	if err := ctx.Save(); err != nil {
		t.Fatalf("save: %v", err)
	}
	if got := nameOf(t, db, 1); got != "alice" {
		t.Errorf("default All must not persist mutation: got %q, want alice", got)
	}
}

// AsTracking on a *[]*T collection query attaches each element, so a later Save
// persists the mutation.
func TestE2E_All_AsTrackingPersists(t *testing.T) {
	db := openTrackingDB(t)
	defer db.Close()
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	var users []*tUser
	if err := ctx.Set(&users).AsTracking().OrderBy("id", query.Asc).All(); err != nil {
		t.Fatal(err)
	}
	if len(users) != 2 {
		t.Fatalf("want 2 users, got %d", len(users))
	}
	users[0].Name = "alice3"
	if err := ctx.Save(); err != nil {
		t.Fatalf("save: %v", err)
	}
	if got := nameOf(t, db, 1); got != "alice3" {
		t.Errorf("AsTracking All should persist mutation: got %q, want alice3", got)
	}
	// The untouched element stays put.
	if got := nameOf(t, db, 2); got != "bob" {
		t.Errorf("untouched row changed: got %q, want bob", got)
	}
}

// AsTracking on a value-slice destination is rejected: a *[]T backing array
// gives no stable per-element address to key the tracker on.
func TestE2E_All_AsTrackingValueSliceErrors(t *testing.T) {
	db := openTrackingDB(t)
	defer db.Close()
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	var users []tUser // value slice, not *[]*T
	err := ctx.Set(&users).AsTracking().All()
	if err == nil {
		t.Fatal("AsTracking on a value slice should error")
	}
	if !strings.Contains(err.Error(), "*[]*T") {
		t.Errorf("error should name the *[]*T requirement, got: %v", err)
	}
}

// When AsTracking re-loads a row already tracked (and mutated) by an earlier
// Find, the identity map must win: the pending mutation survives and the slice
// hands back the tracked instance, rather than a fresh snapshot clobbering the
// dirty entry and dropping the change.
func TestE2E_All_AsTrackingIdentityMapWins(t *testing.T) {
	db := openTrackingDB(t)
	defer db.Close()
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	var u tUser
	if err := ctx.Set(&u).Find(1); err != nil {
		t.Fatal(err)
	}
	u.Name = "alice_dirty" // tracked, dirty, not yet saved

	var users []*tUser
	if err := ctx.Set(&users).AsTracking().OrderBy("id", query.Asc).All(); err != nil {
		t.Fatal(err)
	}
	// The element for row 1 must be the already-tracked instance, not a fresh load.
	if users[0] != &u {
		t.Errorf("identity map should hand back the tracked instance for row 1")
	}
	if users[0].Name != "alice_dirty" {
		t.Errorf("re-query overwrote the pending mutation: got %q", users[0].Name)
	}

	if err := ctx.Save(); err != nil {
		t.Fatalf("save: %v", err)
	}
	if got := nameOf(t, db, 1); got != "alice_dirty" {
		t.Errorf("pending mutation lost across queries: got %q, want alice_dirty", got)
	}
}

func init() {
	schema.Parse(&tUser{})
}
