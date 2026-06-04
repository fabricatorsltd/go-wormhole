package sql_test

import (
	"database/sql"
	"strings"
	"testing"

	_ "github.com/glebarez/sqlite"

	wctx "github.com/fabricatorsltd/go-wormhole/pkg/context"
	"github.com/fabricatorsltd/go-wormhole/pkg/dsl"
	wsql "github.com/fabricatorsltd/go-wormhole/pkg/sql"
)

type ojUser struct {
	ID   int    `db:"column:id;primary_key;auto_increment"`
	Name string `db:"column:name"`
}

type ojOrder struct {
	ID     int `db:"column:id;primary_key;auto_increment"`
	UserID int `db:"column:user_id"`
}

// EntitySet.RightJoin / FullJoin must route to the right keyword. Before the
// buildQuery switch handled JoinRight/JoinFull, both fell through to INNER JOIN.
func TestEntitySet_RightJoin_RoutesKeyword(t *testing.T) {
	db := openOuterJoinDB(t)
	defer db.Close()
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	u, o := &ojUser{}, &ojOrder{}
	sqlRight, _, err := ctx.Set(&ojUser{}).
		RightJoin("oj_order", dsl.JoinEq(o, &o.UserID, u, &u.ID)).
		ToSQL()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sqlRight, "RIGHT JOIN") {
		t.Errorf("RightJoin should emit RIGHT JOIN, got: %s", sqlRight)
	}

	sqlFull, _, err := ctx.Set(&ojUser{}).
		FullJoin("oj_order", dsl.JoinEq(o, &o.UserID, u, &u.ID)).
		ToSQL()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sqlFull, "FULL JOIN") {
		t.Errorf("FullJoin should emit FULL JOIN, got: %s", sqlFull)
	}
}

func openOuterJoinDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	stmts := []string{
		`CREATE TABLE "oj_user" ("id" INTEGER PRIMARY KEY AUTOINCREMENT, "name" TEXT NOT NULL)`,
		`CREATE TABLE "oj_order" ("id" INTEGER PRIMARY KEY AUTOINCREMENT, "user_id" INTEGER NOT NULL)`,
		`INSERT INTO "oj_user" ("id","name") VALUES (1,'alice'),(2,'bob')`,
		`INSERT INTO "oj_order" ("user_id") VALUES (1),(1)`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

// A RIGHT JOIN executes on the bundled SQLite driver (3.39+) and the primary
// rows scan back. Keeping all order rows (right side) yields alice twice.
func TestE2E_RightJoin_Executes(t *testing.T) {
	db := openOuterJoinDB(t)
	defer db.Close()
	ctx := wctx.New(wsql.New(db))
	defer ctx.Close()

	u, o := &ojUser{}, &ojOrder{}
	var rows []ojUser
	if err := ctx.Set(&rows).
		RightJoin("oj_order", dsl.JoinEq(o, &o.UserID, u, &u.ID)).
		All(); err != nil {
		t.Fatalf("right join execute: %v", err)
	}
	if len(rows) != 2 {
		t.Errorf("RIGHT JOIN row count: got %d, want 2 (one per order)", len(rows))
	}
	for _, r := range rows {
		if r.Name != "alice" {
			t.Errorf("expected alice rows (the user with orders), got %q", r.Name)
		}
	}
}

func init() {
	dsl.Register(ojUser{})
	dsl.Register(ojOrder{})
}
