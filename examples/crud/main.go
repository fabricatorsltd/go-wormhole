// Command crud shows the core write/read loop: define a model, insert through
// the change tracker, read it back, mutate in memory, and let Save emit a
// partial UPDATE of only the changed columns.
//
//	go run ./examples/crud
package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/glebarez/sqlite"

	wh "github.com/fabricatorsltd/go-wormhole/pkg/context"
	"github.com/fabricatorsltd/go-wormhole/pkg/dsl"
	"github.com/fabricatorsltd/go-wormhole/pkg/query"
	wsql "github.com/fabricatorsltd/go-wormhole/pkg/sql"
)

// User is the whole schema: the struct tags map fields to columns.
type User struct {
	ID    int    `db:"column:id;primary_key;auto_increment"`
	Name  string `db:"column:name"`
	Email string `db:"column:email"`
	Age   int    `db:"column:age"`
}

func init() {
	// Register pre-computes field offsets for the pointer-tracking DSL.
	dsl.Register(User{})
}

func main() {
	db := open()
	defer db.Close()

	ctx := wh.New(wsql.New(db))
	defer ctx.Close()

	// Insert: stage two new rows, then flush in one unit of work.
	alice := &User{Name: "Alice", Email: "alice@acme.test", Age: 30}
	bob := &User{Name: "Bob", Email: "bob@acme.test", Age: 41}
	ctx.Add(alice, bob)
	must(ctx.Save())
	// Ids are assigned by the database; a batched insert need not follow add order.
	fmt.Printf("inserted: alice#%d bob#%d\n", alice.ID, bob.ID)

	// Read by primary key. The result is tracked as Unchanged.
	var u User
	must(ctx.Set(&u).Find(alice.ID))
	fmt.Printf("found: %s <%s> age %d\n", u.Name, u.Email, u.Age)

	// Mutate in memory. No explicit "update" call: the tracker snapshots the
	// original and, on Save, emits UPDATE "user" SET "age" = ? WHERE "id" = ?
	// touching only the column that changed.
	u.Age = 31
	must(ctx.Save())

	// Query with the type-safe DSL.
	var adults []User
	must(ctx.Set(&adults).
		Where(dsl.Gte(&u, &u.Age, 18)).
		OrderBy("age", query.Desc).
		All())
	fmt.Print("adults: ")
	for _, a := range adults {
		fmt.Printf("%s(%d) ", a.Name, a.Age)
	}
	fmt.Println()

	// Delete: stage the removal, then flush.
	ctx.Remove(bob)
	must(ctx.Save())

	var left []User
	must(ctx.Set(&left).All())
	fmt.Printf("remaining: %d row(s)\n", len(left))
}

func open() *sql.DB {
	db, err := sql.Open("sqlite", ":memory:")
	must(err)
	db.SetMaxOpenConns(1) // keep the in-memory schema on one connection
	_, err = db.Exec(`CREATE TABLE "user" (
		"id"    INTEGER PRIMARY KEY AUTOINCREMENT,
		"name"  TEXT NOT NULL,
		"email" TEXT NOT NULL,
		"age"   INTEGER NOT NULL)`)
	must(err)
	return db
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
