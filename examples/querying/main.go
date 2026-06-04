// Command querying covers the read side: DSL predicates, ordering, pagination,
// joins, and grouped aggregates scanned into a result struct.
//
//	go run ./examples/querying
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

type Customer struct {
	ID      int    `db:"column:id;primary_key;auto_increment"`
	Name    string `db:"column:name"`
	Country string `db:"column:country"`
	Age     int    `db:"column:age"`
}

type Order struct {
	ID         int `db:"column:id;primary_key;auto_increment"`
	CustomerID int `db:"column:customer_id"`
	Amount     int `db:"column:amount"`
}

// Revenue is a projection target for the grouped aggregate query below.
type Revenue struct {
	CustomerID int `db:"column:customer_id"`
	Total      int `db:"column:total"`
}

func init() {
	dsl.Register(Customer{})
	dsl.Register(Order{})
}

func main() {
	db := open()
	defer db.Close()

	ctx := wh.New(wsql.New(db))
	defer ctx.Close()
	anna, bjorn, mara := seed(ctx)
	name := map[int]string{anna.ID: "Anna", bjorn.ID: "Bjorn", mara.ID: "Mara"}

	c := &Customer{}

	// Comparison + set membership.
	var nordics []Customer
	must(ctx.Set(&nordics).
		Where(dsl.In(c, &c.Country, "SE", "NO", "FI")).
		OrderBy("name", query.Asc).
		All())
	fmt.Print("nordic customers: ")
	for _, n := range nordics {
		fmt.Printf("%s ", n.Name)
	}
	fmt.Println()

	// Substring match + pagination (skip 1, take 1).
	var page []Customer
	must(ctx.Set(&page).
		Where(dsl.Contains(c, &c.Name, "a")).
		OrderBy("name", query.Asc).
		Offset(1).Limit(1).
		All())
	fmt.Printf("name contains 'a', page 2: %s\n", page[0].Name)

	// Join orders to customers and keep only the big spenders.
	o := &Order{}
	var big []Customer
	must(ctx.Set(&big).
		Join("order", dsl.JoinEq(o, &o.CustomerID, c, &c.ID)). // ON order.customer_id = customer.id
		Where(dsl.Gt(o, &o.Amount, 500)).
		Distinct().
		OrderBy("name", query.Asc).
		All())
	fmt.Print("customers with an order over 500: ")
	for _, b := range big {
		fmt.Printf("%s ", b.Name)
	}
	fmt.Println()

	// Grouped aggregate: total spend per customer, keep totals over 100.
	// HAVING references the aggregate alias, so it is a raw predicate.
	var revenue []Revenue
	must(ctx.Set(&revenue).From("order").
		GroupBy("customer_id").
		Aggregate(query.AggSum, "amount", "total").
		Having(query.Predicate{Field: "total", Op: query.OpGt, Value: 100}).
		OrderBy("total", query.Desc).
		All())
	for _, r := range revenue {
		fmt.Printf("%s total spend: %d\n", name[r.CustomerID], r.Total)
	}
}

// seed inserts the customers, then the orders. Auto-increment keys are
// assigned by the database and written back onto the structs by Save, so the
// orders reference the real ids rather than guessed ones.
func seed(ctx *wh.DbContext) (anna, bjorn, mara *Customer) {
	anna = &Customer{Name: "Anna", Country: "SE", Age: 34}
	bjorn = &Customer{Name: "Bjorn", Country: "NO", Age: 52}
	mara = &Customer{Name: "Mara", Country: "IT", Age: 29}
	ctx.Add(anna, bjorn, mara)
	must(ctx.Save())

	ctx.Add(
		&Order{CustomerID: anna.ID, Amount: 800},
		&Order{CustomerID: anna.ID, Amount: 150},
		&Order{CustomerID: bjorn.ID, Amount: 300},
		&Order{CustomerID: mara.ID, Amount: 600},
	)
	must(ctx.Save())
	return anna, bjorn, mara
}

func open() *sql.DB {
	db, err := sql.Open("sqlite", ":memory:")
	must(err)
	db.SetMaxOpenConns(1)
	_, err = db.Exec(`
		CREATE TABLE "customer" (
			"id"      INTEGER PRIMARY KEY AUTOINCREMENT,
			"name"    TEXT NOT NULL,
			"country" TEXT NOT NULL,
			"age"     INTEGER NOT NULL);
		CREATE TABLE "order" (
			"id"          INTEGER PRIMARY KEY AUTOINCREMENT,
			"customer_id" INTEGER NOT NULL,
			"amount"      INTEGER NOT NULL);`)
	must(err)
	return db
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
