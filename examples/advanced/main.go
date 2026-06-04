// Command advanced shows the richer query shapes: DISTINCT projections,
// subquery filters, set operations, and CASE expressions in the SELECT list
// and ORDER BY.
//
//	go run ./examples/advanced
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

// Explicit primary keys keep the seed data deterministic across runs.
type Employee struct {
	ID         int    `db:"column:id;primary_key"`
	Name       string `db:"column:name"`
	Department string `db:"column:department"`
	Salary     int    `db:"column:salary"`
}

type Project struct {
	ID         int    `db:"column:id;primary_key"`
	EmployeeID int    `db:"column:employee_id"`
	Name       string `db:"column:name"`
}

// dept receives a single projected column.
type dept struct {
	Department string `db:"column:department"`
}

// band receives a name plus a computed CASE label.
type band struct {
	Name string `db:"column:name"`
	Tier string `db:"column:tier"`
}

func init() {
	dsl.Register(Employee{})
	dsl.Register(Project{})
}

func main() {
	db := open()
	defer db.Close()

	ctx := wh.New(wsql.New(db))
	defer ctx.Close()
	seed(ctx)

	e := &Employee{}

	// DISTINCT over a single projected column: the set of departments.
	var depts []dept
	must(ctx.Set(&depts).From("employee").
		Select("department").
		Distinct().
		OrderBy("department", query.Asc).
		All())
	fmt.Print("departments: ")
	for _, d := range depts {
		fmt.Printf("%s ", d.Department)
	}
	fmt.Println()

	// Subquery: employees who lead at least one project.
	// WHERE id IN (SELECT employee_id FROM project)
	leadIDs := query.From("project").Select("employee_id").Build()
	var leads []Employee
	must(ctx.Set(&leads).
		Where(dsl.InSub(e, &e.ID, leadIDs)).
		OrderBy("name", query.Asc).
		All())
	fmt.Print("project leads: ")
	for _, l := range leads {
		fmt.Printf("%s ", l.Name)
	}
	fmt.Println()

	// The complement, with a NOT IN subquery: employees leading nothing.
	var idle []Employee
	must(ctx.Set(&idle).
		Where(dsl.NotInSub(e, &e.ID, leadIDs)).
		OrderBy("name", query.Asc).
		All())
	fmt.Print("no project: ")
	for _, i := range idle {
		fmt.Printf("%s ", i.Name)
	}
	fmt.Println()

	// Set operation: engineers UNION the high earners (>= 120k), de-duplicated.
	var combined []Employee
	must(ctx.Set(&combined).
		Where(dsl.Eq(e, &e.Department, "Engineering")).
		Union(ctx.Set(&Employee{}).Where(dsl.Gte(e, &e.Salary, 120000))).
		All())
	fmt.Printf("engineers or high earners: %d distinct people\n", len(combined))

	// EXCEPT: engineers who are not high earners.
	var modestEng []Employee
	must(ctx.Set(&modestEng).
		Where(dsl.Eq(e, &e.Department, "Engineering")).
		Except(ctx.Set(&Employee{}).Where(dsl.Gte(e, &e.Salary, 120000))).
		All())
	fmt.Printf("engineers below 120k: %d\n", len(modestEng))

	// CASE in the SELECT list computes a per-row salary tier label. A second,
	// numeric CASE drives ORDER BY so seniors sort first (rank 0), then mid,
	// then junior, with name as the tie-breaker.
	tier := dsl.Case().
		When(dsl.Gte(e, &e.Salary, 120000), "senior").
		When(dsl.Gte(e, &e.Salary, 90000), "mid").
		Else("junior")
	rank := dsl.Case().
		When(dsl.Gte(e, &e.Salary, 120000), 0).
		When(dsl.Gte(e, &e.Salary, 90000), 1).
		Else(2)
	var bands []band
	must(ctx.Set(&bands).From("employee").
		Select("name").
		SelectCase(tier, "tier").
		OrderByCase(rank, query.Asc).
		OrderBy("name", query.Asc).
		All())
	for _, b := range bands {
		fmt.Printf("%-6s %s\n", b.Name, b.Tier)
	}
}

func seed(ctx *wh.DbContext) {
	ctx.Add(
		&Employee{ID: 1, Name: "Ada", Department: "Engineering", Salary: 130000},
		&Employee{ID: 2, Name: "Ben", Department: "Engineering", Salary: 95000},
		&Employee{ID: 3, Name: "Cleo", Department: "Sales", Salary: 80000},
		&Employee{ID: 4, Name: "Dan", Department: "Sales", Salary: 125000},
	)
	must(ctx.Save())
	ctx.Add(
		&Project{ID: 1, EmployeeID: 1, Name: "Compiler"},
		&Project{ID: 2, EmployeeID: 4, Name: "Pipeline"},
	)
	must(ctx.Save())
}

func open() *sql.DB {
	db, err := sql.Open("sqlite", ":memory:")
	must(err)
	db.SetMaxOpenConns(1)
	_, err = db.Exec(`
		CREATE TABLE "employee" (
			"id"         INTEGER PRIMARY KEY,
			"name"       TEXT NOT NULL,
			"department" TEXT NOT NULL,
			"salary"     INTEGER NOT NULL);
		CREATE TABLE "project" (
			"id"          INTEGER PRIMARY KEY,
			"employee_id" INTEGER NOT NULL,
			"name"        TEXT NOT NULL);`)
	must(err)
	return db
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
