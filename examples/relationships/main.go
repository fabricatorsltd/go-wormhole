// Command relationships shows navigation fields and eager loading: a one-to-many
// collection, a one-to-one, and a belongs-to back-reference, each loaded with
// Include.
//
//	go run ./examples/relationships
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

// Author has many Books (1:N) and one Bio (1:1). Navigation fields are pointers
// or slices of pointers and are not stored as columns.
type Author struct {
	ID    int     `db:"column:id;primary_key"`
	Name  string  `db:"column:name"`
	Books []*Book `db:"fk:author_id"` // 1:N, FK author_id on book
	Bio   *Bio    `db:"fk:author_id"` // 1:1, FK author_id on bio
}

type Book struct {
	ID       int     `db:"column:id;primary_key"`
	AuthorID int     `db:"column:author_id"`
	Title    string  `db:"column:title"`
	Author   *Author `db:"ref"` // belongs-to, via author_id on this table
}

type Bio struct {
	ID       int    `db:"column:id;primary_key"`
	AuthorID int    `db:"column:author_id"`
	Summary  string `db:"column:summary"`
}

func init() {
	dsl.Register(Author{})
	dsl.Register(Book{})
	dsl.Register(Bio{})
}

func main() {
	db := open()
	defer db.Close()

	ctx := wh.New(wsql.New(db))
	defer ctx.Close()
	seed(ctx)

	// Eager-load the collection and the one-to-one in batched follow-up queries.
	var authors []*Author
	must(ctx.Set(&authors).
		Include("Books").
		Include("Bio").
		OrderBy("id", query.Asc).
		All())
	for _, a := range authors {
		fmt.Printf("%s (%s): ", a.Name, a.Bio.Summary)
		for _, b := range a.Books {
			fmt.Printf("%q ", b.Title)
		}
		fmt.Println()
	}

	// Belongs-to: load each book with its parent author.
	var books []*Book
	must(ctx.Set(&books).
		Include("Author").
		OrderBy("id", query.Asc).
		All())
	for _, b := range books {
		fmt.Printf("%q was written by %s\n", b.Title, b.Author.Name)
	}
}

func seed(ctx *wh.DbContext) {
	ctx.Add(
		&Author{ID: 1, Name: "Le Guin"},
		&Author{ID: 2, Name: "Borges"},
	)
	ctx.Add(
		&Bio{ID: 1, AuthorID: 1, Summary: "speculative fiction"},
		&Bio{ID: 2, AuthorID: 2, Summary: "labyrinths and mirrors"},
	)
	ctx.Add(
		&Book{ID: 1, AuthorID: 1, Title: "A Wizard of Earthsea"},
		&Book{ID: 2, AuthorID: 1, Title: "The Dispossessed"},
		&Book{ID: 3, AuthorID: 2, Title: "Ficciones"},
	)
	must(ctx.Save())
}

func open() *sql.DB {
	db, err := sql.Open("sqlite", ":memory:")
	must(err)
	db.SetMaxOpenConns(1)
	_, err = db.Exec(`
		CREATE TABLE "author" (
			"id"   INTEGER PRIMARY KEY,
			"name" TEXT NOT NULL);
		CREATE TABLE "book" (
			"id"        INTEGER PRIMARY KEY,
			"author_id" INTEGER NOT NULL,
			"title"     TEXT NOT NULL);
		CREATE TABLE "bio" (
			"id"        INTEGER PRIMARY KEY,
			"author_id" INTEGER NOT NULL,
			"summary"   TEXT NOT NULL);`)
	must(err)
	return db
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
