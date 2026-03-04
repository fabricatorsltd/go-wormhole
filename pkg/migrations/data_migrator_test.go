package migrations_test

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/glebarez/sqlite"

	"github.com/fabricatorsltd/go-wormhole/pkg/migrations"
	sqlprovider "github.com/fabricatorsltd/go-wormhole/pkg/sql"
)

func TestDataMigratorFullSyncSQLite(t *testing.T) {
	src, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()

	dst, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer dst.Close()

	_, err = src.Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
INSERT INTO users(id, name) VALUES (1, 'alice'), (2, 'bob');`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = dst.Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
INSERT INTO users(id, name) VALUES (1, 'old');`)
	if err != nil {
		t.Fatal(err)
	}

	srcProv := sqlprovider.New(src, sqlprovider.WithName("sqlite"))
	dstProv := sqlprovider.New(dst, sqlprovider.WithName("sqlite"))
	m := migrations.NewDataMigrator(srcProv, dstProv, nil).WithBatchSize(1)
	if err := m.FullSync(context.Background()); err != nil {
		t.Fatalf("full sync failed: %v", err)
	}

	rows, err := dst.Query(`SELECT id, name FROM users ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	type row struct {
		id   int
		name string
	}
	var got []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.name); err != nil {
			t.Fatal(err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	if len(got) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(got))
	}
	if got[0].id != 1 || got[0].name != "alice" {
		t.Fatalf("unexpected first row: %+v", got[0])
	}
	if got[1].id != 2 || got[1].name != "bob" {
		t.Fatalf("unexpected second row: %+v", got[1])
	}
}
