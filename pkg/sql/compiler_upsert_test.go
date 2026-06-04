package sql_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/fabricatorsltd/go-wormhole/pkg/model"
	"github.com/fabricatorsltd/go-wormhole/pkg/provider"
	wsql "github.com/fabricatorsltd/go-wormhole/pkg/sql"
)

func upsertMeta() *model.EntityMeta {
	meta := &model.EntityMeta{
		Name: "messages",
		GoType: reflect.TypeOf(struct {
			ID     string
			Hash   string
			Status string
		}{}),
		Fields: []model.FieldMeta{
			{FieldName: "ID", Column: "id", PrimaryKey: true},
			{FieldName: "Hash", Column: "hash"},
			{FieldName: "Status", Column: "status"},
		},
	}
	meta.PrimaryKey = &meta.Fields[0]
	meta.BuildIndex()
	return meta
}

func TestInsertOnConflict_DoNothing(t *testing.T) {
	c := &wsql.Compiler{}
	meta := upsertMeta()
	values := map[string]any{"ID": "m1", "Hash": "0xabc", "Status": "pending"}

	out := c.InsertOnConflict(meta, values, provider.ConflictClause{Columns: []string{"id"}})

	want := `INSERT INTO "messages" ("id", "hash", "status") VALUES (?, ?, ?) ON CONFLICT ("id") DO NOTHING`
	if out.SQL != want {
		t.Fatalf("SQL mismatch:\n got: %s\nwant: %s", out.SQL, want)
	}
	if len(out.Params) != 3 {
		t.Fatalf("params: want 3, got %d", len(out.Params))
	}
}

func TestInsertOnConflict_DoUpdate(t *testing.T) {
	c := &wsql.Compiler{}
	meta := upsertMeta()
	values := map[string]any{"ID": "m1", "Hash": "0xabc", "Status": "confirmed"}

	out := c.InsertOnConflict(meta, values, provider.ConflictClause{
		Columns: []string{"id"},
		Update:  []string{"hash", "status"},
	})

	want := `INSERT INTO "messages" ("id", "hash", "status") VALUES (?, ?, ?) ON CONFLICT ("id") DO UPDATE SET "hash" = EXCLUDED."hash", "status" = EXCLUDED."status"`
	if out.SQL != want {
		t.Fatalf("SQL mismatch:\n got: %s\nwant: %s", out.SQL, want)
	}
	if len(out.Params) != 3 {
		t.Fatalf("params: want 3, got %d", len(out.Params))
	}
}

func TestInsertOnConflict_MySQL_DoNothing(t *testing.T) {
	c := &wsql.Compiler{Backtick: true}
	meta := upsertMeta()
	values := map[string]any{"ID": "m1", "Hash": "0xabc", "Status": "pending"}

	out := c.InsertOnConflict(meta, values, provider.ConflictClause{Columns: []string{"id"}})

	want := "INSERT INTO `messages` (`id`, `hash`, `status`) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE `id` = `id`"
	if out.SQL != want {
		t.Fatalf("SQL mismatch:\n got: %s\nwant: %s", out.SQL, want)
	}
	if len(out.Params) != 3 {
		t.Fatalf("params: want 3, got %d", len(out.Params))
	}
}

func TestInsertOnConflict_MySQL_DoUpdate(t *testing.T) {
	c := &wsql.Compiler{Backtick: true}
	meta := upsertMeta()
	values := map[string]any{"ID": "m1", "Hash": "0xabc", "Status": "confirmed"}

	out := c.InsertOnConflict(meta, values, provider.ConflictClause{
		Columns: []string{"id"},
		Update:  []string{"hash", "status"},
	})

	want := "INSERT INTO `messages` (`id`, `hash`, `status`) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE `hash` = VALUES(`hash`), `status` = VALUES(`status`)"
	if out.SQL != want {
		t.Fatalf("SQL mismatch:\n got: %s\nwant: %s", out.SQL, want)
	}
	if len(out.Params) != 3 {
		t.Fatalf("params: want 3, got %d", len(out.Params))
	}
}

func TestInsertOnConflict_MSSQL_DoUpdate(t *testing.T) {
	c := &wsql.Compiler{AtPrefixed: true, BracketQuote: true, UseTOP: true}
	meta := upsertMeta()
	values := map[string]any{"ID": "m1", "Hash": "0xabc", "Status": "confirmed"}

	out := c.InsertOnConflict(meta, values, provider.ConflictClause{
		Columns: []string{"id"},
		Update:  []string{"hash", "status"},
	})

	want := `MERGE INTO [messages] WITH (HOLDLOCK) AS [tgt] USING (VALUES (@p1, @p2, @p3)) AS [src] ([id], [hash], [status]) ON [tgt].[id] = [src].[id] WHEN MATCHED THEN UPDATE SET [hash] = [src].[hash], [status] = [src].[status] WHEN NOT MATCHED THEN INSERT ([id], [hash], [status]) VALUES ([src].[id], [src].[hash], [src].[status]);`
	if out.SQL != want {
		t.Fatalf("SQL mismatch:\n got: %s\nwant: %s", out.SQL, want)
	}
	if len(out.Params) != 3 {
		t.Fatalf("params: want 3, got %d", len(out.Params))
	}
}

func TestInsertOnConflict_MSSQL_DoNothing(t *testing.T) {
	c := &wsql.Compiler{AtPrefixed: true, BracketQuote: true, UseTOP: true}
	meta := upsertMeta()
	values := map[string]any{"ID": "m1", "Hash": "0xabc", "Status": "pending"}

	out := c.InsertOnConflict(meta, values, provider.ConflictClause{Columns: []string{"id"}})

	want := `MERGE INTO [messages] WITH (HOLDLOCK) AS [tgt] USING (VALUES (@p1, @p2, @p3)) AS [src] ([id], [hash], [status]) ON [tgt].[id] = [src].[id] WHEN NOT MATCHED THEN INSERT ([id], [hash], [status]) VALUES ([src].[id], [src].[hash], [src].[status]);`
	if out.SQL != want {
		t.Fatalf("SQL mismatch:\n got: %s\nwant: %s", out.SQL, want)
	}
}

// HOLDLOCK must be present on every MERGE: without the range lock two
// concurrent merges can both fall through to WHEN NOT MATCHED and double-insert,
// defeating the upsert. This is the assertion a single-threaded shape test would
// otherwise miss.
func TestInsertOnConflict_MSSQL_HasHoldlock(t *testing.T) {
	c := &wsql.Compiler{AtPrefixed: true, BracketQuote: true, UseTOP: true}
	meta := upsertMeta()
	values := map[string]any{"ID": "m1", "Hash": "0xabc", "Status": "x"}

	out := c.InsertOnConflict(meta, values, provider.ConflictClause{
		Columns: []string{"id"},
		Update:  []string{"hash"},
	})

	if !strings.Contains(out.SQL, "WITH (HOLDLOCK)") {
		t.Fatalf("MERGE missing WITH (HOLDLOCK):\n%s", out.SQL)
	}
}

// upsertMetaWithDefault adds a `default:`-tagged column so a zero value drops it
// from the insert column set (insertColumns), exercising the source-column union
// in the MERGE path.
func upsertMetaWithDefault() *model.EntityMeta {
	meta := &model.EntityMeta{
		Name: "messages",
		GoType: reflect.TypeOf(struct {
			ID     string
			Hash   string
			Status string
		}{}),
		Fields: []model.FieldMeta{
			{FieldName: "ID", Column: "id", PrimaryKey: true},
			{FieldName: "Hash", Column: "hash"},
			{FieldName: "Status", Column: "status", Tags: map[string]string{"default": "'active'"}},
		},
	}
	meta.PrimaryKey = &meta.Fields[0]
	meta.BuildIndex()
	return meta
}

// Discriminator for the MERGE source-column union: when an Update column was
// dropped from the insert set (zero-valued default), [src] must still declare it
// so [src].status resolves. Before the fix the derived table omitted it and SQL
// Server raised a bind error. The INSERT branch still omits the dropped column
// so the DB default applies.
func TestInsertOnConflict_MSSQL_UpdatesDroppedDefaultColumn(t *testing.T) {
	c := &wsql.Compiler{AtPrefixed: true, BracketQuote: true, UseTOP: true}
	meta := upsertMetaWithDefault()
	// Status is zero, so it drops out of the INSERT set but is requested in Update.
	values := map[string]any{"ID": "m1", "Hash": "0xabc", "Status": ""}

	out := c.InsertOnConflict(meta, values, provider.ConflictClause{
		Columns: []string{"id"},
		Update:  []string{"hash", "status"},
	})

	want := `MERGE INTO [messages] WITH (HOLDLOCK) AS [tgt] USING (VALUES (@p1, @p2, @p3)) AS [src] ([id], [hash], [status]) ON [tgt].[id] = [src].[id] WHEN MATCHED THEN UPDATE SET [hash] = [src].[hash], [status] = [src].[status] WHEN NOT MATCHED THEN INSERT ([id], [hash]) VALUES ([src].[id], [src].[hash]);`
	if out.SQL != want {
		t.Fatalf("SQL mismatch:\n got: %s\nwant: %s", out.SQL, want)
	}
	// Source row carries all three columns in order, including the dropped one.
	wantParams := []any{"m1", "0xabc", ""}
	if len(out.Params) != len(wantParams) {
		t.Fatalf("params: want %d, got %d", len(wantParams), len(out.Params))
	}
	for i := range wantParams {
		if out.Params[i] != wantParams[i] {
			t.Errorf("param %d: got %v, want %v", i, out.Params[i], wantParams[i])
		}
	}
}

// A composite conflict target renders an AND-joined ON clause.
func TestInsertOnConflict_MSSQL_CompositeTarget(t *testing.T) {
	c := &wsql.Compiler{AtPrefixed: true, BracketQuote: true, UseTOP: true}
	meta := upsertMeta()
	values := map[string]any{"ID": "m1", "Hash": "0xabc", "Status": "x"}

	out := c.InsertOnConflict(meta, values, provider.ConflictClause{
		Columns: []string{"id", "hash"},
		Update:  []string{"status"},
	})

	if !strings.Contains(out.SQL, "ON [tgt].[id] = [src].[id] AND [tgt].[hash] = [src].[hash]") {
		t.Fatalf("composite ON clause not AND-joined:\n%s", out.SQL)
	}
}
