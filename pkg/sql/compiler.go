package sql

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/fabricatorsltd/go-wormhole/pkg/model"
	"github.com/fabricatorsltd/go-wormhole/pkg/provider"
	"github.com/fabricatorsltd/go-wormhole/pkg/query"
)

// Compiled holds a parameterized SQL statement ready for execution.
type Compiled struct {
	SQL    string
	Params []any
}

// Compiler translates query AST nodes into parameterized SQL.
type Compiler struct {
	// Placeholder style: "?" (mysql/sqlite) or "$N" (postgres).
	// Default is "?".
	Numbered bool

	// AtPrefixed uses @p1, @p2 style placeholders (MSSQL).
	AtPrefixed bool

	// BracketQuote uses [brackets] instead of "double quotes" (MSSQL).
	BracketQuote bool

	// Backtick uses `backticks` for identifiers (MySQL). It also serves as
	// MySQL's dialect marker: MySQL shares the "?" placeholder with SQLite, so
	// the quoting flag is what tells the two apart when emitting dialect SQL.
	Backtick bool

	// UseTOP uses SELECT TOP N instead of LIMIT N (MSSQL).
	UseTOP bool
}

// dialect names the SQL dialect this compiler targets, derived from the
// placeholder/quoting flags. It is the single place that maps the low-level
// flags to a dialect identity, used where the emitted SQL diverges by backend
// (e.g. upsert). Postgres is also implied by Numbered at provider.go's RETURNING
// checks; the same flag convention is centralized here.
func (c *Compiler) dialect() string {
	switch {
	case c.AtPrefixed:
		return "mssql"
	case c.Backtick:
		return "mysql"
	case c.Numbered:
		return "postgres"
	default:
		return "sqlite"
	}
}

// Select compiles a query.Query into a SELECT statement.
func (c *Compiler) Select(meta *model.EntityMeta, q query.Query) Compiled {
	var b strings.Builder
	var params []any

	// SELECT columns (with optional DISTINCT and TOP for MSSQL)
	b.WriteString("SELECT ")
	// DISTINCT applies to row projection, not to an aggregate result (where it
	// would be redundant alongside GROUP BY); skip it in the aggregate branch.
	if q.Distinct && len(q.Aggregates) == 0 {
		b.WriteString("DISTINCT ")
	}
	if c.UseTOP && q.Limit > 0 && q.Offset == 0 {
		b.WriteString(fmt.Sprintf("TOP %d ", q.Limit))
	}

	if len(q.Aggregates) > 0 {
		// Aggregate query: emit GROUP BY fields first, then aggregate expressions.
		first := true
		for _, field := range q.GroupBy {
			if !first {
				b.WriteString(", ")
			}
			b.WriteString(c.quote(fieldColumn(meta, field)))
			first = false
		}
		for _, agg := range q.Aggregates {
			if !first {
				b.WriteString(", ")
			}
			b.WriteString(c.compileAggregate(meta, agg))
			first = false
		}
	} else {
		// Qualify SELECT-list columns with the source table when the query
		// has joins, so unqualified column references don't become ambiguous.
		// Single-table queries stay unqualified for readability + back-compat.
		fromTable := q.EntityName
		if fromTable == "" {
			fromTable = meta.Name
		}
		qualifySelect := len(q.Joins) > 0
		// Projected subset (Select) takes precedence; otherwise every mapped
		// field. Projected names may be field or column names, resolved the same
		// way as ORDER BY / GROUP BY.
		cols := make([]string, 0, len(meta.Fields))
		if len(q.Columns) > 0 {
			for _, name := range q.Columns {
				cols = append(cols, fieldColumn(meta, name))
			}
		} else {
			for _, f := range meta.Fields {
				cols = append(cols, f.Column)
			}
		}
		for i, col := range cols {
			if i > 0 {
				b.WriteString(", ")
			}
			if qualifySelect {
				b.WriteString(c.quote(fromTable))
				b.WriteString(".")
			}
			b.WriteString(c.quote(col))
		}
	}

	// FROM — use the entity name carried in the query so that aggregate
	// queries can pass a custom result-struct meta without affecting the table.
	b.WriteString(" FROM ")
	entityName := q.EntityName
	if entityName == "" {
		entityName = meta.Name
	}
	b.WriteString(c.quote(entityName))

	// JOINs (INNER / LEFT / RIGHT / FULL)
	for _, j := range q.Joins {
		b.WriteString(" ")
		b.WriteString(j.Type.Keyword())
		b.WriteString(" ")
		b.WriteString(c.quote(j.Entity))
		b.WriteString(" ON ")
		c.compileNode(&b, &params, j.On)
	}

	// WHERE
	if q.Where != nil {
		b.WriteString(" WHERE ")
		c.compileNode(&b, &params, q.Where)
	}

	// GROUP BY
	if len(q.GroupBy) > 0 {
		b.WriteString(" GROUP BY ")
		for i, field := range q.GroupBy {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(c.quote(fieldColumn(meta, field)))
		}
	}

	// HAVING
	if q.Having != nil {
		b.WriteString(" HAVING ")
		c.compileNode(&b, &params, q.Having)
	}

	// ORDER BY
	for i, s := range q.OrderBy {
		if i == 0 {
			b.WriteString(" ORDER BY ")
		} else {
			b.WriteString(", ")
		}
		if s.Case != nil {
			c.compileCaseExpr(&b, &params, meta, *s.Case)
		} else {
			col := fieldColumn(meta, s.Field)
			b.WriteString(c.quote(col))
		}
		if s.Dir == query.Desc {
			b.WriteString(" DESC")
		} else {
			b.WriteString(" ASC")
		}
	}

	// LIMIT / OFFSET
	if c.UseTOP {
		// MSSQL: OFFSET...FETCH when both limit and offset are set
		if q.Offset > 0 {
			b.WriteString(fmt.Sprintf(" OFFSET %d ROWS", q.Offset))
			if q.Limit > 0 {
				b.WriteString(fmt.Sprintf(" FETCH NEXT %d ROWS ONLY", q.Limit))
			}
		}
		// TOP-only case already handled above
	} else {
		if q.Limit > 0 {
			b.WriteString(fmt.Sprintf(" LIMIT %d", q.Limit))
		}
		if q.Offset > 0 {
			b.WriteString(fmt.Sprintf(" OFFSET %d", q.Offset))
		}
	}

	return Compiled{SQL: b.String(), Params: params}
}

// Insert compiles an INSERT statement for all fields of an entity.
//
// Auto-increment PKs are always omitted (DB generates them). Fields
// declared with `default:<value>` in the struct tag are also omitted
// when their value equals the Go zero-value for the field type — the
// caller hasn't set them, so let the column DEFAULT in the DB apply.
// This matches EF Core's HasDefaultValue sentinel behaviour and
// Hibernate's @DynamicInsert: when an ORM-tracked default exists,
// "unset" means "use the DB default", not "write empty string / 0".
//
// Callers who explicitly want to write the zero-value (e.g. an empty
// string is a legitimate non-default value in the domain) should not
// declare a `default:` on that field.
func (c *Compiler) Insert(meta *model.EntityMeta, values map[string]any) Compiled {
	fields := insertColumns(meta, values)
	cols := make([]string, len(fields))
	placeholders := make([]string, len(fields))
	params := make([]any, len(fields))
	for i, f := range fields {
		cols[i] = c.quote(f.Column)
		placeholders[i] = c.ph(i + 1)
		params[i] = values[f.FieldName]
	}

	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		c.quote(meta.Name),
		strings.Join(cols, ", "),
		strings.Join(placeholders, ", "),
	)
	return Compiled{SQL: sql, Params: params}
}

// InsertMany compiles a single multi-row INSERT for several entities that
// share the same emitted column set. The caller MUST group rows by
// insertColumnKey first: every map in `rows` has to produce the same columns,
// otherwise the VALUES tuples would not line up. No RETURNING is emitted, so
// this is for client-assigned primary keys only (the flush path enforces that).
func (c *Compiler) InsertMany(meta *model.EntityMeta, rows []map[string]any) Compiled {
	if len(rows) == 0 {
		return Compiled{}
	}
	fields := insertColumns(meta, rows[0])
	cols := make([]string, len(fields))
	for i, f := range fields {
		cols[i] = c.quote(f.Column)
	}

	params := make([]any, 0, len(rows)*len(fields))
	tuples := make([]string, len(rows))
	idx := 1
	for r, row := range rows {
		ph := make([]string, len(fields))
		for i, f := range fields {
			ph[i] = c.ph(idx)
			params = append(params, row[f.FieldName])
			idx++
		}
		tuples[r] = "(" + strings.Join(ph, ", ") + ")"
	}

	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		c.quote(meta.Name),
		strings.Join(cols, ", "),
		strings.Join(tuples, ", "),
	)
	return Compiled{SQL: sql, Params: params}
}

// insertColumns returns the fields an INSERT writes for the given entity
// values, in declaration order. Auto-increment PKs are always skipped (the DB
// generates them); `default:`-tagged fields are skipped when their value is the
// Go zero value so the column DEFAULT applies. This is the single source of
// truth for which columns an INSERT carries, shared by Insert and InsertMany.
func insertColumns(meta *model.EntityMeta, values map[string]any) []model.FieldMeta {
	out := make([]model.FieldMeta, 0, len(meta.Fields))
	for _, f := range meta.Fields {
		if f.AutoIncr {
			continue
		}
		if _, hasDefault := f.Tags["default"]; hasDefault {
			if isZeroValue(values[f.FieldName]) {
				continue
			}
		}
		out = append(out, f)
	}
	return out
}

// insertColumnKey returns a stable key identifying the emitted column set for
// an entity's values. Two entities batch into one multi-row INSERT only when
// their keys match, because `default:`-tagged columns drop out per row when
// zero and a single statement needs one shared column list.
func insertColumnKey(meta *model.EntityMeta, values map[string]any) string {
	fields := insertColumns(meta, values)
	names := make([]string, len(fields))
	for i, f := range fields {
		names[i] = f.Column
	}
	return strings.Join(names, ",")
}

// InsertOnConflict compiles an insert-or-update for the compiler's dialect. An
// empty conflict.Update means "leave the existing row untouched"; otherwise the
// listed columns are overwritten from the proposed row.
//
//   - PostgreSQL / SQLite: INSERT ... ON CONFLICT (cols) DO NOTHING | DO UPDATE.
//   - MySQL:               INSERT ... ON DUPLICATE KEY UPDATE.
//   - SQL Server:          MERGE ... WHEN MATCHED / WHEN NOT MATCHED.
//
// Note the MySQL divergence documented on provider.ConflictClause: MySQL fires
// on any unique/PK index and ignores the conflict target columns.
func (c *Compiler) InsertOnConflict(meta *model.EntityMeta, values map[string]any, conflict provider.ConflictClause) Compiled {
	switch c.dialect() {
	case "mysql":
		return c.insertOnDuplicateKey(meta, values, conflict)
	case "mssql":
		return c.mergeUpsert(meta, values, conflict)
	default:
		return c.insertOnConflictStandard(meta, values, conflict)
	}
}

// insertOnConflictStandard emits the PostgreSQL / SQLite ON CONFLICT form,
// referencing the proposed row through the EXCLUDED pseudo-table.
func (c *Compiler) insertOnConflictStandard(meta *model.EntityMeta, values map[string]any, conflict provider.ConflictClause) Compiled {
	base := c.Insert(meta, values)

	var b strings.Builder
	b.WriteString(base.SQL)
	b.WriteString(" ON CONFLICT (")
	for i, col := range conflict.Columns {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(c.quote(col))
	}
	b.WriteString(") ")

	if len(conflict.Update) == 0 {
		b.WriteString("DO NOTHING")
	} else {
		b.WriteString("DO UPDATE SET ")
		for i, col := range conflict.Update {
			if i > 0 {
				b.WriteString(", ")
			}
			q := c.quote(col)
			b.WriteString(q)
			b.WriteString(" = EXCLUDED.")
			b.WriteString(q)
		}
	}

	return Compiled{SQL: b.String(), Params: base.Params}
}

// insertOnDuplicateKey emits the MySQL ON DUPLICATE KEY UPDATE form. MySQL
// triggers on any unique or primary-key collision and cannot target specific
// conflict columns, so conflict.Columns is used only to pick a no-op column for
// the leave-untouched case. Updated columns read the proposed value through the
// VALUES() function, which is kept (over the 8.0.19+ aliased form) for
// compatibility with MySQL 5.7 and early 8.0.
func (c *Compiler) insertOnDuplicateKey(meta *model.EntityMeta, values map[string]any, conflict provider.ConflictClause) Compiled {
	base := c.Insert(meta, values)

	var b strings.Builder
	b.WriteString(base.SQL)
	b.WriteString(" ON DUPLICATE KEY UPDATE ")

	if len(conflict.Update) == 0 {
		// MySQL has no DO NOTHING; assign a key column to itself as a no-op.
		q := c.quote(c.conflictAnchor(meta, conflict))
		b.WriteString(q)
		b.WriteString(" = ")
		b.WriteString(q)
	} else {
		for i, col := range conflict.Update {
			if i > 0 {
				b.WriteString(", ")
			}
			q := c.quote(col)
			b.WriteString(q)
			b.WriteString(" = VALUES(")
			b.WriteString(q)
			b.WriteString(")")
		}
	}

	return Compiled{SQL: b.String(), Params: base.Params}
}

// mergeUpsert emits the SQL Server MERGE form. WITH (HOLDLOCK) takes a range
// lock on the target so two concurrent merges cannot both fall through to
// WHEN NOT MATCHED and double-insert; without it MERGE is not a safe upsert.
// An empty conflict.Update omits the WHEN MATCHED clause (leave-untouched).
//
// Unlike Postgres EXCLUDED and MySQL VALUES(), the [src] derived table only
// exposes the columns it lists, so it must carry every column referenced by the
// ON clause or the UPDATE SET, not just the inserted ones. The insert column
// set still honours default-dropping (insertColumns), so WHEN NOT MATCHED lets
// the DB DEFAULT apply; the source row is the union of insert, match, and update
// columns so no [src].col is ever unbound.
func (c *Compiler) mergeUpsert(meta *model.EntityMeta, values map[string]any, conflict provider.ConflictClause) Compiled {
	insertFields := insertColumns(meta, values)

	// With no explicit conflict target, match on the full primary key (every
	// column for a composite key), not just the first.
	match := conflict.Columns
	if len(match) == 0 {
		match = c.keyColumns(meta)
	}

	// Source columns: insert columns first, then any match/update column not
	// already present, so every [src].col below resolves.
	srcFields := append([]model.FieldMeta(nil), insertFields...)
	seen := make(map[string]bool, len(srcFields))
	for _, f := range srcFields {
		seen[f.Column] = true
	}
	addCol := func(col string) {
		if col == "" || seen[col] {
			return
		}
		if f := meta.FieldByColumn(col); f != nil {
			srcFields = append(srcFields, *f)
			seen[col] = true
		}
	}
	for _, col := range match {
		addCol(col)
	}
	for _, col := range conflict.Update {
		addCol(col)
	}

	params := make([]any, len(srcFields))
	placeholders := make([]string, len(srcFields))
	srcDecl := make([]string, len(srcFields))
	for i, f := range srcFields {
		params[i] = values[f.FieldName]
		placeholders[i] = c.ph(i + 1)
		srcDecl[i] = c.quote(f.Column)
	}

	insertCols := make([]string, len(insertFields))
	insertVals := make([]string, len(insertFields))
	for i, f := range insertFields {
		insertCols[i] = c.quote(f.Column)
		insertVals[i] = "[src]." + c.quote(f.Column)
	}

	on := make([]string, len(match))
	for i, col := range match {
		q := c.quote(col)
		on[i] = "[tgt]." + q + " = [src]." + q
	}

	var b strings.Builder
	fmt.Fprintf(&b, "MERGE INTO %s WITH (HOLDLOCK) AS [tgt] USING (VALUES (%s)) AS [src] (%s) ON %s",
		c.quote(meta.Name),
		strings.Join(placeholders, ", "),
		strings.Join(srcDecl, ", "),
		strings.Join(on, " AND "),
	)
	if len(conflict.Update) > 0 {
		sets := make([]string, len(conflict.Update))
		for i, col := range conflict.Update {
			q := c.quote(col)
			sets[i] = q + " = [src]." + q
		}
		fmt.Fprintf(&b, " WHEN MATCHED THEN UPDATE SET %s", strings.Join(sets, ", "))
	}
	fmt.Fprintf(&b, " WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s);",
		strings.Join(insertCols, ", "),
		strings.Join(insertVals, ", "),
	)

	return Compiled{SQL: b.String(), Params: params}
}

// conflictAnchor returns a single column to anchor a no-op update or a missing
// merge match: the first declared conflict column, falling back to the primary
// key.
func (c *Compiler) conflictAnchor(meta *model.EntityMeta, conflict provider.ConflictClause) string {
	if len(conflict.Columns) > 0 {
		return conflict.Columns[0]
	}
	if meta.PrimaryKey != nil {
		return meta.PrimaryKey.Column
	}
	if len(meta.Fields) > 0 {
		return meta.Fields[0].Column
	}
	return ""
}

// isZeroValue reports whether v is the Go zero-value for its type.
// Used by Insert to decide whether to honour a DB column default.
//
// nil interface{} is zero. For everything else we reflect: nil pointer
// is zero, IsZero() covers the rest (empty string, 0, false, zero
// time.Time, empty slice header, etc.). Slices and maps with nil
// underlying storage are zero; populated empty containers like `[]int{}`
// are not — they were explicitly constructed by the caller.
func isZeroValue(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Ptr && rv.IsNil() {
		return true
	}
	return rv.IsZero()
}

// Update compiles a partial UPDATE for only the specified changed fields.
func (c *Compiler) Update(meta *model.EntityMeta, values map[string]any, changed []string, pkValue any) Compiled {
	var sets []string
	var params []any
	idx := 1

	changedSet := make(map[string]struct{}, len(changed))
	for _, ch := range changed {
		changedSet[ch] = struct{}{}
	}

	for _, f := range meta.Fields {
		if _, ok := changedSet[f.FieldName]; !ok {
			continue
		}
		// The version column is bumped server-side below, never set directly.
		if meta.Version != nil && f.FieldName == meta.Version.FieldName {
			continue
		}
		sets = append(sets, fmt.Sprintf("%s = %s", c.quote(f.Column), c.ph(idx)))
		params = append(params, values[f.FieldName])
		idx++
	}

	// Optimistic concurrency: increment the version column and guard the WHERE
	// on its current value, so a stale write matches zero rows.
	if meta.Version != nil {
		vcol := c.quote(meta.Version.Column)
		sets = append(sets, fmt.Sprintf("%s = %s + 1", vcol, vcol))
	}

	if len(sets) == 0 {
		return Compiled{}
	}

	var where strings.Builder
	idx = c.writeKeyWhere(&where, &params, meta, pkValue, idx)
	if meta.Version != nil {
		where.WriteString(fmt.Sprintf(" AND %s = %s", c.quote(meta.Version.Column), c.ph(idx)))
		params = append(params, values[meta.Version.FieldName])
	}

	sql := fmt.Sprintf("UPDATE %s SET %s WHERE %s",
		c.quote(meta.Name),
		strings.Join(sets, ", "),
		where.String(),
	)
	return Compiled{SQL: sql, Params: params}
}

// Delete compiles a DELETE by primary key.
func (c *Compiler) Delete(meta *model.EntityMeta, pkValue any) Compiled {
	var w strings.Builder
	var params []any
	c.writeKeyWhere(&w, &params, meta, pkValue, 1)
	sql := fmt.Sprintf("DELETE FROM %s WHERE %s", c.quote(meta.Name), w.String())
	return Compiled{SQL: sql, Params: params}
}

// keyColumns returns the entity's primary-key columns in declaration order,
// falling back to the singular PrimaryKey, then to "id".
func (c *Compiler) keyColumns(meta *model.EntityMeta) []string {
	if len(meta.PrimaryKeys) > 0 {
		cols := make([]string, len(meta.PrimaryKeys))
		for i, k := range meta.PrimaryKeys {
			cols[i] = k.Column
		}
		return cols
	}
	if meta.PrimaryKey != nil {
		return []string{meta.PrimaryKey.Column}
	}
	return []string{"id"}
}

// keyValues unpacks a primary-key argument into its component values: a []any is
// a composite key tuple, anything else is a single scalar key. A single-column
// key whose Go value is itself a []any (an exotic JSON/array PK) would be
// misread as composite; such a column should not be a primary key.
func keyValues(pkValue any) []any {
	if vs, ok := pkValue.([]any); ok {
		return vs
	}
	return []any{pkValue}
}

// writeKeyWhere appends "col1 = ?i AND col2 = ?i+1 ..." for the primary-key
// columns, starting at placeholder index startIdx, and appends the matching
// values (in column order) to params. It returns the next free placeholder
// index. For a single-column key this emits exactly the old single-clause WHERE.
func (c *Compiler) writeKeyWhere(w *strings.Builder, params *[]any, meta *model.EntityMeta, pkValue any, startIdx int) int {
	cols := c.keyColumns(meta)
	vals := keyValues(pkValue)
	for i, col := range cols {
		if i > 0 {
			w.WriteString(" AND ")
		}
		w.WriteString(c.quote(col))
		w.WriteString(" = ")
		w.WriteString(c.ph(startIdx))
		if i < len(vals) {
			*params = append(*params, vals[i])
		} else {
			*params = append(*params, nil)
		}
		startIdx++
	}
	return startIdx
}

// DeleteVersioned compiles a DELETE guarded on the optimistic-concurrency
// version column, so the row is removed only if it has not changed since it was
// loaded. Falls back to a plain Delete when the entity has no version column.
func (c *Compiler) DeleteVersioned(meta *model.EntityMeta, pkValue, version any) Compiled {
	if meta.Version == nil {
		return c.Delete(meta, pkValue)
	}
	var w strings.Builder
	var params []any
	idx := c.writeKeyWhere(&w, &params, meta, pkValue, 1)
	w.WriteString(" AND ")
	w.WriteString(c.quote(meta.Version.Column))
	w.WriteString(" = ")
	w.WriteString(c.ph(idx))
	params = append(params, version)
	sql := fmt.Sprintf("DELETE FROM %s WHERE %s", c.quote(meta.Name), w.String())
	return Compiled{SQL: sql, Params: params}
}

// DeleteWhere compiles a bulk DELETE … WHERE … from a query AST.
// The AST's OrderBy/Limit/Offset/Aggregates/GroupBy/Having are ignored
// because standard SQL DELETE does not support them portably.
// An empty Where clause emits an unconditional DELETE (full table wipe).
func (c *Compiler) DeleteWhere(meta *model.EntityMeta, q query.Query) Compiled {
	var b strings.Builder
	var params []any

	b.WriteString("DELETE FROM ")
	b.WriteString(c.quote(meta.Name))

	if q.Where != nil {
		b.WriteString(" WHERE ")
		c.compileNode(&b, &params, q.Where)
	}

	return Compiled{SQL: b.String(), Params: params}
}

// UpdateWhere compiles a bulk UPDATE ... SET ... WHERE ... from a query AST and
// a list of column assignments. SET columns are emitted first (so their
// placeholders precede the WHERE parameters), then the WHERE clause from the
// AST. OrderBy/Limit/Offset/Aggregates/GroupBy/Having are ignored, matching
// DeleteWhere. An empty Where clause updates every row.
func (c *Compiler) UpdateWhere(meta *model.EntityMeta, q query.Query, sets []query.Assignment) Compiled {
	var b strings.Builder
	var params []any

	b.WriteString("UPDATE ")
	b.WriteString(c.quote(meta.Name))
	b.WriteString(" SET ")
	for i, s := range sets {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(c.quote(s.Field))
		b.WriteString(" = ")
		b.WriteString(c.ph(len(params) + 1))
		params = append(params, s.Value)
	}

	if q.Where != nil {
		b.WriteString(" WHERE ")
		c.compileNode(&b, &params, q.Where)
	}

	return Compiled{SQL: b.String(), Params: params}
}

// FindByPK compiles a SELECT … WHERE pk = ? for a single entity.
func (c *Compiler) FindByPK(meta *model.EntityMeta, pkValue any) Compiled {
	var cols []string
	for _, f := range meta.Fields {
		cols = append(cols, c.quote(f.Column))
	}

	var b strings.Builder
	var params []any
	b.WriteString("SELECT ")
	if c.UseTOP {
		b.WriteString("TOP 1 ")
	}
	b.WriteString(strings.Join(cols, ", "))
	b.WriteString(" FROM ")
	b.WriteString(c.quote(meta.Name))
	b.WriteString(" WHERE ")
	c.writeKeyWhere(&b, &params, meta, pkValue, 1)
	if !c.UseTOP {
		b.WriteString(" LIMIT 1")
	}

	return Compiled{SQL: b.String(), Params: params}
}

// --- Join support ---

// SelectWithJoins compiles a query with LEFT JOIN clauses for eager-loaded relations.
func (c *Compiler) SelectWithJoins(meta *model.EntityMeta, q query.Query, joins []JoinSpec) Compiled {
	compiled := c.Select(meta, q)
	if len(joins) == 0 {
		return compiled
	}

	// Inject JOINs right after FROM table
	fromClause := fmt.Sprintf("FROM %s", c.quote(meta.Name))
	var joinSQL strings.Builder
	joinSQL.WriteString(fromClause)
	for _, j := range joins {
		joinSQL.WriteString(fmt.Sprintf(" LEFT JOIN %s ON %s.%s = %s.%s",
			c.quote(j.Table),
			c.quote(meta.Name), c.quote(j.LocalKey),
			c.quote(j.Table), c.quote(j.ForeignKey),
		))
	}

	compiled.SQL = strings.Replace(compiled.SQL, fromClause, joinSQL.String(), 1)
	return compiled
}

// JoinSpec describes a LEFT JOIN relationship.
type JoinSpec struct {
	Table      string // related table name
	LocalKey   string // column on the source table
	ForeignKey string // column on the related table
}

// --- internal ---

// compileAggregate renders a single aggregate expression, e.g. COUNT(*) AS "total".
func (c *Compiler) compileAggregate(meta *model.EntityMeta, agg query.Aggregate) string {
	var fn string
	switch agg.Func {
	case query.AggSum:
		fn = "SUM"
	case query.AggAvg:
		fn = "AVG"
	case query.AggMin:
		fn = "MIN"
	case query.AggMax:
		fn = "MAX"
	default: // AggCount
		fn = "COUNT"
	}

	field := agg.Field
	if field == "" || field == "*" {
		field = "*"
	} else {
		field = c.quote(fieldColumn(meta, field))
	}

	result := fmt.Sprintf("%s(%s)", fn, field)
	if agg.Alias != "" {
		result += " AS " + c.quote(agg.Alias)
	}
	return result
}

// compileCaseExpr renders CASE WHEN … THEN … [WHEN … THEN …] ELSE … END.
// Predicates inside WHEN reuse compileNode so all predicate ops are supported.
// Then/Else values are emitted as parameter placeholders so types preserve
// across drivers.
func (c *Compiler) compileCaseExpr(b *strings.Builder, params *[]any, meta *model.EntityMeta, ce query.CaseExpr) {
	b.WriteString("CASE")
	for _, branch := range ce.Branches {
		b.WriteString(" WHEN ")
		c.compileNode(b, params, branch.When)
		b.WriteString(" THEN ")
		b.WriteString(c.ph(len(*params) + 1))
		*params = append(*params, branch.Then)
	}
	if ce.Else != nil {
		b.WriteString(" ELSE ")
		b.WriteString(c.ph(len(*params) + 1))
		*params = append(*params, ce.Else)
	}
	b.WriteString(" END")
}

// writeColumnRef emits a single column reference, qualifying it with the
// table name when present. Single-table queries built via direct AST
// (Predicate.Table == "") render unqualified for back-compat; queries
// built via the pointer-tracking DSL always carry the source table and
// therefore render qualified — which is correct in joined queries and
// harmlessly verbose in single-table ones.
func (c *Compiler) writeColumnRef(b *strings.Builder, table, field string) {
	if table != "" {
		b.WriteString(c.quote(table))
		b.WriteString(".")
	}
	b.WriteString(c.quote(field))
}

func (c *Compiler) compileNode(b *strings.Builder, params *[]any, node query.Node) {
	switch n := node.(type) {
	case query.Predicate:
		c.compilePredicate(b, params, n)
	case query.Composite:
		c.compileComposite(b, params, n)
	case query.Subquery:
		c.compileSubquery(b, params, n)
	}
}

// compileSubquery renders an IN / NOT IN / EXISTS / NOT EXISTS predicate whose
// right side is a nested SELECT. The sub-SELECT is compiled into the same params
// slice, so placeholder numbering stays continuous across dialects.
func (c *Compiler) compileSubquery(b *strings.Builder, params *[]any, sq query.Subquery) {
	switch sq.Op {
	case query.OpExists, query.OpNotExists:
		if sq.Op == query.OpNotExists {
			b.WriteString("NOT ")
		}
		b.WriteString("EXISTS (")
		c.writeSubSelect(b, params, sq.Query, "1")
	default: // OpIn / OpNotIn
		c.writeColumnRef(b, "", sq.Field)
		if sq.Op == query.OpNotIn {
			b.WriteString(" NOT IN (")
		} else {
			b.WriteString(" IN (")
		}
		col := "1"
		if len(sq.Query.Columns) > 0 {
			col = c.quote(sq.Query.Columns[0])
		}
		c.writeSubSelect(b, params, sq.Query, col)
	}
	b.WriteString(")")
}

// writeSubSelect emits "SELECT <selectExpr> FROM <table> [WHERE ...]" for a
// nested query, appending its parameters to the shared slice.
func (c *Compiler) writeSubSelect(b *strings.Builder, params *[]any, q query.Query, selectExpr string) {
	b.WriteString("SELECT ")
	b.WriteString(selectExpr)
	b.WriteString(" FROM ")
	b.WriteString(c.quote(q.EntityName))
	if q.Where != nil {
		b.WriteString(" WHERE ")
		c.compileNode(b, params, q.Where)
	}
}

func (c *Compiler) compilePredicate(b *strings.Builder, params *[]any, p query.Predicate) {
	c.writeColumnRef(b, p.Table, p.Field)

	// If the right-hand side is itself a column reference (typical for JOIN ON
	// clauses), render "tableA"."colA" OP "tableB"."colB" with no placeholder.
	if cr, ok := p.Value.(query.ColumnRef); ok {
		switch p.Op {
		case query.OpEq:
			b.WriteString(" = ")
		case query.OpNeq:
			b.WriteString(" != ")
		case query.OpGt:
			b.WriteString(" > ")
		case query.OpGte:
			b.WriteString(" >= ")
		case query.OpLt:
			b.WriteString(" < ")
		case query.OpLte:
			b.WriteString(" <= ")
		default:
			b.WriteString(" = ")
		}
		c.writeColumnRef(b, cr.Table, cr.Field)
		return
	}

	switch p.Op {
	case query.OpEq:
		b.WriteString(" = ")
		b.WriteString(c.ph(len(*params) + 1))
		*params = append(*params, p.Value)
	case query.OpNeq:
		b.WriteString(" != ")
		b.WriteString(c.ph(len(*params) + 1))
		*params = append(*params, p.Value)
	case query.OpGt:
		b.WriteString(" > ")
		b.WriteString(c.ph(len(*params) + 1))
		*params = append(*params, p.Value)
	case query.OpGte:
		b.WriteString(" >= ")
		b.WriteString(c.ph(len(*params) + 1))
		*params = append(*params, p.Value)
	case query.OpLt:
		b.WriteString(" < ")
		b.WriteString(c.ph(len(*params) + 1))
		*params = append(*params, p.Value)
	case query.OpLte:
		b.WriteString(" <= ")
		b.WriteString(c.ph(len(*params) + 1))
		*params = append(*params, p.Value)
	case query.OpLike:
		b.WriteString(" LIKE ")
		b.WriteString(c.ph(len(*params) + 1))
		*params = append(*params, p.Value)
	case query.OpIsNil:
		b.WriteString(" IS NULL")
	case query.OpIsNotNil:
		b.WriteString(" IS NOT NULL")
	case query.OpIn, query.OpNotIn:
		op := " IN ("
		if p.Op == query.OpNotIn {
			op = " NOT IN ("
		}
		items, ok := p.Value.([]any)
		if !ok || len(items) == 0 {
			// Empty IN () is invalid SQL; emit a tautology / contradiction so
			// the surrounding WHERE still parses cleanly.
			if p.Op == query.OpNotIn {
				b.WriteString(" IS NOT NULL") // NOT IN (∅) ≡ always true for non-null
			} else {
				b.WriteString(" = NULL") // IN (∅) ≡ always false
			}
			return
		}
		b.WriteString(op)
		for i, item := range items {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(c.ph(len(*params) + 1))
			*params = append(*params, item)
		}
		b.WriteString(")")
	}
}

func (c *Compiler) compileComposite(b *strings.Builder, params *[]any, comp query.Composite) {
	op := " AND "
	if comp.Logic == query.LogicOr {
		op = " OR "
	}

	b.WriteString("(")
	for i, child := range comp.Children {
		if i > 0 {
			b.WriteString(op)
		}
		c.compileNode(b, params, child)
	}
	b.WriteString(")")
}

func (c *Compiler) ph(n int) string {
	if c.AtPrefixed {
		return fmt.Sprintf("@p%d", n)
	}
	if c.Numbered {
		return fmt.Sprintf("$%d", n)
	}
	return "?"
}

func (c *Compiler) quote(s string) string {
	if c.BracketQuote {
		return "[" + s + "]"
	}
	if c.Backtick {
		return "`" + s + "`"
	}
	return `"` + s + `"`
}

func quoteIdent(s string) string {
	return `"` + s + `"`
}

func fieldColumn(meta *model.EntityMeta, fieldName string) string {
	if f := meta.Field(fieldName); f != nil {
		return f.Column
	}
	if f := meta.FieldByColumn(fieldName); f != nil {
		return f.Column
	}
	return fieldName
}
