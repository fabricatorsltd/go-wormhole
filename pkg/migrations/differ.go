package migrations

import (
	"fmt"
	"reflect"
	"sort"

	"github.com/fabricatorsltd/go-wormhole/pkg/model"
)

// ValidateModels reports the first reason a set of models cannot yet be turned
// into a migration. Composite (multi-column) primary keys are not supported by
// the DDL generator, which renders one inline PRIMARY KEY per column; emitting a
// table for such an entity would produce invalid SQL, so it is rejected rather
// than generated wrong. Declare these tables by hand for now. Runtime CRUD on
// composite-key entities is supported by the SQL providers.
func ValidateModels(targets []*model.EntityMeta) error {
	for _, m := range targets {
		if len(m.PrimaryKeys) > 1 {
			return fmt.Errorf("entity %q has a composite primary key, which migration generation does not yet support; define the table manually", m.Name)
		}
		for _, f := range m.Fields {
			if f.Computed {
				return fmt.Errorf("entity %q column %q is computed; generated-column DDL is not yet supported, define the column manually", m.Name, f.Column)
			}
		}
		if err := validateIndexPositions(m); err != nil {
			return err
		}
	}
	return nil
}

// validateIndexPositions rejects a named index whose member fields mix explicit
// positions with implicit ones, or repeat a position. Order must be all-or-none
// and unique so a composite's column order is never ambiguous. Gaps (1, 3) are
// allowed; the columns sort by value.
func validateIndexPositions(m *model.EntityMeta) error {
	positions := map[string][]int{}
	cols := map[string]map[string]bool{}
	for _, f := range m.Fields {
		for _, ref := range effectiveRefs(f.Indexes, f.Index, f.IndexOrder, f.Indexed, f.Unique) {
			n := indexName(m.Name, f.Column, ref.Name, ref.Unique)
			positions[n] = append(positions[n], ref.Order)
			if cols[n] == nil {
				cols[n] = map[string]bool{}
			}
			if cols[n][f.Column] {
				return fmt.Errorf("entity %q index %q lists column %q more than once", m.Name, n, f.Column)
			}
			cols[n][f.Column] = true
		}
	}
	for name, ps := range positions {
		if len(ps) < 2 {
			continue // single-column index: position is irrelevant
		}
		zero, nonzero := 0, 0
		seen := map[int]bool{}
		for _, p := range ps {
			if p == 0 {
				zero++
				continue
			}
			if p < 1 {
				return fmt.Errorf("entity %q index %q has invalid column position %d; positions are 1-based", m.Name, name, p)
			}
			nonzero++
			if seen[p] {
				return fmt.Errorf("entity %q index %q repeats column position %d", m.Name, name, p)
			}
			seen[p] = true
		}
		if zero > 0 && nonzero > 0 {
			return fmt.Errorf("entity %q index %q mixes explicit and implicit column order; give all of its fields a position or none", m.Name, name)
		}
	}
	return nil
}

// ComputeDiff compares target models (from Go structs) against the
// current database schema and returns the list of DDL operations
// needed to bring the database in sync.
func ComputeDiff(targets []*model.EntityMeta, current DatabaseSchema) []MigrationOp {
	var ops []MigrationOp

	if current.Tables == nil {
		current.Tables = make(map[string]*TableSchema)
	}

	targetNames := make(map[string]bool, len(targets))
	fks := buildForeignKeys(targets)

	for _, meta := range targets {
		targetNames[meta.Name] = true
		existing, exists := current.Tables[meta.Name]

		if !exists {
			// Whole table is new
			ops = append(ops, createTableFromMeta(meta, fks[meta.Name]))
			// All of the table's indexes are new.
			ops = append(ops, diffIndexOps(meta, nil)...)
			continue
		}

		// Table exists: diff columns and indexes against the snapshot.
		ops = append(ops, diffColumns(meta, existing, fks[meta.Name])...)
		ops = append(ops, diffIndexOps(meta, existing)...)
	}

	// Detect dropped tables (in DB but not in code)
	for name := range current.Tables {
		if !targetNames[name] {
			ops = append(ops, DropTableOp{Table: name})
		}
	}

	return ops
}

func createTableFromMeta(meta *model.EntityMeta, fks map[string]*ColumnRef) CreateTableOp {
	cols := make([]ColumnDef, 0, len(meta.Fields))
	for _, f := range meta.Fields {
		cd := fieldToColumnDef(f)
		if ref, ok := fks[cd.Name]; ok {
			cd.Ref = ref
		}
		cols = append(cols, cd)
	}
	return CreateTableOp{Table: meta.Name, Columns: cols}
}

// buildForeignKeys resolves the set of foreign keys for every table from the
// relation metadata across all targets. The result is keyed by table name then
// by FK column name. Many-to-many join tables are skipped (the join entity, if
// modeled, declares its own FKs via BelongsTo).
func buildForeignKeys(targets []*model.EntityMeta) map[string]map[string]*ColumnRef {
	byName := make(map[string]*model.EntityMeta, len(targets))
	for _, m := range targets {
		byName[m.Name] = m
	}
	pkColumn := func(table, fallback string) string {
		if m := byName[table]; m != nil && m.PrimaryKey != nil {
			return m.PrimaryKey.Column
		}
		return fallback
	}

	out := make(map[string]map[string]*ColumnRef)
	put := func(table, col string, ref *ColumnRef) {
		if out[table] == nil {
			out[table] = make(map[string]*ColumnRef)
		}
		// One FK may be modeled from both sides (BelongsTo + OneToMany). Keep the
		// first reference, but adopt a referential action from the other side so
		// on_delete is not lost to iteration order when only one side declares it.
		if existing, ok := out[table][col]; ok {
			if existing.OnDelete == "" {
				existing.OnDelete = ref.OnDelete
			}
			return
		}
		out[table][col] = ref
	}

	for _, owner := range targets {
		for _, rel := range owner.Relations {
			switch rel.Kind {
			case model.RelationBelongsTo:
				// FK on the owner referencing the target's primary key.
				put(owner.Name, rel.LocalKey, &ColumnRef{
					Table:    rel.TargetEntity,
					Column:   pkColumn(rel.TargetEntity, rel.ForeignKey),
					OnDelete: rel.OnDelete,
				})
			case model.RelationOneToMany, model.RelationOneToOne:
				// FK on the related table referencing the owner's local key.
				put(rel.TargetEntity, rel.ForeignKey, &ColumnRef{
					Table:    owner.Name,
					Column:   rel.LocalKey,
					OnDelete: rel.OnDelete,
				})
			}
		}
	}
	return out
}

func diffColumns(meta *model.EntityMeta, table *TableSchema, fks map[string]*ColumnRef) []MigrationOp {
	var ops []MigrationOp

	codeFields := make(map[string]bool, len(meta.Fields))

	for _, f := range meta.Fields {
		codeFields[f.Column] = true
		existing, exists := table.Columns[f.Column]

		if !exists {
			// New column
			cd := fieldToColumnDef(f)
			if ref, ok := fks[cd.Name]; ok {
				cd.Ref = ref
			}
			ops = append(ops, AddColumnOp{
				Table:  meta.Name,
				Column: cd,
			})
			continue
		}

		// Column exists — check for type/constraint changes
		newDef := fieldToColumnDef(f)
		// Preserve the introspected DB type when the model did not
		// explicitly override it via a `type:` tag.
		if newDef.SQLType == "" && existing.SQLType != "" {
			newDef.SQLType = existing.SQLType
		}
		if columnChanged(existing, &newDef) {
			ops = append(ops, AlterColumnOp{
				Table:  meta.Name,
				Column: newDef,
			})
		}
	}

	// Columns in DB but not in code
	for colName := range table.Columns {
		if !codeFields[colName] {
			ops = append(ops, DropColumnOp{
				Table:  meta.Name,
				Column: colName,
			})
		}
	}

	return ops
}

func columnChanged(old *ColumnDef, new *ColumnDef) bool {
	oldType := old.SQLType
	newType := new.SQLType
	if oldType == "" && old.GoType != nil {
		oldType = GoTypeToSQL(old.GoType)
	}
	if newType == "" && new.GoType != nil {
		newType = GoTypeToSQL(new.GoType)
	}
	if oldType != newType {
		return true
	}
	if old.Nullable != new.Nullable {
		return true
	}
	if old.Default != new.Default {
		return true
	}
	return false
}

// indexDef is a resolved index used for diffing. Columns holds one entry for a
// single-column index, or several for a composite index built from fields that
// share an explicit index name. ordered is true when every member carries an
// explicit position (index:name:N): the columns are then sorted by it and the
// diff compares column order, not just the column set.
type indexDef struct {
	name    string
	columns []string
	unique  bool
	ordered bool
}

// indexMember is one column's contribution to an index during grouping.
type indexMember struct {
	column string
	order  int
}

// resolveIndex turns grouped member columns into an indexDef. When every member
// has an explicit position the columns are sorted by it and the index is marked
// ordered; otherwise they keep accumulation order (declaration for the model, map
// order for a snapshot) and the index is unordered.
func resolveIndex(name string, members []indexMember, unique bool) indexDef {
	ordered := len(members) > 0
	for _, m := range members {
		if m.order == 0 {
			ordered = false
			break
		}
	}
	if ordered {
		sort.SliceStable(members, func(i, j int) bool { return members[i].order < members[j].order })
	}
	cols := make([]string, len(members))
	for i, m := range members {
		cols[i] = m.column
	}
	return indexDef{name: name, columns: cols, unique: unique, ordered: ordered}
}

// indexName derives a deterministic, table-qualified index name (engines like
// Postgres have a schema-global index namespace). An explicit name wins.
func indexName(table, column, explicit string, unique bool) string {
	if explicit != "" {
		return explicit
	}
	prefix := "idx"
	if unique {
		prefix = "uniq"
	}
	return prefix + "_" + table + "_" + column
}

// modelIndexes returns the indexes the model declares, keyed by index name.
// Fields that share an explicit index name combine into one composite index,
// with columns in field-declaration order; the index is unique if any of its
// fields is marked unique.
func modelIndexes(meta *model.EntityMeta) map[string]indexDef {
	groups := map[string][]indexMember{}
	unique := map[string]bool{}
	for _, f := range meta.Fields {
		for _, ref := range effectiveRefs(f.Indexes, f.Index, f.IndexOrder, f.Indexed, f.Unique) {
			n := indexName(meta.Name, f.Column, ref.Name, ref.Unique)
			groups[n] = append(groups[n], indexMember{column: f.Column, order: ref.Order})
			if ref.Unique {
				unique[n] = true
			}
		}
	}
	out := make(map[string]indexDef, len(groups))
	for n, ms := range groups {
		out[n] = resolveIndex(n, ms, unique[n])
	}
	return out
}

// effectiveRefs returns the index memberships of a field or column, falling back
// to the legacy single-index fields when the Indexes list is empty. Old snapshots
// and hand-built metadata predate the list, so the differ must still read them.
func effectiveRefs(refs []model.IndexRef, name string, order int, indexed, unique bool) []model.IndexRef {
	if len(refs) > 0 {
		return refs
	}
	if name == "" && !indexed && !unique {
		return nil
	}
	return []model.IndexRef{{Name: name, Order: order, Unique: unique}}
}

// snapshotIndexes returns the indexes a snapshot table records, keyed by name.
// Composite members are grouped by index name; their order is the snapshot map's
// (non-deterministic), so the diff compares column sets, not column order.
func snapshotIndexes(table *TableSchema) map[string]indexDef {
	if table == nil {
		return map[string]indexDef{}
	}
	groups := map[string][]indexMember{}
	unique := map[string]bool{}
	for _, c := range table.Columns {
		for _, ref := range effectiveRefs(c.Indexes, c.Index, c.IndexOrder, c.Indexed, c.Unique) {
			n := indexName(table.Name, c.Name, ref.Name, ref.Unique)
			groups[n] = append(groups[n], indexMember{column: c.Name, order: ref.Order})
			if ref.Unique {
				unique[n] = true
			}
		}
	}
	out := make(map[string]indexDef, len(groups))
	for n, ms := range groups {
		out[n] = resolveIndex(n, ms, unique[n])
	}
	return out
}

// sameColumns compares a desired index against the current (snapshot) one. It
// compares column order exactly only when BOTH sides record an explicit order;
// otherwise it falls back to a set comparison. This keeps unpositioned composites
// order-insensitive (the v1.11.0 contract) and avoids a spurious rebuild the
// first time a positioned model is diffed against a snapshot written before
// positions existed (current side unordered -> set comparison).
func sameColumns(desired, current indexDef) bool {
	if desired.ordered && current.ordered {
		if len(desired.columns) != len(current.columns) {
			return false
		}
		for i := range desired.columns {
			if desired.columns[i] != current.columns[i] {
				return false
			}
		}
		return true
	}
	return sameColumnSet(desired.columns, current.columns)
}

// sameColumnSet reports whether two column lists hold the same set (order
// insensitive), used because the snapshot does not preserve composite column
// order.
func sameColumnSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	as := append([]string(nil), a...)
	bs := append([]string(nil), b...)
	sort.Strings(as)
	sort.Strings(bs)
	for i := range as {
		if as[i] != bs[i] {
			return false
		}
	}
	return true
}

// diffIndexOps emits CreateIndex/DropIndex only for indexes that actually
// changed between the snapshot and the model, so an unchanged indexed model
// diffs to zero ops (no spurious re-create). Output is sorted for determinism.
func diffIndexOps(meta *model.EntityMeta, existing *TableSchema) []MigrationOp {
	desired := modelIndexes(meta)
	current := snapshotIndexes(existing)

	var creates, drops []string
	for name, d := range desired {
		c, ok := current[name]
		changed := ok && (c.unique != d.unique || !sameColumns(d, c))
		if !ok || changed {
			creates = append(creates, name)
		}
		if changed {
			drops = append(drops, name) // recreate with new uniqueness or columns
		}
	}
	for name := range current {
		if _, ok := desired[name]; !ok {
			drops = append(drops, name)
		}
	}
	sort.Strings(creates)
	sort.Strings(drops)

	var ops []MigrationOp
	for _, name := range drops {
		ops = append(ops, DropIndexOp{Name: name, Table: meta.Name})
	}
	for _, name := range creates {
		d := desired[name]
		ops = append(ops, CreateIndexOp{
			Name:    name,
			Table:   meta.Name,
			Columns: d.columns,
			Unique:  d.unique,
		})
	}
	return ops
}

func fieldToColumnDef(f model.FieldMeta) ColumnDef {
	sqlType := ""
	if v, ok := f.Tags["type"]; ok {
		sqlType = v
	}
	def := ""
	if v, ok := f.Tags["default"]; ok {
		def = v
	}
	return ColumnDef{
		Name:       f.Column,
		SQLType:    sqlType,
		PrimaryKey: f.PrimaryKey,
		AutoIncr:   f.AutoIncr,
		Nullable:   f.Nullable,
		Default:    def,
		Index:      f.Index,
		IndexOrder: f.IndexOrder,
		Indexes:    f.Indexes,
		Indexed:    f.Indexed,
		Unique:     f.Unique,
		GoType:     f.GoType,
	}
}

// MetaToSnapshot converts a list of EntityMeta into a DatabaseSchema
// for use as the "current" snapshot.
func MetaToSnapshot(metas []*model.EntityMeta) DatabaseSchema {
	schema := DatabaseSchema{Tables: make(map[string]*TableSchema, len(metas))}
	for _, meta := range metas {
		ts := &TableSchema{
			Name:    meta.Name,
			Columns: make(map[string]*ColumnDef, len(meta.Fields)),
		}
		for _, f := range meta.Fields {
			cd := fieldToColumnDef(f)
			ts.Columns[f.Column] = &cd
		}
		schema.Tables[meta.Name] = ts
	}
	return schema
}

// Ensure reflect is used (for GoType in ColumnDef)
var _ = reflect.TypeOf
