package migrations

import (
	"encoding/json"
	"fmt"
)

// opKindNames maps each OpKind to its stable JSON kind string. opUnmarshalers is
// the inverse for decoding. Both must cover every MigrationOp; TestOpRegistry
// guards completeness so a new op type cannot silently vanish on round-trip.
var opKindNames = map[OpKind]string{
	OpCreateTable: "create_table",
	OpDropTable:   "drop_table",
	OpAddColumn:   "add_column",
	OpDropColumn:  "drop_column",
	OpAlterColumn: "alter_column",
	OpCreateIndex: "create_index",
	OpDropIndex:   "drop_index",
	OpRawSQL:      "raw_sql",
}

var opUnmarshalers = map[string]func([]byte) (MigrationOp, error){
	"create_table": func(b []byte) (MigrationOp, error) { var o CreateTableOp; e := json.Unmarshal(b, &o); return o, e },
	"drop_table":   func(b []byte) (MigrationOp, error) { var o DropTableOp; e := json.Unmarshal(b, &o); return o, e },
	"add_column":   func(b []byte) (MigrationOp, error) { var o AddColumnOp; e := json.Unmarshal(b, &o); return o, e },
	"drop_column":  func(b []byte) (MigrationOp, error) { var o DropColumnOp; e := json.Unmarshal(b, &o); return o, e },
	"alter_column": func(b []byte) (MigrationOp, error) { var o AlterColumnOp; e := json.Unmarshal(b, &o); return o, e },
	"create_index": func(b []byte) (MigrationOp, error) { var o CreateIndexOp; e := json.Unmarshal(b, &o); return o, e },
	"drop_index":   func(b []byte) (MigrationOp, error) { var o DropIndexOp; e := json.Unmarshal(b, &o); return o, e },
	"raw_sql":      func(b []byte) (MigrationOp, error) { var o RawSQLOp; e := json.Unmarshal(b, &o); return o, e },
}

// opEnvelope is the on-disk form of one operation: a kind tag plus the encoded
// operation. Nested (not flattened) so the tag can never collide with an op
// field.
type opEnvelope struct {
	Kind string          `json:"kind"`
	Data json.RawMessage `json:"data"`
}

// migrationDoc is the JSON manifest written per migration: ordered up and down
// operations that a standalone CLI can apply without compiling the project.
type migrationDoc struct {
	ID   string       `json:"id"`
	Up   []opEnvelope `json:"up"`
	Down []opEnvelope `json:"down"`
}

// normalizeOp resolves the SQLType of any ColumnDef the op carries, so a column
// whose type was only known via reflect.Type still serializes with a concrete
// type. Without this, an untagged/GoType-driven column would persist with no
// type and apply broken DDL.
func normalizeOp(op MigrationOp) MigrationOp {
	switch o := op.(type) {
	case CreateTableOp:
		cols := make([]ColumnDef, len(o.Columns))
		for i, c := range o.Columns {
			cols[i] = normalizeColumn(c)
		}
		o.Columns = cols
		return o
	case AddColumnOp:
		o.Column = normalizeColumn(o.Column)
		return o
	case AlterColumnOp:
		o.Column = normalizeColumn(o.Column)
		return o
	default:
		return op
	}
}

func marshalOps(ops []MigrationOp) ([]opEnvelope, error) {
	out := make([]opEnvelope, 0, len(ops))
	for _, op := range ops {
		kind, ok := opKindNames[op.Kind()]
		if !ok {
			return nil, fmt.Errorf("unregistered migration op %T", op)
		}
		data, err := json.Marshal(normalizeOp(op))
		if err != nil {
			return nil, err
		}
		out = append(out, opEnvelope{Kind: kind, Data: data})
	}
	return out, nil
}

func unmarshalOps(envs []opEnvelope) ([]MigrationOp, error) {
	out := make([]MigrationOp, 0, len(envs))
	for _, env := range envs {
		fn, ok := opUnmarshalers[env.Kind]
		if !ok {
			return nil, fmt.Errorf("unknown migration op kind %q", env.Kind)
		}
		op, err := fn(env.Data)
		if err != nil {
			return nil, err
		}
		out = append(out, op)
	}
	return out, nil
}

// MarshalMigration encodes a migration's up and down operations into the JSON
// manifest format. The output is deterministic (stable key order) for clean
// version control.
func MarshalMigration(id string, up, down []MigrationOp) ([]byte, error) {
	upEnv, err := marshalOps(up)
	if err != nil {
		return nil, err
	}
	downEnv, err := marshalOps(down)
	if err != nil {
		return nil, err
	}
	b, err := json.MarshalIndent(migrationDoc{ID: id, Up: upEnv, Down: downEnv}, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}

// UnmarshalMigration decodes a migration manifest produced by MarshalMigration.
func UnmarshalMigration(data []byte) (id string, up, down []MigrationOp, err error) {
	var doc migrationDoc
	if err = json.Unmarshal(data, &doc); err != nil {
		return "", nil, nil, err
	}
	if up, err = unmarshalOps(doc.Up); err != nil {
		return "", nil, nil, err
	}
	if down, err = unmarshalOps(doc.Down); err != nil {
		return "", nil, nil, err
	}
	return doc.ID, up, down, nil
}
