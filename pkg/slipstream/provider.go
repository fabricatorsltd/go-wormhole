package slipstream

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/fabricatorsltd/go-wormhole/pkg/model"
	"github.com/fabricatorsltd/go-wormhole/pkg/provider"
	"github.com/fabricatorsltd/go-wormhole/pkg/query"
	"github.com/mirkobrombin/go-slipstream/pkg/engine"
	"github.com/mirkobrombin/go-slipstream/pkg/tx"
	"github.com/mirkobrombin/go-slipstream/pkg/wal"
)

// record is the storage unit: a map of column→value pairs.
type record = map[string]any

// Provider implements provider.Provider on top of a go-slipstream Engine.
// Entities are stored as JSON-encoded maps keyed by primary-key value.
// TODO: switch to fast binary codec (msgpack) for production
type Provider struct {
	eng     *engine.Engine[record]
	wm      *wal.Manager
	indices map[string]struct{} // tracks registered secondary index names
}

var _ provider.Provider = (*Provider)(nil)

var slipstreamCapabilities = provider.Capabilities{
	Transactions:     true,
	PartialUpdate:    true,
	OffsetPagination: true,
	NestedFilters:    true,
	SchemaEvolution:  true,
}

// New creates a Slipstream provider that persists data under dataDir.
func New(dataDir string, opts ...engine.Option[record]) (*Provider, error) {
	wm, err := wal.NewManager(dataDir)
	if err != nil {
		return nil, fmt.Errorf("slipstream wal: %w", err)
	}

	codec := func(r record) ([]byte, error) {
		return json.Marshal(r)
	}
	decoder := func(b []byte) (record, error) {
		var r record
		err := json.Unmarshal(b, &r)
		return r, err
	}

	eng := engine.New[record](wm, codec, decoder, opts...)

	return &Provider{eng: eng, wm: wm, indices: make(map[string]struct{})}, nil
}

// Engine exposes the underlying Engine for advanced usage (adding indices, etc.).
func (p *Provider) Engine() *engine.Engine[record] { return p.eng }

func (p *Provider) Name() string { return "slipstream" }

func (p *Provider) Capabilities() provider.Capabilities {
	return slipstreamCapabilities
}

func (p *Provider) Open(_ context.Context, _ string) error {
	// engine is already open from New()
	return nil
}

func (p *Provider) Close() error {
	return p.eng.Close()
}

// --- CRUD ---

func (p *Provider) Insert(ctx context.Context, meta *model.EntityMeta, entity any) (any, error) {
	rec, pk := toRecord(meta, entity)
	key := fmt.Sprintf("%s:%v", meta.Name, pk)
	if err := p.eng.Put(ctx, key, rec, 0); err != nil {
		return nil, err
	}
	return pk, nil
}

func (p *Provider) Update(ctx context.Context, meta *model.EntityMeta, entity any, changed []string) error {
	rec, pk := toRecord(meta, entity)
	key := fmt.Sprintf("%s:%v", meta.Name, pk)

	// partial: read existing, merge changed fields
	existing, err := p.eng.Get(ctx, key)
	if err != nil {
		return fmt.Errorf("read before update: %w", err)
	}

	changedSet := changedColumns(meta, changed)
	for k, v := range rec {
		if _, ok := changedSet[k]; ok {
			existing[k] = v
		}
	}

	return p.eng.Put(ctx, key, existing, 0)
}

func (p *Provider) Delete(ctx context.Context, meta *model.EntityMeta, pkValue any) error {
	key := fmt.Sprintf("%s:%v", meta.Name, pkValue)
	return p.eng.Delete(ctx, key)
}

func (p *Provider) Find(ctx context.Context, meta *model.EntityMeta, pkValue any, dest any) error {
	key := fmt.Sprintf("%s:%v", meta.Name, pkValue)
	rec, err := p.eng.Get(ctx, key)
	if err != nil {
		return err
	}
	return fromRecord(meta, rec, dest)
}

// --- Query ---

func (p *Provider) Execute(ctx context.Context, meta *model.EntityMeta, q query.Query, dest any) error {
	if err := provider.ValidateQueryCapabilities(p.Capabilities(), q); err != nil {
		return err
	}
	return p.executeQuery(ctx, meta, q, dest)
}

// --- Transactions ---

func (p *Provider) Begin(ctx context.Context) (provider.Tx, error) {
	stx, err := p.eng.Begin()
	if err != nil {
		return nil, err
	}
	return &slipTx{tx: stx, eng: p.eng}, nil
}

// slipTx wraps a slipstream Transaction as a provider.Tx.
type slipTx struct {
	tx  tx.Transaction[record]
	eng *engine.Engine[record]
}

func (t *slipTx) Commit() error   { return t.tx.Commit(context.Background()) }
func (t *slipTx) Rollback() error { t.tx.Rollback(); return nil }

func (t *slipTx) Insert(ctx context.Context, meta *model.EntityMeta, entity any) (any, error) {
	rec, pk := toRecord(meta, entity)
	key := fmt.Sprintf("%s:%v", meta.Name, pk)
	return pk, t.tx.Put(ctx, key, rec, 0)
}

func (t *slipTx) Update(ctx context.Context, meta *model.EntityMeta, entity any, changed []string) error {
	rec, pk := toRecord(meta, entity)
	key := fmt.Sprintf("%s:%v", meta.Name, pk)

	existing, err := t.tx.Get(ctx, key)
	if err != nil {
		return fmt.Errorf("tx read before update: %w", err)
	}

	changedSet := changedColumns(meta, changed)
	for k, v := range rec {
		if _, ok := changedSet[k]; ok {
			existing[k] = v
		}
	}

	return t.tx.Put(ctx, key, existing, 0)
}

func (t *slipTx) Delete(ctx context.Context, meta *model.EntityMeta, pkValue any) error {
	key := fmt.Sprintf("%s:%v", meta.Name, pkValue)
	return t.tx.Delete(ctx, key)
}

func (t *slipTx) Find(ctx context.Context, meta *model.EntityMeta, pkValue any, dest any) error {
	key := fmt.Sprintf("%s:%v", meta.Name, pkValue)
	rec, err := t.tx.Get(ctx, key)
	if err != nil {
		return err
	}
	return fromRecord(meta, rec, dest)
}

func (t *slipTx) Execute(ctx context.Context, meta *model.EntityMeta, q query.Query, dest any) error {
	if err := provider.ValidateQueryCapabilities(slipstreamCapabilities, q); err != nil {
		return err
	}
	return executeQueryOnEngine(ctx, t.eng, nil, meta, q, dest)
}

// --- helpers ---

// toRecord converts a struct to a map[string]any using EntityMeta.
func toRecord(meta *model.EntityMeta, entity any) (record, any) {
	val := reflect.ValueOf(entity)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	rec := make(record, len(meta.Fields))
	var pk any
	for _, f := range meta.Fields {
		v := val.FieldByName(f.FieldName).Interface()
		rec[f.Column] = v
		if f.PrimaryKey {
			pk = v
		}
	}
	return rec, pk
}

// fromRecord populates a struct pointer from a map[string]any.
func fromRecord(meta *model.EntityMeta, rec record, dest any) error {
	val := reflect.ValueOf(dest)
	if val.Kind() != reflect.Ptr || val.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("dest must be a pointer to struct")
	}
	val = val.Elem()

	for _, f := range meta.Fields {
		rv, ok := rec[f.Column]
		if !ok {
			continue
		}
		fv := val.FieldByName(f.FieldName)
		if !fv.CanSet() {
			continue
		}
		setField(fv, rv)
	}
	return nil
}

// setField converts a JSON-decoded value to the target field type.
func setField(fv reflect.Value, rv any) {
	if rv == nil {
		return
	}
	src := reflect.ValueOf(rv)
	if src.Type().AssignableTo(fv.Type()) {
		fv.Set(src)
		return
	}
	if src.Type().ConvertibleTo(fv.Type()) {
		fv.Set(src.Convert(fv.Type()))
		return
	}
	// JSON numbers decode as float64; handle int fields
	if src.Kind() == reflect.Float64 {
		switch fv.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			fv.SetInt(int64(src.Float()))
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			fv.SetUint(uint64(src.Float()))
		}
	}
}

// AddEntityIndex registers a secondary index on the underlying engine
// for a given entity column name.
func (p *Provider) AddEntityIndex(meta *model.EntityMeta, fieldName string) {
	idxName := meta.Name + "." + fieldName
	p.eng.AddIndex(idxName, func(r record) string {
		if v, ok := r[fieldName]; ok {
			return fmt.Sprintf("%v", v)
		}
		return ""
	})
	p.indices[idxName] = struct{}{}
}

// HasIndex reports whether a secondary index has been registered.
func (p *Provider) HasIndex(name string) bool {
	_, ok := p.indices[name]
	return ok
}

func changedColumns(meta *model.EntityMeta, changed []string) map[string]struct{} {
	out := make(map[string]struct{}, len(changed))
	for _, name := range changed {
		if f := meta.Field(name); f != nil {
			out[f.Column] = struct{}{}
			continue
		}
		out[name] = struct{}{}
	}
	return out
}

// executeQuery is the Provider-aware entry point that can check registered indices.
func (p *Provider) executeQuery(ctx context.Context, meta *model.EntityMeta, q query.Query, dest any) error {
	return executeQueryOnEngine(ctx, p.eng, p.indices, meta, q, dest)
}

// executeQueryOnEngine translates the AST to slipstream operations.
// indices may be nil (e.g. when called from a transaction), in which
// case the indexed path is skipped.
func executeQueryOnEngine(ctx context.Context, eng *engine.Engine[record], indices map[string]struct{}, meta *model.EntityMeta, q query.Query, dest any) error {
	// Try indexed path only if the index is actually registered
	if pred, ok := q.Where.(query.Predicate); ok && pred.Op == query.OpEq && indices != nil {
		idxName := meta.Name + "." + pred.Field
		if _, registered := indices[idxName]; registered {
			result := eng.GetByIndex(ctx, idxName, fmt.Sprintf("%v", pred.Value))
			if result != nil {
				return collectResults(result, meta, q, dest)
			}
		}
	}

	// Fallback: full scan with in-memory filter
	var allRecs []record
	prefix := meta.Name + ":"
	err := eng.ForEach(func(key string, val record) error {
		if len(key) > len(prefix) && key[:len(prefix)] == prefix {
			if matchNode(val, q.Where) {
				allRecs = append(allRecs, val)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	return scanInto(allRecs, meta, q, dest)
}

func collectResults(r *engine.Result[record], meta *model.EntityMeta, q query.Query, dest any) error {
	if q.Limit > 0 {
		r = r.Limit(q.Limit)
	}
	if q.Offset > 0 {
		r = r.Offset(q.Offset)
	}

	recs, err := r.All()
	if err != nil {
		return err
	}
	return scanInto(recs, meta, q, dest)
}

func scanInto(recs []record, meta *model.EntityMeta, q query.Query, dest any) error {
	// Apply offset/limit for full-scan path
	if q.Offset > 0 && q.Offset < len(recs) {
		recs = recs[q.Offset:]
	}
	if q.Limit > 0 && q.Limit < len(recs) {
		recs = recs[:q.Limit]
	}

	dv := reflect.ValueOf(dest)
	if dv.Kind() != reflect.Ptr || dv.Elem().Kind() != reflect.Slice {
		return fmt.Errorf("dest must be *[]T or *[]*T")
	}

	sliceVal := dv.Elem()
	elemType := sliceVal.Type().Elem()
	isPtr := elemType.Kind() == reflect.Ptr

	for _, rec := range recs {
		var elem reflect.Value
		if isPtr {
			// dest is *[]*User — allocate a new *User
			elem = reflect.New(elemType.Elem())
		} else {
			// dest is *[]User — allocate a new User
			elem = reflect.New(elemType)
		}

		target := elem.Elem()

		for _, f := range meta.Fields {
			rv, ok := rec[f.Column]
			if !ok {
				continue
			}
			fv := target.FieldByName(f.FieldName)
			if fv.CanSet() {
				setField(fv, rv)
			}
		}

		if isPtr {
			sliceVal = reflect.Append(sliceVal, elem)
		} else {
			sliceVal = reflect.Append(sliceVal, target)
		}
	}

	dv.Elem().Set(sliceVal)
	return nil
}

// matchNode evaluates a query AST node against a record.
func matchNode(rec record, node query.Node) bool {
	if node == nil {
		return true
	}
	switch n := node.(type) {
	case query.Predicate:
		return matchPredicate(rec, n)
	case query.Composite:
		return matchComposite(rec, n)
	default:
		return true
	}
}

func matchPredicate(rec record, p query.Predicate) bool {
	val, ok := rec[p.Field]
	if !ok {
		return p.Op == query.OpIsNil
	}

	switch p.Op {
	case query.OpEq:
		return fmt.Sprintf("%v", val) == fmt.Sprintf("%v", p.Value)
	case query.OpNeq:
		return fmt.Sprintf("%v", val) != fmt.Sprintf("%v", p.Value)
	case query.OpIsNil:
		return val == nil
	case query.OpIn:
		return matchIn(val, p.Value)
	case query.OpGt, query.OpGte, query.OpLt, query.OpLte:
		return compareOrdered(val, p.Value, p.Op)
	case query.OpLike:
		return matchLike(fmt.Sprintf("%v", val), fmt.Sprintf("%v", p.Value))
	default:
		return false
	}
}

func matchComposite(rec record, c query.Composite) bool {
	if c.Logic == query.LogicAnd {
		for _, child := range c.Children {
			if !matchNode(rec, child) {
				return false
			}
		}
		return true
	}
	// OR
	for _, child := range c.Children {
		if matchNode(rec, child) {
			return true
		}
	}
	return false
}

func matchIn(val, list any) bool {
	s := fmt.Sprintf("%v", val)
	if items, ok := list.([]any); ok {
		for _, item := range items {
			if fmt.Sprintf("%v", item) == s {
				return true
			}
		}
	}
	return false
}

func compareOrdered(a, b any, op query.Op) bool {
	fa, oka := toFloat(a)
	fb, okb := toFloat(b)
	if !oka || !okb {
		return false
	}
	switch op {
	case query.OpGt:
		return fa > fb
	case query.OpGte:
		return fa >= fb
	case query.OpLt:
		return fa < fb
	case query.OpLte:
		return fa <= fb
	}
	return false
}

func toFloat(v any) (float64, bool) {
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(rv.Int()), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(rv.Uint()), true
	case reflect.Float32, reflect.Float64:
		return rv.Float(), true
	}
	return 0, false
}

func matchLike(val, pattern string) bool {
	// simplified LIKE: only supports %prefix, suffix%, %contains%
	if len(pattern) == 0 {
		return val == ""
	}
	startWild := pattern[0] == '%'
	endWild := pattern[len(pattern)-1] == '%'
	core := pattern
	if startWild {
		core = core[1:]
	}
	if endWild && len(core) > 0 {
		core = core[:len(core)-1]
	}

	switch {
	case startWild && endWild:
		return contains(val, core)
	case startWild:
		return len(val) >= len(core) && val[len(val)-len(core):] == core
	case endWild:
		return len(val) >= len(core) && val[:len(core)] == core
	default:
		return val == core
	}
}

func contains(s, sub string) bool {
	return len(sub) == 0 || len(s) >= len(sub) && findSub(s, sub)
}

func findSub(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// RegisterDefault creates a Slipstream provider and registers it as the default.
func RegisterDefault(dataDir string, opts ...engine.Option[record]) (*Provider, error) {
	p, err := New(dataDir, opts...)
	if err != nil {
		return nil, err
	}
	provider.Register("slipstream", p)
	provider.SetDefault("slipstream")
	return p, nil
}
