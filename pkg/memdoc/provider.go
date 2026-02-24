package memdoc

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"

	"github.com/fabricatorsltd/go-wormhole/pkg/model"
	"github.com/fabricatorsltd/go-wormhole/pkg/provider"
	"github.com/fabricatorsltd/go-wormhole/pkg/query"
)

var (
	ErrNotFound          = errors.New("memdoc: not found")
	ErrMissingPrimaryKey = errors.New("memdoc: missing primary key")
)

// Provider is a deterministic, in-memory document store.
//
// Documents are stored as map[column]any using EntityMeta.Column keys.
// It is intended for tests and local-only usage.
//
// Capabilities:
// - Transactions:     true (snapshot/copy-on-write)
// - PartialUpdate:    true
// - NestedFilters:    true (Composite AST)
// - OffsetPagination: true
// - SchemaEvolution:  true
// - Sorting/Aggregations: false
// - SchemaMigrations: false
type Provider struct {
	mu       sync.RWMutex
	name     string
	opened   bool
	data     map[string]map[string]document // entity -> pkKey -> doc
	autoIncr map[string]int64               // entity -> next id
}

type document = map[string]any

type Option func(*Provider)

func WithName(name string) Option {
	return func(p *Provider) { p.name = name }
}

func New(opts ...Option) *Provider {
	p := &Provider{
		name:     "memdoc",
		data:     make(map[string]map[string]document),
		autoIncr: make(map[string]int64),
	}
	for _, o := range opts {
		o(p)
	}
	return p
}

var _ provider.Provider = (*Provider)(nil)

var memdocCapabilities = provider.Capabilities{
	Transactions:     true,
	PartialUpdate:    true,
	OffsetPagination: true,
	NestedFilters:    true,
	SchemaEvolution:  true,
}

func (p *Provider) Name() string { return p.name }

func (p *Provider) Capabilities() provider.Capabilities { return memdocCapabilities }

func (p *Provider) Open(_ context.Context, _ string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.opened = true
	return nil
}

func (p *Provider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.opened = false
	return nil
}

func (p *Provider) Insert(ctx context.Context, meta *model.EntityMeta, entity any) (any, error) {
	_ = ctx
	pkField := meta.PrimaryKey
	if pkField == nil {
		return nil, ErrMissingPrimaryKey
	}

	doc := toDocument(meta, entity)
	pk, err := pkValueOrAuto(meta, entity, p.nextID(meta.Name))
	if err != nil {
		return nil, err
	}
	doc[pkField.Column] = pk

	p.mu.Lock()
	defer p.mu.Unlock()
	p.ensureOpen()

	coll := p.collection(meta.Name)
	key := pkKey(pk)
	coll[key] = doc
	return pk, nil
}

func (p *Provider) Update(ctx context.Context, meta *model.EntityMeta, entity any, changed []string) error {
	_ = ctx
	pk, ok := pkValue(meta, entity)
	if !ok {
		return fmt.Errorf("memdoc: missing primary key value")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ensureOpen()

	coll := p.collection(meta.Name)
	key := pkKey(pk)
	existing, ok := coll[key]
	if !ok {
		return ErrNotFound
	}
	if len(changed) == 0 {
		return nil
	}

	changedCols := make(map[string]struct{}, len(changed))
	for _, name := range changed {
		if f := meta.Field(name); f != nil {
			changedCols[f.Column] = struct{}{}
		} else {
			changedCols[name] = struct{}{}
		}
	}

	doc := toDocument(meta, entity)
	for col := range changedCols {
		if v, ok := doc[col]; ok {
			existing[col] = v
		}
	}
	coll[key] = existing
	return nil
}

func (p *Provider) Delete(ctx context.Context, meta *model.EntityMeta, pkValue any) error {
	_ = ctx
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ensureOpen()

	coll := p.collection(meta.Name)
	key := pkKey(pkValue)
	if _, ok := coll[key]; !ok {
		return ErrNotFound
	}
	delete(coll, key)
	return nil
}

func (p *Provider) Find(ctx context.Context, meta *model.EntityMeta, pkValue any, dest any) error {
	_ = ctx
	p.mu.RLock()
	defer p.mu.RUnlock()
	p.ensureOpen()

	coll := p.collection(meta.Name)
	key := pkKey(pkValue)
	doc, ok := coll[key]
	if !ok {
		return ErrNotFound
	}
	return fromDocument(meta, doc, dest)
}

func (p *Provider) Execute(ctx context.Context, meta *model.EntityMeta, q query.Query, dest any) error {
	_ = ctx
	if err := provider.ValidateQueryCapabilities(p.Capabilities(), q); err != nil {
		return err
	}

	p.mu.RLock()
	defer p.mu.RUnlock()
	p.ensureOpen()

	coll := p.collection(meta.Name)
	recs := make([]document, 0, len(coll))
	for _, doc := range coll {
		if matchNode(doc, q.Where) {
			recs = append(recs, doc)
		}
	}
	return scanInto(recs, meta, q, dest)
}

func (p *Provider) Begin(ctx context.Context) (provider.Tx, error) {
	_ = ctx
	if !p.Capabilities().Transactions {
		return nil, fmt.Errorf("memdoc: provider does not support transactions")
	}

	p.mu.RLock()
	defer p.mu.RUnlock()
	p.ensureOpen()

	return &memTx{
		p:        p,
		data:     deepCopyData(p.data),
		autoIncr: copyAutoIncr(p.autoIncr),
	}, nil
}

type memTx struct {
	p        *Provider
	closed   bool
	data     map[string]map[string]document
	autoIncr map[string]int64
}

func (t *memTx) Commit() error {
	if t.closed {
		return nil
	}
	t.closed = true

	t.p.mu.Lock()
	defer t.p.mu.Unlock()
	t.p.ensureOpen()
	t.p.data = t.data
	t.p.autoIncr = t.autoIncr
	return nil
}

func (t *memTx) Rollback() error {
	t.closed = true
	return nil
}

func (t *memTx) Insert(ctx context.Context, meta *model.EntityMeta, entity any) (any, error) {
	_ = ctx
	pkField := meta.PrimaryKey
	if pkField == nil {
		return nil, ErrMissingPrimaryKey
	}

	doc := toDocument(meta, entity)
	next := func() int64 {
		id := t.autoIncr[meta.Name] + 1
		t.autoIncr[meta.Name] = id
		return id
	}
	pk, err := pkValueOrAuto(meta, entity, next)
	if err != nil {
		return nil, err
	}
	doc[pkField.Column] = pk

	coll := txCollection(t.data, meta.Name)
	coll[pkKey(pk)] = doc
	return pk, nil
}

func (t *memTx) Update(ctx context.Context, meta *model.EntityMeta, entity any, changed []string) error {
	_ = ctx
	pk, ok := pkValue(meta, entity)
	if !ok {
		return fmt.Errorf("memdoc: missing primary key value")
	}
	coll := txCollection(t.data, meta.Name)
	key := pkKey(pk)
	existing, ok := coll[key]
	if !ok {
		return ErrNotFound
	}
	if len(changed) == 0 {
		return nil
	}

	changedCols := make(map[string]struct{}, len(changed))
	for _, name := range changed {
		if f := meta.Field(name); f != nil {
			changedCols[f.Column] = struct{}{}
		} else {
			changedCols[name] = struct{}{}
		}
	}

	doc := toDocument(meta, entity)
	for col := range changedCols {
		if v, ok := doc[col]; ok {
			existing[col] = v
		}
	}
	coll[key] = existing
	return nil
}

func (t *memTx) Delete(ctx context.Context, meta *model.EntityMeta, pkValue any) error {
	_ = ctx
	coll := txCollection(t.data, meta.Name)
	key := pkKey(pkValue)
	if _, ok := coll[key]; !ok {
		return ErrNotFound
	}
	delete(coll, key)
	return nil
}

func (t *memTx) Find(ctx context.Context, meta *model.EntityMeta, pkValue any, dest any) error {
	_ = ctx
	coll := txCollection(t.data, meta.Name)
	doc, ok := coll[pkKey(pkValue)]
	if !ok {
		return ErrNotFound
	}
	return fromDocument(meta, doc, dest)
}

func (t *memTx) Execute(ctx context.Context, meta *model.EntityMeta, q query.Query, dest any) error {
	_ = ctx
	if err := provider.ValidateQueryCapabilities(memdocCapabilities, q); err != nil {
		return err
	}
	coll := txCollection(t.data, meta.Name)
	recs := make([]document, 0, len(coll))
	for _, doc := range coll {
		if matchNode(doc, q.Where) {
			recs = append(recs, doc)
		}
	}
	return scanInto(recs, meta, q, dest)
}

// --- internal helpers ---

func (p *Provider) ensureOpen() {
	if !p.opened {
		// For tests, we keep this as a hard failure to catch missing Open().
		// Open() is cheap/no-op.
		panic("memdoc: provider is not open")
	}
}

func (p *Provider) collection(entity string) map[string]document {
	coll, ok := p.data[entity]
	if !ok {
		coll = make(map[string]document)
		p.data[entity] = coll
	}
	return coll
}

func txCollection(data map[string]map[string]document, entity string) map[string]document {
	coll, ok := data[entity]
	if !ok {
		coll = make(map[string]document)
		data[entity] = coll
	}
	return coll
}

func (p *Provider) nextID(entity string) func() int64 {
	return func() int64 {
		p.autoIncr[entity] = p.autoIncr[entity] + 1
		return p.autoIncr[entity]
	}
}

func pkKey(v any) string { return fmt.Sprintf("%T:%v", v, v) }

func pkValue(meta *model.EntityMeta, entity any) (any, bool) {
	if meta.PrimaryKey == nil {
		return nil, false
	}
	val := reflect.ValueOf(entity)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	pk := val.FieldByName(meta.PrimaryKey.FieldName)
	if !pk.IsValid() {
		return nil, false
	}
	if pk.IsZero() {
		return nil, false
	}
	return pk.Interface(), true
}

func pkValueOrAuto(meta *model.EntityMeta, entity any, nextID func() int64) (any, error) {
	if meta.PrimaryKey == nil {
		return nil, ErrMissingPrimaryKey
	}
	val := reflect.ValueOf(entity)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	pkField := val.FieldByName(meta.PrimaryKey.FieldName)
	if !pkField.IsValid() {
		return nil, ErrMissingPrimaryKey
	}

	if meta.PrimaryKey.AutoIncr && pkField.IsZero() {
		id := nextID()
		// set back on entity when possible
		if pkField.CanSet() {
			switch pkField.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				pkField.SetInt(id)
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				pkField.SetUint(uint64(id))
			}
		}
		return coerceIntLike(id, pkField.Kind()), nil
	}

	if pkField.IsZero() {
		return nil, fmt.Errorf("memdoc: missing primary key value")
	}
	return pkField.Interface(), nil
}

func coerceIntLike(v int64, k reflect.Kind) any {
	switch k {
	case reflect.Int8:
		return int8(v)
	case reflect.Int16:
		return int16(v)
	case reflect.Int32:
		return int32(v)
	case reflect.Int:
		return int(v)
	case reflect.Uint8:
		return uint8(v)
	case reflect.Uint16:
		return uint16(v)
	case reflect.Uint32:
		return uint32(v)
	case reflect.Uint:
		return uint(v)
	default:
		return v
	}
}

func toDocument(meta *model.EntityMeta, entity any) document {
	val := reflect.ValueOf(entity)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	out := make(document, len(meta.Fields))
	for _, f := range meta.Fields {
		out[f.Column] = val.FieldByName(f.FieldName).Interface()
	}
	return out
}

func fromDocument(meta *model.EntityMeta, doc document, dest any) error {
	val := reflect.ValueOf(dest)
	if val.Kind() != reflect.Ptr || val.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("dest must be a pointer to struct")
	}
	val = val.Elem()

	for _, f := range meta.Fields {
		rv, ok := doc[f.Column]
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
	if src.Kind() == reflect.Float64 {
		switch fv.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			fv.SetInt(int64(src.Float()))
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			fv.SetUint(uint64(src.Float()))
		}
	}
}

func deepCopyData(in map[string]map[string]document) map[string]map[string]document {
	out := make(map[string]map[string]document, len(in))
	for ent, coll := range in {
		oc := make(map[string]document, len(coll))
		for k, doc := range coll {
			nd := make(document, len(doc))
			for dk, dv := range doc {
				nd[dk] = dv
			}
			oc[k] = nd
		}
		out[ent] = oc
	}
	return out
}

func copyAutoIncr(in map[string]int64) map[string]int64 {
	out := make(map[string]int64, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// --- query matching + scanning ---

func scanInto(recs []document, meta *model.EntityMeta, q query.Query, dest any) error {
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

	for _, doc := range recs {
		var elem reflect.Value
		if isPtr {
			elem = reflect.New(elemType.Elem())
		} else {
			elem = reflect.New(elemType)
		}

		if err := fromDocument(meta, doc, elem.Interface()); err != nil {
			return err
		}

		if isPtr {
			sliceVal = reflect.Append(sliceVal, elem)
		} else {
			sliceVal = reflect.Append(sliceVal, elem.Elem())
		}
	}

	dv.Elem().Set(sliceVal)
	return nil
}

func matchNode(doc document, node query.Node) bool {
	if node == nil {
		return true
	}
	switch n := node.(type) {
	case query.Predicate:
		return matchPredicate(doc, n)
	case query.Composite:
		return matchComposite(doc, n)
	default:
		return true
	}
}

func matchPredicate(doc document, p query.Predicate) bool {
	val, ok := doc[p.Field]
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

func matchComposite(doc document, c query.Composite) bool {
	if c.Logic == query.LogicAnd {
		for _, child := range c.Children {
			if !matchNode(doc, child) {
				return false
			}
		}
		return true
	}
	for _, child := range c.Children {
		if matchNode(doc, child) {
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
	if sub == "" {
		return true
	}
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
