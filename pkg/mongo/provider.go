package mongo

import (
	"context"
	"fmt"
	"reflect"

	"github.com/fabricatorsltd/go-wormhole/pkg/model"
	"github.com/fabricatorsltd/go-wormhole/pkg/provider"
	"github.com/fabricatorsltd/go-wormhole/pkg/query"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type Provider struct {
	client *mongo.Client
	db     *mongo.Database
	dbName string
	name   string
}

type Option func(*Provider)

func WithName(name string) Option {
	return func(p *Provider) { p.name = name }
}

func New(client *mongo.Client, dbName string, opts ...Option) *Provider {
	p := &Provider{
		client: client,
		dbName: dbName,
		name:   "mongo",
	}
	for _, o := range opts {
		o(p)
	}
	if p.client != nil && p.dbName != "" {
		p.db = p.client.Database(p.dbName)
	}
	return p
}

var _ provider.Provider = (*Provider)(nil)

var mongoCapabilities = provider.Capabilities{
	Transactions:     true,
	PartialUpdate:    true,
	Sorting:          true,
	OffsetPagination: true,
	NestedFilters:    true,
	SchemaEvolution:  true,
}

func (p *Provider) Name() string { return p.name }

func (p *Provider) Database() *mongo.Database { return p.db }

func (p *Provider) Capabilities() provider.Capabilities {
	return mongoCapabilities
}

func (p *Provider) Open(ctx context.Context, dsn string) error {
	if p.client == nil {
		client, err := mongo.Connect(ctx, options.Client().ApplyURI(dsn))
		if err != nil {
			return err
		}
		p.client = client
	}

	if p.dbName == "" {
		return fmt.Errorf("mongo database name is required")
	}
	p.db = p.client.Database(p.dbName)
	return p.client.Ping(ctx, readpref.Primary())
}

func (p *Provider) Close() error {
	if p.client == nil {
		return nil
	}
	return p.client.Disconnect(context.Background())
}

func (p *Provider) Insert(ctx context.Context, meta *model.EntityMeta, entity any) (any, error) {
	doc := toDocument(meta, entity)
	res, err := p.collection(meta).InsertOne(ctx, doc)
	if err != nil {
		return nil, err
	}
	return res.InsertedID, nil
}

func (p *Provider) Update(ctx context.Context, meta *model.EntityMeta, entity any, changed []string) error {
	id, ok := pkValue(meta, entity)
	if !ok {
		return fmt.Errorf("missing primary key value")
	}
	setDoc := changedDocument(meta, entity, changed)
	if len(setDoc) == 0 {
		return nil
	}
	_, err := p.collection(meta).UpdateByID(ctx, id, bson.D{{Key: "$set", Value: setDoc}})
	return err
}

func (p *Provider) Delete(ctx context.Context, meta *model.EntityMeta, pkValue any) error {
	_, err := p.collection(meta).DeleteOne(ctx, bson.D{{Key: "_id", Value: pkValue}})
	return err
}

func (p *Provider) Find(ctx context.Context, meta *model.EntityMeta, pkValue any, dest any) error {
	var raw bson.M
	err := p.collection(meta).FindOne(ctx, bson.D{{Key: "_id", Value: pkValue}}).Decode(&raw)
	if err != nil {
		return err
	}
	return fromDocument(meta, raw, dest)
}

func (p *Provider) Execute(ctx context.Context, meta *model.EntityMeta, q query.Query, dest any) error {
	if _, err := provider.ValidateQueryCapabilities(meta, p.Capabilities(), q); err != nil {
		return err
	}

	filter, err := BuildFilter(q.Where)
	if err != nil {
		return err
	}
	cur, err := p.collection(meta).Find(ctx, filter, BuildFindOptions(q))
	if err != nil {
		return err
	}
	defer cur.Close(ctx)

	return scanCursor(meta, cur, dest)
}

func (p *Provider) Begin(ctx context.Context) (provider.Tx, error) {
	if p.client == nil || p.db == nil {
		return nil, fmt.Errorf("mongo provider is not open")
	}
	session, err := p.client.StartSession()
	if err != nil {
		return nil, err
	}
	if err := session.StartTransaction(); err != nil {
		session.EndSession(ctx)
		return nil, err
	}
	return &mongoTx{p: p, session: session, ctx: ctx}, nil
}

type mongoTx struct {
	p       *Provider
	session mongo.Session
	ctx     context.Context
}

func (t *mongoTx) Commit() error {
	defer t.session.EndSession(t.ctx)
	return t.session.CommitTransaction(t.ctx)
}

func (t *mongoTx) Rollback() error {
	defer t.session.EndSession(t.ctx)
	return t.session.AbortTransaction(t.ctx)
}

func (t *mongoTx) Insert(ctx context.Context, meta *model.EntityMeta, entity any) (any, error) {
	doc := toDocument(meta, entity)
	res, err := t.p.collection(meta).InsertOne(t.sc(ctx), doc)
	if err != nil {
		return nil, err
	}
	return res.InsertedID, nil
}

func (t *mongoTx) Update(ctx context.Context, meta *model.EntityMeta, entity any, changed []string) error {
	id, ok := pkValue(meta, entity)
	if !ok {
		return fmt.Errorf("missing primary key value")
	}
	setDoc := changedDocument(meta, entity, changed)
	if len(setDoc) == 0 {
		return nil
	}
	_, err := t.p.collection(meta).UpdateByID(t.sc(ctx), id, bson.D{{Key: "$set", Value: setDoc}})
	return err
}

func (t *mongoTx) Delete(ctx context.Context, meta *model.EntityMeta, pkValue any) error {
	_, err := t.p.collection(meta).DeleteOne(t.sc(ctx), bson.D{{Key: "_id", Value: pkValue}})
	return err
}

func (t *mongoTx) Find(ctx context.Context, meta *model.EntityMeta, pkValue any, dest any) error {
	var raw bson.M
	err := t.p.collection(meta).FindOne(t.sc(ctx), bson.D{{Key: "_id", Value: pkValue}}).Decode(&raw)
	if err != nil {
		return err
	}
	return fromDocument(meta, raw, dest)
}

func (t *mongoTx) Execute(ctx context.Context, meta *model.EntityMeta, q query.Query, dest any) error {
	if _, err := provider.ValidateQueryCapabilities(meta, mongoCapabilities, q); err != nil {
		return err
	}
	filter, err := BuildFilter(q.Where)
	if err != nil {
		return err
	}
	cur, err := t.p.collection(meta).Find(t.sc(ctx), filter, BuildFindOptions(q))
	if err != nil {
		return err
	}
	defer cur.Close(t.sc(ctx))

	return scanCursor(meta, cur, dest)
}

func (t *mongoTx) sc(ctx context.Context) mongo.SessionContext {
	if ctx == nil {
		ctx = t.ctx
	}
	return mongo.NewSessionContext(ctx, t.session)
}

func (p *Provider) collection(meta *model.EntityMeta) *mongo.Collection {
	return p.db.Collection(meta.Name)
}

func toDocument(meta *model.EntityMeta, entity any) bson.M {
	val := reflect.ValueOf(entity)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	doc := bson.M{}
	for _, f := range meta.Fields {
		fv := val.FieldByName(f.FieldName)
		key := f.Column
		if f.PrimaryKey {
			key = "_id"
		}
		doc[key] = fv.Interface()
	}
	return doc
}

func changedDocument(meta *model.EntityMeta, entity any, changed []string) bson.M {
	val := reflect.ValueOf(entity)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	changedSet := make(map[string]struct{}, len(changed))
	for _, c := range changed {
		changedSet[c] = struct{}{}
	}

	doc := bson.M{}
	for _, f := range meta.Fields {
		if f.PrimaryKey {
			continue
		}
		if _, ok := changedSet[f.FieldName]; !ok {
			continue
		}
		doc[f.Column] = val.FieldByName(f.FieldName).Interface()
	}
	return doc
}

func pkValue(meta *model.EntityMeta, entity any) (any, bool) {
	if meta.PrimaryKey == nil {
		return nil, false
	}
	val := reflect.ValueOf(entity)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	fv := val.FieldByName(meta.PrimaryKey.FieldName)
	if !fv.IsValid() {
		return nil, false
	}
	return fv.Interface(), !isZeroValue(fv)
}

func scanCursor(meta *model.EntityMeta, cur *mongo.Cursor, dest any) error {
	dv := reflect.ValueOf(dest)
	if dv.Kind() != reflect.Ptr || dv.Elem().Kind() != reflect.Slice {
		return fmt.Errorf("dest must be *[]T or *[]*T")
	}

	sliceVal := dv.Elem()
	elemType := sliceVal.Type().Elem()
	isPtr := elemType.Kind() == reflect.Ptr

	for cur.Next(context.Background()) {
		var raw bson.M
		if err := cur.Decode(&raw); err != nil {
			return err
		}

		var elem reflect.Value
		if isPtr {
			elem = reflect.New(elemType.Elem())
		} else {
			elem = reflect.New(elemType)
		}

		if err := fromDocument(meta, raw, elem.Interface()); err != nil {
			return err
		}

		if isPtr {
			sliceVal = reflect.Append(sliceVal, elem)
		} else {
			sliceVal = reflect.Append(sliceVal, elem.Elem())
		}
	}
	if err := cur.Err(); err != nil {
		return err
	}
	dv.Elem().Set(sliceVal)
	return nil
}

func fromDocument(meta *model.EntityMeta, doc bson.M, dest any) error {
	val := reflect.ValueOf(dest)
	if val.Kind() != reflect.Ptr || val.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("dest must be *struct")
	}
	val = val.Elem()

	for _, f := range meta.Fields {
		key := f.Column
		if f.PrimaryKey {
			key = "_id"
		}
		raw, ok := doc[key]
		if !ok {
			continue
		}
		setField(val.FieldByName(f.FieldName), raw)
	}
	return nil
}

func setField(dst reflect.Value, raw any) {
	if !dst.IsValid() || !dst.CanSet() || raw == nil {
		return
	}
	src := reflect.ValueOf(raw)
	if src.Type().AssignableTo(dst.Type()) {
		dst.Set(src)
		return
	}
	if src.Type().ConvertibleTo(dst.Type()) {
		dst.Set(src.Convert(dst.Type()))
		return
	}

	switch v := raw.(type) {
	case primitive.ObjectID:
		if dst.Kind() == reflect.String {
			dst.SetString(v.Hex())
		}
	case int32:
		if isIntKind(dst.Kind()) {
			dst.SetInt(int64(v))
		}
	case int64:
		if isIntKind(dst.Kind()) {
			dst.SetInt(v)
		}
	case float64:
		if isIntKind(dst.Kind()) {
			dst.SetInt(int64(v))
		}
	}
}

func isZeroValue(v reflect.Value) bool {
	return reflect.DeepEqual(v.Interface(), reflect.Zero(v.Type()).Interface())
}

func isIntKind(k reflect.Kind) bool {
	switch k {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return true
	default:
		return false
	}
}

func RegisterDefault(client *mongo.Client, dbName string, opts ...Option) *Provider {
	p := New(client, dbName, opts...)
	provider.Register(p.name, p)
	provider.SetDefault(p.name)
	return p
}
