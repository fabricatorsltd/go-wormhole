package nosqlmigrations

import (
	"context"
	"fmt"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoExecutor struct {
	db *mongo.Database
}

func NewMongoExecutor(db *mongo.Database) *MongoExecutor {
	return &MongoExecutor{db: db}
}

func (m *MongoExecutor) BackfillField(ctx context.Context, collection, field string, value any) error {
	_, err := m.db.Collection(collection).UpdateMany(ctx,
		bson.M{field: bson.M{"$exists": false}},
		bson.M{"$set": bson.M{field: value}},
	)
	return err
}

func (m *MongoExecutor) RenameField(ctx context.Context, collection, from, to string) error {
	_, err := m.db.Collection(collection).UpdateMany(ctx, bson.M{}, bson.M{"$rename": bson.M{from: to}})
	return err
}

func (m *MongoExecutor) SplitField(ctx context.Context, collection, field string, targets []string, delimiter string) error {
	cur, err := m.db.Collection(collection).Find(ctx, bson.M{field: bson.M{"$exists": true}})
	if err != nil {
		return err
	}
	defer cur.Close(ctx)
	for cur.Next(ctx) {
		var doc bson.M
		if err := cur.Decode(&doc); err != nil {
			return err
		}
		parts := strings.Split(fmt.Sprintf("%v", doc[field]), delimiter)
		set := bson.M{}
		for i, t := range targets {
			if i < len(parts) {
				set[t] = parts[i]
			}
		}
		_, err := m.db.Collection(collection).UpdateByID(ctx, doc["_id"], bson.M{"$set": set})
		if err != nil {
			return err
		}
	}
	return cur.Err()
}

func (m *MongoExecutor) MergeFields(ctx context.Context, collection string, fields []string, target, delimiter string) error {
	cur, err := m.db.Collection(collection).Find(ctx, bson.M{})
	if err != nil {
		return err
	}
	defer cur.Close(ctx)
	for cur.Next(ctx) {
		var doc bson.M
		if err := cur.Decode(&doc); err != nil {
			return err
		}
		parts := make([]string, 0, len(fields))
		for _, f := range fields {
			parts = append(parts, fmt.Sprintf("%v", doc[f]))
		}
		_, err := m.db.Collection(collection).UpdateByID(ctx, doc["_id"], bson.M{"$set": bson.M{target: strings.Join(parts, delimiter)}})
		if err != nil {
			return err
		}
	}
	return cur.Err()
}

func (m *MongoExecutor) CreateIndex(ctx context.Context, collection, name string, keys map[string]int, unique bool) error {
	idxKeys := bson.D{}
	for k, v := range keys {
		idxKeys = append(idxKeys, bson.E{Key: k, Value: v})
	}
	_, err := m.db.Collection(collection).Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    idxKeys,
		Options: options.Index().SetName(name).SetUnique(unique),
	})
	return err
}

func (m *MongoExecutor) DropIndex(ctx context.Context, collection, name string) error {
	_, err := m.db.Collection(collection).Indexes().DropOne(ctx, name)
	return err
}
