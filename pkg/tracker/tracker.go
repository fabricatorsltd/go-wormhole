package tracker

import (
	"fmt"
	"reflect"

	"github.com/mirkobrombin/go-foundation/pkg/safemap"
	"github.com/mirkobrombin/go-wormhole/pkg/model"
	"github.com/mirkobrombin/go-wormhole/pkg/schema"
)

// Tracker implements the Identity Map and change-detection logic
// (Unit of Work pattern). It uses a ShardedMap for safe concurrent access.
type Tracker struct {
	entries *safemap.ShardedMap[string, *model.Entry]
}

// New creates a Tracker backed by a 32-shard concurrent map.
func New() *Tracker {
	return &Tracker{
		entries: safemap.NewSharded[string, *model.Entry](safemap.StringHasher, 32),
	}
}

// Track begins tracking an entity in the given state.
// The entity is snapshotted so changes can be detected later.
func (t *Tracker) Track(entity any, state model.EntityState) {
	meta := schema.Parse(entity)
	key := t.entityKey(meta, entity)

	entry := &model.Entry{
		Entity:   entity,
		Meta:     meta,
		State:    state,
		Snapshot: snapshot(meta, entity),
	}
	t.entries.Set(key, entry)
}

// Attach starts tracking an entity as Unchanged (loaded from DB).
func (t *Tracker) Attach(entity any) {
	t.Track(entity, model.Unchanged)
}

// Add marks an entity as Added (pending insert).
func (t *Tracker) Add(entity any) {
	t.Track(entity, model.Added)
}

// Remove marks an entity as Deleted.
func (t *Tracker) Remove(entity any) {
	meta := schema.Parse(entity)
	key := t.entityKey(meta, entity)

	if e, ok := t.entries.Get(key); ok {
		e.State = model.Deleted
		t.entries.Set(key, e)
		return
	}
	// not tracked yet — track as deleted
	t.Track(entity, model.Deleted)
}

// Detach stops tracking an entity entirely.
func (t *Tracker) Detach(entity any) {
	meta := schema.Parse(entity)
	key := t.entityKey(meta, entity)
	t.entries.Delete(key)
}

// Entry returns the tracking entry for an entity, if tracked.
func (t *Tracker) Entry(entity any) (*model.Entry, bool) {
	meta := schema.Parse(entity)
	key := t.entityKey(meta, entity)
	return t.entries.Get(key)
}

// DetectChanges scans all Unchanged entries and promotes them to
// Modified if any field value differs from the snapshot.
func (t *Tracker) DetectChanges() {
	t.entries.Range(func(_ string, e *model.Entry) bool {
		if e.State != model.Unchanged {
			return true
		}
		if changed := ChangedFields(e); len(changed) > 0 {
			e.State = model.Modified
		}
		return true
	})
}

// Pending returns all entries that require persistence
// (Added, Modified, Deleted).
func (t *Tracker) Pending() []*model.Entry {
	var out []*model.Entry
	t.entries.Range(func(_ string, e *model.Entry) bool {
		if e.State == model.Added || e.State == model.Modified || e.State == model.Deleted {
			out = append(out, e)
		}
		return true
	})
	return out
}

// AcceptAll resets all entries to Unchanged and re-snapshots.
// Called after a successful SaveChanges.
func (t *Tracker) AcceptAll() {
	// Collect keys to delete outside Range to avoid deadlock
	// (Range holds RLock, Delete needs Lock on the same shard).
	var deleteKeys []string
	t.entries.Range(func(k string, e *model.Entry) bool {
		if e.State == model.Deleted {
			deleteKeys = append(deleteKeys, k)
			return true
		}
		e.State = model.Unchanged
		e.Snapshot = snapshot(e.Meta, e.Entity)
		return true
	})
	for _, k := range deleteKeys {
		t.entries.Delete(k)
	}
}

// Clear removes all tracked entities.
func (t *Tracker) Clear() {
	t.entries.Clear()
}

// ChangedFields returns the list of field names whose current value
// differs from the snapshot.
func ChangedFields(e *model.Entry) []string {
	var changed []string
	val := reflect.ValueOf(e.Entity)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	for _, f := range e.Meta.Fields {
		cur := val.FieldByName(f.FieldName).Interface()
		old, exists := e.Snapshot[f.FieldName]
		if !exists || !reflect.DeepEqual(cur, old) {
			changed = append(changed, f.FieldName)
		}
	}
	return changed
}

// --- internal helpers ---

func snapshot(meta *model.EntityMeta, entity any) map[string]any {
	val := reflect.ValueOf(entity)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	snap := make(map[string]any, len(meta.Fields))
	for _, f := range meta.Fields {
		snap[f.FieldName] = val.FieldByName(f.FieldName).Interface()
	}
	return snap
}

func (t *Tracker) entityKey(meta *model.EntityMeta, entity any) string {
	val := reflect.ValueOf(entity)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	if meta.PrimaryKey != nil {
		pk := val.FieldByName(meta.PrimaryKey.FieldName).Interface()
		// For auto-increment PKs with zero value (not yet assigned),
		// use pointer address to avoid collisions between new entities.
		if meta.PrimaryKey.AutoIncr && reflect.ValueOf(pk).IsZero() {
			return fmt.Sprintf("%s#ptr(%d)", meta.Name, val.UnsafeAddr())
		}
		return fmt.Sprintf("%s#%v", meta.Name, pk)
	}
	// fallback: use pointer address
	return fmt.Sprintf("%s#ptr(%d)", meta.Name, val.UnsafeAddr())
}
