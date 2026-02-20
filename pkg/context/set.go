package context

import (
	"reflect"

	"github.com/mirkobrombin/go-wormhole/pkg/model"
	"github.com/mirkobrombin/go-wormhole/pkg/query"
	"github.com/mirkobrombin/go-wormhole/pkg/schema"
)

// EntitySet provides a fluent API for querying and managing entities
// of a single type through the DbContext.
type EntitySet struct {
	ctx   *DbContext
	dest  any
	meta  *model.EntityMeta
	preds []query.Predicate
	sorts []query.Sort
	lim   int
	off   int
}

// Set creates an EntitySet bound to the given destination.
// dest can be a *Struct (for Find) or *[]Struct / *[]*Struct (for queries).
func (c *DbContext) Set(dest any) *EntitySet {
	t := reflect.TypeOf(dest)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() == reflect.Slice {
		t = t.Elem()
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
	}
	meta := schema.ParseType(t)
	return &EntitySet{
		ctx:  c,
		dest: dest,
		meta: meta,
	}
}

// Find retrieves a single entity by primary key, populates dest,
// and auto-tracks it as Unchanged.
func (s *EntitySet) Find(pk any) error {
	ctx := s.ctx.opCtx()
	err := s.ctx.withReadResilience(ctx, func() error {
		return s.ctx.provider.Find(ctx, s.meta, pk, s.dest)
	})
	if err != nil {
		return err
	}
	s.ctx.tracker.Attach(s.dest)
	return nil
}

// Where appends filter predicates (AND logic). Chainable.
func (s *EntitySet) Where(preds ...query.Predicate) *EntitySet {
	s.preds = append(s.preds, preds...)
	return s
}

// OrderBy appends a sort clause. Chainable.
func (s *EntitySet) OrderBy(field string, dir query.SortDir) *EntitySet {
	s.sorts = append(s.sorts, query.Sort{Field: field, Dir: dir})
	return s
}

// Limit sets the maximum number of results.
func (s *EntitySet) Limit(n int) *EntitySet {
	s.lim = n
	return s
}

// Offset sets the number of results to skip.
func (s *EntitySet) Offset(n int) *EntitySet {
	s.off = n
	return s
}

// All executes the built query and scans results into dest
// (must be *[]T or *[]*T).
func (s *EntitySet) All() error {
	b := query.From(s.meta.Name)
	if len(s.preds) > 0 {
		b.Filter(s.preds...)
	}
	for _, sort := range s.sorts {
		b.OrderBy(sort.Field, sort.Dir)
	}
	if s.lim > 0 {
		b.Limit(s.lim)
	}
	if s.off > 0 {
		b.Offset(s.off)
	}
	q := b.Build()

	ctx := s.ctx.opCtx()
	return s.ctx.withReadResilience(ctx, func() error {
		return s.ctx.provider.Execute(ctx, s.meta, q, s.dest)
	})
}

// Add marks entities for insertion.
func (s *EntitySet) Add(entities ...any) {
	for _, e := range entities {
		s.ctx.tracker.Add(e)
	}
}

// Remove marks entities for deletion.
func (s *EntitySet) Remove(entities ...any) {
	for _, e := range entities {
		s.ctx.tracker.Remove(e)
	}
}
