package dsl_test

import (
	"reflect"
	"testing"

	"github.com/fabricatorsltd/go-wormhole/pkg/dsl"
	"github.com/fabricatorsltd/go-wormhole/pkg/query"
)

type compositeUser struct {
	ID     int
	Status int
}

func init() {
	dsl.Register(compositeUser{})
}

func TestAnd_TwoConditions(t *testing.T) {
	u := &compositeUser{}
	comp := dsl.And(
		dsl.Gt(u, &u.Status, 0),
		dsl.Lt(u, &u.Status, 10),
	)
	if comp.Logic != query.LogicAnd {
		t.Errorf("logic = %v, want And", comp.Logic)
	}
	if len(comp.Children) != 2 {
		t.Fatalf("children = %d, want 2", len(comp.Children))
	}
	if _, ok := comp.Children[0].(query.Predicate); !ok {
		t.Errorf("child 0 must be Predicate")
	}
}

func TestOr_TwoConditions(t *testing.T) {
	u := &compositeUser{}
	comp := dsl.Or(
		dsl.NotIn(u, &u.Status, 3, -3),
		dsl.Eq(u, &u.ID, 42),
	)
	if comp.Logic != query.LogicOr {
		t.Errorf("logic = %v, want Or", comp.Logic)
	}
	if len(comp.Children) != 2 {
		t.Fatalf("children = %d, want 2", len(comp.Children))
	}
}

func TestOrNodes_NestedAnd(t *testing.T) {
	u := &compositeUser{}
	root := dsl.OrNodes(
		dsl.Eq(u, &u.Status, 1),
		dsl.And(
			dsl.Eq(u, &u.Status, 2),
			dsl.Gt(u, &u.ID, 100),
		),
	)
	if root.Logic != query.LogicOr {
		t.Errorf("outer logic = %v", root.Logic)
	}
	if len(root.Children) != 2 {
		t.Fatalf("outer children = %d, want 2", len(root.Children))
	}
	inner, ok := root.Children[1].(query.Composite)
	if !ok {
		t.Fatalf("inner child should be Composite, got %T", root.Children[1])
	}
	if inner.Logic != query.LogicAnd || len(inner.Children) != 2 {
		t.Errorf("inner logic/children wrong: %+v", inner)
	}
}

// TestCompositeImplementsNode asserts that composites returned by dsl helpers
// can be passed wherever query.Node is accepted (Builder.Filter, EntitySet.Where, …).
func TestCompositeImplementsNode(t *testing.T) {
	u := &compositeUser{}
	var _ query.Node = dsl.And(dsl.Eq(u, &u.ID, 1))
	var _ query.Node = dsl.Or(dsl.Eq(u, &u.ID, 1))
	var _ query.Node = dsl.OrNodes(dsl.Eq(u, &u.ID, 1))
	// Smoke-test that we can also mix raw Predicates and Composites in
	// the same Builder.Filter call.
	q := query.From("composite_user").
		Filter(dsl.Eq(u, &u.ID, 1), dsl.Or(dsl.Eq(u, &u.Status, 1))).
		Build()
	got := reflect.TypeOf(q.Where).String()
	if got == "" {
		t.Errorf("Where should be populated")
	}
}
