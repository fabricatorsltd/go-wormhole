package sql_test

import (
	"strings"
	"testing"

	"github.com/fabricatorsltd/go-wormhole/pkg/provider"
	"github.com/fabricatorsltd/go-wormhole/pkg/query"
)

func subGuardQ(op query.Op, cols []string) query.Query {
	return query.Query{
		EntityName: "users",
		Where:      query.Subquery{Field: "id", Op: op, Query: query.Query{EntityName: "orders", Columns: cols}},
	}
}

// A subquery filter is rejected when the provider does not report Subqueries
// support, rather than silently vanishing in a backend that cannot translate it.
func TestValidateCapabilities_SubqueryUnsupported(t *testing.T) {
	_, err := provider.ValidateQueryCapabilities(nil, provider.Capabilities{Subqueries: false}, subGuardQ(query.OpIn, []string{"user_id"}))
	if err == nil || !strings.Contains(err.Error(), "subquery") {
		t.Fatalf("want subquery rejection, got %v", err)
	}
}

// An IN subquery must project exactly one column; EXISTS ignores projection.
func TestValidateCapabilities_SubqueryArity(t *testing.T) {
	caps := provider.Capabilities{Subqueries: true}
	for _, cols := range [][]string{{}, {"a", "b"}} {
		if _, err := provider.ValidateQueryCapabilities(nil, caps, subGuardQ(query.OpIn, cols)); err == nil || !strings.Contains(err.Error(), "exactly one column") {
			t.Errorf("cols %v: want arity error, got %v", cols, err)
		}
	}
	if _, err := provider.ValidateQueryCapabilities(nil, caps, subGuardQ(query.OpIn, []string{"user_id"})); err != nil {
		t.Errorf("single-column IN should pass, got %v", err)
	}
	if _, err := provider.ValidateQueryCapabilities(nil, caps, subGuardQ(query.OpExists, nil)); err != nil {
		t.Errorf("EXISTS should pass with no projection, got %v", err)
	}
}
