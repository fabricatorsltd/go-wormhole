package dsl

import "github.com/fabricatorsltd/go-wormhole/pkg/query"

// And combines two or more conditions with AND logic, returning a Composite
// usable anywhere a query.Node is accepted (Builder.Filter, EntitySet.Where,
// nested inside another And/Or, etc.).
//
//	db.Set(&users).Where(
//	    dsl.Eq(u, &u.Status, models.StatusActive),
//	    dsl.And(
//	        dsl.NotIn(u, &u.RoleId, 3, -3),
//	        dsl.Eq(u, &u.CountryCode, "IT"),
//	    ),
//	).All()
//
// Note: top-level multiple arguments to Where are already AND'd, so And() is
// most useful when nested inside an Or() to build (X AND Y) OR (Z).
func And(conds ...Condition) query.Composite {
	children := make([]query.Node, len(conds))
	for i, c := range conds {
		children[i] = c
	}
	return query.Composite{Logic: query.LogicAnd, Children: children}
}

// Or combines two or more conditions with OR logic, returning a Composite
// usable anywhere a query.Node is accepted.
//
//	db.Set(&posts).Where(
//	    dsl.Eq(p, &p.Status, "active"),
//	    dsl.Or(
//	        dsl.NotIn(u, &u.Status, 3, -3),
//	        dsl.Eq(u, &u.Id, requesterID),
//	    ),
//	).All()
func Or(conds ...Condition) query.Composite {
	children := make([]query.Node, len(conds))
	for i, c := range conds {
		children[i] = c
	}
	return query.Composite{Logic: query.LogicOr, Children: children}
}

// AndNodes / OrNodes mirror And / Or but accept arbitrary query.Node values,
// enabling nested composites (e.g. an Or that contains an And subtree).
//
//	dsl.OrNodes(
//	    dsl.Eq(u, &u.Status, statusActive),
//	    dsl.And(
//	        dsl.Eq(u, &u.Status, statusSuspended),
//	        dsl.Lt(u, &u.SuspendedAt, weekAgo),
//	    ),
//	)
func AndNodes(nodes ...query.Node) query.Composite {
	return query.Composite{Logic: query.LogicAnd, Children: nodes}
}

func OrNodes(nodes ...query.Node) query.Composite {
	return query.Composite{Logic: query.LogicOr, Children: nodes}
}
