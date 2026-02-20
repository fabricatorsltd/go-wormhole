package context

import (
	"github.com/mirkobrombin/go-foundation/pkg/di"
	"github.com/mirkobrombin/go-wormhole/pkg/provider"
)

// RegisterServices wires the standard wormhole services into a DI container.
//
//	container := di.New()
//	wormhole.RegisterServices(container, myProvider)
//	ctx := di.MustResolve[*DbContext](container, "wormhole.context")
func RegisterServices(c *di.Container, p provider.Provider, opts ...Option) {
	c.Provide("wormhole.provider", p)
	c.Provide("wormhole.context", New(p, opts...))
}

// FromContainer resolves a DbContext previously registered via RegisterServices.
func FromContainer(c *di.Container) *DbContext {
	return di.MustResolve[*DbContext](c, "wormhole.context")
}
