package provider

import (
	"github.com/mirkobrombin/go-foundation/pkg/adapters"
)

// Registry is the global adapter registry for storage providers.
// Use Register() at init-time and Resolve() at runtime.
var Registry = adapters.NewRegistry[Provider]()

// Register adds a Provider implementation under the given name.
func Register(name string, p Provider) {
	Registry.Register(name, p)
}

// Resolve returns the Provider registered with the given name.
func Resolve(name string) (Provider, bool) {
	return Registry.Get(name)
}

// MustResolve returns the Provider or panics.
func MustResolve(name string) Provider {
	return Registry.MustGet(name)
}

// SetDefault marks a provider as the default backend.
func SetDefault(name string) {
	Registry.SetDefault(name)
}

// Default returns the default provider.
func Default() Provider {
	return Registry.Default()
}
