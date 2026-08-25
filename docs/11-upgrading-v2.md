# Upgrading to v2

Wormhole v2 moves its runtime dependencies to Foundation v2, Slipstream v2 and
Warp v2. Because Foundation and Slipstream types appear in Wormhole's public
API, this is a breaking change and Wormhole uses the `/v2` semantic import
path.

## Update the module

```bash
go get github.com/fabricatorsltd/go-wormhole/v2@latest
go mod tidy
```

Update Wormhole imports:

```go
import wh "github.com/fabricatorsltd/go-wormhole/v2/pkg/context"
```

The standalone CLI also uses the v2 path:

```bash
go install github.com/fabricatorsltd/go-wormhole/v2/cmd/wormhole@latest
```

## Update Foundation imports

Applications that pass Foundation types to Wormhole must migrate those imports
at the same time. The public Wormhole APIs affected are `WithRetry`,
`WithReadRetry`, `Before`, `After`, `RegisterServices` and `FromContainer`.

```go
import (
    "github.com/mirkobrombin/go-foundation/v2/app/di"
    "github.com/mirkobrombin/go-foundation/v2/core/resiliency"
)
```

The v1 packages map to v2 as follows:

| Foundation v1 | Foundation v2 |
|---|---|
| `pkg/di` | `v2/app/di` |
| `pkg/resiliency` | `v2/core/resiliency` |
| `pkg/hooks` | `v2/core/hooks` |
| `pkg/errutil` | `v2/core/errutil` |

## Update Slipstream options

Code that passes Slipstream engine options to `slipstream.New` must import the
v2 engine package:

```go
import "github.com/mirkobrombin/go-slipstream/v2/pkg/engine"
```

Wormhole brings Warp v2 through Slipstream v2. Applications do not need to add
Warp directly unless they use its API themselves.

## Compatibility boundary

Wormhole v1 remains available under
`github.com/fabricatorsltd/go-wormhole`. Wormhole v2 is installed and imported
from `github.com/fabricatorsltd/go-wormhole/v2`; the two major versions can
therefore coexist in a module graph while an application migrates.
