<p align="center">
  <img src="assets/cago-icon.png" alt="Cago gopher inside a C-shaped channel with connection terminals" width="240">
</p>

# cago

A pure-Go EPICS Channel Access client and CLI. The library uses only the standard
library; no libca, EPICS Base installation, cgo, or external repeater is required
at runtime. Requires **Go 1.27 or newer**.

## Install

```sh
go get github.com/wakonig/cago/pkg/cago
go install github.com/wakonig/cago/cmd/cago@latest
```

## Library

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/wakonig/cago/pkg/cago"
)

func main() {
    ctx := context.Background()
    client, err := cago.New(ctx, cago.Options{})
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    events, err := client.Subscribe(ctx, "SLS:STATUS", cago.WithMetadata(cago.Time))
    if err != nil {
        log.Fatal(err)
    }
    for event := range events {
        fmt.Println(event.PV, event.Value.Slice(), event.Value.Metadata().Timestamp)
    }
}
```

Subscriptions reconnect automatically. Cancel their context to unsubscribe;
recoverable monitor failures are reported through `client.Errors()`.

Features include reads and writes, shared subscriptions, native CA types and
metadata, array support, grouped operations, and client metrics. Discovery uses
standard `EPICS_CA_*` environment variables and can be configured with `cago.Options`.
Supports IPv4 Channel Access clients; CA servers and pvAccess are outside its scope.

## CLI

```sh
cago get PV:NAME
cago put PV:SETPOINT 1.25
cago info PV:NAME
cago monitor --timestamp PV:NAME
```

Use `cago <command> --help` for options. For local discovery, for example:

```sh
EPICS_CA_ADDR_LIST=127.0.0.1 EPICS_CA_AUTO_ADDR_LIST=NO cago get PV:NAME
```

## Development

```sh
go test -race -shuffle=on -coverprofile=/tmp/cago-coverage.out ./...
./scripts/check-coverage.sh /tmp/cago-coverage.out 90
go vet ./...
CGO_ENABLED=0 go build ./...
```

IOC integration tests require EPICS Base's `softIoc` on `PATH`:

```sh
CAGO_REQUIRE_IOC=1 go test -race -tags=e2e -timeout=5m ./test/e2e
```

See the [technical reference](docs/reference.md) for API behavior, configuration,
CLI details, metrics, and compatibility limits.

## Icon attribution

The Cago icon is an adaptation of the Go gopher by Renée French, licensed under
[Creative Commons Attribution 4.0](https://creativecommons.org/licenses/by/4.0/).
