# Technical reference

[Back to the README](../README.md). Run commands from the repository root.

The Go module lives at the repository root. `pkg/cago` exposes the application
API, while `internal/ca` owns the Channel Access implementation. The layout follows
[Go Project Layout](https://github.com/golang-standards/project-layout):

```text
go.mod, go.sum       Module definition and dependencies
pkg/cago/           Supported application API and API tests
cmd/cago/           CLI entry point
internal/ca/        Private client runtime and implementation tests
internal/cli/       Command implementations
internal/wire/      Private protocol framing
test/e2e/           IOC integration tests and testdata
scripts/            Development and environment helpers
bin/                Generated executables and local artifacts (ignored)
```

Run source-tree commands from the repository root. Import the library as
`github.com/bec-project/cago/pkg/cago`.

The public package exposes client/channel/subscription handles, values and
metadata, configuration, groups, and metrics. Handle and value types are selected
aliases so shared identity and event streams are preserved across this boundary.
Discovery, circuits, attachment and reconnect logic, decoding, queueing, and the
repeater stay under `internal`; applications cannot import those packages.
Raw `DecodeValue` and standalone `StartRepeater` helpers are internal rather than
part of the supported application API. `Put`, `PutNotify`, and `Group.Put` accept
Go scalars, arrays, slices, or an existing `Value`. Use `NewValue` to prepare a
reusable write value; received values come from reads and subscriptions.

On arm64, `GOEXPERIMENT=simd` enables an optional experimental NEON path for
FLOAT/DOUBLE array encoding and decoding. Standard builds and other architectures
use scalar conversion. The vector path preserves floating-point bits, supports
unaligned data and handles partial vectors with scalar tails. It remains cgo-free.
`-tags=cago_scalar` forces the scalar backend even with the experiment enabled,
for diagnosis and controlled comparisons.

```go
import "github.com/bec-project/cago/pkg/cago"

client, err := cago.New(ctx, cago.Options{})
if err != nil { return err }
defer client.Close()
events, err := client.Subscribe(ctx, "SLS:STATUS", cago.WithMetadata(cago.Time))
if err != nil { return err }
for event := range events {
    fmt.Println(event.PV, event.Value.Slice(), event.Value.Metadata().Timestamp)
}
```

Cancel `ctx` to unsubscribe and close the returned channel. Recoverable monitor
failures go to `client.Errors()` with the PV name and are retried automatically.
Requested monitor counts are capped at the channel's current native capacity,
including after reconnects, matching libca subscriptions. For a handle
with a separate error stream and explicit `Cancel`, use
`client.Channel("SLS:STATUS").Subscribe(ctx, options...)`.
Both APIs use the client's shared channel handles; cancelling one subscription
preserves that handle for other users until `Channel.Close` or `Client.Close`.

Matching subscriptions also share one IOC monitor and fan updates out locally.
Each consumer keeps its own buffer, context, and error stream. The last consumer
leaving cancels the IOC monitor; a later subscription starts a new one. A late
joiner immediately receives the latest cached event, retaining its observation
timestamp. Disconnects and monitor failures invalidate that cache, so joiners
wait for a fresh snapshot before receiving values.

Sharing requires matching requested type, metadata level, element count, and
effective event mask on the same channel/priority. Buffer size is local and does
not affect sharing. `Native` and an explicit type remain separate because
`Native` can resolve differently after an IOC restart. Monitor errors reach each
handle's error stream; direct subscribers produce one shared `client.Errors()`
entry per failure rather than one entry per consumer.
Full DBR metadata types are normalized to their equivalent base type and
metadata level, so both spellings share monitors and property-event behavior.

## Features

- MTU-bounded batched UDP broadcast/unicast and TCP nameserver search, exponential search retry,
  embedded repeater, beacon-triggered rediscovery, shared TCP virtual circuits.
- Durable channel handles, reconnect and subscription restoration, native type,
  maximum element count, host and access-right information, lifecycle events.
- Acknowledged reads and writes, unacknowledged writes, type conversion requested
  from the IOC, partial and dynamic array reads, alarm acknowledgement writes.
- Native STRING, SHORT, FLOAT, ENUM, CHAR, LONG and DOUBLE values; status, time,
  graphics/control metadata, enum labels, STSACK_STRING and CLASS_NAME decoding.
- Extended CA frame headers and configurable payload limits; connection priorities
  0–99 with separate circuits, heartbeat/inactivity detection and idle retirement.
- Serialized socket writes, cancellation-aware dialing, writes and waits; close
  waits for local delivery to stop. Separate bounded value/error streams keep
  monitor errors from evicting the newest value. Rejected monitors retry.
- Bounded-concurrency `Group` reads/writes with cancellation and reset, client
  statistics, pluggable metrics and asynchronous server exception reporting.

UDP discovery combines searches queued within a two-millisecond window into
datagrams bounded by `SearchMTU` (1400 bytes by default, including CA framing).
Retries keep their per-channel backoff. Each configured destination receives
the batch; repeated queued searches for one channel are coalesced.
`SearchMTU` accepts 576–65507 bytes, so even the minimum accommodates the maximum
accepted channel name.

`MaxConcurrentCreates` defaults to four simultaneous channel attachments per IOC
address, shared across priorities. Slots cover the shared-dial wait and Create
handshake, including cleanup of a successful late response after cancellation.
A stalled IOC does not consume another IOC's slots. Waiting operations retain
their caller deadlines; durable channel handles continue background attachment
until closed. Queued attachments follow updated search results and beacon wakeups,
and restart discovery after `ContextTimeout` waiting for admission. Closing a
queued handle cancels its admission promptly.

Channel names and priorities identify shared handles: repeated `Channel` calls
return the same handle until it is closed. Channel.Close permanently disposes
that handle; a subsequent Client.Channel call creates a fresh one. `Info` is a
snapshot; `Connect` waits for attachment. `StateChanges` includes access updates.
Subscription `ctx` controls its entire lifetime. Values are immutable; array
accessors return copies. Value queues default to one element and drop oldest on
overflow. Cancellation discards queued values and closes both subscription
streams before Done closes. `Cancel` and `Close` are safe to repeat.

`NewValue` rejects overflow and strings longer than 39 bytes (CA STRING has a
40-byte NUL-terminated wire slot); use a CHAR waveform for longer text. Writes
validate NUL termination in every STRING slot, including decoded values. Default
manual `MaxArrayBytes` is 16384, configurable through Options or
EPICS_CA_MAX_ARRAY_BYTES. Receive buffers grow automatically by default, matching
libca's EPICS_CA_AUTO_ARRAY_BYTES=YES. Set that variable to NO or set
`Options.AutoArrayBytes` to a pointer to false to use a manual receive limit.
Only case-insensitive YES enables automatic growth through the environment.
`WithCount(0)` requests the current length on CA 4.13 servers; older servers use
the attached maximum count. `Group` is a set of independent operations, not an
atomic transaction; reads and writes in a group may run in any order.

`MaxArrayBytes` controls manual receive storage; it does not restrict writes or
read/monitor admission. Manual mode accepts at least 16384 payload bytes and adds
24 bytes to the configured limit, as libca does. Oversized incoming messages are
drained and ignored while the shared circuit stays connected; the affected call
can time out waiting for a usable reply. Protocol message-size limits still apply.

Reads and monitors follow libca's callback API: a successful response supplies its
actual DBR type and element count. Only ECA_NORMAL (1) completes a read, write or
monitor update successfully. Empty monitor messages are ignored, and unknown TCP
commands disconnect the circuit.

`Value.Bytes()` returns value elements in network byte order. `Value.DBRBytes()`
returns a copy of the complete DBR record, preserving metadata, structure padding,
fixed string slots and raw numeric bits. Compact scalar STRING replies expand to
a 40-byte slot with zero-filled missing bytes. Frame padding is excluded.
`Metadata.SecondsPastEpoch`, `Nanoseconds` and signed `EnumCount` retain the server's
original fields, including unusual values accepted by libca. `Timestamp` normalizes
the timestamp for Go; the convenience enum label slice is bounded to sixteen slots.
Raw signed alarm fields can be read as `int16(m.Status)` and `int16(m.Severity)`;
STSACK alarm fields are unsigned. All original representations remain in DBRBytes.
Truncated payloads are rejected instead of reading bytes outside the message.

## Configuration and tools

`Options` accepts explicit search addresses, UDP/TCP search transport, operation
and inactivity timeouts, maximum search period, array limits, repeater port,
client/host identity and default priority. Per-channel `WithPriority` overrides
the default. Discovery honors EPICS_CA_ADDR_LIST, EPICS_CA_AUTO_ADDR_LIST,
EPICS_CA_SERVER_PORT, EPICS_CA_REPEATER_PORT, EPICS_CA_CONN_TMO,
EPICS_CA_MAX_SEARCH_PERIOD, EPICS_CA_MAX_ARRAY_BYTES and EPICS_CA_AUTO_ARRAY_BYTES. Explicit search addresses
replace automatic address discovery.
The default client identity uses `USER`, falling back to the operating-system
username when it is unset (the bare username on Windows, matching libca).
An explicit `ClientName` overrides both.
Initial search-address DNS resolution honors cancellation and is bounded by
`ContextTimeout`. This initialization deadline does not shorten the client's
lifetime. The `cago get`, `cago put`, and `cago info` timeout options also bound this setup step.
TCP nameservers are searched independently, with repeated searches coalesced per
channel while an endpoint is busy. A stalled nameserver does not delay replies
from healthy endpoints. Embedded repeater startup also honors cancellation
during DNS resolution.

Install the unified CLI, then select an operation with a subcommand:

```sh
go install github.com/bec-project/cago/cmd/cago@latest
cago get PV:NAME
cago get --timeout 2s --count 10 PV:WAVEFORM
cago put PV:SETPOINT 1.25
cago put -a PV:WAVEFORM 3 1 2 3
cago put -S PV:CHAR_WAVEFORM 'operator ready'
cago info PV:NAME
cago monitor PV:NAME
```

From a checkout, install with `go install ./cmd/cago` or replace `cago` with
`go run ./cmd/cago`. Discovery uses the
same EPICS environment variables as the library; for example,
`EPICS_CA_ADDR_LIST=127.0.0.1 EPICS_CA_AUTO_ADDR_LIST=NO cago get PV:NAME`.

`cago --help` (or `cago help`) lists the commands and each command's flags,
including descriptions and defaults. `cago get --help` (or `cago help get`) shows
just that command's options. Cobra long flags use `--`, such as
`cago get --timeout 2s --count 10 PV:WAVEFORM` and
`cago put --completion --wait 2 PV:SETPOINT 1.25`. Existing EPICS short flags and
legacy `-timeout` / `-count` spellings remain supported after the subcommand.
Put options before the first PV; everything after it is treated as an operand,
including negative numbers and text beginning with `-`. Use `--` before a PV
whose name starts with `-`.

`cago get` starts discovery for all requested PVs and gives each read its own
operation timeout, so an unavailable PV does not suppress healthy PV results.
Scalar PV names occupy at least 30 columns, arrays print their count followed by space-separated elements,
floating-point values use six significant digits, and strings use EPICS escaping
inside double quotes, including CHAR arrays printed with `-S`. ENUMs print
quoted labels; `-n` selects numeric indices.
`get -t` omits PV names and `get -S` / `monitor -S` print CHAR arrays as text.
Like synchronous `caget`, default `get` reads native capacity and prints zeros
for unused waveform elements. `get -c` follows callback `caget -c` and prints the
actual returned length. `--count N` limits either mode, clamped to native capacity.
`get` omits timestamps by default. Add `--timestamp` (`-T`) to print local record
timestamps with six fractional digits and two spaces before the value. Monitor
uses an interactive history table by default; see the live terminal table section.

`cago info` prints the compact `cainfo` connection report: state, host, access,
native and request types, and element count. It does not read the PV value.
`cago info --verbose` adds a detailed report with description, record type, native DBF
type, maximum element count, timestamp, value, named alarm status/severity, IOC
address, separate read/write access, state, and priority. STRING values are quoted
and timestamps use local time. Values preview up to 16 elements; `--count N`
changes the limit and `--count 0` requests the current full value. Manual receive
limits apply when automatic array growth is disabled. The element count describes
channel capacity, not preview size.
`--timeout` bounds each PV's connection and value read, and `--priority` selects
the CA priority (0–99). Optional description (`.DESC`) and record type (`.RTYP`,
with `DBR_CLASS_NAME` fallback) share a short bounded lookup; unavailable fields
are marked explicitly. Write-only
PVs still report connection details, with value/time/alarm marked unavailable.

`cago put` follows EPICS Base's completion switch: plain writes do not wait for
record-processing completion; `-c` waits for the IOC's put-completion response.
`-w 10` sets the timeout in seconds, not the completion mode. The default is one
second, overridden by `EPICS_CLI_TIMEOUT`. Each connect, read, write or completion
wait receives its own timeout; `-timeout 10s` is a duration-valued alias.
For example, `cago put -c -w 10 PV:SETPOINT 1.25` waits for completion.
A timed-out or disconnected write can still have reached the IOC; the command
returns failure and never automatically retries it. Completion reflects IOC
record/link processing, not necessarily physical motion completion.

It supports scalar writes,
`-a PV COUNT VALUE...` arrays, `-S` NUL-terminated CHAR text, ENUM labels or indices
(`-n` forces indices, `-s` forces labels), `-p` priority (0–99), and `-t` terse
readback. Options precede the PV; scalar value arguments are joined with
spaces. Like EPICS Base, ordinary values use STRING conversion on the server;
ENUM auto mode resolves labels before numeric indices. Strings are literal
(escape sequences are not expanded) and oversized STRING values are rejected
instead of truncated. ENUM indices must be unsigned 16-bit integers.
Default output prints old and new IOC readbacks; `-t` prints only the new value.
Without `-c`, readback is not a processing-completion guarantee. Readbacks cover
the waveform's native capacity, as in `caput`, including zero-filled unused
elements. Readback failures return a nonzero status, including after a write was
sent. A failed old readback does not prevent the write, so write-only PVs can
still be written; their unreadable final value produces a nonzero exit status.
For write-only ENUMs in auto mode, the IOC converts STRING input, resolving labels
before numeric indices and applying its own index validation. Forced-label mode
(`-s`) requires readable labels; `-n` writes explicitly validated numeric indices.
Positive, finite timeouts of at least one nanosecond are required; invalid
timeouts and priorities are rejected rather than ignored or clamped. Other
intentional differences include strict array counts and enum index validation.
This is a small command interface, not a replacement for every EPICS Base tool
option (such as `-l`, `-F`, and configurable numeric precision).

## Live terminal table

Use `cago monitor PV...` for a sequential event view with the
timestamp in the leftmost column and each PV name in its own column header.
Each update fills only its PV column; other cells are blank to make the event
sequence clear. Alarm status/severity appear beside the value when active.
Each value update adds a row, including repeated
values. Events appear in delivery order across PVs; record timestamps may go
backwards and are not used to sort the rows. Connection changes also add rows,
using the local time and a message under the affected PV. Column widths adapt to
the terminal; below 80 columns the timestamp shows time only.
The fixed header remains visible as the table grows. The newest 10,000 rows are
retained in a rolling buffer. The view follows new rows automatically. Scroll up,
Page Up, or Home to pause following; press End or `G` to resume. While paused,
new events are still recorded; the selected event stays in place until it expires
from the retained history. Use `cago monitor --plain PV...` for EPICS-style line output suitable for pipes
and log files. Add `--timestamp` (`-T`) for record timestamps; `--numeric` and
`--long-string` retain their existing behavior. `--plain` and `--table` are mutually
exclusive.

Run `cago monitor --table PV...` for an interactive Bubble Tea table. Each unique
PV occupies one row in command-line order; incoming values replace that row.
The header stays fixed while arrow keys, Page Up/Down, Home, and End scroll the
rows. Press `q` or Ctrl+C to stop subscriptions and restore the terminal.

PV, value, and connection state are always available. Terminals at least 126
columns wide also show record timestamps, alarm status, and severity. Resizing
reflows the columns. The table refreshes at most ten times per second and keeps
only the latest row per PV, rather than an event history. Array previews show
at most sixteen elements followed by an ellipsis when truncated. `--numeric`
and `--long-string` retain their ordinary formatting behavior; timestamps are
shown automatically when space permits, without an extra flag.

Missing PVs remain visible while discovery continues. Disconnects mark the row
and retain the last value and timestamp; a new value restores the connected
state. Connection and subscription errors appear in the connection column.
Interactive terminal input and output are required for both table views;
`--plain` works without a terminal.

## Metrics

Every client keeps atomic counters and gauges, available through `client.Metrics()`.
The returned map is independent of client storage; individual metric reads are
atomic, while a snapshot across metrics is not a transaction. Existing `Stats()`
fields retain their meanings and read the same counters.

```go
snapshot := client.Metrics()
fmt.Println(snapshot[cago.MetricSearchDatagrams])
fmt.Println(snapshot[cago.MetricCreatesInFlight])
fmt.Println(snapshot[cago.MetricMonitorsActive])
```

Metrics distinguish logical search requests from successfully sent UDP datagrams,
connection attempts from failures, and Create attempts from failures. Search-send
failures count failed UDP writes and TCP search sends; dialing failures have
their own counter. Create
attempts begin when a binding starts trying to send its handshake, including
pre-send cancellation; dialing and retirement failures are separate. The
in-flight creation gauge counts admitted attachment callers, including their
wait for a shared connection and any late-response cleanup.

Monitor activation attempts and failures are counted per IOC generation, so
matching local consumers share one active monitor gauge entry. Rejection, send
failure, cancellation and binding loss release that entry. Intentional
subscription cancellation does not count as an activation failure. Other
counters cover dropped values/errors, protocol-error frames and circuit
identifier rotations. Queue overflow counts lost entries; cancellation discarding
queued values is not overflow. `Metric.String()` returns stable collector names.

Set `Options.Metrics` to a `Metrics` sink to export deltas, or use a shared
`*ClientMetrics` collector to aggregate multiple clients. Sink `Add` calls run
inline, may be concurrent, and must return promptly without calling back into
the client. Sink panics are recovered and counted as `MetricSinkPanics`. Polling
`client.Metrics()` requires no sink.

## Validation

The default suite uses isolated protocol peers and includes parser/numeric fuzz
targets. Run unit checks and the strict per-package coverage gate with:

```sh
go test -race -shuffle=on -coverprofile=/tmp/cago-coverage.out ./...
./scripts/check-coverage.sh /tmp/cago-coverage.out 90
go vet ./...
CGO_ENABLED=0 go build ./...
```

With EPICS Base `softIoc` on `PATH`, run the independent IOC suite:

```sh
CAGO_REQUIRE_IOC=1 go test -race -tags=e2e -shuffle=on -count=2 -timeout=5m -v ./test/e2e
```

Each IOC fixture uses a unique PV prefix and temporary database, binds loopback
only, and cleans up its subprocess and sockets. The suite checks native values,
metadata, large and partial arrays, priorities, shared monitoring, cancellation,
and mass attachment and recovery after IOC restart. Without `CAGO_REQUIRE_IOC`,
a missing `softIoc` skips these local tests; CI requires it.

CI runs unit/race/vet checks, builds without cgo, requires statement coverage
strictly above 90% for every package and the weighted total, and runs the IOC
suite against a checksum-verified EPICS Base 7.0.8.1 build on Ubuntu 24.04.
The libca and upstream comparison harnesses remain external.

## Compatibility scope

The reference feature inventory comes from EPICS Base `7.0/modules/ca/src`,
particularly `cadef.h`, `caProto.h`, `db_access.h`, `acctst.c`, `test/ca_test.c`,
`catime.c`, `caConnTest.cpp` and `test_event.cpp`. Regression scenarios are also
compared with `epics-engineer/ca` v0.1.0. Protocol definitions and test scenarios
are references, not copied implementation code.

This is a Go API, not a C ABI replacement. C thread-local contexts, manual
`pend_event`/`flush_io` callback pumping, file descriptor callbacks, IOC-local
DB access, legacy aliases, Perl bindings and native diagnostic executables are
outside its API. Requests are flushed directly and Go contexts/channels replace
callback pumping. It supports IPv4 CA; IPv6 is outside the CA transport target.
Protocol minors below 4 are rejected. Legacy server compatibility needs separate
qualification; do not infer it from modern-IOC tests. Clients do not implement
CA servers or pvAccess/PVA.

Request and monitor identifiers are unique within each TCP connection. When
that connection exhausts its 32-bit identifier budget, it stops admitting new
operations, drains operations already admitted, then closes. Durable channel
handles reconnect and subscriptions are restored on a replacement connection
with a fresh identifier budget. Replies from the old connection cannot reach
the replacement's routing tables.
Before closing, a successful tagged Echo confirms the IOC has handled preceding
protocol messages, preserving plain-write/read ordering across the replacement. This is
not record-processing completion; use `PutNotify` when that is required.

`RotationDrainTimeout` bounds the drain and Echo barrier and defaults to `ContextTimeout` (five
seconds by default). Set it long enough for expected record-processing times.
Cancellation and client shutdown interrupt the drain. If the timeout expires,
unfinished calls fail with a disconnect error; a write may already have taken
effect at the IOC. Sent writes are never automatically replayed. Operations
waiting for admission use their own contexts and continue after rotation only
if they have not sent any bytes.
If the barrier times out, ordering is unconfirmed and earlier plain writes may
remain uncertain even if `Put` already returned successfully.

Channel discovery IDs have a separate, client-wide budget and are never reused,
since they also appear in UDP searches. Only creating a new channel handle
consumes one; reads, writes, monitor activations and reconnections do not. After
4,294,967,295 channel handles, creation returns `ErrIDExhausted`; close and replace
the client. Routine high-rate I/O no longer exhausts this client-wide budget.

Independent unit/fault tests remain in this repository. Development-only libca
and `epics-engineer/ca` comparison harnesses and their results are kept outside
this repository, as requested. Passing those checks is not a claim of complete
libca regression-suite equivalence or production qualification.
