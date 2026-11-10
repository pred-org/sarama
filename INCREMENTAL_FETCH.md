# Optional incremental fetch sessions (KIP-227)

This branch is a narrow backport on Sarama v1.48.1. It keeps the upstream module
path and exported types. `Config.Consumer.Fetch.IncrementalSessions` defaults to
false. `SetIncrementalFetchSessions(true)` enables it for Fetch v7+ (Kafka 1.1+).

Each broker consumer owns its session, epoch and last requested partition state.
Full requests establish a session. Incremental requests carry added/changed
partitions and forget removed/paused partitions. An empty delta is still sent.
Responses may omit unchanged partitions; existing consumer delivery and offset
commit paths remain in use. Rejected/malformed session responses are discarded
before any delivery, with a full retry after the existing loop's backoff.

Session eviction, invalid epochs, broker refusal/closure, throttling, epoch wrap,
protocol changes, pause/resume and empty subscription cleanup are covered by
focused tests. Network errors use the existing abort/redispatch path and discard
local session state. An unreachable broker's old cache entry expires server-side.
Session close is best effort and uses existing network timeouts.

Validation commands:

```
go test -race -timeout=5m ./...
go test -race -run 'TestIncrementalFetch|TestFetchRequest' -count=20 -timeout=120s .
SARAMA_INCREMENTAL_TEST_BROKER=127.0.0.1:39092 go test -race -run '^TestIncrementalFetchIntegration$' -count=3 -timeout=90s .
```

The opt-in integration test creates and deletes a unique topic on a disposable
Kafka 3.9.1 broker. It asserts actual nonzero session IDs, empty updates,
pause/resume, recovery after deliberate session closure, and exact payload and
offset ordering through the public consumer API. Do not use a production broker.

The three-broker fault test starts its own local Kafka processes (RF=3,
min.insync.replicas=2), kills each broker, verifies delivery while it is down,
restarts it, waits for ISR recovery, and explicitly restores preferred leaders.
It asserts exact offsets/payloads for 210 records per run with an idempotent
producer. The independent metadata observer waits for all running data brokers to agree
on leadership and ISR; it does not refresh the consumer under test. Startup
waits for broker registration and topic metadata propagation. Each test broker
has a 512 MiB heap and a 16 MiB log-cleaner dedupe buffer so restart recovery
does not exhaust the local fixture heap.

```
SARAMA_INCREMENTAL_KAFKA_HOME=/path/to/kafka-3.9.1 \
SARAMA_INCREMENTAL_JAVA_HOME=/path/to/jdk-17 \
go test -race -run '^TestIncrementalFetchClusterRecovery$' -count=2 -timeout=8m .
```

All broker ports bind to localhost, storage is temporary, and test cleanup stops
its child processes. The test is skipped unless both opt-in paths are provided.

These are correctness/recovery tests, not broker CPU benchmarks. They do not
reproduce GCP authentication, cross-cloud latency or production load. Adoption
remains opt-in, with production performance measured during a controlled rollout.
