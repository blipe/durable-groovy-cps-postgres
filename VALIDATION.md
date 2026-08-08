# Validation

## Automated Maven tests

The test suite covers:

- basic CPS execution and asynchronous step completion
- handler invocation isolated from the poll scheduler
- bounded local in-flight step dispatch
- quiesce/resume admission and graceful drain shutdown
- schema startup fail/migrate modes
- file-backed continuation recovery
- early and awaited signals
- retry attempts with one stable invocation ID
- dead-lettering after retry exhaustion
- administrative retry to successful completion
- missing exact definition dead-letter and recovery after restoring it
- multiple definition versions registered in one engine
- persisted telemetry context restoration around CPS and steps
- listener lifecycle events
- filtered administration search and health degradation
- file-store compare-and-swap conflicts
- JDBC workflow and signal round-trip
- exclusive workflow leases
- lease expiry and increasing fencing tokens
- atomic workflow suspension plus step insertion
- exclusive step claiming
- atomic step completion plus workflow resume/dead-letter
- stale step-token rejection
- JDBC administrative step recovery and cancellation
- JDBC status counts and health query
- two engine nodes contending for one live PostgreSQL step
- concurrent PostgreSQL idempotency calls producing one business mutation and one cached result
- recovery after crashes around suspension, handler execution, and result commit

Run:

```bash
./scripts/verify.sh
```

PostgreSQL tests use Testcontainers 2.x and are disabled automatically when Docker or another supported container runtime is unavailable.

## Validation performed while producing this artifact

- All 84 main Java sources compiled under JDK 17-compatible language rules against API-compatible Groovy/CPS, Micrometer, and OpenTelemetry stubs.
- All 8 test Java sources compiled against API-compatible JUnit, HSQLDB, and Testcontainers stubs.
- A real JDBC operational smoke program ran against the HSQLDB engine available in the build environment and passed:
  - workflow creation with persisted propagation context
  - store health and status counts
  - filtered administration query
  - exclusive workflow lease
  - transactional suspension and step creation
  - dead-letter step/workflow persistence
  - atomic administrative step requeue
  - atomic workflow and pending-step cancellation
- A coordinated JDBC engine-administration smoke program passed:
  - atomic operator dead-letter plus pending-step cancellation
  - atomic retry plus pending-step requeue
- A standalone engine-administration smoke program passed:
  - operator dead-letter
  - degraded health
  - filtered dead-letter search
  - retry to `WAITING`
  - repeated quarantine
  - cancellation
  - workflow listener events
- XML parsing of `pom.xml` passed.

The full Maven suite and PostgreSQL Testcontainers tests were not executed in this environment because Maven dependencies and a container runtime were unavailable. They are included for execution in a normal development or CI environment.

## Milestone 3 additions

- snapshot envelope metadata and SHA-256 corruption rejection
- legacy 0.3 raw snapshot fixture decode
- strict class admission and explicit application-package admission
- bounded workflow history
- definition preflight report and startup failure mode
- metadata-only JDBC definition inventory
- transactional terminal workflow/step archival
- archive retrieval and pagination
- opt-in 100,000-workflow capacity smoke (`mvn -Pload-tests test`)


## Milestone 4 additions

- dedicated poll, handler-entry, callback/control, and lease-renewal executors
- blocking handler entry cannot starve continuation-completion callbacks
- asynchronous handler entry so blocking handler setup cannot stall polling
- bounded local step concurrency
- `RUNNING`, `QUIESCING`, and `CLOSED` lifecycle
- bounded graceful shutdown result with unfinished-work counts
- ordered/checksummed JDBC migration history
- PostgreSQL advisory migration lock and schema startup preflight
- workflow diagnostic view with snapshot envelope and pending-step detail
- lifecycle/schema health reporting
- local lifecycle, callback-isolation, zero-timeout takeover, and schema-migrator smoke programs
- 0.5 snapshot fixture decode and checksum-corruption rejection
- shell syntax validation for all release scripts


## Milestone 5 additions

Validated while producing this artifact:

- all main sources compiled under Java 17 rules using API-compatible dependency stubs
- all test sources compiled under Java 17 rules
- AES-GCM write/read, historical-key rotation, authentication failure on tampering, and plaintext rolling-upgrade decode
- real JDBC smoke against the locally available HSQLDB engine for persisted global concurrency, exclusive capacity enforcement, and stuck-step reporting
- scheduled retention on its dedicated maintenance executor
- 0.6.0 current-format compatibility fixture generation and decode
- schema migration inventory advanced to V003
- admin CLI source and shell syntax compilation
- POM parsing and ZIP integrity

The full Maven dependency graph and PostgreSQL/Testcontainers suite remain included for normal CI. They were not executable in this environment because Maven and a container runtime were unavailable.
