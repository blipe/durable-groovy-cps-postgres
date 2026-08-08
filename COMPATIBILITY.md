# Compatibility contract

## Durable identity

A resumable workflow is bound to `(definition_id, definition_version, SHA-256(source))`. The engine never substitutes another version or source.

## Snapshot envelope

Current writes use envelope version 1 and codec `java-serialization` version 1. Readers reject:

- unknown envelope versions
- unknown/newer codec versions
- malformed lengths
- trailing data
- SHA-256 mismatches
- snapshots exceeding configured limits
- classes outside the configured admission policy
- dynamic proxies unless explicitly enabled

Legacy raw Java-serialization snapshots remain readable. The fixtures under `src/test/resources/compatibility` include the raw 0.3 format and the 0.5 and 0.6 checksummed envelopes; all must pass every compatible release.

## Release procedure

Before releasing N:

1. Run `mvn clean test`.
2. Run PostgreSQL/Testcontainers crash and two-node tests.
3. Decode every fixture under `src/test/resources/compatibility`.
4. Persist representative workflows with release N-1, restart with N, and complete them.
5. Add an N fixture for the next release before changing serialized state classes.
6. Run `mvn -Pload-tests test`.

Changing a serialized field type, `serialVersionUID`, generated Groovy/CPS dependency, or workflow classloader behavior is a compatibility event and requires an explicit migration or an intentionally breaking release.


## Database schema

Runtime schema compatibility is independent of CPS snapshot compatibility. Applied migration versions and SHA-256 checksums are persisted in `durable_schema_history`. Existing migration resources are immutable. A release that needs a storage change adds a new ordered migration and must support rolling application versions against the resulting schema or document a coordinated deployment requirement.


## Optional encryption envelope

AES-GCM encryption is an outer envelope. The inner 0.6 snapshot remains the compatibility unit after decryption. Encrypted readers accept unencrypted historical bytes, but plaintext-only 0.5 readers cannot read new encrypted writes. Therefore enable encryption only after every possible reader has been upgraded. Key identifiers are durable compatibility data: historical keys must remain resolvable for as long as matching active or archived snapshots may be read.

Schema migration V003 is additive and introduces global step-concurrency metadata. A 0.5 runtime may continue operating against the migrated schema, while a 0.6 runtime requires V003 before claiming steps.
