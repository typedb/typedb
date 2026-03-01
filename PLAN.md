# TypeDB SQL Facade — Implementation Plan

## Phase 1 — Static Projections (MVP)

### 1. Projection Definition in TypeQL Schema

> **Adaptation note**: The `typeql` crate is an external git dependency (tag 3.8.0)
> whose `Definable` enum cannot be extended without forking. Instead of modifying
> typeql parsing and the `query/define.rs` path, projection definitions are managed
> through a self-contained parser (`schema_parser.rs`) and in-memory store
> (`projection_store.rs`) within the `projection` crate. This achieves the same
> goals — DDL parsing, define/undefine lifecycle, persistence, and flat-row
> validation — through an independent path that the server layer invokes directly.

- [x] ~~Extend `typeql` crate parser~~ → Create `projection/schema_parser.rs` — custom DDL parser for `define projection <Name>(<col>: <type>, ...) as "<query>";` and `undefine projection <Name>;` syntax (30 tests)
- [x] ~~Add new IR node~~ → Not needed (schema definitions bypass IR in TypeDB; functions work the same way)
- [x] ~~Add `IndexNameToDefinitionProjection` storage prefix~~ → `ProjectionStore` in-memory HashMap with case-insensitive name keys (follows `FunctionManager` pattern)
- [x] ~~Store via `DefinitionKey`~~ → `ProjectionStore::define()` / `get()` / `list()` — named definition CRUD with duplicate-name detection (23 tests)
- [x] ~~Extend `query/define.rs`~~ → `ProjectionStore::define()` — validates + stores, invoked by server layer
- [x] ~~Extend `query/undefine.rs`~~ → `ProjectionStore::undefine()` — removes by name, returns removed definition
- [x] Validate projections produce flat rows only (no nested fetch) at define-time — `is_flat_type()` rejects `Struct` columns

### 2. Projection Materialization Engine — New `projection/` crate

- [x] Create `projection/Cargo.toml` and `projection/BUILD`
- [x] Create `projection/lib.rs` — crate root, re-exports
- [x] Create `projection/definition.rs` — `ProjectionDefinition` struct with validated constructor (15 tests)
- [x] Create `projection/catalog.rs` — `MaterializedCatalog` implementing `ProjectionCatalog` trait (24 tests)
- [x] Create `projection/materializer.rs` — `Materializer` + `SourceQueryExecutor` trait, refresh orchestration (23 tests)
- [x] Create `projection/type_mapping.rs` — `ValueType` → Postgres OID mapping (81 tests, 40 OID constants):
  - [x] Boolean → `bool` (OID 16)
  - [x] Integer → `int8` (OID 20)
  - [x] Double → `float8` (OID 701)
  - [x] Decimal → `numeric` (OID 1700)
  - [x] Date → `date` (OID 1082)
  - [x] DateTime → `timestamp` (OID 1114)
  - [x] DateTimeTZ → `timestamptz` (OID 1184)
  - [x] Duration → `interval` (OID 1186)
  - [x] String → `text` (OID 25)
  - [x] Entity/Relation → `text` as hex IID (OID 25)
  - [x] Struct → `jsonb` (OID 3802)
- [x] Trigger materialization on database open — `Materializer::register_and_refresh_all()` (4 tests)
- [x] Support manual refresh via internal API — `Materializer::refresh_one()` / `refresh_all()`
- [x] Implement atomic swap-on-write (`Arc` swap) for concurrent refresh + read safety — `Arc<Vec<…>>` row snapshots in `MaterializedCatalog` (4 tests)

### 3. Postgres Wire Protocol Service — `projection/pgwire/` module

- [x] Create `projection/pgwire/mod.rs` — module declarations
- [x] Create `projection/pgwire/messages.rs` — Postgres wire protocol message encode/decode (31 tests + 19 golden + 12 live):
  - [x] `StartupMessage` (client→server, connection init + database selection)
  - [x] `AuthenticationOk` / `AuthenticationMD5Password` / `AuthenticationSASL` (server→client)
  - [x] `Query` — Simple Query protocol (client→server)
  - [x] `RowDescription` (server→client, column names + OIDs)
  - [x] `DataRow` (server→client, one row of values)
  - [x] `CommandComplete` (server→client, row count)
  - [x] `ReadyForQuery` (server→client, idle signal)
  - [x] `ErrorResponse` (server→client, SQLSTATE error codes)
- [x] Create `projection/pgwire/listener.rs` — `TcpListener` accept loop on configured address (7 tests):
- [x] Create `projection/pgwire/connection.rs` — per-connection state machine (startup → auth → query loop) (13 tests):
  - [x] `PgConnection<S, H>` generic over `AsyncRead + AsyncWrite` + `QueryHandler`
  - [x] Startup handshake: AuthOk → ParameterStatus×5 → BackendKeyData → ReadyForQuery
  - [x] Query loop: Q → T + D×n + C + Z | E + Z
  - [x] Terminate handling (graceful close)
- [x] Create `projection/pgwire/authenticator.rs` — bridge Postgres auth handshake to existing `CredentialVerifier` + `TokenManager` (24 tests + 3 connection integration tests):
- [x] Create `projection/pgwire/query_executor.rs` — SQL → `ProjectionCatalog` lookup → stream `RowDescription` + `DataRow` messages (34 tests):
  - [x] `ProjectionCatalog` trait + `CatalogColumn` / `ProjectionInfo` types
  - [x] SHOW TABLES → list projections
  - [x] SELECT * / named columns / aliases → resolve + project
  - [x] WHERE filtering (=, !=, <, <=, >, >=, NULL, AND, float)
  - [x] ORDER BY (asc, desc, multi-column, numeric-aware, NULLs-last)
  - [x] LIMIT / OFFSET
  - [x] `information_schema.tables` + `information_schema.columns` virtual tables
  - [x] Error responses: undefined_table (42P01), undefined_column (42703)
- [x] Create `projection/pgwire/value_format.rs` — `Value` → Postgres text format serialization (46 tests)

### 4. SQL Parser (minimal) — `sqlparser-rs` 0.61 (PostgreSQL dialect)

> Decision: `sqlparser-rs` over `polyglot-sql` — battle-tested (6yr, DataFusion/Arrow ecosystem),
> minimal footprint (PG dialect only, no 32-dialect transpiler weight), stable API.
> `polyglot-sql` revisitable in Phase 3 for multi-dialect BI tool support.

- [x] Add `sqlparser-rs` dependency (Postgres dialect)
- [x] Create `projection/pgwire/sql_parser.rs` — parse and dispatch (37 tests):
  - [x] `SELECT * FROM <projection_name>`
  - [x] `SELECT col1, col2 FROM <projection_name> WHERE col = 'x' ORDER BY col LIMIT N`
  - [x] `SHOW TABLES` → list projections
  - [x] `SELECT * FROM information_schema.tables` → minimal catalog response
  - [x] `SELECT * FROM information_schema.columns` → minimal catalog response
- [x] Post-filter materialized rows for `WHERE` clauses (in `query_executor.rs`)
- [x] Apply `ORDER BY` / `LIMIT` / `OFFSET` on materialized results (in `query_executor.rs`)

### 5. Config Extension

- [x] Add `PgWireEndpointConfig` struct to `server/parameters/config.rs` (enabled, address, Default impl, 3 tests)
- [x] Add `pgwire` section to `server/config.yml` (default: `enabled: true`, `address: 0.0.0.0:5432`)
- [x] Wire config parsing through `ServerConfig` deserialization + CLI overrides

### 6. Server Startup Extension

- [x] Add `serve_pgwire()` method to `Server` in `server/lib.rs` — binds listener, bridges shutdown, waits for connections
- [x] Join `serve_pgwire()` into the existing `tokio::join!` alongside `serve_grpc()` and `serve_http()`
- [x] Pass `BoxServerState` + `ProjectionCatalog` to the pgwire listener — `ServerStateAuthenticator` + `CatalogQueryHandler` in `server/service/pgwire.rs`
- [x] Respect `shutdown_receiver` for graceful termination — bridges `Receiver<()>` → `Receiver<bool>` for listener shutdown
- [x] Add `with_auth_mode()` builder to `PgWireListener` — propagates auth mode to spawned connections (2 tests)

---

## Phase 2 — Incremental Refresh

- [x] Hook into WAL (`durability/wal.rs`) to detect writes affecting projection source types — `projection/change_detector.rs`: `ChangeDetector` trait + `ChangeEvent` + `ChangeSummary` (16 tests)
- [x] Define per-projection refresh policies (eager, lazy, periodic) — `projection/refresh_policy.rs`: `RefreshPolicy` enum (Eager/Periodic/Manual) + `ProjectionRefreshConfig` with sequence tracking (25 tests)
- [x] Implement projection versioning via snapshot isolation (`storage/isolation_manager.rs`) — `ProjectionRefreshConfig::last_sequence_number` tracks per-projection WAL position, `needs_refresh(watermark)` for consistent read points
- [x] Spawn background refresh tasks on the Tokio runtime — `projection/background_refresher.rs`: `BackgroundRefresher::run_loop()` with `tokio::select!` + `watch::Receiver<bool>` shutdown (17 tests)
- [x] Support backfill + replay for new/modified projections — `run_cycle()` auto-backfills (full materialization) on first run, then incremental based on policy; `force_refresh()` for on-demand

---

## Phase 3 — Ecosystem Polish

- [x] Full `information_schema` catalog views (tables, columns, schemata)
- [x] Extended Query protocol support (Parse/Bind/Describe/Execute/Sync/Close)
- [x] `pg_catalog` compatibility (`pg_namespace`, `pg_class`, `pg_attribute`, `pg_type`, `pg_database`)
- [x] BI tool no-op commands (`SET`, `BEGIN`, `COMMIT`, `ROLLBACK`, `DEALLOCATE`, `DISCARD ALL`)
- [x] Custom `command_tag` on `QueryResult` for correct CommandComplete tags
- [x] `typedb psql` convenience command
- [x] dbt adapter (thin Python wrapper)
- [x] BI tool presets (Looker, PowerBI, Metabase)
- [x] Schema introspection completeness for ORM compatibility
