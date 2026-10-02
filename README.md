# Forge PostgreSQL

A Forge source module with pooled PostgreSQL connections, parameterized queries,
explicit transactions and numbered migrations with the SQLx table layout. `postgres.fg`
contains the public API and migration orchestration. `src/bridge.c` provides the
libpq FFI, resource ownership and bounded thread-safe pool.

Requires Forge with imported extern declarations and module forward declarations,
PostgreSQL libpq development headers, C11, CMake and pthreads. Build the native
bridge with `cmake -S . -B build && cmake --build build`. Import `postgres` with
`forge -I /path/to/forge-postgres`, and link `libforge_postgres.a -lpq -lpthread`.

A connection is exclusively owned from `acquire` to `release`. Release rolls back
unfinished transactions. A wrong-pool or duplicate release returns `0` before
performing database operations. An unfinished COPY or failed rollback closes the
session, and the next acquire reconnects it. COPY streaming is not implemented.
Close the pool only after all workers have stopped; `pool_close` returns `0` while
leases are outstanding. Concurrent pool close/acquire and stale lease handles are
not supported.
`query` takes a parameter vector (`params`, `push`/`push_null`, `params_close`).
Values from `value` remain valid until `clear`. Always clear results and parameter
vectors, including failed queries. `exec` is exclusively for trusted SQL such as
checked-in migrations; never concatenate user values into SQL.

`query_prepared` reuses up to 32 statements per pool connection. SQL over 16 KiB
and queries after the cache fills use the regular parameterized query path.
Statements are cleared after connection resets and trusted `DEALLOCATE ALL` or
`DISCARD ALL` commands. Only SQL plans are cached; parameter values are sent on
every execution. Use it for stable repeated queries rather than dynamic SQL.
Statement names in the `_forge_pg_` namespace belong to this module. An execution
error is returned unchanged, without retrying a write. After SQLSTATE `26000`, a
later request first describes the cached name: it re-prepares only if the outer
statement was removed. Errors raised inside a function therefore cannot leak new
server statements. A cached-plan result-type change still requires an explicit
trusted `DEALLOCATE ALL` before a later query; schema migration orchestration must
handle that case.

The pool contains at most 64 connections, waits at most 10 seconds for a lease,
and applies 5-second statement and 3-second lock timeouts. Connection attempts use a fixed 5-second libpq timeout. No database credentials
are stored in this repository. See the [libpq documentation](https://www.postgresql.org/docs/16/libpq-exec.html).

Migration compatibility is limited to the `_sqlx_migrations` table layout,
numbered versions, transaction boundaries and advisory-lock serialization.
`migrate` currently stores the placeholder checksum byte `00` and checks only the
success flag when a version already exists. It does not verify migration-content
checksums, and its newly recorded checksums are not compatible with SQLx's
checksum validation. Do not edit an applied migration or assume that switching
back to SQLx will pass checksum checks. Introducing real checksums requires an
explicit compatibility plan for existing rows; this patch does not rewrite them.

Run the native prepared-query regression against a disposable PostgreSQL 16
instance with `DATABASE_URL=... ctest --test-dir build --output-on-failure`.
It covers parameter binding, cache bounds, nested SQLSTATE errors, statement
reset commands, transaction cleanup, lease ownership, COPY cleanup and reconnect.
Without `DATABASE_URL`, CTest marks this integration test skipped.
