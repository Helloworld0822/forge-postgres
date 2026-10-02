# Forge PostgreSQL

A Forge source module with pooled PostgreSQL connections, parameterized queries,
explicit transactions and SQLx-compatible numbered migrations. `postgres.fg`
contains the public API and migration orchestration. `src/bridge.c` provides the
libpq FFI, resource ownership and bounded thread-safe pool.

Requires Forge with imported extern declarations and module forward declarations,
PostgreSQL libpq development headers, C11, CMake and pthreads. Build the native
bridge with `cmake -S . -B build && cmake --build build`. Import `postgres` with
`forge -I /path/to/forge-postgres`, and link `libforge_postgres.a -lpq -lpthread`.

A connection is exclusively owned from `acquire` to `release`. Release rolls back
unfinished transactions. Close the pool only after all workers have stopped.
`query` takes a parameter vector (`params`, `push`/`push_null`, `params_close`).
Values from `value` remain valid until `clear`. Always clear results and parameter
vectors, including failed queries. `exec` is exclusively for trusted SQL such as
checked-in migrations; never concatenate user values into SQL.

`query_prepared` reuses up to 32 statements per pool connection. SQL over 16 KiB
and queries after the cache fills use the regular parameterized query path.
Statements are cleared after connection resets and trusted `DEALLOCATE ALL` or
`DISCARD ALL` commands. Only SQL plans are cached; parameter values are sent on
every execution. Use it for stable repeated queries rather than dynamic SQL.

The pool contains at most 64 connections, waits at most 10 seconds for a lease,
and applies 5-second statement and 3-second lock timeouts. Connection attempts use a fixed 5-second libpq timeout. No database credentials
are stored in this repository. See the [libpq documentation](https://www.postgresql.org/docs/16/libpq-exec.html).
