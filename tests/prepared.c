#include "forge_postgres.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(c) do { if (!(c)) { fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #c); exit(1); } } while (0)
static void success(int64_t result) {
  if (!fpg_ok(result)) {
    fprintf(stderr, "database error [%s]: %s\n", fpg_state(result), fpg_error(result));
    exit(1);
  }
}
static void command(int64_t conn, const char *sql) {
  int64_t result = fpg_exec(conn, sql);
  success(result);
  fpg_clear(result);
}
static int64_t prepared_count(int64_t conn) {
  int64_t result = fpg_query(conn, "SELECT count(*) FROM pg_prepared_statements WHERE name LIKE '_forge_pg_%'", 0);
  success(result);
  int64_t count = strtoll(fpg_value(result, 0, 0), NULL, 10);
  fpg_clear(result);
  return count;
}
int main(void) {
  const char *url = getenv("DATABASE_URL");
  if (!url || !*url) { puts("DATABASE_URL not set: prepared integration skipped"); return 77; }
  int64_t pool = fpg_pool_open(url, 1);
  int64_t control_pool = fpg_pool_open(url, 1);
  CHECK(pool && control_pool);
  int64_t conn = fpg_acquire(pool), control = fpg_acquire(control_pool);
  CHECK(conn && control);
  command(conn, "CREATE TEMP TABLE prepared_test(value text UNIQUE)");
  const char *sql = "SELECT $1::text,$2::text";
  for (int i = 0; i < 8; i++) {
    int64_t args = fpg_params();
    CHECK(args && fpg_push(args, "Robert'); DROP TABLE prepared_test; --") && fpg_push_null(args));
    int64_t result = fpg_query_prepared(conn, sql, args);
    success(result);
    CHECK(strcmp(fpg_value(result, 0, 0), "Robert'); DROP TABLE prepared_test; --") == 0);
    CHECK(fpg_is_null(result, 0, 1));
    fpg_clear(result); fpg_params_close(args);
    CHECK(fpg_release(pool, conn)); conn = fpg_acquire(pool); CHECK(conn);
  }
  CHECK(prepared_count(conn) == 1);
  command(conn, "INSERT INTO prepared_test VALUES('first')");
  const char *data_sql = "SELECT count(*) FROM prepared_test";
  int64_t result = fpg_query_prepared(conn, data_sql, 0);
  success(result); CHECK(strcmp(fpg_value(result, 0, 0), "1") == 0); fpg_clear(result);
  command(conn, "INSERT INTO prepared_test VALUES('second')");
  result = fpg_query_prepared(conn, data_sql, 0);
  success(result); CHECK(strcmp(fpg_value(result, 0, 0), "2") == 0); fpg_clear(result);
  CHECK(prepared_count(conn) == 2);

  /* Prepare errors do not consume cache entries or hide the SQLSTATE. */
  result = fpg_query_prepared(conn, "SELECT * FROM missing_prepared_table", 0);
  CHECK(!fpg_ok(result) && strcmp(fpg_state(result), "42P01") == 0); fpg_clear(result);
  CHECK(prepared_count(conn) == 2);

  /* Deallocation through the explicit trusted API invalidates metadata. */
  command(conn, "DEALLOCATE ALL");
  CHECK(prepared_count(conn) == 0);
  result = fpg_query_prepared(conn, data_sql, 0); success(result); fpg_clear(result);
  CHECK(prepared_count(conn) == 1);

  /* Deallocation outside fpg_exec is returned unchanged once, then re-prepared
   * on a later request. No automatic replay occurs, even for write SQL. */
  result = fpg_query(conn, "DEALLOCATE ALL", 0); success(result); fpg_clear(result);
  result = fpg_query_prepared(conn, data_sql, 0);
  CHECK(!fpg_ok(result) && strcmp(fpg_state(result), "26000") == 0); fpg_clear(result);
  result = fpg_query_prepared(conn, data_sql, 0); success(result); fpg_clear(result);
  CHECK(prepared_count(conn) == 1);

  /* An application function can emit 26000 even while the outer prepared
   * statement exists. Repeated failures must not leak server statements. */
  command(conn, "CREATE TEMP TABLE prepared_counter(n int); INSERT INTO prepared_counter VALUES(0)");
  command(conn, "CREATE FUNCTION pg_temp.prepared_raise(fail bool) RETURNS int LANGUAGE plpgsql AS $$ BEGIN IF fail THEN RAISE EXCEPTION 'nested failure' USING ERRCODE='26000'; END IF; UPDATE prepared_counter SET n=n+1; RETURN 42; END $$");
  const char *nested_sql = "SELECT pg_temp.prepared_raise($1::boolean)";
  int64_t failing_args = fpg_params(); CHECK(failing_args && fpg_push(failing_args, "true"));
  int64_t count_before = prepared_count(conn);
  for (int i = 0; i < 100; i++) {
    result = fpg_query_prepared(conn, nested_sql, failing_args);
    CHECK(!fpg_ok(result) && strcmp(fpg_state(result), "26000") == 0); fpg_clear(result);
    CHECK(prepared_count(conn) == count_before + 1);
  }
  fpg_params_close(failing_args);
  int64_t valid_args = fpg_params(); CHECK(valid_args && fpg_push(valid_args, "false"));
  result = fpg_query_prepared(conn, nested_sql, valid_args);
  success(result); CHECK(strcmp(fpg_value(result, 0, 0), "42") == 0); fpg_clear(result);
  result = fpg_query(conn, "SELECT n FROM prepared_counter", 0);
  success(result); CHECK(strcmp(fpg_value(result, 0, 0), "1") == 0); fpg_clear(result);
  fpg_params_close(valid_args);

  /* Reset commands are observed even when they are not the last result. */
  command(conn, "DEALLOCATE ALL; SELECT 1");
  CHECK(prepared_count(conn) == 0);
  result = fpg_query_prepared(conn, data_sql, 0); success(result); fpg_clear(result);
  result = fpg_exec(conn, "DEALLOCATE ALL; SELECT * FROM missing_prepared_table");
  CHECK(!fpg_ok(result) && strcmp(fpg_state(result), "42P01") == 0); fpg_clear(result);
  CHECK(prepared_count(conn) == 0);
  result = fpg_query_prepared(conn, data_sql, 0); success(result); fpg_clear(result);

  /* Wrong-pool release may not roll back a transaction or free its lease. */
  command(conn, "BEGIN; INSERT INTO prepared_test VALUES('wrong-pool-kept')");
  CHECK(fpg_release(control_pool, conn) == 0);
  CHECK(fpg_pool_close(pool) == 0);
  command(conn, "COMMIT");
  result = fpg_query(conn, "SELECT count(*) FROM prepared_test WHERE value='wrong-pool-kept'", 0);
  success(result); CHECK(strcmp(fpg_value(result, 0, 0), "1") == 0); fpg_clear(result);
  CHECK(fpg_release(pool, conn));
  CHECK(fpg_release(pool, conn) == 0);
  conn = fpg_acquire(pool); CHECK(conn);

  /* Transaction errors remain aborted until release rolls back. */
  command(conn, "BEGIN");
  result = fpg_query_prepared(conn, "SELECT 1 / $1::int", 0);
  CHECK(!fpg_ok(result)); fpg_clear(result);
  result = fpg_query_prepared(conn, data_sql, 0);
  CHECK(!fpg_ok(result) && strcmp(fpg_state(result), "25P02") == 0); fpg_clear(result);
  CHECK(fpg_release(pool, conn)); conn = fpg_acquire(pool); CHECK(conn);
  result = fpg_query_prepared(conn, data_sql, 0); success(result); fpg_clear(result);

  command(conn, "DEALLOCATE ALL");
  for (int i = 0; i < 100; i++) {
    char query[96]; snprintf(query, sizeof(query), "SELECT %d::int", i);
    result = fpg_query_prepared(conn, query, 0);
    success(result); CHECK(strtol(fpg_value(result, 0, 0), NULL, 10) == i); fpg_clear(result);
  }
  CHECK(prepared_count(conn) == 32);
  char *oversized = malloc(17000);
  CHECK(oversized);
  memset(oversized, ' ', 16990); memcpy(oversized + 16990, "SELECT 1", 9);
  result = fpg_query_prepared(conn, oversized, 0); success(result); fpg_clear(result); free(oversized);
  CHECK(prepared_count(conn) == 32);

  /* A denied DISCARD must neither invalidate valid statements nor mask errors. */
  int64_t before_denied = prepared_count(conn);
  result = fpg_exec(conn, "DISCARD ALL; SELECT 1");
  CHECK(!fpg_ok(result) && strcmp(fpg_state(result), "25001") == 0); fpg_clear(result);
  CHECK(prepared_count(conn) == before_denied);
  result = fpg_query_prepared(conn, "SELECT 0::int", 0); success(result); fpg_clear(result);
  command(conn, "DISCARD ALL");
  CHECK(prepared_count(conn) == 0);
  result = fpg_query_prepared(conn, "SELECT 42", 0); success(result); fpg_clear(result);
  CHECK(prepared_count(conn) == 1);

  /* Reconnect is per-connection and drops stale statement metadata. */
  result = fpg_query(conn, "SELECT pg_backend_pid()", 0); success(result);
  char terminate[96]; snprintf(terminate, sizeof(terminate), "SELECT pg_terminate_backend(%s)", fpg_value(result, 0, 0));
  fpg_clear(result);
  command(control, terminate);
  result = fpg_query_prepared(conn, "SELECT 42", 0);
  CHECK(!fpg_ok(result)); fpg_clear(result);
  CHECK(fpg_release(pool, conn)); conn = fpg_acquire(pool); CHECK(conn);
  CHECK(prepared_count(conn) == 0);
  result = fpg_query_prepared(conn, "SELECT 42", 0); success(result);
  CHECK(strcmp(fpg_value(result, 0, 0), "42") == 0); fpg_clear(result);
  CHECK(prepared_count(conn) == 1);
  /* Unsupported unfinished COPY cannot poison a future lease. */
  command(conn, "CREATE TEMP TABLE prepared_copy(value text)");
  result = fpg_exec(conn, "COPY prepared_copy FROM STDIN");
  CHECK(result && !fpg_ok(result)); fpg_clear(result);
  CHECK(fpg_release(pool, conn)); conn = fpg_acquire(pool); CHECK(conn);
  CHECK(prepared_count(conn) == 0);
  result = fpg_query_prepared(conn, "SELECT 42", 0); success(result); fpg_clear(result);
  CHECK(fpg_release(pool, conn)); CHECK(fpg_release(control_pool, control));
  CHECK(fpg_pool_close(pool)); CHECK(fpg_pool_close(control_pool));
  puts("prepared query integration passed");
  return 0;
}
