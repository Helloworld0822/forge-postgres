#define _POSIX_C_SOURCE 200809L
#include "forge_postgres.h"
#include <errno.h>
#include <stdio.h>
#include <libpq-fe.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define PREPARED_LIMIT 32
#define PREPARED_SQL_LIMIT 16384

typedef struct {
  char *sql;
  size_t sql_size;
  int params;
  int needs_check;
  char name[48];
} Prepared;
typedef struct {
  PGconn *pg;
  Prepared prepared[PREPARED_LIMIT];
} Connection;

static void prepared_forget(Connection *conn) {
  for (int i = 0; i < PREPARED_LIMIT; i++) {
    free(conn->prepared[i].sql);
    conn->prepared[i].sql = NULL;
  }
}

typedef struct {
  Connection **conns;
  unsigned char *busy;
  int size;
  char *url;
  pthread_mutex_t mutex;
  pthread_cond_t changed;
} Pool;
typedef struct {
  char **values;
  int size;
  int capacity;
} Params;
#define HANDLE(p) ((int64_t)(intptr_t)(p))
#define PTR(t, h) ((t *)(intptr_t)(h))
static PGconn *connect_database(const char *url) {
  const char *keys[] = {"dbname", "connect_timeout", NULL};
  const char *values[] = {url, "5", NULL};
  return PQconnectdbParams(keys, values, 1);
}

static int configure(PGconn *c) {
  if (!c || PQstatus(c) != CONNECTION_OK)
    return 0;
  PGresult *r = PQexec(
      c, "SET statement_timeout='5s'; SET lock_timeout='3s'; SET "
         "idle_in_transaction_session_timeout='15s'; SET timezone='UTC'");
  int ok = r && PQresultStatus(r) == PGRES_COMMAND_OK;
  PQclear(r);
  return ok;
}
int64_t fpg_pool_open(const char *url, int64_t size) {
  if (!url || !*url || size < 1 || size > 64)
    return 0;
  Pool *p = calloc(1, sizeof(*p));
  if (!p)
    return 0;
  p->size = (int)size;
  p->url = strdup(url);
  p->conns = calloc(size, sizeof(Connection *));
  p->busy = calloc(size, 1);
  pthread_mutex_init(&p->mutex, NULL);
  pthread_cond_init(&p->changed, NULL);
  if (!p->url || !p->conns || !p->busy) {
    fpg_pool_close(HANDLE(p));
    return 0;
  }
  for (int i = 0; i < p->size; ++i) {
    p->conns[i] = calloc(1, sizeof(Connection));
    if (p->conns[i]) p->conns[i]->pg = connect_database(url);
    if (!p->conns[i] || !configure(p->conns[i]->pg)) {
      fpg_pool_close(HANDLE(p));
      return 0;
    }
  }
  return HANDLE(p);
}
int64_t fpg_acquire(int64_t h) {
  Pool *p = PTR(Pool, h);
  if (!p)
    return 0;
  struct timespec until;
  clock_gettime(CLOCK_REALTIME, &until);
  until.tv_sec += 10;
  pthread_mutex_lock(&p->mutex);
  for (;;) {
    for (int i = 0; i < p->size; ++i)
      if (!p->busy[i]) {
        p->busy[i] = 1;
        Connection *c = p->conns[i];
        pthread_mutex_unlock(&p->mutex);
        if (!c->pg || PQstatus(c->pg) != CONNECTION_OK) {
          /* A reset creates a new server session: none of its named statements
           * survive. Discard metadata before reconnecting, even if reset fails. */
          prepared_forget(c);
          if (c->pg) PQreset(c->pg);
          else c->pg = connect_database(p->url);
          if (!configure(c->pg)) {
            PQfinish(c->pg);
            c->pg = NULL;
            fpg_release(h, HANDLE(c));
            return 0;
          }
        }
        return HANDLE(c);
      }
    if (pthread_cond_timedwait(&p->changed, &p->mutex, &until) == ETIMEDOUT) {
      pthread_mutex_unlock(&p->mutex);
      return 0;
    }
  }
}
int64_t fpg_release(int64_t h, int64_t conn) {
  Pool *p = PTR(Pool, h);
  Connection *c = PTR(Connection, conn);
  if (!p || !c) return 0;
  /* Verify ownership before touching libpq. Mark the lease as releasing while
   * rollback runs outside the pool lock, rejecting duplicate releases. */
  pthread_mutex_lock(&p->mutex);
  int slot = -1;
  for (int i = 0; i < p->size; ++i) {
    if (p->conns[i] == c && p->busy[i] == 1) {
      slot = i;
      p->busy[i] = 2;
      break;
    }
  }
  pthread_mutex_unlock(&p->mutex);
  if (slot < 0) return 0;
  if (c->pg && PQtransactionStatus(c->pg) != PQTRANS_IDLE &&
      PQstatus(c->pg) == CONNECTION_OK) {
    int clean = 0;
    if (PQtransactionStatus(c->pg) != PQTRANS_ACTIVE) {
      PGresult *r = PQexec(c->pg, "ROLLBACK");
      clean = r && PQresultStatus(r) == PGRES_COMMAND_OK &&
              PQtransactionStatus(c->pg) == PQTRANS_IDLE;
      PQclear(r);
    }
    if (!clean) {
      /* COPY or a protocol failure may prevent rollback. Close this session
       * rather than lending a dirty connection; acquire will reconnect it. */
      prepared_forget(c);
      PQfinish(c->pg);
      c->pg = NULL;
    }
  }
  pthread_mutex_lock(&p->mutex);
  p->busy[slot] = 0;
  pthread_cond_signal(&p->changed);
  pthread_mutex_unlock(&p->mutex);
  return 1;
}

int64_t fpg_pool_close(int64_t h) {
  Pool *p = PTR(Pool, h);
  if (!p)
    return 0;
  pthread_mutex_lock(&p->mutex);
  if (p->busy) {
    for (int i = 0; i < p->size; i++) {
      if (p->busy[i]) { pthread_mutex_unlock(&p->mutex); return 0; }
    }
  }
  pthread_mutex_unlock(&p->mutex);
  if (p->conns)
    for (int i = 0; i < p->size; ++i)
      if (p->conns[i]) {
        prepared_forget(p->conns[i]);
        PQfinish(p->conns[i]->pg);
        free(p->conns[i]);
      }
  pthread_mutex_destroy(&p->mutex);
  pthread_cond_destroy(&p->changed);
  free(p->conns);
  free(p->busy);
  free(p->url);
  free(p);
  return 1;
}
int64_t fpg_params(void) { return HANDLE(calloc(1, sizeof(Params))); }
int64_t fpg_push(int64_t h, const char *v) {
  Params *p = PTR(Params, h);
  if (!p || p->size >= 1024)
    return 0;
  if (p->size == p->capacity) {
    int cap = p->capacity ? p->capacity * 2 : 8;
    char **a = realloc(p->values, cap * sizeof(char *));
    if (!a)
      return 0;
    p->values = a;
    p->capacity = cap;
  }
  char *copy = v ? strdup(v) : NULL;
  if (v && !copy)
    return 0;
  p->values[p->size++] = copy;
  return 1;
}
int64_t fpg_push_null(int64_t p) { return fpg_push(p, NULL); }
int64_t fpg_params_close(int64_t h) {
  Params *p = PTR(Params, h);
  if (!p)
    return 0;
  for (int i = 0; i < p->size; ++i)
    free(p->values[i]);
  free(p->values);
  free(p);
  return 1;
}
int64_t fpg_query(int64_t c, const char *sql, int64_t h) {
  Params *p = PTR(Params, h);
  if (!c || !sql)
    return 0;
  return HANDLE(PQexecParams(PTR(Connection, c)->pg, sql, p ? p->size : 0, NULL,
                             p ? (const char *const *)p->values : NULL, NULL,
                             NULL, 0));
}
int64_t fpg_query_prepared(int64_t c, const char *sql, int64_t h) {
  if (!c || !sql) return 0;
  Connection *conn = PTR(Connection, c);
  Params *params = PTR(Params, h);
  int count = params ? params->size : 0;
  size_t size = strnlen(sql, PREPARED_SQL_LIMIT + 1);
  if (size > PREPARED_SQL_LIMIT) return fpg_query(c, sql, h);

  Prepared *entry = NULL, *available = NULL;
  for (int i = 0; i < PREPARED_LIMIT; i++) {
    Prepared *candidate = &conn->prepared[i];
    if (!candidate->sql) {
      if (!available) available = candidate;
    } else if (candidate->params == count && candidate->sql_size == size &&
               strcmp(candidate->sql, sql) == 0) {
      entry = candidate;
      break;
    }
  }
  if (!entry) {
    /* The cache never evicts a live statement. A full cache uses the ordinary
     * parameterized path, so variable SQL cannot grow server-side state. */
    if (!available) return fpg_query(c, sql, h);
    char *copy = strdup(sql);
    if (!copy) return fpg_query(c, sql, h);
    snprintf(available->name, sizeof(available->name), "_forge_pg_%d",
             (int)(available - conn->prepared));
    PGresult *prepared = PQprepare(conn->pg, available->name, sql, count, NULL);
    if (!prepared || PQresultStatus(prepared) != PGRES_COMMAND_OK) {
      free(copy);
      return HANDLE(prepared);
    }
    PQclear(prepared);
    entry = available;
    entry->sql = copy;
    entry->sql_size = size;
    entry->params = count;
    entry->needs_check = 0;
  } else if (entry->needs_check) {
    /* 26000 may originate inside a trigger/function while this outer statement
     * still exists. Describe its name on a later request before replacing it;
     * metadata inspection never executes or replays the application query. */
    PGresult *described = PQdescribePrepared(conn->pg, entry->name);
    if (!described) return 0;
    if (PQresultStatus(described) != PGRES_COMMAND_OK) {
      const char *state = PQresultErrorField(described, PG_DIAG_SQLSTATE);
      if (!state || strcmp(state, "26000") != 0) return HANDLE(described);
      PQclear(described);
      PGresult *prepared = PQprepare(conn->pg, entry->name, sql, count, NULL);
      if (!prepared || PQresultStatus(prepared) != PGRES_COMMAND_OK)
        return HANDLE(prepared);
      PQclear(prepared);
    } else PQclear(described);
    entry->needs_check = 0;
  }
  PGresult *result = PQexecPrepared(
      conn->pg, entry->name, count,
      params ? (const char *const *)params->values : NULL, NULL, NULL, 0);
  const char *state = result ? PQresultErrorField(result, PG_DIAG_SQLSTATE) : NULL;
  if (state && strcmp(state, "26000") == 0) {
    /* Preserve the slot/name until actual server metadata is checked. Merely
     * forgetting it could leak live statements if a nested function raised it. */
    entry->needs_check = 1;
  }
  return HANDLE(result);
}

int64_t fpg_exec(int64_t c, const char *sql) {
  if (!c || !sql) return 0;
  Connection *conn = PTR(Connection, c);
  /* PQexec returns only the final result. Drain every result so a successful
   * DISCARD/DEALLOCATE ALL earlier in a trusted multi-statement command cannot
   * leave stale cache metadata, even if a later statement fails. */
  if (!PQsendQuery(conn->pg, sql))
    return HANDLE(PQmakeEmptyPGresult(conn->pg, PGRES_FATAL_ERROR));
  PGresult *last = NULL, *result;
  while ((result = PQgetResult(conn->pg)) != NULL) {
    if (PQresultStatus(result) == PGRES_COMMAND_OK) {
      const char *tag = PQcmdStatus(result);
      if (strcmp(tag, "DEALLOCATE ALL") == 0 || strcmp(tag, "DISCARD ALL") == 0)
        prepared_forget(conn);
    }
    PQclear(last);
    last = result;
    ExecStatusType status = PQresultStatus(result);
    if (status == PGRES_COPY_IN || status == PGRES_COPY_OUT || status == PGRES_COPY_BOTH)
      break; /* COPY is unfinished; release closes it if rollback cannot clean it. */
  }
  return HANDLE(last);
}

int64_t fpg_ok(int64_t h) {
  if (!h)
    return 0;
  ExecStatusType s = PQresultStatus(PTR(PGresult, h));
  return s == PGRES_TUPLES_OK || s == PGRES_COMMAND_OK;
}
int64_t fpg_rows(int64_t h) { return h ? PQntuples(PTR(PGresult, h)) : 0; }
int64_t fpg_affected(int64_t h) {
  return h ? strtoll(PQcmdTuples(PTR(PGresult, h)), NULL, 10) : 0;
}
int64_t fpg_is_null(int64_t h, int64_t row, int64_t col) {
  PGresult *r = PTR(PGresult, h);
  return !r || row < 0 || row >= PQntuples(r) || col < 0 ||
         col >= PQnfields(r) || PQgetisnull(r, row, col);
}
const char *fpg_value(int64_t h, int64_t row, int64_t col) {
  return fpg_is_null(h, row, col) ? "" : PQgetvalue(PTR(PGresult, h), row, col);
}
const char *fpg_error(int64_t h) {
  return h ? PQresultErrorMessage(PTR(PGresult, h)) : "database unavailable";
}
const char *fpg_state(int64_t h) {
  const char *s =
      h ? PQresultErrorField(PTR(PGresult, h), PG_DIAG_SQLSTATE) : NULL;
  return s ? s : "";
}
int64_t fpg_clear(int64_t h) {
  if (h)
    PQclear(PTR(PGresult, h));
  return 1;
}
