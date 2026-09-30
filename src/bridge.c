#define _POSIX_C_SOURCE 200809L
#include "forge_postgres.h"
#include <errno.h>
#include <libpq-fe.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

typedef struct {
  PGconn **conns;
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
  p->conns = calloc(size, sizeof(PGconn *));
  p->busy = calloc(size, 1);
  pthread_mutex_init(&p->mutex, NULL);
  pthread_cond_init(&p->changed, NULL);
  if (!p->url || !p->conns || !p->busy) {
    fpg_pool_close(HANDLE(p));
    return 0;
  }
  for (int i = 0; i < p->size; ++i) {
    const char *keys[] = {"dbname", "connect_timeout", NULL};
    const char *values[] = {url, "5", NULL};
    p->conns[i] = PQconnectdbParams(keys, values, 1);
    if (!configure(p->conns[i])) {
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
        PGconn *c = p->conns[i];
        pthread_mutex_unlock(&p->mutex);
        if (PQstatus(c) != CONNECTION_OK) {
          PQreset(c);
          if (!configure(c)) {
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
  PGconn *c = PTR(PGconn, conn);
  if (!p || !c)
    return 0;
  if (PQtransactionStatus(c) != PQTRANS_IDLE && PQstatus(c) == CONNECTION_OK) {
    PGresult *r = PQexec(c, "ROLLBACK");
    PQclear(r);
  }
  pthread_mutex_lock(&p->mutex);
  for (int i = 0; i < p->size; ++i)
    if (p->conns[i] == c) {
      p->busy[i] = 0;
      pthread_cond_signal(&p->changed);
      pthread_mutex_unlock(&p->mutex);
      return 1;
    }
  pthread_mutex_unlock(&p->mutex);
  return 0;
}
int64_t fpg_pool_close(int64_t h) {
  Pool *p = PTR(Pool, h);
  if (!p)
    return 0;
  if (p->conns)
    for (int i = 0; i < p->size; ++i)
      if (p->conns[i])
        PQfinish(p->conns[i]);
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
  return HANDLE(PQexecParams(PTR(PGconn, c), sql, p ? p->size : 0, NULL,
                             p ? (const char *const *)p->values : NULL, NULL,
                             NULL, 0));
}
int64_t fpg_exec(int64_t c, const char *sql) {
  return c && sql ? HANDLE(PQexec(PTR(PGconn, c), sql)) : 0;
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
