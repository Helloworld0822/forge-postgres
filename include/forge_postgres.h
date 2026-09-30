#ifndef FORGE_POSTGRES_H
#define FORGE_POSTGRES_H
#include <stdint.h>
int64_t fpg_pool_open(const char *, int64_t);
int64_t fpg_acquire(int64_t);
int64_t fpg_release(int64_t, int64_t);
int64_t fpg_pool_close(int64_t);
int64_t fpg_params(void);
int64_t fpg_push(int64_t, const char *);
int64_t fpg_push_null(int64_t);
int64_t fpg_params_close(int64_t);
int64_t fpg_query(int64_t, const char *, int64_t);
int64_t fpg_exec(int64_t, const char *);
int64_t fpg_ok(int64_t);
int64_t fpg_rows(int64_t);
int64_t fpg_affected(int64_t);
int64_t fpg_is_null(int64_t, int64_t, int64_t);
const char *fpg_value(int64_t, int64_t, int64_t);
const char *fpg_error(int64_t);
const char *fpg_state(int64_t);
int64_t fpg_clear(int64_t);
#endif
