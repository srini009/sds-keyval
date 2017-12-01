#ifndef sds_keyval_h
#define sds_keyval_h

#if defined(__cplusplus)
extern "C" {
#endif
#include <margo.h>
#include <stdint.h>

// some setup to support simple benchmarking
typedef struct {
  int64_t nkeys;
  double insert_time;
  double read_time;
  double overhead;
} bench_result_t;


typedef struct kv_context_s kv_context_t;
// kv-client API
kv_context_t *kv_client_register(const margo_instance_id mid);
kv_context_t *kv_server_register(const margo_instance_id mid);

// both the same: should probably move to common?
hg_return_t kv_client_deregister(kv_context_t *context);
hg_return_t kv_server_deregister(kv_context_t *context);

// server-side routine
hg_return_t kv_server_wait_for_shutdown(kv_context_t *context);

// client-side routines wrapping up all the RPC stuff
hg_return_t kv_client_signal_shutdown(kv_context_t *context);
hg_return_t kv_open(kv_context_t *context, const char *server, const char *db_name);
hg_return_t kv_put(kv_context_t *context, void *key, hg_size_t ksize, void *value, hg_size_t vsize);
hg_return_t kv_get(kv_context_t *context, void *key, hg_size_t ksize, void *value, hg_size_t *vsize);
hg_return_t kv_close(kv_context_t *context);

// benchmark routine
bench_result_t *kv_benchmark(kv_context_t *context, int32_t count);

#if defined(__cplusplus)
}
#endif

#endif // sds_keyval_h
