#include <stdint.h>
#include <assert.h>

#include <mercury.h>
#include <mercury_macros.h>
#include <mercury_proc_string.h>

#include <margo.h>
#include <abt.h>
#include <abt-snoozer.h>

#ifndef sds_keyval_h
#define sds_keyval_h

// uncomment to re-enable print statements
//#define KV_DEBUG

#if defined(__cplusplus)
extern "C" {
#endif

typedef int kv_id;

// define client/server key/value types here (before mercury includes)

// kv-client POD types supported:
//   integer (e.g. int, long, long long)
//   unsigned integer (e.g. unsigned int, long, and long long)
//   floating point single precision (e.g. float)
//   floating point double precision (e.g. double)
//   string (use hg_string_t)

// key type
typedef uint64_t kv_key_t;
// value type for POD put/get interface (e.g. long)
typedef int kv_value_t;

// kv-client bulk data:
//   bulk (pack/unpack values in/out of memory buffer)
//   see datastore.h/cc for implementation (ds_bulk_t)

/* do we need one for server, one for client? */
typedef struct kv_context_s {
	margo_instance_id mid;
	hg_addr_t svr_addr;
	hg_id_t put_id;
	hg_id_t bulk_put_id;
	hg_id_t get_id;
	hg_id_t bulk_get_id;
	hg_id_t open_id;
	hg_id_t close_id;
	hg_id_t bench_id;
	hg_id_t shutdown_id;
	hg_handle_t put_handle;
        hg_handle_t bulk_put_handle; // necessary?
	hg_handle_t get_handle;
	hg_handle_t bulk_get_handle; // necessary?
	hg_handle_t shutdown_handle;
	/* some keyval dodad goes here so the server can discriminate
	 * seems like it should be some universal identifier we can
	 * share with other clients */
	kv_id kv;
} kv_context;

/* struggling a bit with types */

MERCURY_GEN_PROC(put_in_t,
		 ((uint64_t)(key)) ((int32_t)(value)))
MERCURY_GEN_PROC(put_out_t, ((int32_t)(ret)))
DECLARE_MARGO_RPC_HANDLER(put_handler)

MERCURY_GEN_PROC(get_in_t,
		 ((uint64_t)(key)))
MERCURY_GEN_PROC(get_out_t,
		 ((int32_t)(value)) ((int32_t)(ret)))
DECLARE_MARGO_RPC_HANDLER(get_handler)

MERCURY_GEN_PROC(open_in_t,
		 ((hg_string_t)(name)))
MERCURY_GEN_PROC(open_out_t, ((int32_t)(ret)))
DECLARE_MARGO_RPC_HANDLER(open_handler)

MERCURY_GEN_PROC(close_out_t, ((int32_t)(ret)))
DECLARE_MARGO_RPC_HANDLER(close_handler)

MERCURY_GEN_PROC(bench_in_t, ((int32_t)(count)) )

typedef struct {
  hg_size_t nkeys;
  double insert_time;
  double read_time;
  double overhead;
} bench_result;

static inline hg_return_t hg_proc_double(hg_proc_t proc, void *data)
{
  return hg_proc_memcpy(proc, data, sizeof(double));
}

static inline hg_return_t hg_proc_bench_result(hg_proc_t proc, void *data)
{
  hg_return_t ret;
  bench_result *in = (bench_result*)data;

  ret = hg_proc_hg_size_t(proc, &in->nkeys);
  assert(ret == HG_SUCCESS);
  ret = hg_proc_double(proc, &in->insert_time);
  assert(ret == HG_SUCCESS);
  ret = hg_proc_double(proc, &in->read_time);
  assert(ret == HG_SUCCESS);
  ret = hg_proc_double(proc, &in->overhead);
  assert(ret == HG_SUCCESS);

  return HG_SUCCESS;
}

DECLARE_MARGO_RPC_HANDLER(bench_handler)
MERCURY_GEN_PROC(bench_out_t, ((bench_result)(result)) )

// for handling bulk puts/gets (e.g. for ParSplice use case)
MERCURY_GEN_PROC(bulk_put_in_t,
		 ((uint64_t)(key))		\
		 ((uint64_t)(size))		\
		 ((hg_bulk_t)(bulk_handle)) )
MERCURY_GEN_PROC(bulk_put_out_t, ((int32_t)(ret)))
DECLARE_MARGO_RPC_HANDLER(bulk_put_handler)

MERCURY_GEN_PROC(bulk_get_in_t,
		 ((uint64_t)(key))		\
		 ((uint64_t)(size))		\
		 ((hg_bulk_t)(bulk_handle)) )
MERCURY_GEN_PROC(bulk_get_out_t,
		 ((uint64_t)(size)) ((int32_t)(ret)))
DECLARE_MARGO_RPC_HANDLER(bulk_get_handler)


kv_context *kv_client_register(const char *addr_str=0);
kv_context * kv_server_register(margo_instance_id mid);

DECLARE_MARGO_RPC_HANDLER(shutdown_handler)

/* both the same: should probably move to common */
hg_return_t kv_client_deregister(kv_context *context);
hg_return_t kv_server_deregister(kv_context *context);

/* server-side routine */
hg_return_t kv_server_wait_for_shutdown(kv_context *context);

/* client-side routines wrapping up all the RPC stuff  */
hg_return_t kv_client_signal_shutdown(kv_context *context);
hg_return_t kv_open(kv_context *context, const char *server, const char *db_name);
hg_return_t kv_put(kv_context *context, void *key, void *value);
hg_return_t kv_bulk_put(kv_context *context, void *key, void *data, size_t *data_size);
hg_return_t kv_get(kv_context *context, void *key, void *value);
hg_return_t kv_bulk_get(kv_context *context, void *key, void *data, size_t *data_size);
hg_return_t kv_close(kv_context *context);
bench_result *kv_benchmark(kv_context *context, int count);

#if defined(__cplusplus)
}
#endif

#endif // sds_keyval_h
