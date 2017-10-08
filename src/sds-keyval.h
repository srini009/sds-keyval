#include <mercury.h>
#include <mercury_macros.h>
#include <mercury_proc_string.h>

#include <margo.h>
#include <abt.h>
#include <abt-snoozer.h>

#if defined(__cplusplus)
extern "C" {
#endif

#if 0
sdskeyval_put();
sdskeyval_get();
#endif

typedef int kv_id;

typedef enum {
	KV_INT,
	KV_UINT,
	KV_FLOAT,
	KV_DOUBLE,
	KV_STRING,
	KV_BULK
} kv_type;

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
	hg_handle_t bench_handle;
	hg_handle_t shutdown_handle;
	/* some keyval dodad goes here so the server can discriminate.  Seems
	 * like it should be some universal identifier we can share with other
	 * clients */
	kv_id kv;

} kv_context;

/* struggling a bit with types.  We'll need to create one of these for every
 * type? */

MERCURY_GEN_PROC(put_in_t,
		((int32_t)(key))\
		((int32_t)(value)) )

MERCURY_GEN_PROC(put_out_t, ((int32_t)(ret)) )

DECLARE_MARGO_RPC_HANDLER(put_handler)

MERCURY_GEN_PROC(get_in_t,
		((int32_t)(key)) )

MERCURY_GEN_PROC(get_out_t,
		((int32_t)(value)) ((int32_t)(ret)) )

DECLARE_MARGO_RPC_HANDLER(get_handler)

MERCURY_GEN_PROC(open_in_t,
		((hg_string_t)(name))\
		((int32_t) (keytype))\
		((int32_t) (valtype)) )

MERCURY_GEN_PROC(open_out_t, ((int32_t)(ret)))

DECLARE_MARGO_RPC_HANDLER(open_handler)

MERCURY_GEN_PROC(close_in_t,
		((int32_t)(x))\
		((int32_t)(y)) )

MERCURY_GEN_PROC(close_out_t, ((int32_t)(ret)) )
DECLARE_MARGO_RPC_HANDLER(close_handler)

MERCURY_GEN_PROC(bench_in_t, ((int32_t)(count)) )

typedef struct {
    size_t nkeys;
    double insert_time;
    double read_time;
    double overhead;
} bench_result;

static inline hg_return_t hg_proc_bench_result( hg_proc_t proc, bench_result *result)
{
    /* TODO: needs a portable encoding */
    return(hg_proc_memcpy(proc, result, sizeof(*result))) ;
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
		 ((uint64_t)(size))		\
		 ((int32_t)(ret)))
DECLARE_MARGO_RPC_HANDLER(bulk_get_handler)

kv_context *kv_client_register(char *addr_str=0);
kv_context * kv_server_register(margo_instance_id mid);

DECLARE_MARGO_RPC_HANDLER(shutdown_handler)


/* both the same: should probably move to common */
hg_return_t kv_client_deregister(kv_context *context);
hg_return_t kv_server_deregister(kv_context *context);

/* server-side routine */
hg_return_t kv_server_wait_for_shutdown(kv_context *context);

/* client-side routines wrapping up all the RPC stuff  */
hg_return_t kv_client_shutdown_server(kv_context *context);
hg_return_t kv_open(kv_context *context, char *server, char *name,
		kv_type keytype, kv_type valtype);
hg_return_t kv_put(kv_context *context, void *key, void *value);
hg_return_t kv_bulk_put(kv_context *context, void *key, void *data, hg_size_t data_size);
hg_return_t kv_get(kv_context *context, void *key, void *value);
hg_return_t kv_bulk_get(kv_context *context, void *key, void *data, hg_size_t data_size);
hg_return_t kv_close(kv_context *context);
bench_result *kv_benchmark(kv_context *context, int count);

#if defined(__cplusplus)
}
#endif

