#include <mercury.h>
#include <mercury_macros.h>
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

/* do we need one for server, one for client? */
typedef struct kv_context_s {
	hg_class_t * hg_class;
	hg_context_t *hg_context;
	margo_instance_id mid;
	hg_addr_t svr_addr;
	ABT_xstream xstream;
	hg_id_t put_id;
	hg_id_t get_id;
	hg_id_t open_id;
	hg_id_t close_id;
	/* some keyval dodad goes here for the server I guess? */

} kv_context;

MERCURY_GEN_PROC(put_in_t,
		((int32_t)(x))\
		((int32_t)(y)))

MERCURY_GEN_PROC(put_out_t, ((int32_t)(ret)))

MERCURY_GEN_PROC(get_in_t,
		((int32_t)(x))\
		((int32_t)(y)))

MERCURY_GEN_PROC(get_out_t, ((int32_t)(ret)))

MERCURY_GEN_PROC(open_in_t,
		((int32_t)(x))\
		((int32_t)(y)))

MERCURY_GEN_PROC(open_out_t, ((int32_t)(ret)))


MERCURY_GEN_PROC(close_in_t,
		((int32_t)(x))\
		((int32_t)(y)))

MERCURY_GEN_PROC(close_out_t, ((int32_t)(ret)))




kv_context *kv_client_register(int argc, char **argv);
kv_context * kv_server_register(int argc, char **argv);

/* both the same: should probably move to common */
int kv_client_deregister(kv_context *context);
int kv_server_deregister(kv_context *context);


#if defined(__cplusplus)
}
#endif

