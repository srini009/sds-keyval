#include "sds-keyval.h"
#include <mercury.h>
#include <margo.h>
#include <abt-snoozer.h>
#include <abt.h>

#include <assert.h>



kv_context *kv_client_register(int argc, char **argv) {
	int ret;
	kv_context * context;
	context = malloc(sizeof(kv_context));

	/* client side: no custom xstreams */

	context->mid = margo_init("cci+tcp://localhost:1234",
		MARGO_CLIENT_MODE, 0, -1);

	context->put_id = MARGO_REGISTER(context->mid, "put",
			put_in_t, put_out_t, NULL);

	context->get_id = MARGO_REGISTER(context->mid, "get",
			get_in_t, get_out_t, NULL);

	context->open_id = MARGO_REGISTER(context->mid, "open",
			open_in_t, open_out_t, NULL);

	context->close_id = MARGO_REGISTER(context->mid, "close",
			close_in_t, close_out_t, NULL);

	context->bench_id= MARGO_REGISTER(context->mid, "bench",
			bench_in_t, bench_out_t, NULL);
	return context;

}

int kv_open(kv_context *context, char * server, char *name,
		kv_type keytype, kv_type valtype) {
	int ret = HG_SUCCESS;
	hg_handle_t handle;
	open_in_t open_in;
	open_out_t open_out;

	ret = margo_addr_lookup(context->mid, server, &(context->svr_addr));
	assert(ret == HG_SUCCESS);

	ret = margo_create(context->mid, context->svr_addr,
			context->open_id, &handle);
	assert(ret == HG_SUCCESS);

	open_in.name = name;

	ret = margo_forward(handle, &open_in);
	assert(ret == HG_SUCCESS);
	ret = HG_Get_output(handle, &open_out);
	assert(ret == HG_SUCCESS);
	ret = open_out.ret;

	/* set up the other calls here: idea is we'll pay the registration cost
	 * once here but can reuse the handles and identifiers multiple times*/
	/* put/get handles: can have as many in flight as we have registered.
	 * BAKE has a handle-caching mechanism we should consult.
	 * should margo be caching handles? */
	ret = margo_create(context->mid, context->svr_addr,
			context->put_id, &(context->put_handle) );
	assert(ret == HG_SUCCESS);
	ret = margo_create(context->mid, context->svr_addr,
			context->get_id, &(context->get_handle) );
	assert(ret == HG_SUCCESS);
	ret = margo_create(context->mid, context->svr_addr,
		context->bench_id, &(context->bench_handle) );

	margo_free_output(handle, &open_out);
	margo_destroy(handle);
	return ret;
}

/* we gave types in the open call.  Will need to maintain in 'context' the
 * size. */
int kv_put(kv_context *context, void *key, void *value) {
	int ret;
	put_in_t put_in;
	put_out_t put_out;

	put_in.key = *(int*)key;
	put_in.value = *(int*)value;
	ret = margo_forward(context->put_handle, &put_in);
	assert(ret == HG_SUCCESS);
	ret = HG_Get_output(context->put_handle, &put_out);
	assert(ret == HG_SUCCESS);
	HG_Free_output(context->put_handle, &put_out);
}

int kv_get(kv_context *context, void *key, void *value)
{
	int ret;
	get_in_t get_in;
	get_out_t get_out;

	get_in.key = *(int*)key;
	ret = margo_forward(context->get_handle, &get_in);
	assert(ret == HG_SUCCESS);
	ret = HG_Get_output(context->get_handle, &get_out);
	*(int*) value  = get_out.value;
	assert(ret == HG_SUCCESS);
	HG_Free_output(context->get_handle, &get_out);
}
int kv_close(kv_context *context)
{
	int ret;
	hg_handle_t handle;
	put_in_t close_in;
	put_out_t close_out;

	ret = margo_create(context->mid, context->svr_addr,
			context->close_id, &handle);
	assert(ret == HG_SUCCESS);
	ret = margo_forward(handle, &close_in);
	assert(ret == HG_SUCCESS);
	ret = HG_Get_output(handle, &close_out);
	assert(ret == HG_SUCCESS);
	HG_Free_output(handle, &close_out);

	HG_Destroy(context->put_handle);
	HG_Destroy(context->get_handle);
	HG_Destroy(handle);
}

char *kv_benchmark(kv_context *context, int count) {
    int ret;
    hg_handle_t handle;
    bench_in_t bench_in;
    bench_out_t bench_out;
    char *output = NULL;

    bench_in.count = count;
    ret = margo_create(context->mid, context->svr_addr,
	    context->bench_id, &handle);
    assert(ret == HG_SUCCESS);
    ret = margo_forward(context->bench_handle, &bench_in);
    assert(ret == HG_SUCCESS);
    ret = HG_Get_output(context->bench_handle, &bench_out);
    HG_Free_output(handle, &bench_out);

    return output;
}

int kv_client_deregister(kv_context *context) {
	margo_addr_free(context->mid, context->svr_addr);
	margo_finalize(context->mid);
	free(context);
	return 0;
}
