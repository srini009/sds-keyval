#include "sds-keyval.h"
#include <mercury.h>
#include <margo.h>
#include <abt-snoozer.h>
#include <abt.h>

#include <assert.h>



// pass in NULL pointer to get default behavior
kv_context *kv_client_register(const char *addr_str) {
	hg_return_t ret;
	kv_context * context;
	context = malloc(sizeof(kv_context));

	/* client side: no custom xstreams */

	if (!addr_str) {
	  context->mid = margo_init("ofi+tcp://",
				    MARGO_CLIENT_MODE, 0, -1);
	}
	else {
	  context->mid = margo_init(addr_str,
				    MARGO_CLIENT_MODE, 0, -1);
	}

	context->put_id = MARGO_REGISTER(context->mid, "put",
					 put_in_t, put_out_t, NULL);

	context->bulk_put_id = MARGO_REGISTER(context->mid, "bulk_put",
					      bulk_put_in_t, bulk_put_out_t, NULL);

	context->get_id = MARGO_REGISTER(context->mid, "get",
					 get_in_t, get_out_t, NULL);

	context->bulk_get_id = MARGO_REGISTER(context->mid, "bulk_get",
					      bulk_get_in_t, bulk_get_out_t, NULL);

	context->open_id = MARGO_REGISTER(context->mid, "open",
					  open_in_t, open_out_t, NULL);

	context->close_id = MARGO_REGISTER(context->mid, "close",
					   close_in_t, close_out_t, NULL);

	context->bench_id= MARGO_REGISTER(context->mid, "bench",
					  bench_in_t, bench_out_t, NULL);

	context->shutdown_id= MARGO_REGISTER(context->mid, "shutdown",
					     void, void, NULL);
	return context;
}

hg_return_t kv_open(kv_context *context, const char *server_addr, const char *db_name,
		    kv_type keytype, kv_type valtype) {
	hg_return_t ret = HG_SUCCESS;
	hg_handle_t handle;
	open_in_t open_in;
	open_out_t open_out;

	printf("CLIENT: kv_open, server_addr %s\n", server_addr);
	ret = margo_addr_lookup(context->mid, server_addr, &(context->svr_addr));
	assert(ret == HG_SUCCESS);

	ret = margo_create(context->mid, context->svr_addr,
			   context->open_id, &handle);
	assert(ret == HG_SUCCESS);

	strcpy(open_in.name, db_name);

	ret = margo_forward(handle, &open_in);
	assert(ret == HG_SUCCESS);
	ret = margo_get_output(handle, &open_out);
	assert(ret == HG_SUCCESS);
	assert(open_out.ret == HG_SUCCESS);

	/* set up the other calls here: idea is we'll pay the registration cost
	 * once here but can reuse the handles and identifiers multiple times*/
	/* put/get handles: can have as many in flight as we have registered.
	 * BAKE has a handle-caching mechanism we should consult.
	 * should margo be caching handles? */
	ret = margo_create(context->mid, context->svr_addr,
			   context->put_id, &(context->put_handle));
	assert(ret == HG_SUCCESS);
	ret = margo_create(context->mid, context->svr_addr,
			   context->bulk_put_id, &(context->bulk_put_handle));
	assert(ret == HG_SUCCESS);
	ret = margo_create(context->mid, context->svr_addr,
			   context->get_id, &(context->get_handle));
	assert(ret == HG_SUCCESS);
	ret = margo_create(context->mid, context->svr_addr,
			   context->bulk_get_id, &(context->bulk_get_handle));
	assert(ret == HG_SUCCESS);
	ret = margo_create(context->mid, context->svr_addr,
			   context->bench_id, &(context->bench_handle));
	assert(ret == HG_SUCCESS);
	ret = margo_create(context->mid, context->svr_addr,
			   context->shutdown_id, &(context->shutdown_handle));
	assert(ret == HG_SUCCESS);
	
	margo_free_output(handle, &open_out);
	margo_destroy(handle);
	
	return HG_SUCCESS;
}

/* we gave types in the open call.  Will need to maintain in 'context' the
 * size. */
hg_return_t kv_put(kv_context *context, void *key, void *value) {
	hg_return_t ret;
	put_in_t put_in;
	put_out_t put_out;

	put_in.key = *(int*)key;
	put_in.value = *(int*)value;
	ret = margo_forward(context->put_handle, &put_in);
	assert(ret == HG_SUCCESS);
	ret = margo_get_output(context->put_handle, &put_out);
	assert(ret == HG_SUCCESS);
	assert(put_out.ret == HG_SUCCESS);
	margo_free_output(context->put_handle, &put_out);
	return ret;
}

hg_return_t kv_bulk_put(kv_context *context, void *key, void *data, uint64_t *data_size) {
	hg_return_t ret;
	bulk_put_in_t bpin;
	bulk_put_out_t bpout;

	bpin.key = *(uint64_t*)key;
	bpin.size = *(uint64_t*)data_size;
	ret = margo_bulk_create(context->mid, 1, &data, data_size,
				HG_BULK_READ_ONLY, &bpin.bulk_handle);
	assert(ret == HG_SUCCESS);
	ret = margo_forward(context->bulk_put_handle, &bpin);
	assert(ret == HG_SUCCESS);
	ret = margo_get_output(context->bulk_put_handle, &bpout);
	assert(ret == HG_SUCCESS);
	assert(bpout.ret == HG_SUCCESS); // make sure the server side says all is OK
	margo_free_output(context->bulk_put_handle, &bpout);

	return HG_SUCCESS;
}

hg_return_t kv_get(kv_context *context, void *key, void *value)
{
	hg_return_t ret;
	get_in_t get_in;
	get_out_t get_out;

	get_in.key = *(int*)key;
	ret = margo_forward(context->get_handle, &get_in);
	assert(ret == HG_SUCCESS);
	ret = margo_get_output(context->get_handle, &get_out);
	assert(ret == HG_SUCCESS);
	assert(get_out.ret == HG_SUCCESS);
	*(int*) value  = get_out.value;
	margo_free_output(context->get_handle, &get_out);
	return ret;
}

hg_return_t kv_bulk_get(kv_context *context, void *key, void *data, uint64_t *data_size)
{
	hg_return_t ret;
	bulk_get_in_t bgin;
	bulk_get_out_t bgout;
	size_t bgsize;
	int32_t bgret;

	bgin.key = *(uint64_t*)key;
	bgin.size = *(uint64_t*)data_size;
	ret = margo_bulk_create(context->mid, 1, &data, data_size,
				HG_BULK_WRITE_ONLY, &bgin.bulk_handle);
	assert(ret == HG_SUCCESS);
	ret = margo_forward(context->bulk_get_handle, &bgin);
	assert(ret == HG_SUCCESS);
	ret = margo_get_output(context->bulk_get_handle, &bgout);
	assert(ret == HG_SUCCESS);
	assert(bgout.ret == HG_SUCCESS); // make sure the server side says all is OK
	*data_size = (uint64_t)bgout.size; // report actual size of data transferred to caller
	margo_free_output(context->bulk_get_handle, &bgout);

	return HG_SUCCESS;
}

hg_return_t kv_close(kv_context *context)
{
	hg_return_t ret;
	hg_handle_t handle;
	put_in_t close_in;
	put_out_t close_out;

	ret = margo_create(context->mid, context->svr_addr,
			   context->close_id, &handle);
	assert(ret == HG_SUCCESS);
	ret = margo_forward(handle, &close_in);
	assert(ret == HG_SUCCESS);
	ret = margo_get_output(handle, &close_out);
	assert(ret == HG_SUCCESS);
	assert(close_out.ret == HG_SUCCESS);

	margo_free_output(handle, &close_out);
	margo_destroy(handle);

	return HG_SUCCESS;
}

bench_result *kv_benchmark(kv_context *context, int count) {
    hg_return_t ret;
    hg_handle_t handle;
    bench_in_t bench_in;
    bench_out_t bench_out;
    bench_result *result=NULL;

    context->bench_id= MARGO_REGISTER(context->mid, "bench",
			bench_in_t, bench_out_t, NULL);
    ret = margo_create(context->mid, context->svr_addr,
		context->bench_id, &(context->bench_handle) );

    bench_in.count = count;
    ret = margo_create(context->mid, context->svr_addr,
		       context->bench_id, &handle);
    assert(ret == HG_SUCCESS);
    ret = margo_forward(context->bench_handle, &bench_in);
    assert(ret == HG_SUCCESS);
    ret = margo_get_output(context->bench_handle, &bench_out);
    
    margo_free_output(handle, &bench_out);
    margo_destroy(handle);

    result = malloc(sizeof(bench_result));
    result->nkeys = bench_out.result.nkeys;
    result->insert_time = bench_out.result.insert_time;
    result->read_time = bench_out.result.read_time;

    margo_free_output(handle, &bench_out);
    //margo_destroy(handle);

    HG_Destroy(context->bench_handle);

    return result;
}

hg_return_t kv_client_deregister(kv_context *context) {
  hg_return_t ret;

  margo_destroy(context->put_handle);
  margo_destroy(context->get_handle);
  margo_destroy(context->bulk_put_handle);
  margo_destroy(context->bulk_get_handle);
  margo_destroy(context->bench_handle);
  margo_destroy(context->shutdown_handle);

  assert(ret == HG_SUCCESS);
  ret = margo_addr_free(context->mid, context->svr_addr);

  assert(ret == HG_SUCCESS);
  margo_finalize(context->mid);

  free(context);

  return HG_SUCCESS;
}

hg_return_t kv_client_signal_shutdown(kv_context *context) {
  hg_return_t ret;

  ret = margo_forward(context->shutdown_handle, NULL);
  assert(ret == HG_SUCCESS);

  return HG_SUCCESS;
}
