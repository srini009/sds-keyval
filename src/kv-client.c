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

	context->hg_class = HG_Init("cci+tcp://localhost:1234", HG_FALSE);
	context->hg_context = HG_Context_create(context->hg_class);
	ABT_init(argc, argv);
	ABT_snoozer_xstream_self_set();
	ret  = ABT_xstream_self(&(context->xstream));
	context->mid = margo_init(0, 0, context->hg_context);

	context->put_id = MERCURY_REGISTER(context->hg_class, "put",
			put_in_t, put_out_t, NULL);

	context->get_id = MERCURY_REGISTER(context->hg_class, "get",
			get_in_t, get_out_t, NULL);

	context->open_id = MERCURY_REGISTER(context->hg_class, "open",
			open_in_t, open_out_t, NULL);

	context->close_id = MERCURY_REGISTER(context->hg_class, "close",
			close_in_t, close_out_t, NULL);
	return context;
}

int kv_open(kv_context *context, char *name,
		kv_type keytype, kv_type valtype) {
	int ret = HG_SUCCESS;
	hg_handle_t handle;
	open_in_t open_in;
	open_out_t open_out;

	ret = HG_Create(context->hg_context, context->svr_addr,
			context->open_id, &handle);
	assert(ret == HG_SUCCESS);

	open_in.name = name;

	ret = margo_forward(context->mid, handle, &open_in);
	assert(ret == HG_SUCCESS);
	ret = HG_Get_output(handle, &open_out);
	assert(ret == HG_SUCCESS);
	ret = open_out.ret;
	HG_Free_output(handle, &open_out);
	HG_Destroy(handle);
	return ret;
}

/* we gave types in the open call.  Will need to maintain in 'context' the
 * size. */
int kv_put(kv_context *context, void *key, void *value) {
	int ret;
	hg_handle_t handle;
	put_in_t put_in;
	put_out_t put_out;
	ret = HG_Create(context->hg_context, context->svr_addr,
			context->put_id, &handle);
	assert(ret == HG_SUCCESS);

	put_in.key = *(int*)key;
	put_in.value = *(int*)value;
	ret = margo_forward(context->mid, handle, &put_in);
	assert(ret == HG_SUCCESS);
	ret = HG_Get_output(handle, &put_out);
	assert(ret == HG_SUCCESS);
	HG_Free_output(handle, &put_out);
	HG_Destroy(handle);
}

int kv_get(kv_context *context, void *key, void *value)
{
	int ret;
	hg_handle_t handle;
	get_in_t get_in;
	get_out_t get_out;

	ret = HG_Create(context->hg_context, context->svr_addr,
			context->get_id, &handle);

	get_in.key = *(int*)key;
	ret = margo_forward(context->mid, handle, &get_in);
	assert(ret == HG_SUCCESS);
	ret = HG_Get_output(handle, &get_out);
	*(int*) value  = get_out.value;
	assert(ret == HG_SUCCESS);
	HG_Free_output(handle, &get_out);
	HG_Destroy(handle);
}
int kv_close(kv_context *context)
{
	int ret;
	hg_handle_t handle;
	put_in_t close_in;
	put_out_t close_out;

	ret = HG_Create(context->hg_context, context->svr_addr,
			context->close_id, &handle);
	assert(ret == HG_SUCCESS);
	ret = margo_forward(context->mid, handle, &close_in);
	assert(ret == HG_SUCCESS);
	ret = HG_Get_output(handle, &close_out);
	assert(ret == HG_SUCCESS);
	HG_Free_output(handle, &close_out);
	HG_Destroy(handle);
}

int kv_client_deregister(kv_context *context) {
	HG_Addr_free(context->hg_class, context->svr_addr);
	margo_finalize(context->mid);
	ABT_finalize();
	HG_Context_destroy(context->hg_context);
	HG_Finalize(context->hg_class);
	free(context);
	return 0;
}
