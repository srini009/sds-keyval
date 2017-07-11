#include "sds-keyval.h"
#include <assert.h>

int main(int argc, char **argv) {
	int ret;
	kv_context * context = kv_client_register(argc, argv);
	hg_handle_t handle;
	open_in_t open_in;
	open_out_t open_out;
	get_in_t get_in;
	get_in_t get_out;
	put_in_t put_in;
	put_out_t put_out;
	close_in_t close_in;
	close_out_t close_out;

	ret = margo_addr_lookup(context->mid, argv[1], &(context->svr_addr));
	assert(ret == HG_SUCCESS);

	ret = HG_Create(context->hg_context, context->svr_addr,
			context->open_id, &handle);
	assert(ret == HG_SUCCESS);

	ret = margo_forward(context->mid, handle, &open_in);
	assert(ret == HG_SUCCESS);
	ret = HG_Get_output(handle, &open_out);
	assert(ret == HG_SUCCESS);

	ret = margo_forward(context->mid, handle, &put_in);
	assert(ret == HG_SUCCESS);
	ret = HG_Get_output(handle, &put_out);
	assert(ret == HG_SUCCESS);

	ret = margo_forward(context->mid, handle, &get_in);
	assert(ret == HG_SUCCESS);
	ret = HG_Get_output(handle, &get_out);
	assert(ret == HG_SUCCESS);

	ret = margo_forward(context->mid, handle, &close_in);
	assert(ret == HG_SUCCESS);
	ret = HG_Get_output(handle, &close_out);
	assert(ret == HG_SUCCESS);



	kv_client_deregister(context);
}
