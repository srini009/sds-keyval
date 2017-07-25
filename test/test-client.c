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

	/* open */
	ret = kv_open(context, "booger", KV_INT, KV_INT);

	/* put */
	int key = 10;
	int val = 10;
	ret = kv_put(context, &key, &val);

	/* get */
	int remote_val;
	ret = kv_get(context, &key, &remote_val);
	printf("key: %d in: %d out: %d\n", key, val, remote_val);

	/* close */
	ret = kv_close(context);

	kv_client_deregister(context);
}
