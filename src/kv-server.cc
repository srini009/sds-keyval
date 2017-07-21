#if 0
#include "bwtree.h"
#endif
#include "sds-keyval.h"
#include <mercury.h>
#include <margo.h>
#include <abt-snoozer.h>
#include <abt.h>
#include <assert.h>

static hg_return_t open_handler(hg_handle_t h)
{
	hg_return_t ret;
	open_in_t in;
	open_out_t out;
	const struct hg_info* info = HG_Get_info(h);
	margo_instance_id mid;


	printf("SERVER: OPEN\n");
	ret = HG_Get_input(h, &in);
	// this works
	ret = HG_Respond(h, NULL, NULL, &out);
	// but this did not?
	//mid = margo_hg_class_to_instance(info->hg_class);
	//ret = margo_respond(mid, h, &out);
	assert(ret == HG_SUCCESS);

	HG_Free_input(h, &in);
	HG_Destroy(h);

	return HG_SUCCESS;
}
DEFINE_MARGO_RPC_HANDLER(open_handler);

static hg_return_t close_handler(hg_handle_t h)
{
	hg_return_t ret;
	close_in_t in;
	close_out_t out;

	printf("SERVER: CLOSE\n");

	ret = HG_Get_input(h, &in);
	assert(ret == HG_SUCCESS);
	ret = HG_Respond(h, NULL, NULL, &out);
	assert(ret == HG_SUCCESS);

	HG_Free_input(h, &in);
	HG_Destroy(h);

	return HG_SUCCESS;
}

static hg_return_t  put_handler(hg_handle_t h)
{
	hg_return_t ret;
	put_in_t in;
	put_out_t out;

	printf("SERVER: PUT\n");

	ret = HG_Get_input(h, &in);
	assert(ret == HG_SUCCESS);

	ret = HG_Respond(h, NULL, NULL, &out);
	assert(ret == HG_SUCCESS);

	HG_Free_input(h, &in);
	HG_Destroy(h);
	return HG_SUCCESS;
}

static hg_return_t  get_handler(hg_handle_t h)
{
	hg_return_t ret;
	get_in_t in;
	get_out_t out;

	printf("SERVER: GET\n");

	ret = HG_Get_input(h, &in);
	assert(ret == HG_SUCCESS);

	ret = HG_Respond(h, NULL, NULL, &out);
	assert(ret == HG_SUCCESS);

	HG_Free_input(h, &in);
	HG_Destroy(h);

	return HG_SUCCESS;
}

kv_context * kv_server_register(int argc, char **argv)
{
	int ret;
	hg_addr_t addr_self;
	char addr_self_string[128];
	hg_size_t addr_self_string_sz = 128;
	kv_context *context;
	context = (kv_context *)malloc(sizeof(*context));
	/* sds keyval server init */
	context->hg_class = HG_Init("cci+tcp://localhost:52345", HG_TRUE);
	context->hg_context = HG_Context_create(context->hg_class);
	ret = ABT_init(argc, argv);
	assert (ret == 0);

	ret = ABT_snoozer_xstream_self_set();
	assert(ret == 0);

	context->mid = margo_init(0, 0, context->hg_context);
	assert(context->mid);

	/* figure out what address this server is listening on */
	ret = HG_Addr_self(context->hg_class, &addr_self);
	if(ret != HG_SUCCESS)
	{
		fprintf(stderr, "Error: HG_Addr_self()\n");
		HG_Context_destroy(context->hg_context);
		HG_Finalize(context->hg_class);
		return(NULL);
	}
	ret = HG_Addr_to_string(context->hg_class, addr_self_string,
			&addr_self_string_sz, addr_self);
	if(ret != HG_SUCCESS)
	{
		fprintf(stderr, "Error: HG_Addr_self()\n");
		HG_Context_destroy(context->hg_context);
		HG_Finalize(context->hg_class);
		HG_Addr_free(context->hg_class, addr_self);
		return(NULL);
	}
	HG_Addr_free(context->hg_class, addr_self);
	printf("# accepting RPCs on address \"%s\"\n", addr_self_string);

	context->open_id = MERCURY_REGISTER(context->hg_class, "open",
			open_in_t, open_out_t, open_handler);

	context->close_id = MERCURY_REGISTER(context->hg_class, "close",
			close_in_t, close_out_t, close_handler);

	context->put_id = MERCURY_REGISTER(context->hg_class, "put",
			put_in_t, put_out_t, put_handler);

	context->get_id = MERCURY_REGISTER(context->hg_class, "get",
			get_in_t, get_out_t, get_handler);

	return context;
}

/* this is the same as client. should be moved to common utility library */
int kv_server_deregister(kv_context *context) {
	margo_wait_for_finalize(context->mid);
	margo_finalize(context->mid);
	ABT_finalize();
	HG_Context_destroy(context->hg_context);
	HG_Finalize(context->hg_class);
	free(context);
	return 0;
}
