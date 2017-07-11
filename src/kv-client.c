#include "sds-keyval.h"
#include <mercury.h>
#include <margo.h>
#include <abt-snoozer.h>
#include <abt.h>




/* probably ends up in a standalone test program  */
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


int kv_client_deregister(kv_context *context) {
	margo_finalize(context->mid);
	ABT_finalize();
	HG_Context_destroy(context->hg_context);
	HG_Finalize(context->hg_class);
	free(context);
	return 0;
}
