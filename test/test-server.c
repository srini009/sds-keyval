#include "sds-keyval.h"

int main(int argc, char **argv) {
	margo_instance_id mid;
	mid = margo_init("ofi+tcp://", MARGO_SERVER_MODE, 0, -1);
	kv_context * context = kv_server_register(mid);

	margo_wait_for_finalize(context->mid);



	kv_server_deregister(context);
}
