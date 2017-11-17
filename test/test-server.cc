#include "sds-keyval.h"
#include <assert.h>

int main(int argc, char **argv) {

  assert(argc == 2);
  char *addr_str = argv[1];
  
  margo_instance_id mid = kv_margo_init(kv_protocol(addr_str), MARGO_SERVER_MODE);
  kv_context_t *context = kv_server_register(mid);

  hg_return_t ret;
  ret = kv_server_wait_for_shutdown(context);
  assert(ret == HG_SUCCESS);

  ret = kv_server_deregister(context);
  assert(ret == HG_SUCCESS);
}
