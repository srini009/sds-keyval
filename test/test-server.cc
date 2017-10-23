#include "sds-keyval.h"
#include <assert.h>

int main(int argc, char **argv) {
  kv_context *context = kv_server_register(argv[1]);

  hg_return_t ret;
  ret = kv_server_wait_for_shutdown(context);
  assert(ret == HG_SUCCESS);

  ret = kv_server_deregister(context);
  assert(ret == HG_SUCCESS);
}
