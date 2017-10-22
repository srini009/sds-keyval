#include "sds-keyval.h"

int main(int argc, char **argv) {
  kv_context * context = kv_server_register(NULL);

  margo_wait_for_finalize(context->mid);

  kv_server_deregister(context);
}
