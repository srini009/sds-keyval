#include "sds-keyval.h"
#include <unistd.h>
#include <assert.h>

int main(int argc, char **argv) {
  hg_return_t ret;
  kv_context * context = kv_client_register(NULL);

  /* open */
  ret = kv_open(context, argv[1], "booger");
  assert(ret == HG_SUCCESS);

  /* put */
  uint64_t key = 10;
  int val = 10;
  ret = kv_put(context, &key, &val);
  assert(ret == HG_SUCCESS);

  /* get */
  int remote_val;
  ret = kv_get(context, &key, &remote_val);
  assert(ret == HG_SUCCESS);
  printf("key: %d in: %d out: %d\n", key, val, remote_val);

  bench_result *output;
  output = kv_benchmark(context, 1000);
  printf("insert: %zd keys in %f seconds: %f Million-insert per sec\n",
	 output->nkeys, output->insert_time,
	 output->nkeys/(output->insert_time*1024*1024) );
  free(output);
  
  /* close */
  ret = kv_close(context);
  assert(ret == HG_SUCCESS);

  /* signal server */
  ret = kv_client_signal_shutdown(context);
  assert(ret == HG_SUCCESS);

  /* cleanup */
  ret = kv_client_deregister(context);
  assert(ret == HG_SUCCESS);
}
