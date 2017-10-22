#include "sds-keyval.h"
#include <assert.h>

int main(int argc, char **argv) {
  int ret;
  kv_context * context = kv_client_register(NULL);

  /* open */
  ret = kv_open(context, argv[1], "booger");

  /* put */
  int key = 10;
  int val = 10;
  ret = kv_put(context, &key, &val);

  /* get */
  int remote_val;
  ret = kv_get(context, &key, &remote_val);
  printf("key: %d in: %d out: %d\n", key, val, remote_val);

  /* signal server */
  ret = kv_client_signal_shutdown(context);
  
  /* close */
  ret = kv_close(context);

  /* benchmark doesn't require an open keyval */
  bench_result *output;
  output = kv_benchmark(context, 1000);
  printf("insert: %zd keys in %f seconds: %f Million-insert per sec\n",
	 output->nkeys, output->insert_time,
	 output->nkeys/(output->insert_time*1024*1024) );
  free(output);

  kv_client_deregister(context);
}
