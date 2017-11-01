#include <sds-keyval.h>
#include <assert.h>

#include <random>
#include <chrono>

void RandomInsertSpeedTest(kv_context_t *context,
			   size_t key_num, bench_result_t *results)
{
  hg_return_t ret;
  std::random_device r{};
  std::default_random_engine e1(r());
  std::uniform_int_distribution<int32_t> uniform_dist(0, key_num - 1);

  std::chrono::time_point<std::chrono::system_clock> start, end;

  // We loop for keynum * 2 because in average half of the insertion
  // will hit an empty slot
  start = std::chrono::system_clock::now();
  for(size_t i = 0;i < key_num * 2;i++) {
    int32_t key = uniform_dist(e1);
    ret = kv_put(context, &key, sizeof(key), &key, sizeof(key));
    assert(ret == HG_SUCCESS);
  }
  end = std::chrono::system_clock::now();

  std::chrono::duration<double> elapsed_seconds = end - start;

  results->nkeys = key_num *2;
  results->insert_time = elapsed_seconds.count();

  // Then test random read after random insert
  int32_t value;
  hg_size_t vsize;
  start = std::chrono::system_clock::now();
  for(size_t i = 0;i < key_num * 2;i++) {
    int32_t key = uniform_dist(e1);
    vsize = sizeof(value);
    ret = kv_get(context, &key, sizeof(key), &value, &vsize);
    // key might not be in store due to randomness
    // HG_OTHER_ERROR is basically a "key not found" return code
    assert(ret == HG_SUCCESS || ret == HG_OTHER_ERROR);
  }
  end = std::chrono::system_clock::now();

  elapsed_seconds = end - start;
  results->read_time = elapsed_seconds.count();

  results->overhead = 0;

  return;
}

void print_results(bench_result_t *r)
{
  printf("inserts: %zd keys in %f seconds: %f Million-inserts per sec: %f usec per insert\n",
	 r->nkeys, r->insert_time, r->nkeys/(r->insert_time*1000*1000),
	 (r->insert_time*1000*1000)/r->nkeys);
  printf("reads: %zd keys in %f seconds: %f Million-reads per sec: %f usec per read\n",
	 r->nkeys, r->read_time, r->nkeys/(r->read_time*1000*1000),
	 (r->read_time*1000*1000)/r->nkeys);
}

int main(int argc, char **argv)
{
  hg_return_t ret;
  bench_result_t rpc;
  bench_result_t *server;
  kv_context_t *context;
  size_t items = atoi(argv[1]);

  context = kv_client_register(NULL);
  ret = kv_open(context, argv[2], "db/testdb");
  assert(ret == HG_SUCCESS);

  RandomInsertSpeedTest(context, items, &rpc);
  print_results(&rpc);

#if 1 // ifdef out when testing with LevelDB or BerkeleyDB
  server = kv_benchmark(context, items);
  print_results(server);
  free(server);
#endif
  
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
