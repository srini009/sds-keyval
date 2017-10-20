
#include <sds-keyval.h>
#include <random>
#include <chrono>

void RandomInsertSpeedTest(kv_context *context,
	size_t key_num, bench_result *results)
{
    std::random_device r{};
    std::default_random_engine e1(r());
    std::uniform_int_distribution<int> uniform_dist(0, key_num - 1);

    std::chrono::time_point<std::chrono::system_clock> start, end;

    start = std::chrono::system_clock::now();


    // We loop for keynum * 2 because in average half of the insertion
    // will hit an empty slot
    for(size_t i = 0;i < key_num * 2;i++) {
	int key = uniform_dist(e1);
	kv_put(context, &key, &key);
    }
    end = std::chrono::system_clock::now();

    std::chrono::duration<double> elapsed_seconds = end - start;

    results->nkeys = key_num *2;
    results->insert_time = elapsed_seconds.count();

    // Then test random read after random insert
    int v;
    start = std::chrono::system_clock::now();

    for(size_t i = 0;i < key_num * 2;i++) {
	int key = uniform_dist(e1);

	kv_get(context, &key,  &v);
    }

    end = std::chrono::system_clock::now();

    elapsed_seconds = end - start;
    results->read_time = elapsed_seconds.count();

    results->overhead = 0;

    return;
}

void print_results(bench_result *r)
{
    printf("insert: %zd keys in %f seconds: %f Million-insert per sec: %f usec per insert\n",
	    r->nkeys, r->insert_time, r->nkeys/(r->insert_time*1024*1024),
	    (r->insert_time*1000*1000)/r->nkeys);
}

int main(int argc, char **argv)
{
    bench_result rpc;
    bench_result *server;
    kv_context *context;
    size_t items = atoi(argv[1]);

    context = kv_client_register(NULL);
    kv_open(context, argv[2], NULL, KV_INT, KV_INT);

    RandomInsertSpeedTest(context, items, &rpc);
    print_results(&rpc);
    kv_close(context);

    server = kv_benchmark(context, items);
    print_results(server);
    free(server);
    // kv_client_deregister(context);
    return 0;
}
