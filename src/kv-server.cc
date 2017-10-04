#if 0
#include "bwtree.h"
#endif
#include "sds-keyval.h"
#include <mercury.h>
#include <margo.h>
#include <abt-snoozer.h>
#include <abt.h>
#include <assert.h>

#ifdef DISABLE_BWTREE
#ifdef HAVE_DB_H
#include <db.h>
#endif

DB *dbp;


static hg_return_t open_handler(hg_handle_t h)
{
    hg_return_t ret;
    open_in_t in;
    open_out_t out;

    ret = margo_get_input(h, &in);

    db_create(&dbp, NULL, 0);
    u_int32_t oflags = DB_CREATE;
    dbp->open(dbp, NULL, in.name, NULL, DB_BTREE, oflags, 0);

    out.ret = HG_SUCCESS;
    ret = HG_Respond(h, NULL, NULL, &out);
    HG_Free_input(h, &in);
    HG_Destroy(h);

    return ret;
}
DEFINE_MARGO_RPC_HANDLER(open_handler)

static hg_return_t close_handler(hg_handle_t h)
{
    hg_return_t ret;
    close_in_t in;
    close_out_t out;


    ret = HG_Get_input(h, &in);
    assert(ret == HG_SUCCESS);

    dbp->close(dbp, 0);
    ret = HG_Respond(h, NULL, NULL, &out);
    assert(ret == HG_SUCCESS);

    HG_Free_input(h, &in);
    HG_Destroy(h);

    return ret;

}
DEFINE_MARGO_RPC_HANDLER(close_handler)

static hg_return_t put_handler(hg_handle_t h)
{
    DBT key, value;
    int ret;
    put_in_t in;
    put_out_t out;

    ret = margo_get_input(h, &in);
    memset(&key, 0, sizeof(key));
    memset(&value, 0, sizeof(value));

    key.data = &(in.key);
    key.size = sizeof(in.key);

    value.data = &(in.value);
    value.size = sizeof(in.value);

    dbp->put(dbp, NULL, &key, &value,  0);
    ret = HG_Respond(h, NULL, NULL, &out);
    assert(ret == HG_SUCCESS);

    HG_Free_input(h, &in);
    HG_Destroy(h);
    return HG_SUCCESS;
}

DEFINE_MARGO_RPC_HANDLER(put_handler)

static hg_return_t get_handler(hg_handle_t h)
{
    hg_return_t ret;
    get_in_t in;
    get_out_t out;
    DBT key, value;

    ret = margo_get_input(h, &in);
    memset(&key, 0, sizeof(key));
    memset(&value, 0, sizeof(value));

    key.data = &(in.key);
    key.size = sizeof(in.key);

    value.data = &(out.value);
    value.ulen = sizeof(out.value);

    dbp->get(dbp, NULL, &key, &value, 0);
    ret = HG_Respond(h, NULL, NULL, &out);
    assert(ret == HG_SUCCESS);

    HG_Free_input(h, &in);
    HG_Destroy(h);

    return ret;
}
DEFINE_MARGO_RPC_HANDLER(get_handler)

static hg_return_t  bench_handler(hg_handle_t h)
{
    bench_out_t bench_out;

    bench_out.result.nkeys = 0;
    bench_out.result.insert_time = 0;
    bench_out.result.read_time = 0;
    bench_out.result.overhead = 0;

    HG_Respond(h, NULL, NULL, &bench_out);
    return HG_SUCCESS;
}
DEFINE_MARGO_RPC_HANDLER(bench_handler)

#else
/* keyval-specific stuff can go here */
#include <bwtree.h>
#include <vector>

size_t my_hash(const std::vector<char> &v) {
  size_t hash = 0;
  boost::hash_range(hash, v.begin(), v.end());
  return hash;
}

bool my_equal_to(const std::vector<char> &v1, const std::vector<char> &v2) {
  return my_hash(v1) == my_hash(v2);
}

wangziqi2013::bwtree::BwTree<uint64_t, std::vector<char>,
			     std::less<uint64_t>,
			     std::equal_to<uint64_t>,
			     std::hash<uint64_t>,
			     my_equal_to,
			     my_hash> *TREE;


static hg_return_t open_handler(hg_handle_t h)
{
	hg_return_t ret;
	open_in_t in;
	open_out_t out;

	ret = margo_get_input(h, &in);

	TREE = new(wangziqi2013::bwtree::BwTree<uint64_t, std::vector<char>,
		                                std::less<uint64_t>,
		                                std::equal_to<uint64_t>,
                                		std::hash<uint64_t>,
		                                my_equal_to,
                                 		my_hash>);
	TREE->SetDebugLogging(0);
	TREE->UpdateThreadLocal(1);
	TREE->AssignGCID(0);

	/* TODO: something with in.keytype and in.valtype.  In C I would get
	 * away with sloppy casting.  Not sure how to do the same with a C++
	 * template.  */

	/* I don't know how to check for error */
	out.ret = HG_SUCCESS;

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
DEFINE_MARGO_RPC_HANDLER(open_handler)

static hg_return_t close_handler(hg_handle_t h)
{
	hg_return_t ret;
	close_in_t in;
	close_out_t out;


	ret = HG_Get_input(h, &in);
	assert(ret == HG_SUCCESS);
	ret = HG_Respond(h, NULL, NULL, &out);
	assert(ret == HG_SUCCESS);

	HG_Free_input(h, &in);
	HG_Destroy(h);

	return HG_SUCCESS;
}
DEFINE_MARGO_RPC_HANDLER(close_handler)

static hg_return_t  put_handler(hg_handle_t h)
{
	hg_return_t ret;
	put_in_t in;
	put_out_t out;


	ret = HG_Get_input(h, &in);
	TREE->Insert(in.key, in.value);
	assert(ret == HG_SUCCESS);

	ret = HG_Respond(h, NULL, NULL, &out);
	assert(ret == HG_SUCCESS);

	HG_Free_input(h, &in);
	HG_Destroy(h);
	return HG_SUCCESS;
}
DEFINE_MARGO_RPC_HANDLER(put_handler)

static hg_return_t bulk_put_handler(hg_handle_t h)
{
	hg_return_t ret;
	bulk_put_in_t bpin;
	bulk_put_out_t bpout;
	void *buffer;
	hg_bulk_t bulk_handle;
	const struct hg_info *hgi;
	margo_instance_id mid;

	ret = HG_Get_input(h, &bpin);
	printf("SERVER: BULK PUT key = %d size = %d\n", bpin.key, bpin.size);

	/* get handle info and margo instance */
	hgi = margo_get_info(h);
	assert(hgi);
	mid = margo_hg_info_get_instance(hgi);
	assert(mid != MARGO_INSTANCE_NULL);

	std::vector<char> data;
	data.resize(bpin.size);
	ret = margo_bulk_create(mid, 1, &((void*)data.data()), &bpin.size, HG_BULK_WRITE_ONLY, &bulk_handle);
	assert(ret == HG_SUCCESS);
	ret = margo_bulk_transfer(mid, HG_BULK_PULL, hgi->addr, bpin.bulk_handle, 0, bulk_handle, 0, bpin.size);
	assert(ret == HG_SUCCESS);
	
	TREE->Insert(bpin.key, data);
	assert(ret == HG_SUCCESS);

	bpout.ret = ret;
	ret = HG_Respond(h, NULL, NULL, &bpout);
	assert(ret == HG_SUCCESS);

	HG_Free_input(h, &bpin);
	margo_bulk_free(bulk_handle);
	HG_Destroy(h);
	
	return HG_SUCCESS;
}
DEFINE_MARGO_RPC_HANDLER(bulk_put_handler)

static hg_return_t  get_handler(hg_handle_t h)
{
	hg_return_t ret;
	get_in_t in;
	get_out_t out;


	ret = HG_Get_input(h, &in);
	assert(ret == HG_SUCCESS);

	/*void 	GetValue (const KeyType &search_key, std::vector< ValueType > &value_list) */
	std::vector<int> value;
	TREE->GetValue(in.key, value);

	if (value.size() >= 1) {
		out.value = value.front();
	} else {
	    out.ret = -1;
	}

	ret = HG_Respond(h, NULL, NULL, &out);
	assert(ret == HG_SUCCESS);

	HG_Free_input(h, &in);
	HG_Destroy(h);

	return HG_SUCCESS;
}
DEFINE_MARGO_RPC_HANDLER(get_handler)

static hg_return_t bulk_get_handler(hg_handle_t h)
{
	hg_return_t ret;
	bulk_get_in_t bgin;
	bulk_get_out_t bgout;
	hg_bulk_t bulk_handle;
	const struct hg_info *hgi;
	margo_instance_id mid;

	ret = HG_Get_input(h, &bgin);
	assert(ret == HG_SUCCESS);

	/* void GetValue (const KeyType &search_key, std::vector< ValueType > &value_list) */
	std::vector<std::vector<char>> values;
	TREE->GetValue(bgin.key, values); // is this right for values?

	// what do we do if we get more than 1 value?
	// perhaps > 1 or 0 results in an error return value?
	if (values.size() == 1) {
	  printf("SERVER: GET: found 1 value for key=%d\n", bgin.key);
	  std::vector<char> data = values.front();
	  // will the transfer fit on the client side?
	  bgout.size = data.size();
	  if (bgout.size <= bgin.size) {
	    /* get handle info and margo instance */
	    hgi = margo_get_info(h);
	    assert(hgi);
	    mid = margo_hg_info_get_instance(hgi);
	    assert(mid != MARGO_INSTANCE_NULL);

	    ret = margo_bulk_create(mid, 1, &((void*)data.data()), &bgout.size, HG_BULK_READ_ONLY, &bulk_handle);
	    assert(ret == HG_SUCCESS);
	    ret = margo_bulk_transfer(mid, HG_BULK_PUSH, hgi->addr, bgin.bulk_handle, 0, bulk_handle, 0, bgout.size);
	    assert(ret == HG_SUCCESS);

	    bgout.ret = HG_SUCCESS;
	  }
	  else {
	    bgout.ret = HG_SIZE_ERROR;
	  }
	}
	else if (values.size() > 1) {
	  // get on key returned more than 1 value (return number found)
	  printf("SERVER: GET: found %d values for key=%d\n", values.size(), bgin.key);
	  bgout.size = values.size();
	  bgout.ret = HG_OTHER_ERROR;
	}
	else {
	  // get on key did not find a value (return 0 for number found)
	  printf("SERVER: GET: found 0 values for key=%d\n", bgin.key);
	  bgout.size = 0;
	  bgout.ret = HG_OTHER_ERROR;
	}
	
	ret = HG_Respond(h, NULL, NULL, &bgout);
	assert(ret == HG_SUCCESS);

	HG_Free_input(h, &bgin);
	margo_bulk_free(bulk_handle);
	HG_Destroy(h);
	
	return HG_SUCCESS;
}
DEFINE_MARGO_RPC_HANDLER(bulk_get_handler)

/*
 * from BwTree tests:
 * RandomInsertSpeedTest() - Tests how fast it is to insert keys randomly
 */
#include <random>
#include <iostream>

static void RandomInsertSpeedTest(size_t key_num, bench_result *results)
{
  std::random_device r{};
  std::default_random_engine e1(r());
  std::uniform_int_distribution<int> uniform_dist(0, key_num - 1);

  auto *t = new wangziqi2013::bwtree::BwTree<int, int>;
  t->SetDebugLogging(0);
  t->UpdateThreadLocal(1);
  t->AssignGCID(0);

  std::chrono::time_point<std::chrono::system_clock> start, end;

  start = std::chrono::system_clock::now();

  // We loop for keynum * 2 because in average half of the insertion
  // will hit an empty slot
  for(size_t i = 0;i < key_num * 2;i++) {
    int key = uniform_dist(e1);

    t->Insert(key, key);
  }

  end = std::chrono::system_clock::now();

  std::chrono::duration<double> elapsed_seconds = end - start;

  results->nkeys = key_num;
  results->insert_time = elapsed_seconds.count();

  std::cout << "BwTree: at least " << (key_num * 2.0 / (1024 * 1024)) / elapsed_seconds.count()
           << " million random insertion/sec" << "\n";

  // Then test random read after random insert
  std::vector<int> v{};
  v.reserve(100);

  start = std::chrono::system_clock::now();

  for(size_t i = 0;i < key_num * 2;i++) {
    int key = uniform_dist(e1);

    t->GetValue(key, v);

    v.clear();
  }

  end = std::chrono::system_clock::now();

  elapsed_seconds = end - start;
  std::cout << "BwTree: at least " << (key_num * 2.0 / (1024 * 1024)) / elapsed_seconds.count()
            << " million random read after random insert/sec" << "\n";
  results->read_time = elapsed_seconds.count();

  // Measure the overhead

  start = std::chrono::system_clock::now();

  for(size_t i = 0;i < key_num * 2;i++) {
    int key = uniform_dist(e1);

    v.push_back(key);

    v.clear();
  }

  end = std::chrono::system_clock::now();

  std::chrono::duration<double> overhead = end - start;

  std::cout << "    Overhead = " << overhead.count() << " seconds" << std::endl;
  results->overhead = overhead.count();

  return;
}


static hg_return_t  bench_handler(hg_handle_t h)
{
    hg_return_t ret = HG_SUCCESS;
    bench_in_t bench_in;
    bench_out_t bench_out;
    bench_result random_insert;

    ret = HG_Get_input(h, &bench_in);
    printf("benchmarking %d keys\n", bench_in.count);
    RandomInsertSpeedTest(bench_in.count, &random_insert);
    bench_out.result.nkeys = random_insert.nkeys*2;
    bench_out.result.insert_time = random_insert.insert_time;
    bench_out.result.read_time = random_insert.read_time;
    bench_out.result.overhead = random_insert.overhead;

    ret = HG_Respond(h, NULL, NULL, &bench_out);

    HG_Free_input(h, &bench_in);
    HG_Destroy(h);
    return ret;
}
DEFINE_MARGO_RPC_HANDLER(bench_handler)
#endif

kv_context *kv_server_register(margo_instance_id mid);
{
	int ret;
	hg_addr_t addr_self;
	char addr_self_string[128];
	hg_size_t addr_self_string_sz = 128;
	kv_context *context;
	context = (kv_context *)malloc(sizeof(*context));
	/* sds keyval server init */

	context->mid = mid;
	assert(context->mid);

	/* figure out what address this server is listening on */
	ret = margo_addr_self(context->mid, &addr_self);
	if(ret != HG_SUCCESS)
	{
		fprintf(stderr, "Error: mago_addr_selff()\n");
		margo_finalize(context->mid);
		return(NULL);
	}
	ret = margo_addr_to_string(context->mid, addr_self_string,
			&addr_self_string_sz, addr_self);
	if(ret != HG_SUCCESS)
	{
		fprintf(stderr, "Error: HG_Addr_self()\n");
		margo_finalize(context->mid);
		return(NULL);
	}
	margo_addr_free(context->mid, addr_self);
	printf("# accepting RPCs on address \"%s\"\n", addr_self_string);

	context->open_id = MARGO_REGISTER(context->mid, "open",
			open_in_t, open_out_t, open_handler);

	context->close_id = MARGO_REGISTER(context->mid, "close",
			close_in_t, close_out_t, close_handler);

	context->put_id = MARGO_REGISTER(context->mid, "put",
			put_in_t, put_out_t, put_handler);

	context->put_id = MARGO_REGISTER(context->mid, "bulk_put",
			bulk_put_in_t, bulk_put_out_t, bulk_put_handler);

	context->get_id = MARGO_REGISTER(context->mid, "get",
			get_in_t, get_out_t, get_handler);

	context->get_id = MARGO_REGISTER(context->mid, "bulk_get",
			bulk_get_in_t, bulk_get_out_t, bulk_get_handler);

	context->bench_id = MARGO_REGISTER(context->mid, "bench",
		bench_in_t, bench_out_t, bench_handler);

	return context;
}

/* this is the same as client. should be moved to common utility library */
int kv_server_deregister(kv_context *context) {
	margo_wait_for_finalize(context->mid);
	margo_finalize(context->mid);
	free(context);
	delete(TREE);
	return 0;
}
