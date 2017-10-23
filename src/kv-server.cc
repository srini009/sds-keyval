#include "sds-keyval.h"
#include "datastore.h"

#include <mercury.h>
#include <margo.h>
#include <abt-snoozer.h>
#include <abt.h>
#include <assert.h>

#include <random>
#include <iostream>

// since this is global, we're assuming this server instance will manage a single DB
AbstractDataStore *datastore = NULL; // created by caller, passed into kv_server_register
std::string db_name;

static hg_return_t open_handler(hg_handle_t handle)
{
    hg_return_t ret;
    open_in_t in;
    open_out_t out;

    ret = margo_get_input(handle, &in);
    std::string in_name(in.name);
    std::cout << "SERVER: OPEN " << in_name << std::endl;

    if (!datastore) {
	//datastore = new BwTreeDataStore(); // testing BwTree
	datastore = new LevelDBDataStore(); // testing LevelDB
	db_name = in_name;
	datastore->createDatabase(db_name);
	std::cout << "SERVER OPEN: DataStore initialized and ready for " << db_name << std::endl;
	out.ret = HG_SUCCESS;
    }
    else {
	if (db_name == in_name) {
	    std::cout << "SERVER OPEN: DataStore initialized and ready for " << db_name << std::endl;
	    out.ret = HG_SUCCESS;
	}
	else {
	    std::cout << "SERVER OPEN failed: currently managing " << db_name
		<< " and unable to process OPEN request for " << in_name << std::endl;
	    out.ret = HG_OTHER_ERROR;
	}
    }

    ret = margo_respond(handle, &out);
    assert(ret == HG_SUCCESS);

    margo_free_input(handle, &in);
    margo_destroy(handle);

    return HG_SUCCESS;
}
DEFINE_MARGO_RPC_HANDLER(open_handler)

static hg_return_t close_handler(hg_handle_t handle)
{
  hg_return_t ret;
  close_out_t out;

  // there may be cleanup we want to do here
  out.ret = HG_SUCCESS;
  
  ret = margo_respond(handle, &out);
  assert(ret == HG_SUCCESS);

  margo_destroy(handle);

  return HG_SUCCESS;
}
DEFINE_MARGO_RPC_HANDLER(close_handler)

static hg_return_t put_handler(hg_handle_t handle)
{
  hg_return_t ret;
  put_in_t in;
  put_out_t out;

  ret = margo_get_input(handle, &in);
  assert(ret == HG_SUCCESS);
	
  ds_bulk_t data;
  data.resize(sizeof(in.value));
  memcpy(data.data(), &in.value, sizeof(in.value));

  if (datastore->put(in.key, data)) {
    std::cout << "SERVER: PUT succeeded for key = " << in.key
	      << " value = " << in.value << std::endl;
    out.ret = HG_SUCCESS;
  }
  else {
    std::cout << "SERVER: PUT failed for key = " << in.key
	      << " value = " << in.value << std::endl;
    out.ret = HG_OTHER_ERROR;
  }

  ret = margo_respond(handle, &out);
  assert(ret == HG_SUCCESS);

  margo_free_input(handle, &in);
  margo_destroy(handle);
	
  return HG_SUCCESS;
}
DEFINE_MARGO_RPC_HANDLER(put_handler)

static hg_return_t bulk_put_handler(hg_handle_t handle)
{
  hg_return_t ret;
  bulk_put_in_t bpin;
  bulk_put_out_t bpout;
  hg_bulk_t bulk_handle;
  const struct hg_info *hgi;
  margo_instance_id mid;

  ret = margo_get_input(handle, &bpin);
  assert(ret == HG_SUCCESS);

  /* get handle info and margo instance */
  hgi = margo_get_info(handle);
  assert(hgi);
  mid = margo_hg_info_get_instance(hgi);
  assert(mid != MARGO_INSTANCE_NULL);

  ds_bulk_t data;
  data.resize(bpin.size);
  void *buffer = (void*)data.data();
  ret = margo_bulk_create(mid, 1, (void**)&buffer, &bpin.size, HG_BULK_WRITE_ONLY, &bulk_handle);
  assert(ret == HG_SUCCESS);
  ret = margo_bulk_transfer(mid, HG_BULK_PULL, hgi->addr, bpin.bulk_handle, 0, bulk_handle, 0, bpin.size);
  assert(ret == HG_SUCCESS);

  if (datastore->put(bpin.key, data)) {
    std::cout << "SERVER: BULK PUT succeeded for key = " << bpin.key
	      << " size = " << bpin.size << std::endl;
    bpout.ret = HG_SUCCESS;
  }
  else {
    // e.g. put returns false if the key-value pair already
    // exists in the DB and duplicates are not allowed or ignored
    std::cout << "SERVER: BULK PUT failed for key = " << bpin.key << std::endl;
    bpout.ret = HG_OTHER_ERROR;
  }

  bpout.ret = ret;
  ret = margo_respond(handle, &bpout);
  assert(ret == HG_SUCCESS);

  margo_free_input(handle, &bpin);
  margo_bulk_free(bulk_handle);
  margo_destroy(handle);
	
  return HG_SUCCESS;
}
DEFINE_MARGO_RPC_HANDLER(bulk_put_handler)

static hg_return_t get_handler(hg_handle_t handle)
{
  hg_return_t ret;
  get_in_t in;
  get_out_t out;

  ret = margo_get_input(handle, &in);
  assert(ret == HG_SUCCESS);

  ds_bulk_t data;
  if (datastore->get(in.key, data)) {
    kv_value_t value;
    if (data.size() <= sizeof(value)) {
      memcpy(&value, data.data(), data.size());
      std::cout << "SERVER: GET succeeded for key = " << in.key
		<< " value = " << value << std::endl;
      out.value = value;
      out.ret = HG_SUCCESS;
    }
    else {
      std::cout << "SERVER: GET failed for key = " << in.key
		<< " value returned too large for kv_value_t" << std::endl;
      out.ret = HG_SIZE_ERROR; // caller should be checking return value
    }
  }
  else {
    // get on key did not succeed
    std::cout << "SERVER: GET failed for key = " << in.key << std::endl;
    out.ret = HG_OTHER_ERROR; // caller should be checking return value
  }

  ret = margo_respond(handle, &out);
  assert(ret == HG_SUCCESS);

  margo_free_input(handle, &in);
  margo_destroy(handle);

  return HG_SUCCESS;
}
DEFINE_MARGO_RPC_HANDLER(get_handler)

static hg_return_t bulk_get_handler(hg_handle_t handle)
{
  hg_return_t ret;
  bulk_get_in_t bgin;
  bulk_get_out_t bgout;
  hg_bulk_t bulk_handle;
  const struct hg_info *hgi;
  margo_instance_id mid;

  ret = margo_get_input(handle, &bgin);
  assert(ret == HG_SUCCESS);

  ds_bulk_t data;
  if (datastore->get(bgin.key, data)) {
    std::cout << "SERVER: BULK GET succeeded for key = " << bgin.key << std::endl;
    // will the transfer fit on the client side?
    bgout.size = data.size();
    if (bgout.size <= bgin.size) {
      /* get handle info and margo instance */
      hgi = margo_get_info(handle);
      assert(hgi);
      mid = margo_hg_info_get_instance(hgi);
      assert(mid != MARGO_INSTANCE_NULL);

      void *buffer = (void*)data.data();
      ret = margo_bulk_create(mid, 1, (void**)&buffer, &bgout.size, HG_BULK_READ_ONLY, &bulk_handle);
      assert(ret == HG_SUCCESS);
      ret = margo_bulk_transfer(mid, HG_BULK_PUSH, hgi->addr, bgin.bulk_handle, 0, bulk_handle, 0, bgout.size);
      assert(ret == HG_SUCCESS);

      bgout.ret = HG_SUCCESS;
    }
    else {
      std::cout << "SERVER: BULK GET failed for key = " << bgin.key
		<< " value returned too large for kv_value_t" << std::endl;
      bgout.ret = HG_SIZE_ERROR;
    }
  }
  else {
    // get on key did not find a value (return 0 for size)
    std::cout << "SERVER: BULK GET failed for key = " << bgin.key << std::endl;
    bgout.size = 0; // assuming caller will check return code
    bgout.ret = HG_OTHER_ERROR;
  }
	
  ret = margo_respond(handle, &bgout);
  assert(ret == HG_SUCCESS);

  margo_free_input(handle, &bgin);
  margo_bulk_free(bulk_handle);
  margo_destroy(handle);
	
  return HG_SUCCESS;
}
DEFINE_MARGO_RPC_HANDLER(bulk_get_handler)

static void shutdown_handler(hg_handle_t handle)
{
  hg_return_t ret;
  margo_instance_id mid;

  std::cout << "SERVER: got RPC request to shutdown" << std::endl;

  /* get handle info and margo instance */
  mid = margo_hg_handle_get_instance(handle);
  assert(mid != MARGO_INSTANCE_NULL);

  ret = margo_respond(handle, NULL);
  assert(ret == HG_SUCCESS);

  margo_destroy(handle);

  /* NOTE: We assume that the server daemon is using
   * margo_wait_for_finalize() to suspend until this
   * RPC executes, so there is no need to send any 
   * extra signal to notify it.
   */
  margo_finalize(mid);

  std::cout << "SERVER: margo finalized" << std::endl;
}
DEFINE_MARGO_RPC_HANDLER(shutdown_handler)

/*
 * from BwTree tests:
 * RandomInsertSpeedTest() - Tests how fast it is to insert keys randomly
 */

static void RandomInsertSpeedTest(int32_t key_num, bench_result *results)
{
  std::random_device rd;
  std::uniform_int_distribution<int> uniform_dist(0, key_num - 1);

  BwTree<int, int> *t = new BwTree<int, int>();
  t->SetDebugLogging(0);
  t->UpdateThreadLocal(1);
  t->AssignGCID(0);

  std::chrono::time_point<std::chrono::system_clock> start, end;

  start = std::chrono::system_clock::now();

  // We loop for keynum * 2 because in average half of the insertion
  // will hit an empty slot
  for(int32_t i = 0;i < key_num * 2;i++) {
    int key = uniform_dist(rd);

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
  for(int32_t i = 0;i < key_num * 2;i++) {
    int key = uniform_dist(rd);

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

  for(int32_t i = 0;i < key_num * 2;i++) {
    int key = uniform_dist(rd);

    v.push_back(key);

    v.clear();
  }

  end = std::chrono::system_clock::now();

  std::chrono::duration<double> overhead = end - start;

  std::cout << "    Overhead = " << overhead.count() << " seconds" << std::endl;
  results->overhead = overhead.count();

  delete t;
}

static hg_return_t bench_handler(hg_handle_t handle)
{
  hg_return_t ret = HG_SUCCESS;
  bench_in_t bench_in;
  bench_out_t bench_out;
  bench_result random_insert;

  ret = margo_get_input(handle, &bench_in);
  assert(ret == HG_SUCCESS);

  printf("benchmarking %d keys\n", bench_in.count);
  RandomInsertSpeedTest(bench_in.count, &random_insert);

  bench_out.result.nkeys = random_insert.nkeys*2;
  bench_out.result.insert_time = random_insert.insert_time;
  bench_out.result.read_time = random_insert.read_time;
  bench_out.result.overhead = random_insert.overhead;

  ret = margo_respond(handle, &bench_out);
  assert(ret == HG_SUCCESS);

  margo_free_input(handle, &bench_in);
  margo_destroy(handle);

  return HG_SUCCESS;
}
DEFINE_MARGO_RPC_HANDLER(bench_handler)
#endif

kv_context *kv_server_register(margo_instance_id mid);
{
    hg_return_t ret;
    hg_addr_t addr_self;
    char addr_self_string[128];
    hg_size_t addr_self_string_sz = 128;
    kv_context *context;

    /* sds keyval server init */
    context = (kv_context *)malloc(sizeof(*context));
    if (!addr_str) {
	context->mid = margo_init("cci+tcp://",
		MARGO_SERVER_MODE, 0, -1);
    }
    else {
	context->mid = margo_init(addr_str,
		MARGO_SERVER_MODE, 0, -1);
    }
    assert(context->mid);

    /* figure out what address this server is listening on */
    ret = margo_addr_self(context->mid, &addr_self);
    if(ret != HG_SUCCESS)
    {
	std::cerr << "Error: margo_addr_self()" << std::endl;
	margo_finalize(context->mid);
	return NULL;
    }
    ret = margo_addr_to_string(context->mid, addr_self_string,
	    &addr_self_string_sz, addr_self);
    if(ret != HG_SUCCESS)
    {
	std::cerr << "Error: margo_addr_to_string()" << std::endl;
	margo_finalize(context->mid);
	return NULL;
    }
    margo_addr_free(context->mid, addr_self);
    std::cout << "accepting RPCs on address " << std::string(addr_self_string) << std::endl;

    context->open_id = MARGO_REGISTER(context->mid, "open",
	    open_in_t, open_out_t, open_handler);

    context->close_id = MARGO_REGISTER(context->mid, "close",
	    void, close_out_t, close_handler);

    context->put_id = MARGO_REGISTER(context->mid, "put",
	    put_in_t, put_out_t, put_handler);

    context->bulk_put_id = MARGO_REGISTER(context->mid, "bulk_put",
	    bulk_put_in_t, bulk_put_out_t, bulk_put_handler);

    context->get_id = MARGO_REGISTER(context->mid, "get",
	    get_in_t, get_out_t, get_handler);

    context->bulk_get_id = MARGO_REGISTER(context->mid, "bulk_get",
	    bulk_get_in_t, bulk_get_out_t, bulk_get_handler);

    context->bench_id = MARGO_REGISTER(context->mid, "bench",
	    bench_in_t, bench_out_t, bench_handler);

    context->shutdown_id = MARGO_REGISTER(context->mid, "shutdown",
	    void, void, shutdown_handler);

}

hg_return_t kv_server_wait_for_shutdown(kv_context *context) {
  margo_wait_for_finalize(context->mid);
  return HG_SUCCESS;
}

/* this is the same as client. should be moved to common utility library */
hg_return_t kv_server_deregister(kv_context *context) {
  free(context);
  delete datastore;
  std::cout << "SERVER: cleanup complete, deregistered" << std::endl;
  return HG_SUCCESS;
}
