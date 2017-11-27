#include "sds-keyval-group.h"
#include "kv-config.h"

#include <stdlib.h>
#include <time.h>
#include <assert.h>
#include <iostream>

#include <boost/filesystem.hpp>

unsigned long server_indexes[CH_MAX_REPLICATION];

kvgroup_context_t *kvgroup_client_register(margo_instance_id mid, ssg_group_id_t gid)
{
  kvgroup_context_t *context = (kvgroup_context_t*)malloc(sizeof(kvgroup_context_t));
  memset(context, 0, sizeof(kvgroup_context_t));
  
  int sret = ssg_init(mid);
  assert(sret == SSG_SUCCESS);

  sret = ssg_group_attach(gid);
  assert(sret == SSG_SUCCESS);
  
  /* update kvgroup_context_t with MID, GID */
  context->mid = mid;
  context->gid = gid;

  return context;
}

hg_return_t kvgroup_open(kvgroup_context_t *context, const char *db_name)
{
  hg_size_t addr_str_sz = 128;
  char addr_str[addr_str_sz];
  hg_return_t ret = HG_SUCCESS;
  
  std::string dbname(db_name);
  boost::filesystem::path p(dbname);
  std::string basepath = p.parent_path().string();
  std::string name = p.filename().string();
  std::string separator("/");

  // register and open a connection with each kv-server in the group
  hg_size_t gsize = ssg_get_group_size(context->gid);
  context->gsize = gsize;
  context->kv_context = (kv_context_t**)malloc(gsize*sizeof(kv_context_t*));
  for (hg_size_t i=0; i<gsize; i++) {
    // register this client context with Margo
    context->kv_context[i] = kv_client_register(context->mid);
    assert(context->kv_context[i] != NULL);
    hg_addr_t server_addr = ssg_get_addr(context->gid, i);
    // turn server_addr into string
    ret = margo_addr_to_string(context->mid, addr_str, &addr_str_sz, server_addr);
    assert(ret == HG_SUCCESS);
    margo_addr_free(context->mid, server_addr);
    std::string server_dbname = basepath + separator + std::string("kvdb.") + std::to_string(i)
      + separator + name; // each session uses unique db name
    // open client connection with this server
    std::cout << "request open of " << server_dbname << " from server " << addr_str << std::endl;
    ret = kv_open(context->kv_context[i], addr_str, server_dbname.c_str());
    assert(ret == HG_SUCCESS);
  }

  // initialize consistent hash using "hash_lookup3" with gsize servers each with 1 virtual node for now
  context->ch_instance = ch_placement_initialize("hash_lookup3", gsize, 4, 0);

  return HG_SUCCESS;
}

// oid is unique associated with key
// in ParSplice key is already a uint64_t hashed value
hg_return_t kvgroup_put(kvgroup_context_t *context, uint64_t oid,
			void *key, hg_size_t ksize,
			void *value, hg_size_t vsize)
{

  // not using any replication for now (is this right?)
  ch_placement_find_closest(context->ch_instance, oid, 1, server_indexes);
  kv_context_t *kv_context = context->kv_context[server_indexes[0]];
  
  std::cout << "kvgroup_put: key=" << oid << ", server_index=" << server_indexes[0] << std::endl;
  return kv_put(kv_context, key, ksize, value, vsize);
}

// oid is unique associated with key
// in ParSplice key is already a uint64_t hashed value
// vsize is in/out
hg_return_t kvgroup_get(kvgroup_context_t *context, uint64_t oid,
			void *key, hg_size_t ksize,
			void *value, hg_size_t *vsize)
{
  // not using any replication for now (is this right?)
  ch_placement_find_closest(context->ch_instance, oid, 1, server_indexes);
  kv_context_t *kv_context = context->kv_context[server_indexes[0]];
  
  std::cout << "kvgroup_get: key=" << oid << ", server_index=" << server_indexes[0] << std::endl;
  return kv_get(kv_context, key, ksize, value, vsize);
}

hg_return_t kvgroup_close(kvgroup_context_t *context)
{
  hg_return_t ret;
  for (hg_size_t i=0; i<context->gsize; i++) {
    ret = kv_close(context->kv_context[i]);
    assert(ret == HG_SUCCESS);
  }
  return HG_SUCCESS;
}

hg_return_t kvgroup_client_deregister(kvgroup_context_t *context)
{
  hg_return_t ret;
  for (hg_size_t i=0; i<context->gsize; i++) {
    ret = kv_client_deregister(context->kv_context[i]);
    assert(ret == HG_SUCCESS);
  }
  ch_placement_finalize(context->ch_instance);
  ssg_group_detach(context->gid);
  ssg_finalize();
  margo_diag_dump(context->mid, "-", 0);
  //margo_finalize(context->mid); // workaround since we core dump here
  ssg_group_id_free(context->gid);
  free(context->kv_context);
  free(context);
  return HG_SUCCESS;
}

// only one client calls shutdown
hg_return_t kvgroup_client_signal_shutdown(kvgroup_context_t *context)
{
  hg_return_t ret;
  for (hg_size_t i=0; i<context->gsize; i++) {
    ret = kv_client_signal_shutdown(context->kv_context[i]);
    assert(ret == HG_SUCCESS);
  }
  return HG_SUCCESS;
}

// collective along with kvgroup_server_send_gid
// single server rank calls send, all client ranks call recv
// gid is an output argument
void kvgroup_client_recv_gid(ssg_group_id_t *gid, MPI_Comm comm)
{
  char *serialized_gid = NULL;
  size_t gid_size = 0;
  int rank;

  MPI_Comm_rank(comm, &rank);
  // recv size first
  MPI_Bcast(&gid_size, 1, MPI_UNSIGNED_LONG, 0, comm);
  std::cout << "kvgroup_client_recv_gid: comm rank " << rank << ", recv'd gid size " << gid_size << std::endl;
  assert(gid_size != 0);
  // then recv data
  serialized_gid = (char*)malloc(gid_size);
  MPI_Bcast(serialized_gid, gid_size, MPI_BYTE, 0, comm);
  ssg_group_id_deserialize(serialized_gid, gid_size, gid);
  free(serialized_gid);
}
