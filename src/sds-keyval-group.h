#include <assert.h>
#include "sds-keyval.h"

/* initial implementation requires MPI */
#include <mpi.h>
#include <ssg.h>
#include <ssg-mpi.h>

#include "ch-placement.h"

#ifndef sds_keyval_group_h
#define sds_keyval_group_h

#if defined(__cplusplus)
extern "C" {
#endif

typedef struct kv_group_s {
  kv_context_t *kv_context;
  kv_database_t **db;
  margo_instance_id mid;
  ssg_group_id_t gid; // SSG ID
  hg_size_t gsize; // size of SSG
  struct ch_placement_instance *ch_instance;
} kv_group_t;

// helper routine for stripping protocol part of address string
// stored in ssg_group_id_t data structure
// clients can use to dynamically match server's protocol
// caller is responsible for freeing up char buffer
static inline char *kvgroup_protocol(ssg_group_id_t gid) {
  char *addr_str;
  int psize = 24;
  
  char *protocol = (char*)malloc(psize);
  memset(protocol, 0, psize);

  /* figure out protocol to connect with using address information 
   * associated with the SSG group ID
   */
  addr_str = ssg_group_id_get_addr_str(gid);
  assert(addr_str != NULL);

  /* we only need to the proto portion of the address to initialize */
  for(int i=0; i<psize && addr_str[i] != '\0' && addr_str[i] != ':'; i++)
    protocol[i] = addr_str[i];

  return protocol;
}

kv_group_t *kvgroup_server_register(margo_instance_id mid,
					   const char *ssg_name, MPI_Comm ssg_comm);
hg_return_t kvgroup_server_deregister(kv_group_t *group);
hg_return_t kvgroup_server_wait_for_shutdown(kv_group_t *group);

kv_group_t  *kvgroup_client_register(margo_instance_id mid, ssg_group_id_t gid);
hg_return_t kvgroup_open(kv_group_t  *group, const char *db_name);
hg_return_t kvgroup_put(kv_group_t *group, uint64_t oid,
			void *key, hg_size_t ksize,
			void *value, hg_size_t vsize);
hg_return_t kvgroup_get(kv_group_t *group, uint64_t oid,
			void *key, hg_size_t ksize,
			void *value, hg_size_t *vsize);
hg_return_t kvgroup_close(kv_group_t *group);
hg_return_t kvgroup_client_deregister(kv_group_t *group);
hg_return_t kvgroup_client_signal_shutdown(kv_group_t *group);

void kvgroup_server_send_gid(ssg_group_id_t gid, MPI_Comm comm);
void kvgroup_client_recv_gid(ssg_group_id_t *gid, MPI_Comm comm);
  
#if defined(__cplusplus)
}
#endif

#endif // sds_keyval_group_h

