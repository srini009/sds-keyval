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

typedef struct kvgroup_context_s {
  kv_context_t *kv_context;
  margo_instance_id mid;
  ssg_group_id_t gid; // SSG ID
  hg_size_t gsize; // size of SSG
  struct ch_placement_instance *ch_instance;
} kvgroup_context_t;

static char *kvgroup_protocol(ssg_group_id_t gid) {
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
  for(int i=0; i<proto_size && addr_str[i] != '\0' && addr_str[i] != ':'; i++)
    protocol[i] = addr_str[i];

  return protocol;
}

kvgroup_context_t *kvgroup_server_register(margo_instance_id mid,
					   const char *ssg_name, MPI_comm ssg_comm);
hg_return_t kvgroup_server_deregister(kvgroup_context_t *context);
hg_return_t kvgroup_server_wait_for_shutdown(kvgroup_context_t *context);

kvgroup_context_t *kvgroup_client_register(margo_instance_id mid, ssg_group_id_t gid);
hg_return_t kvgroup_open(kvgroup_context_t *context, const char *db_name);
hg_return_t kvgroup_put(kvgroup_context_t *context, uint64_t oid,
			void *key, hg_size_t ksize,
			void *value, hg_size_t vsize);
hg_return_t kvgroup_get(kvgroup_context_t *context, uint64_t oid,
			void *key, hg_size_t ksize,
			void *value, hg_size_t *vsize);
hg_return_t kvgroup_close(kvgroup_context_t *context);
hg_return_t kvgroup_client_deregister(kvgroup_context_t *context);
hg_return_t kvgroup_client_signal_shutdown(kvgroup_context_t *context);
  
#if defined(__cplusplus)
}
#endif

#endif // sds_keyval_group_h

