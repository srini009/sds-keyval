#include "sds-keyval-group.h"

#include <stdlib.h>
#include <time.h>
#include <assert.h>
#include <iostream>

/* a stub for now */
static void group_update_cb(ssg_membership_update_t update, void *cb_dat)
{
  int my_world_rank = *(int *)cb_dat;

  if (update.type == SSG_MEMBER_ADD)
    printf("%d SSG update: ADD member %lu\n", my_world_rank, update.member);
  else if (update.type == SSG_MEMBER_REMOVE)
    printf("%d SSG update: REMOVE member %lu\n", my_world_rank, update.member);

  return;
}

/* this is a collective operation */
extern "C" kv_group_t *kvgroup_server_register(margo_instance_id mid,
	const char *ssg_name, MPI_Comm ssg_comm)
{
  kv_group_t * group = (kv_group_t *)calloc(1, sizeof(kv_group_t));

  /* update kvgroup_context_t with MID */
  group->mid = mid;
  
  group->db = (kv_database_t **)malloc(sizeof(kv_database_t*)); // just 1
  group->kv_context = kv_server_register(mid);
  assert(group->kv_context != NULL);

  int sret = ssg_init(mid);
  assert(sret == SSG_SUCCESS);

  int my_world_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &my_world_rank);
  ssg_group_id_t gid = ssg_group_create_mpi(ssg_name, ssg_comm, &group_update_cb, &my_world_rank);
  assert(gid != SSG_GROUP_ID_NULL);

  /* update kvgroup_context_t with GID */
  group->gid = gid;

  return group;
}

extern "C" hg_return_t kvgroup_server_deregister(kv_group_t *group)
{
  hg_return_t ret = kv_server_deregister(group->kv_context);
  ssg_group_destroy(group->gid);
  free(group->kv_context);
  free(group);
  std::cout << "GROUP_SERVER: deregistered" << std::endl;
  return ret;
}

extern "C" hg_return_t kvgroup_server_wait_for_shutdown(kv_group_t *group)
{
  hg_return_t ret = kv_server_wait_for_shutdown(group->kv_context);
  return ret;
}

// collective along with kvgroup_client_recv_gid
// single server rank calls send, all client ranks call recv
// gid is an input argument
extern "C" void kvgroup_server_send_gid(ssg_group_id_t gid, MPI_Comm comm)
{
  char *serialized_gid = NULL;
  size_t gid_size = 0;
  int rank;

  MPI_Comm_rank(comm, &rank);
  ssg_group_id_serialize(gid, &serialized_gid, &gid_size);
  assert(serialized_gid != NULL && gid_size != 0);
  std::cout << "kvgroup_server_send_gid: comm rank " << rank << ", sending gid size " << gid_size << std::endl;
  // send size first
  MPI_Bcast(&gid_size, 1, MPI_UNSIGNED_LONG, 0, comm);
  // then send data
  MPI_Bcast(serialized_gid, gid_size, MPI_BYTE, 0, comm);
  free(serialized_gid);
}



