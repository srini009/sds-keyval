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
    printf("%d SSG update: ADD member %"PRIu64"\n", my_world_rank, update.member);
  else if (update.type == SSG_MEMBER_REMOVE)
    printf("%d SSG update: REMOVE member %"PRIu64"\n", my_world_rank, update.member);

  return;
}

/* this is a collective operation */
kvgroup_context_t *kvgroup_server_register(margo_instance_id mid, const char *ssg_name, MPI_comm ssg_comm)
{
  kvgroup_context_t context = (kvgroup_context_t*)malloc(sizeof(kvgroup_context_t));
  memset(context, 0, sizeof(kvgroup_context_t));

  /* update kvgroup_context_t with MID */
  context->mid = mid;
  
  context->kv_context = kv_server_register(mid);
  assert(context->kv_context != NULL);

  int sret = ssg_init(mid);
  assert(sret == SSG_SUCCESS);

  int my_world_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &my_world_rank);
  ssg_group_id_t gid = ssg_group_create_mpi(ssg_name, ssg_comm, &group_update_cb, &my_world_rank);
  assert(gid != SSG_GROUP_ID_NULL);

  /* update kvgroup_context_t with GID */
  context->gid = gid;

  return context;
}

hg_return_t kvgroup_server_deregister(kvgroup_context_t *context)
{
  hg_return_t ret = kv_server_deregister(context->kv_context);
  ssg_group_destroy(context->gid);
  free(context);
  std::cout << "GROUP_SERVER: deregistered" << std::endl;
  return ret;
}

hg_return_t kvgroup_server_wait_for_shutdown(kvgroup_context_t *context)
{
  hg_return_t ret = kv_server_wait_for_shutdown(context->kv_context);
  return ret;
}


