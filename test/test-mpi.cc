/*
 * Copyright (c) 2017, Los Alamos National Security, LLC.
 * All rights reserved.
 *
 */

#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <mpi.h>

#include <margo.h>
#include <mercury.h>
#include <abt.h>

#include "sds-keyval.h"

#include <vector>

#define DIE_IF(cond_expr, err_fmt, ...) \
    do { \
        if (cond_expr) { \
            fprintf(stderr, "ERROR at %s:%d (" #cond_expr "): " \
                    err_fmt "\n", __FILE__, __LINE__, ##__VA_ARGS__); \
            exit(1); \
        } \
    } while(0)

static void usage()
{
  fprintf(stderr, "Usage: test-mpi <addr>\n");
}

int main(int argc, char *argv[])
{
    hg_class_t *hgcl = NULL;
    hg_context_t *hgctx = NULL;
    int sleep_time = 0;
    char client_addr_str[128];
    char server_addr_str[128];
    hg_size_t server_addr_str_sz = 128;
    hg_handle_t handle = HG_HANDLE_NULL;
    hg_return_t hret;
    hg_addr_t server_addr;
    int ret;
    int rank;

    MPI_Init(&argc, &argv);

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    MPI_Comm clientComm;

    if (rank == 0) {
      MPI_Comm_split(MPI_COMM_WORLD, MPI_UNDEFINED, rank, &clientComm);
      // kv-server
      kv_context *context = kv_server_register(argv[1]);
      hgctx = margo_get_context(context->mid);
      hgcl = HG_Context_get_class(hgctx);

      // get server address
      hret = HG_Addr_self(hgcl, &server_addr);
      DIE_IF(hret != HG_SUCCESS, "HG_Addr_self");
      hret = HG_Addr_to_string(hgcl, server_addr_str, &server_addr_str_sz, server_addr);
      DIE_IF(hret != HG_SUCCESS, "HG_Addr_to_string");
      HG_Addr_free(hgcl, server_addr);
      printf("server (rank %d): server addr_str: %s\n", rank, server_addr_str);

      // broadcast (send) server address to all clients
      MPI_Bcast(server_addr_str, 128, MPI_BYTE, 0, MPI_COMM_WORLD);

      // process requests until finalized
      kv_server_wait_for_shutdown(context);

      // now finish cleaning up
      kv_server_deregister(context);
      printf("rank %d: server deregistered\n", rank);
    }
    else {
      MPI_Comm_split(MPI_COMM_WORLD, 1, rank, &clientComm);
      // broadcast (recv) server address
      MPI_Bcast(server_addr_str, 128, MPI_BYTE, 0, MPI_COMM_WORLD);
      printf("client (rank %d): server addr_str: %s\n", rank, server_addr_str);

      // kv-client
      sprintf(client_addr_str, "cci+tcp://localhost:534%02d", rank);
      printf("client (rank %d): client addr_str: %s\n", rank, client_addr_str);
      kv_context *context = kv_client_register(client_addr_str);
      
      // open specified "DB" (pass in the server's address)
      const char *db = "kv-test-db";
      ret = kv_open(context, server_addr_str, (char*)db, KV_UINT, KV_BULK);
      
      // put
      uint64_t key = rank;
      int put_val = rank;
      std::vector<char> put_data;
      put_data.resize(sizeof(put_val));
      memcpy(put_data.data(), &put_val, sizeof(put_val));
      ret = kv_bulk_put(context, (void*)&key, (void*)put_data.data(), put_data.size());

      sleep(2);

      // get
      int get_val;
      std::vector<char> get_data;
      get_data.resize(sizeof(get_val));
      ret = kv_bulk_get(context, (void*)&key, (void*)get_data.data(), get_data.size());
      memcpy(&get_val, get_data.data(), sizeof(get_val));
      printf("key: %lu in: %d out: %d\n", key, put_val, get_val);

      // close
      ret = kv_close(context);

      // once all clients are done, one client can signal server
      MPI_Barrier(clientComm);
      if (rank==1) {
	printf("rank %d: sending server a shutdown request\n", rank);
	kv_client_shutdown_server(context);
      }

      // now finish cleaning up
      kv_client_deregister(context);
      printf("rank %d: client deregistered\n", rank);
    }

    MPI_Finalize();

    printf("rank %d: finalized\n", rank);
    return 0;
}
