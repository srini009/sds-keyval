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
#include <stdlib.h>
#include <time.h>

#define DIE_IF(cond_expr, err_fmt, ...) \
    do { \
        if (cond_expr) { \
            fprintf(stderr, "ERROR at %s:%d (" #cond_expr "): " \
                    err_fmt "\n", __FILE__, __LINE__, ##__VA_ARGS__); \
            exit(1); \
        } \
    } while(0)

int main(int argc, char *argv[])
{
    int rank;

    assert(argc == 2);
    char *addr_str = argv[1];
    
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    MPI_Comm clientComm;

    if (rank == 0) {
      hg_size_t addr_str_sz = 128;
      char server_addr_str[addr_str_sz];
      hg_addr_t server_addr;
      hg_return_t hret;
      
      MPI_Comm_split(MPI_COMM_WORLD, MPI_UNDEFINED, rank, &clientComm);
      
      // kv-server
      margo_instance_id mid = margo_init(addr_str, MARGO_SERVER_MODE, 0, -1);
      kv_context_t *context = kv_server_register(mid);

      hret = margo_addr_self(mid, &server_addr);
      DIE_IF(hret != HG_SUCCESS, "margo_addr_self");

      // get server address
      hret = margo_addr_to_string(mid, server_addr_str, &addr_str_sz, server_addr);
      DIE_IF(hret != HG_SUCCESS, "margo_addr_to_string");
      margo_addr_free(mid, server_addr);
      
      // broadcast (send) server address to all clients
      printf("server (rank %d): server addr_str: %s\n", rank, server_addr_str);
      MPI_Bcast(server_addr_str, 128, MPI_BYTE, 0, MPI_COMM_WORLD);

      // process requests until finalized
      kv_server_wait_for_shutdown(context);

      // now finish cleaning up
      kv_server_deregister(context);
      printf("rank %d: server deregistered\n", rank);

      margo_finalize(mid);
    }
    else {
      hg_size_t addr_str_sz = 128;
      char server_addr_str[addr_str_sz];
      char client_addr_str[addr_str_sz];
      hg_addr_t client_addr;
      hg_return_t hret;
      kv_database_t * kv_db;
      
      MPI_Comm_split(MPI_COMM_WORLD, 1, rank, &clientComm);
      
      // broadcast (recv) server address
      MPI_Bcast(server_addr_str, 128, MPI_BYTE, 0, MPI_COMM_WORLD);
      printf("client (rank %d): server addr_str: %s\n", rank, server_addr_str);

      // kv-client
      char *proto = kv_protocol(server_addr_str);
      margo_instance_id mid = margo_init(proto, MARGO_CLIENT_MODE, 0, -1);
      free(proto);
      kv_context_t *context = kv_client_register(mid);

      hret = margo_addr_self(mid, &client_addr);
      DIE_IF(hret != HG_SUCCESS, "margo_addr_self");

      // get client address
      hret = margo_addr_to_string(mid, client_addr_str, &addr_str_sz, client_addr);
      DIE_IF(hret != HG_SUCCESS, "margo_addr_to_string");
      margo_addr_free(mid, client_addr);
      printf("client (rank %d): client addr_str: %s\n", rank, client_addr_str);
      
      // open specified "DB" (pass in the server's address)
      const char *db = "db/minima_store";
      kv_db_type_t db_type = KVDB_BWTREE;
      kv_db = kv_open(context, server_addr_str, db, db_type);
      DIE_IF(kv_db == NULL, "kv_open");
      
      size_t vsize = 1;
      if (argc == 3) {
	vsize = atoi(argv[2]);
      }
      printf("client (rank %d): using vsize = %lu, dsize = %lu\n", rank, vsize, vsize*sizeof(int32_t));

      // put
      for (int i=1; i<1000; i++) {
	int32_t key = 1000*rank + i;
	std::vector<int32_t> put_data;
	put_data.resize(vsize, key);
	hg_size_t data_size = put_data.size()*sizeof(int32_t); // size in char (bytes)

	hret = kv_put(kv_db, (void*)&key, sizeof(key),
		      (void*)put_data.data(), data_size);
	printf("(rank %d: put) key %d, size=%lu\n", rank, key, data_size);
	DIE_IF(hret != HG_SUCCESS, "kv_put");
      }

      sleep(2);

      // get
      for (int i=1; i<1000; i++) {
	int32_t key = 1000*rank + i;
	std::vector<int32_t> expected_get_data;
	expected_get_data.resize(vsize, key);
	std::vector<int32_t> get_data;
	get_data.resize(vsize);
	hg_size_t data_size = get_data.size()*sizeof(int32_t); // size in char (bytes)
	printf("(rank %d: get) key %d, size=%lu\n", rank, key, data_size);
	hret = kv_get(kv_db, (void*)&key, sizeof(key),
		      (void*)get_data.data(), &data_size);
	DIE_IF(hret != HG_SUCCESS, "kv_get");

	get_data.resize(data_size/sizeof(int32_t)); // size in int32_t
	if (expected_get_data == get_data) {
	  printf("(rank %d: put/get succeeded) key %d, actual size=%lu\n", rank, key, data_size);
	}
	else {
	  printf("(rank %d: put/get failed) key %d, actual size=%lu\n", rank, key, data_size);
	}
      }

      // close
      hret = kv_close(kv_db);
      DIE_IF(hret != HG_SUCCESS, "kv_close");

      // once all clients are done with the close, one client can signal server
      MPI_Barrier(clientComm);
      if (rank==1) {
	printf("rank %d: sending server a shutdown request\n", rank);
	kv_client_signal_shutdown(kv_db);
      }

      // now finish cleaning up
      kv_client_deregister(context);
      printf("rank %d: client deregistered\n", rank);

      margo_finalize(mid);
    }

    MPI_Finalize();

    printf("rank %d: finalized\n", rank);
    return 0;
}
