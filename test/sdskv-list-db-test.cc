/*
 * (C) 2015 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#include <vector>
#include <stdio.h>
#include <assert.h>
#include <unistd.h>
#include <margo.h>
#include <string>

#include "sdskv-client.h"

int main(int argc, char *argv[])
{
    char cli_addr_prefix[64] = {0};
    char *sdskv_svr_addr_str;
    char *db_name;
    margo_instance_id mid;
    hg_addr_t svr_addr;
    uint8_t mplex_id;
    sdskv_client_t kvcl;
    sdskv_provider_handle_t kvph;
    hg_return_t hret;
    int ret;

    if(argc != 3)
    {
        fprintf(stderr, "Usage: %s <sdskv_server_addr> <provider_id>\n", argv[0]);
        fprintf(stderr, "  Example: %s tcp://localhost:1234 1\n", argv[0]);
        return(-1);
    }
    sdskv_svr_addr_str = argv[1];
    mplex_id          = atoi(argv[2]);

    /* initialize Margo using the transport portion of the server
     * address (i.e., the part before the first : character if present)
     */
    for(unsigned i=0; (i<63 && sdskv_svr_addr_str[i] != '\0' && sdskv_svr_addr_str[i] != ':'); i++)
        cli_addr_prefix[i] = sdskv_svr_addr_str[i];

    /* start margo */
    mid = margo_init(cli_addr_prefix, MARGO_SERVER_MODE, 0, 0);
    if(mid == MARGO_INSTANCE_NULL)
    {
        fprintf(stderr, "Error: margo_init()\n");
        return(-1);
    }

    ret = sdskv_client_init(mid, &kvcl);
    if(ret != 0)
    {
        fprintf(stderr, "Error: sdskv_client_init()\n");
        margo_finalize(mid);
        return -1;
    }

    /* look up the SDSKV server address */
    hret = margo_addr_lookup(mid, sdskv_svr_addr_str, &svr_addr);
    if(hret != HG_SUCCESS)
    {
        fprintf(stderr, "Error: margo_addr_lookup()\n");
        sdskv_client_finalize(kvcl);
        margo_finalize(mid);
        return(-1);
    }

    /* create a SDSKV provider handle */
    ret = sdskv_provider_handle_create(kvcl, svr_addr, mplex_id, &kvph);
    if(ret != 0)
    {
        fprintf(stderr, "Error: sdskv_provider_handle_create()\n");
        sdskv_shutdown_service(kvcl, svr_addr);
        margo_addr_free(mid, svr_addr);
        sdskv_client_finalize(kvcl);
        margo_finalize(mid);
        return(-1);
    }

    /* count the databases */
    size_t num_db = 0;
    ret = sdskv_count_databases(kvph, &num_db);
    if(ret == 0) {
        printf("Successfuly counted %ld database(s)\n", num_db);
    } else {
        fprintf(stderr, "Error: could not count databases\n");
        sdskv_provider_handle_release(kvph);
        sdskv_shutdown_service(kvcl, svr_addr);
        margo_addr_free(mid, svr_addr);
        sdskv_client_finalize(kvcl);
        margo_finalize(mid);
        return(-1);
    }
    /* list databases */
    std::vector<sdskv_database_id_t> ids(num_db);
    std::vector<char*> db_names(num_db);
    ret = sdskv_list_databases(kvph, &num_db, db_names.data(), ids.data());
    if(ret == 0) {
        printf("Successfuly listed %ld database(s)\n", num_db);
        for(unsigned i=0; i < num_db; i++) {
            printf("Database %s => %ld\n", db_names[i], ids[i]);
            free(db_names[i]);
        }
    } else {
        fprintf(stderr, "Error: could not count databases\n");
        sdskv_provider_handle_release(kvph);
        sdskv_shutdown_service(kvcl, svr_addr);
        margo_addr_free(mid, svr_addr);
        sdskv_client_finalize(kvcl);
        margo_finalize(mid);
        return(-1);
    }

    /* shutdown the server */
    ret = sdskv_shutdown_service(kvcl, svr_addr);

    /**** cleanup ****/
    sdskv_provider_handle_release(kvph);
    margo_addr_free(mid, svr_addr);
    sdskv_client_finalize(kvcl);
    margo_finalize(mid);
    return(ret);
}

