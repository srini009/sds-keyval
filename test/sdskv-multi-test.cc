/*
 * (C) 2015 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#include <stdio.h>
#include <assert.h>
#include <unistd.h>
#include <margo.h>
#include <iostream>
#include <string>
#include <vector>
#include <algorithm>
#include <map>

#include "sdskv-client.h"

static std::string gen_random_string(size_t len);

int main(int argc, char *argv[])
{
    char cli_addr_prefix[64] = {0};
    char *sdskv_svr_addr_str;
    char *db_name;
    margo_instance_id mid;
    hg_addr_t svr_addr;
    uint8_t mplex_id;
    uint32_t num_keys;
    sdskv_client_t kvcl;
    sdskv_provider_handle_t kvph;
    hg_return_t hret;
    int ret;

    if(argc != 5)
    {
        fprintf(stderr, "Usage: %s <sdskv_server_addr> <mplex_id> <db_name> <num_keys>\n", argv[0]);
        fprintf(stderr, "  Example: %s tcp://localhost:1234 1 foo 1000\n", argv[0]);
        return(-1);
    }
    sdskv_svr_addr_str = argv[1];
    mplex_id          = atoi(argv[2]);
    db_name           = argv[3];
    num_keys          = atoi(argv[4]);

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
        margo_addr_free(mid, svr_addr);
        sdskv_client_finalize(kvcl);
        margo_finalize(mid);
        return(-1);
    }

    /* open the database */
    sdskv_database_id_t db_id;
    ret = sdskv_open(kvph, db_name, &db_id);
    if(ret == 0) {
        printf("Successfuly open database %s, id is %ld\n", db_name, db_id);
    } else {
        fprintf(stderr, "Error: could not open database %s\n", db_name);
        sdskv_provider_handle_release(kvph);
        margo_addr_free(mid, svr_addr);
        sdskv_client_finalize(kvcl);
        margo_finalize(mid);
        return(-1);
    }

    /* **** generate multiple key/vals ***** */
    std::vector<std::string> keys;
    std::vector<std::string> vals;
    std::map<std::string, std::string> reference;
    size_t max_value_size = 24;

    for(unsigned i=0; i < num_keys; i++) {
        auto k = gen_random_string(16);
        auto v = gen_random_string(3+i*(max_value_size-3)/num_keys);
        reference[k] = v;
        keys.push_back(k);
        vals.push_back(v);
    }

    std::vector<const void*> keys_ptr(keys.size());
    std::vector<const void*> vals_ptr(vals.size());
    std::vector<hg_size_t> keys_size(keys.size());
    std::vector<hg_size_t> vals_size(vals.size());
    for(unsigned i=0; i < num_keys; i++) {
        keys_ptr[i]  = (const void*)keys[i].data();
        vals_ptr[i]  = (const void*)vals[i].data();
        keys_size[i] = keys[i].size();
        vals_size[i] = vals[i].size();
    }

    /* **** issue a put_multi ***** */
    ret = sdskv_put_multi(kvph, db_id, num_keys, &keys_ptr[0], keys_size.data(),
            &vals_ptr[0], vals_size.data());
    if(ret != 0) {
        fprintf(stderr, "Error: sdskv_put_multi() failed\n");
        sdskv_shutdown_service(kvcl, svr_addr);
        sdskv_provider_handle_release(kvph);
        margo_addr_free(mid, svr_addr);
        sdskv_client_finalize(kvcl);
        margo_finalize(mid);
        return -1;
    }
    printf("Successfuly inserted %d keys\n", num_keys);

    /* check that the values exist */
    std::vector<int> keys_exist(num_keys);

    ret = sdskv_exists_multi(kvph, db_id, num_keys, 
            keys_ptr.data(), keys_size.data(), keys_exist.data());
    if(ret != 0) {
        fprintf(stderr, "Error: sdskv_length_multi() failed\n");
        sdskv_shutdown_service(kvcl, svr_addr);
        sdskv_provider_handle_release(kvph);
        margo_addr_free(mid, svr_addr);
        sdskv_client_finalize(kvcl);
        margo_finalize(mid);
        return -1;
    }
    for(auto& k : keys_exist) {
        if(k != 1) {
            fprintf(stderr, "Error: sdskv_exists_multi() failed (one flag is not 1)\n");
            sdskv_shutdown_service(kvcl, svr_addr);
            sdskv_provider_handle_release(kvph);
            margo_addr_free(mid, svr_addr);
            sdskv_client_finalize(kvcl);
            margo_finalize(mid);
            return -1;
        }
    }

    /* retrieve the length of the values */
    std::vector<hg_size_t> rval_len(num_keys);

    ret = sdskv_length_multi(kvph, db_id, num_keys, 
            keys_ptr.data(), keys_size.data(), rval_len.data());
    if(ret != 0) {
        fprintf(stderr, "Error: sdskv_length_multi() failed\n");
        sdskv_shutdown_service(kvcl, svr_addr);
        sdskv_provider_handle_release(kvph);
        margo_addr_free(mid, svr_addr);
        sdskv_client_finalize(kvcl);
        margo_finalize(mid);
        return -1;
    }

    /* check if the lengths are correct */
    for(unsigned i=0; i < num_keys; i++) {
        if(rval_len[i] != vals_size[i]) {
            fprintf(stderr, "Error: value %d doesn't have the right length (%ld != %ld)\n", i,
                    rval_len[i], vals_size[i]);
            sdskv_shutdown_service(kvcl, svr_addr);
            sdskv_provider_handle_release(kvph);
            margo_addr_free(mid, svr_addr);
            sdskv_client_finalize(kvcl);
            margo_finalize(mid);
            return -1;
        }
    }

    /* **** get keys **** */
    std::vector<std::vector<char>> read_values(num_keys);
    for(unsigned i=0; i < num_keys; i++) {
        read_values[i].resize(rval_len[i]);
    }
    std::vector<void*> read_values_ptr(num_keys);
    for(unsigned i=0; i < num_keys; i++) {
        read_values_ptr[i] = read_values[i].data();
    }

    ret = sdskv_get_multi(kvph, db_id, num_keys,
            keys_ptr.data(), keys_size.data(), 
            read_values_ptr.data(), rval_len.data());
    if(ret != 0) {
        fprintf(stderr, "Error: sdskv_get_multi() failed\n");
        sdskv_shutdown_service(kvcl, svr_addr);
        sdskv_provider_handle_release(kvph);
        margo_addr_free(mid, svr_addr);
        sdskv_client_finalize(kvcl);
        margo_finalize(mid);
        return -1;
    }

    /* check the keys we received against reference */
    for(unsigned i=0; i < num_keys; i++) {
        std::string vstring(read_values[i].data());
        vstring.resize(rval_len[i]);
        auto& k = keys[i];
        std::cout << "Got " << k << " ===> " << vstring << "\t(size = " << vstring.size() 
            << ") expected: " << reference[k] << " (size = " << reference[k].size() << ")"
            <<  std::endl;
        if(vstring != reference[k]) {
            fprintf(stderr, "Error: sdskv_get_multi() returned a value different from the reference\n");
            sdskv_shutdown_service(kvcl, svr_addr);
            sdskv_provider_handle_release(kvph);
            margo_addr_free(mid, svr_addr);
            sdskv_client_finalize(kvcl);
            margo_finalize(mid);
            return -1;
        }
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

static std::string gen_random_string(size_t len) {
    static const char alphanum[] =
                "0123456789"
                "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                "abcdefghijklmnopqrstuvwxyz";
    std::string s(len, ' ');
    for (unsigned i = 0; i < len; ++i) {
        s[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
    }
    return s;
}
