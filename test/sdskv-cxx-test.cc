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

#include "sdskv-client.hpp"

static std::string gen_random_string(size_t len);

static int put_get_erase_test(sdskv::database& DB, uint32_t num_keys);

int main(int argc, char *argv[])
{
    char cli_addr_prefix[64] = {0};
    std::string sdskv_svr_addr;
    std::string db_name;
    margo_instance_id mid;
    hg_addr_t svr_addr;
    uint16_t provider_id;
    uint32_t num_keys;

    hg_return_t hret;

    if(argc != 5)
    {
        fprintf(stderr, "Usage: %s <sdskv_server_addr> <mplex_id> <db_name> <num_keys>\n", argv[0]);
        fprintf(stderr, "  Example: %s tcp://localhost:1234 1 foo 1000\n", argv[0]);
        return(-1);
    }
    sdskv_svr_addr = argv[1];
    provider_id    = atoi(argv[2]);
    db_name        = argv[3];
    num_keys       = atoi(argv[4]);

    /* initialize Margo using the transport portion of the server
     * address (i.e., the part before the first : character if present)
     */
    for(unsigned i=0; (i<63 && sdskv_svr_addr[i] != '\0' && sdskv_svr_addr[i] != ':'); i++)
        cli_addr_prefix[i] = sdskv_svr_addr[i];

    /* start margo */
    mid = margo_init(cli_addr_prefix, MARGO_SERVER_MODE, 0, 0);
    if(mid == MARGO_INSTANCE_NULL)
        throw std::runtime_error("margo_init failed");

    {

        sdskv::client kvcl(mid);
        sdskv::provider_handle kvph;

        /* look up the SDSKV server address */
        hret = margo_addr_lookup(mid, sdskv_svr_addr.c_str(), &svr_addr);
        if(hret != HG_SUCCESS)
            throw std::runtime_error("margo_addr_lookup failed");

        /* create a SDSKV provider handle */
        kvph = sdskv::provider_handle(kvcl, svr_addr, provider_id);

        /* open the database */
        sdskv::database DB = kvcl.open(kvph, db_name);

        /* Put get erase test */
        put_get_erase_test(DB, num_keys);

        /* shutdown the server */
        kvcl.shutdown(svr_addr);

        /**** cleanup ****/
        margo_addr_free(mid, svr_addr);

    }
    
    margo_finalize(mid);

    return 0;
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

static int put_get_erase_test(sdskv::database& DB, uint32_t num_keys) {

    /* **** put keys ***** */
    std::vector<std::string> keys;
    std::map<std::string, std::string> reference;
    size_t max_value_size = 24;

    for(unsigned i=0; i < num_keys; i++) {
        auto k = gen_random_string(16);
        // half of the entries will be put using bulk
        auto v = gen_random_string(3+i*(max_value_size-3)/num_keys);
        DB.put(k, v);
        std::cout << "Inserted " << k << "\t ===> " << v << std::endl;
        reference[k] = v;
        keys.push_back(k);
    }
    std::cout << "Successfuly inserted " << num_keys << " keys" << std::endl;

    /* **** get keys **** */
    for(unsigned i=0; i < num_keys; i++) {
        auto k = keys[rand() % keys.size()];
        size_t value_size = max_value_size;
        std::vector<char> v(max_value_size);
        DB.get(k,v);
        std::string vstring((char*)(v.data()));
        std::cout << "Got " << k << " ===> " << vstring << std::endl;
        if(vstring != reference[k]) {
            throw std::runtime_error("DB.get() returned a value different from the reference");
        }
    }

    /* erase keys */
    for(unsigned i=0; i < num_keys; i++) {
        DB.erase(keys[i]);
    }

    return 0;
}

