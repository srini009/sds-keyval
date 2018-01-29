/*
 * (C) 2015 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#include <map>
#include <iostream>
#include <unordered_map>
#define SDSKV
#include "datastore/datastore_factory.h"
#include "sdskv-rpc-types.h"
#include "sdskv-server.h"

struct sdskv_server_context_t
{
    std::unordered_map<sdskv_database_id_t, AbstractDataStore*> databases;
    std::map<std::string, sdskv_database_id_t> name2id;
    std::map<sdskv_database_id_t, std::string> id2name;
};

DECLARE_MARGO_RPC_HANDLER(sdskv_put_ult)
DECLARE_MARGO_RPC_HANDLER(sdskv_length_ult)
DECLARE_MARGO_RPC_HANDLER(sdskv_get_ult)
DECLARE_MARGO_RPC_HANDLER(sdskv_open_ult)
DECLARE_MARGO_RPC_HANDLER(sdskv_bulk_put_ult)
DECLARE_MARGO_RPC_HANDLER(sdskv_bulk_get_ult)
DECLARE_MARGO_RPC_HANDLER(sdskv_list_ult)

static void sdskv_server_finalize_cb(void *data);

extern "C" int sdskv_provider_register(
        margo_instance_id mid,
        uint8_t mplex_id,
        ABT_pool abt_pool,
        sdskv_provider_t* provider)
{
    sdskv_server_context_t *tmp_svr_ctx;

    /* check if a provider with the same multiplex id already exists */
    {
        hg_id_t id;
        hg_bool_t flag;
        margo_registered_name_mplex(mid, "sdskv_put_rpc", mplex_id, &id, &flag);
        if(flag == HG_TRUE) {
            fprintf(stderr, "sdskv_provider_register(): a provider with the same mplex id (%d) already exists\n", mplex_id);
            return -1;
        }
    }

    /* allocate the resulting structure */    
    tmp_svr_ctx = new sdskv_server_context_t;
    if(!tmp_svr_ctx)
        return(-1);

    /* register RPCs */
    hg_id_t rpc_id;
    rpc_id = MARGO_REGISTER_MPLEX(mid, "sdskv_put_rpc",
            put_in_t, put_out_t,
            sdskv_put_ult, mplex_id, abt_pool);
    margo_register_data_mplex(mid, rpc_id, mplex_id, (void*)tmp_svr_ctx, NULL);
    rpc_id = MARGO_REGISTER_MPLEX(mid, "sdskv_bulk_put_rpc",
            bulk_put_in_t, bulk_put_out_t,
            sdskv_bulk_put_ult, mplex_id, abt_pool);
    margo_register_data_mplex(mid, rpc_id, mplex_id, (void*)tmp_svr_ctx, NULL);
    rpc_id = MARGO_REGISTER_MPLEX(mid, "sdskv_get_rpc",
            get_in_t, get_out_t,
            sdskv_get_ult, mplex_id, abt_pool);
    margo_register_data_mplex(mid, rpc_id, mplex_id, (void*)tmp_svr_ctx, NULL);
    rpc_id = MARGO_REGISTER_MPLEX(mid, "sdskv_length_rpc",
            length_in_t, length_out_t,
            sdskv_length_ult, mplex_id, abt_pool);
    margo_register_data_mplex(mid, rpc_id, mplex_id, (void*)tmp_svr_ctx, NULL);
    rpc_id = MARGO_REGISTER_MPLEX(mid, "sdskv_bulk_get_rpc",
            bulk_get_in_t, bulk_get_out_t,
            sdskv_bulk_get_ult, mplex_id, abt_pool);
    margo_register_data_mplex(mid, rpc_id, mplex_id, (void*)tmp_svr_ctx, NULL);
    rpc_id = MARGO_REGISTER_MPLEX(mid, "sdskv_open_rpc",
            open_in_t, open_out_t,
            sdskv_open_ult, mplex_id, abt_pool);
    margo_register_data_mplex(mid, rpc_id, mplex_id, (void*)tmp_svr_ctx, NULL);
    rpc_id = MARGO_REGISTER_MPLEX(mid, "sdskv_list_rpc",
            list_in_t, list_out_t,
            sdskv_list_ult, mplex_id, abt_pool);
    margo_register_data_mplex(mid, rpc_id, mplex_id, (void*)tmp_svr_ctx, NULL);

    /* install the bake server finalize callback */
    margo_push_finalize_callback(mid, &sdskv_server_finalize_cb, tmp_svr_ctx);

    if(provider != SDSKV_PROVIDER_IGNORE)
        *provider = tmp_svr_ctx;

    return(0);
}

extern "C" int sdskv_provider_add_database(
        sdskv_provider_t provider,
        const char *db_name,
        sdskv_db_type_t db_type,
        sdskv_database_id_t* db_id)
{
    auto db = datastore_factory::create_datastore(db_type, std::string(db_name));
    if(db == nullptr) return -1;
    sdskv_database_id_t id = (sdskv_database_id_t)(db);

    provider->name2id[std::string(db_name)] = id;
    provider->id2name[id] = std::string(db_name);
    provider->databases[id] = db;

    return 0;
}

extern "C" int sdskv_provider_remove_database(
        sdskv_provider_t provider,
        sdskv_database_id_t db_id)
{
    if(provider->databases.count(db_id)) {
        auto dbname = provider->id2name[db_id];
        provider->id2name.erase(db_id);
        provider->name2id.erase(dbname);
        auto db = provider->databases[db_id];
        delete db;
        provider->databases.erase(db_id);
        return 0;
    } else {
        return -1;
    }
}

extern "C" int sdskv_provider_remove_all_databases(
        sdskv_provider_t provider)
{
    for(auto db : provider->databases) {
        delete db.second;
    }
    provider->databases.clear();
    provider->name2id.clear();
    provider->id2name.clear();

    return 0;
}

extern "C" int sdskv_provider_count_databases(
        sdskv_provider_t provider,
        uint64_t* num_db)
{
    *num_db = provider->databases.size();
    return 0;
}

extern "C" int sdskv_provider_list_databases(
        sdskv_provider_t provider,
        sdskv_database_id_t* targets)
{
    unsigned i = 0;
    for(auto p : provider->name2id) {
        targets[i] = p.second;
        i++;
    }
    return 0;
}

static void sdskv_put_ult(hg_handle_t handle)
{
    hg_return_t hret;
    put_in_t in;
    put_out_t out;

    margo_instance_id mid = margo_hg_handle_get_instance(handle);
    assert(mid);
    const struct hg_info* info = margo_get_info(handle);
    sdskv_provider_t svr_ctx = 
        (sdskv_provider_t)margo_registered_data_mplex(mid, info->id, info->target_id);
    if(!svr_ctx) {
        fprintf(stderr, "Error: SDSKV could not find provider\n"); 
        out.ret = -1;
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }
    
    hret = margo_get_input(handle, &in);
    if(hret != HG_SUCCESS) {
        out.ret = -1;
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    auto it = svr_ctx->databases.find(in.db_id);
    if(it == svr_ctx->databases.end()) {
        out.ret = -1;
        margo_respond(handle, &out);
        margo_free_input(handle, &in);
        margo_destroy(handle);
        return;
    }

    ds_bulk_t kdata(in.key, in.key+in.ksize);
    ds_bulk_t vdata(in.value, in.value+in.vsize);

    if(it->second->put(kdata, vdata)) {
        out.ret = 0;
    } else {
        out.ret = -1;
    }

    margo_respond(handle, &out);
    margo_free_input(handle, &in);
    margo_destroy(handle);
}
DEFINE_MARGO_RPC_HANDLER(sdskv_put_ult)

static void sdskv_length_ult(hg_handle_t handle)
{

    hg_return_t hret;
    length_in_t in;
    length_out_t out;

    margo_instance_id mid = margo_hg_handle_get_instance(handle);
    assert(mid);
    const struct hg_info* info = margo_get_info(handle);
    sdskv_provider_t svr_ctx = 
        (sdskv_provider_t)margo_registered_data_mplex(mid, info->id, info->target_id);
    if(!svr_ctx) {
        fprintf(stderr, "Error: SDSKV could not find provider\n"); 
        out.ret = -1;
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    hret = margo_get_input(handle, &in);
    if(hret != HG_SUCCESS) {
        out.ret = -1;
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    auto it = svr_ctx->databases.find(in.db_id);
    if(it == svr_ctx->databases.end()) {
        out.ret   = -1;
        margo_respond(handle, &out);
        margo_free_input(handle, &in);
        margo_destroy(handle);
        return;
    }
    
    ds_bulk_t kdata(in.key, in.key+in.ksize);

    ds_bulk_t vdata;
    if(it->second->get(kdata, vdata)) {
        out.size = vdata.size();
        out.ret  = 0; 
    } else {
        out.size = 0;
        out.ret  = -1;
    }

    margo_respond(handle, &out);
    margo_free_input(handle, &in);
    margo_destroy(handle); 

    return;
}
DEFINE_MARGO_RPC_HANDLER(sdskv_length_ult)

static void sdskv_get_ult(hg_handle_t handle)
{

    hg_return_t hret;
    get_in_t in;
    get_out_t out;

    margo_instance_id mid = margo_hg_handle_get_instance(handle);
    assert(mid);
    const struct hg_info* info = margo_get_info(handle);
    sdskv_provider_t svr_ctx = 
        (sdskv_provider_t)margo_registered_data_mplex(mid, info->id, info->target_id);
    if(!svr_ctx) {
        fprintf(stderr, "Error: SDSKV could not find provider\n"); 
        out.ret = -1;
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    hret = margo_get_input(handle, &in);
    if(hret != HG_SUCCESS) {
        out.ret = -1;
        out.value = nullptr;
        out.vsize = 0;
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    auto it = svr_ctx->databases.find(in.db_id);
    if(it == svr_ctx->databases.end()) {
        out.ret = -1;
        out.value = nullptr;
        out.vsize = 0;
        margo_respond(handle, &out);
        margo_free_input(handle, &in);
        margo_destroy(handle);
        return;
    }
    
    ds_bulk_t kdata(in.key, in.key+in.ksize);

    ds_bulk_t vdata;
    if(it->second->get(kdata, vdata)) {
        if(vdata.size() <= in.vsize) {
            out.vsize = vdata.size();
            out.value = vdata.data();
            out.ret   = 0;
        } else {
            out.vsize = 0;
            out.value = nullptr;
            out.ret   = -1;
        }
    } else {
        out.vsize = 0;
        out.value = nullptr;
        out.ret   = -1;
    }

    margo_respond(handle, &out);
    margo_free_input(handle, &in);
    margo_destroy(handle); 

    return;
}
DEFINE_MARGO_RPC_HANDLER(sdskv_get_ult)

static void sdskv_open_ult(hg_handle_t handle)
{

    hg_return_t hret;
    open_in_t in;
    open_out_t out;

    margo_instance_id mid = margo_hg_handle_get_instance(handle);
    assert(mid);
    const struct hg_info* info = margo_get_info(handle);
    sdskv_provider_t svr_ctx = 
        (sdskv_provider_t)margo_registered_data_mplex(mid, info->id, info->target_id);
    if(!svr_ctx) {
        fprintf(stderr, "Error: SDSKV could not find provider\n"); 
        out.ret = -1;
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    hret = margo_get_input(handle, &in);
    if(hret != HG_SUCCESS) {
        out.ret = -1;
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    auto it = svr_ctx->name2id.find(std::string(in.name));
    if(it == svr_ctx->name2id.end()) {
        out.ret   = -1;
        margo_respond(handle, &out);
        margo_free_input(handle, &in);
        margo_destroy(handle);
        return;
    }

    out.db_id = it->second;
    out.ret  = 0;

    margo_respond(handle, &out);
    margo_free_input(handle, &in);
    margo_destroy(handle);
    return;
}
DEFINE_MARGO_RPC_HANDLER(sdskv_open_ult)

static void sdskv_bulk_put_ult(hg_handle_t handle)
{

    hg_return_t hret;
    bulk_put_in_t in;
    bulk_put_out_t out;
    hg_bulk_t bulk_handle;

    margo_instance_id mid = margo_hg_handle_get_instance(handle);
    assert(mid);
    const struct hg_info* info = margo_get_info(handle);
    sdskv_provider_t svr_ctx = 
        (sdskv_provider_t)margo_registered_data_mplex(mid, info->id, info->target_id);
    if(!svr_ctx) {
        fprintf(stderr, "Error: SDSKV could not find provider\n"); 
        out.ret = -1;
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    hret = margo_get_input(handle, &in);
    if(hret != HG_SUCCESS) {
        out.ret = -1;
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    auto it = svr_ctx->databases.find(in.bulk.db_id);
    if(it == svr_ctx->databases.end()) {
        out.ret = -1;
        margo_respond(handle, &out);
        margo_free_input(handle, &in);
        margo_destroy(handle);
        return;
    }
   
    ds_bulk_t vdata(in.bulk.vsize);
    void *buffer = (void*)vdata.data();
    hg_size_t size = vdata.size();
    hret = margo_bulk_create(mid, 1, (void**)&buffer, &size,
            HG_BULK_WRITE_ONLY, &bulk_handle);
    if(hret != HG_SUCCESS) {
        out.ret = -1;
        margo_respond(handle, &out);
        margo_free_input(handle, &in);
        margo_destroy(handle);
        return;
    }

    hret = margo_bulk_transfer(mid, HG_BULK_PULL, info->addr, in.bulk.handle, 0,
            bulk_handle, 0, vdata.size());
    if(hret != HG_SUCCESS) {
        out.ret = -1;
        margo_respond(handle, &out);
        margo_free_input(handle, &in);
        margo_bulk_free(bulk_handle);
        margo_destroy(handle);
        return;
    }

    ds_bulk_t kdata(in.bulk.key, in.bulk.key+in.bulk.ksize);

    auto b = it->second->put(kdata, vdata);

    if(b) {
        out.ret = 0;
    } else {
        out.ret = -1;
    }

    margo_respond(handle, &out);
    margo_free_input(handle, &in);
    margo_bulk_free(bulk_handle);
    margo_destroy(handle);

    return;
}
DEFINE_MARGO_RPC_HANDLER(sdskv_bulk_put_ult)

static void sdskv_bulk_get_ult(hg_handle_t handle)
{

    hg_return_t hret;
    bulk_get_in_t in;
    bulk_get_out_t out;
    hg_bulk_t bulk_handle;

    margo_instance_id mid = margo_hg_handle_get_instance(handle);
    assert(mid);
    const struct hg_info* info = margo_get_info(handle);
    sdskv_provider_t svr_ctx = 
        (sdskv_provider_t)margo_registered_data_mplex(mid, info->id, info->target_id);
    if(!svr_ctx) {
        fprintf(stderr, "Error: SDSKV could not find provider\n"); 
        out.ret = -1;
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    hret = margo_get_input(handle, &in);
    if(hret != HG_SUCCESS) {
        out.ret = -1;
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    auto it = svr_ctx->databases.find(in.bulk.db_id);
    if(it == svr_ctx->databases.end()) {
        out.ret = -1;
        margo_respond(handle, &out);
        margo_free_input(handle, &in);
        margo_destroy(handle);
        return;
    }
    
    ds_bulk_t kdata(in.bulk.key, in.bulk.key+in.bulk.ksize);

    ds_bulk_t vdata;
    auto b = it->second->get(kdata, vdata);

    if(!b || vdata.size() > in.bulk.vsize) {
        out.size = 0;
        out.ret = -1;
        margo_respond(handle, &out);
        margo_free_input(handle, &in);
        margo_destroy(handle);
        return;
    }

    void *buffer = (void*)vdata.data();
    hg_size_t size = vdata.size();
    hret = margo_bulk_create(mid, 1, (void**)&buffer, &size,
            HG_BULK_READ_ONLY, &bulk_handle);
    if(hret != HG_SUCCESS) {
        out.size = 0;
        out.ret = -1;
        margo_respond(handle, &out);
        margo_free_input(handle, &in);
        margo_destroy(handle);
        return;
    }

    hret = margo_bulk_transfer(mid, HG_BULK_PUSH, info->addr, in.bulk.handle, 0,
            bulk_handle, 0, vdata.size());
    if(hret != HG_SUCCESS) {
        out.size = 0;
        out.ret = -1;
        margo_respond(handle, &out);
        margo_free_input(handle, &in);
        margo_bulk_free(bulk_handle);
        margo_destroy(handle);
        return;
    }

    out.size = vdata.size();
    out.ret  = 0;

    margo_respond(handle, &out);
    margo_free_input(handle, &in);
    margo_bulk_free(bulk_handle);
    margo_destroy(handle);

    return;
}
DEFINE_MARGO_RPC_HANDLER(sdskv_bulk_get_ult)

static void sdskv_list_ult(hg_handle_t handle)
{

    hg_return_t hret;
    list_in_t in;
    list_out_t out;

    margo_instance_id mid = margo_hg_handle_get_instance(handle);
    assert(mid);
    const struct hg_info* info = margo_get_info(handle);
    sdskv_provider_t svr_ctx = 
        (sdskv_provider_t)margo_registered_data_mplex(mid, info->id, info->target_id);
    if(!svr_ctx) {
        fprintf(stderr, "Error: SDSKV could not find provider\n"); 
        out.ret    = -1;
        out.nkeys  = 0;
        out.keys   = nullptr;
        out.ksizes = nullptr;
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    hret = margo_get_input(handle, &in);
    if(hret != HG_SUCCESS) {
        out.ret    = -1;
        out.nkeys  = 0;
        out.keys   = nullptr;
        out.ksizes = nullptr;
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    auto it = svr_ctx->databases.find(in.db_id);
    if(it == svr_ctx->databases.end()) {
        out.ret   = -1;
        out.nkeys  = 0;
        out.keys   = nullptr;
        out.ksizes = nullptr;
        margo_respond(handle, &out);
        margo_free_input(handle, &in);
        margo_destroy(handle);
        return;
    }
    
    ds_bulk_t start_kdata(in.start_key, in.start_key+in.start_ksize);
    auto keys = it->second->list(start_kdata, in.max_keys);
    out.nkeys = keys.size();
    /* create the array of sizes */
    std::vector<hg_size_t> sizes(out.nkeys);
    for(unsigned i = 0; i < out.nkeys; i++) {
        sizes[i] = keys[i].size();
    }
    out.ksizes = sizes.data();
    /* create the packed data */
    std::vector<kv_data_t> packed_keys(keys.size());
    for(unsigned i = 0; i < out.nkeys; i++) {
        packed_keys[i] = (char*)(keys[i].data());
    }
    out.keys = packed_keys.data();
    out.ret = 0;

    margo_respond(handle, &out);
    margo_free_input(handle, &in);
    margo_destroy(handle); 

    return;
}
DEFINE_MARGO_RPC_HANDLER(sdskv_list_ult)

static void sdskv_server_finalize_cb(void *data)
{
    sdskv_provider_t svr_ctx = (sdskv_provider_t)data;
    assert(svr_ctx);

    sdskv_provider_remove_all_databases(svr_ctx);

    delete svr_ctx;

    return;
}

