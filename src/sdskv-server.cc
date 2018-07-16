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

    hg_id_t sdskv_put_id;
    hg_id_t sdskv_bulk_put_id;
    hg_id_t sdskv_get_id;
    hg_id_t sdskv_exists_id;
    hg_id_t sdskv_erase_id;
    hg_id_t sdskv_length_id;
    hg_id_t sdskv_bulk_get_id;
    hg_id_t sdskv_open_id;
    hg_id_t sdskv_list_keys_id;
    hg_id_t sdskv_list_keyvals_id;
    /* migration */
    hg_id_t sdskv_migrate_keys_id;
    hg_id_t sdskv_migrate_key_range_id;
    hg_id_t sdskv_migrate_keys_prefixed_id;
    hg_id_t sdskv_migrate_all_keys_id;
    hg_id_t sdskv_migrate_database_id;
};

template<typename F>
struct scoped_call {
    F _f;
    scoped_call(const F& f) : _f(f) {}
    ~scoped_call() { _f(); }
};

template<typename F>
inline scoped_call<F> at_exit(F&& f) {
    return scoped_call<F>(std::forward<F>(f));
}

DECLARE_MARGO_RPC_HANDLER(sdskv_put_ult)
DECLARE_MARGO_RPC_HANDLER(sdskv_length_ult)
DECLARE_MARGO_RPC_HANDLER(sdskv_get_ult)
DECLARE_MARGO_RPC_HANDLER(sdskv_open_ult)
DECLARE_MARGO_RPC_HANDLER(sdskv_bulk_put_ult)
DECLARE_MARGO_RPC_HANDLER(sdskv_bulk_get_ult)
DECLARE_MARGO_RPC_HANDLER(sdskv_list_keys_ult)
DECLARE_MARGO_RPC_HANDLER(sdskv_list_keyvals_ult)
DECLARE_MARGO_RPC_HANDLER(sdskv_erase_ult)
DECLARE_MARGO_RPC_HANDLER(sdskv_exists_ult)
DECLARE_MARGO_RPC_HANDLER(sdskv_migrate_keys_ult)
DECLARE_MARGO_RPC_HANDLER(sdskv_migrate_key_range_ult)
DECLARE_MARGO_RPC_HANDLER(sdskv_migrate_keys_prefixed_ult)
DECLARE_MARGO_RPC_HANDLER(sdskv_migrate_all_keys_ult)
DECLARE_MARGO_RPC_HANDLER(sdskv_migrate_database_ult)
/*
DECLARE_MARGO_RPC_HANDLER(__migrate_keys_ult)
DECLARE_MARGO_RPC_HANDLER(__migrate_key_range_ult)
DECLARE_MARGO_RPC_HANDLER(__migrate_keys_prefixed_ult)
DECLARE_MARGO_RPC_HANDLER(__migrate_all_keys_ult)
DECLARE_MARGO_RPC_HANDLER(__migrate_database_ult)
*/
static void sdskv_server_finalize_cb(void *data);

extern "C" int sdskv_provider_register(
        margo_instance_id mid,
        uint16_t provider_id,
        ABT_pool abt_pool,
        sdskv_provider_t* provider)
{
    sdskv_server_context_t *tmp_svr_ctx;

    /* check if a provider with the same multiplex id already exists */
    {
        hg_id_t id;
        hg_bool_t flag;
        margo_provider_registered_name(mid, "sdskv_put_rpc", provider_id, &id, &flag);
        if(flag == HG_TRUE) {
            fprintf(stderr, "sdskv_provider_register(): a provider with the same provider id (%d) already exists\n", provider_id);
            return SDSKV_ERR_MERCURY;
        }
    }

    /* allocate the resulting structure */    
    tmp_svr_ctx = new sdskv_server_context_t;
    if(!tmp_svr_ctx)
        return SDSKV_ERR_ALLOCATION;

    /* register RPCs */
    hg_id_t rpc_id;
    rpc_id = MARGO_REGISTER_PROVIDER(mid, "sdskv_put_rpc",
            put_in_t, put_out_t,
            sdskv_put_ult, provider_id, abt_pool);
    tmp_svr_ctx->sdskv_put_id = rpc_id;
    margo_register_data(mid, rpc_id, (void*)tmp_svr_ctx, NULL);
    rpc_id = MARGO_REGISTER_PROVIDER(mid, "sdskv_bulk_put_rpc",
            bulk_put_in_t, bulk_put_out_t,
            sdskv_bulk_put_ult, provider_id, abt_pool);
    tmp_svr_ctx->sdskv_bulk_put_id = rpc_id;
    margo_register_data(mid, rpc_id, (void*)tmp_svr_ctx, NULL);
    rpc_id = MARGO_REGISTER_PROVIDER(mid, "sdskv_get_rpc",
            get_in_t, get_out_t,
            sdskv_get_ult, provider_id, abt_pool);
    tmp_svr_ctx->sdskv_get_id = rpc_id;
    margo_register_data(mid, rpc_id, (void*)tmp_svr_ctx, NULL);
    rpc_id = MARGO_REGISTER_PROVIDER(mid, "sdskv_length_rpc",
            length_in_t, length_out_t,
            sdskv_length_ult, provider_id, abt_pool);
    tmp_svr_ctx->sdskv_length_id = rpc_id;
    margo_register_data(mid, rpc_id, (void*)tmp_svr_ctx, NULL);
    rpc_id = MARGO_REGISTER_PROVIDER(mid, "sdskv_exists_rpc",
            exists_in_t, exists_out_t,
            sdskv_exists_ult, provider_id, abt_pool);
    tmp_svr_ctx->sdskv_exists_id = rpc_id;
    margo_register_data(mid, rpc_id, (void*)tmp_svr_ctx, NULL);
    rpc_id = MARGO_REGISTER_PROVIDER(mid, "sdskv_bulk_get_rpc",
            bulk_get_in_t, bulk_get_out_t,
            sdskv_bulk_get_ult, provider_id, abt_pool);
    tmp_svr_ctx->sdskv_bulk_get_id = rpc_id;
    margo_register_data(mid, rpc_id, (void*)tmp_svr_ctx, NULL);
    rpc_id = MARGO_REGISTER_PROVIDER(mid, "sdskv_open_rpc",
            open_in_t, open_out_t,
            sdskv_open_ult, provider_id, abt_pool);
    tmp_svr_ctx->sdskv_open_id = rpc_id;
    margo_register_data(mid, rpc_id, (void*)tmp_svr_ctx, NULL);
    rpc_id = MARGO_REGISTER_PROVIDER(mid, "sdskv_list_keys_rpc",
            list_keys_in_t, list_keys_out_t,
            sdskv_list_keys_ult, provider_id, abt_pool);
    tmp_svr_ctx->sdskv_list_keys_id = rpc_id;
    margo_register_data(mid, rpc_id, (void*)tmp_svr_ctx, NULL);
    rpc_id = MARGO_REGISTER_PROVIDER(mid, "sdskv_list_keyvals_rpc",
            list_keyvals_in_t, list_keyvals_out_t,
            sdskv_list_keyvals_ult, provider_id, abt_pool);
    tmp_svr_ctx->sdskv_list_keyvals_id = rpc_id;
    margo_register_data(mid, rpc_id, (void*)tmp_svr_ctx, NULL);
    rpc_id = MARGO_REGISTER_PROVIDER(mid, "sdskv_erase_rpc",
            erase_in_t, erase_out_t,
            sdskv_erase_ult, provider_id, abt_pool);
    tmp_svr_ctx->sdskv_erase_id = rpc_id;
    margo_register_data(mid, rpc_id, (void*)tmp_svr_ctx, NULL);
    /* migration RPC */
    rpc_id = MARGO_REGISTER_PROVIDER(mid, "sdskv_migrate_keys_rpc",
            migrate_keys_in_t, migrate_keys_out_t,
            sdskv_migrate_keys_ult, provider_id, abt_pool);
    tmp_svr_ctx->sdskv_migrate_keys_id = rpc_id;
    margo_register_data(mid, rpc_id, (void*)tmp_svr_ctx, NULL);
    rpc_id = MARGO_REGISTER_PROVIDER(mid, "sdskv_migrate_key_range_rpc",
            migrate_key_range_in_t, migrate_keys_out_t,
            sdskv_migrate_key_range_ult, provider_id, abt_pool);
    tmp_svr_ctx->sdskv_migrate_key_range_id = rpc_id;
    margo_register_data(mid, rpc_id, (void*)tmp_svr_ctx, NULL);
    rpc_id = MARGO_REGISTER_PROVIDER(mid, "sdskv_migrate_keys_prefixed_rpc",
            migrate_keys_prefixed_in_t, migrate_keys_out_t,
            sdskv_migrate_keys_prefixed_ult, provider_id, abt_pool);
    tmp_svr_ctx->sdskv_migrate_keys_prefixed_id = rpc_id;
    margo_register_data(mid, rpc_id, (void*)tmp_svr_ctx, NULL);
    rpc_id = MARGO_REGISTER_PROVIDER(mid, "sdskv_migrate_all_keys_rpc",
            migrate_all_keys_in_t, migrate_keys_out_t,
            sdskv_migrate_all_keys_ult, provider_id, abt_pool);
    tmp_svr_ctx->sdskv_migrate_all_keys_id = rpc_id;
    margo_register_data(mid, rpc_id, (void*)tmp_svr_ctx, NULL);
    rpc_id = MARGO_REGISTER_PROVIDER(mid, "sdskv_migrate_database_rpc",
            migrate_database_in_t, migrate_database_out_t,
            sdskv_migrate_database_ult, provider_id, abt_pool);
    tmp_svr_ctx->sdskv_migrate_database_id = rpc_id;
    margo_register_data(mid, rpc_id, (void*)tmp_svr_ctx, NULL);
    /*
    rpc_id = margo_register_provider(mid, "__migrate_keys_rpc",
            __migrate_keys_in_t, __migrate_keys_out_t,
            sdskv_migrate_keys_ult, provider_id, abt_pool);
    margo_register_data(mid, rpc_id, (void*)tmp_svr_ctx, NULL);
    rpc_id = margo_register_provider(mid, "sdskv_migrate_key_range_rpc",
            migrate_key_range_in_t, migrate_keys_out_t,
            sdskv_migrate_key_range_ult, provider_id, abt_pool);
    margo_register_data(mid, rpc_id, (void*)tmp_svr_ctx, NULL);
    rpc_id = margo_register_provider(mid, "sdskv_migrate_keys_prefixed_rpc",
            migrate_prefixed_keys_in_t, migrate_keys_out_t,
            sdskv_migrate_prefixed_keys_ult, provider_id, abt_pool);
    margo_register_data(mid, rpc_id, (void*)tmp_svr_ctx, NULL);
    rpc_id = margo_register_provider(mid, "sdskv_migrate_all_keys_rpc",
            migrate_all_keys_in_t, migrate_keys_out_t,
            sdskv_migrate_all_keys_ult, provider_id, abt_pool);
    margo_register_data(mid, rpc_id, (void*)tmp_svr_ctx, NULL);
    rpc_id = margo_register_provider(mid, "sdskv_migrate_database_rpc",
            migrate_database_in_t, migrate_database_out_t,
            sdskv_migrate_database_ult, provider_id, abt_pool);
    margo_register_data(mid, rpc_id, (void*)tmp_svr_ctx, NULL);
    */
    /* install the bake server finalize callback */
    margo_push_finalize_callback(mid, &sdskv_server_finalize_cb, tmp_svr_ctx);

    if(provider != SDSKV_PROVIDER_IGNORE)
        *provider = tmp_svr_ctx;

    return SDSKV_SUCCESS;
}

extern "C" int sdskv_provider_attach_database(
        sdskv_provider_t provider,
        const sdskv_config_t* config,
        sdskv_database_id_t* db_id)
{
    auto db = datastore_factory::open_datastore(config->db_type, 
            std::string(config->db_name), std::string(config->db_path));
    if(db == nullptr) return SDSKV_ERR_DB_CREATE;
    db->set_comparison_function(config->db_comparison_fn);
    sdskv_database_id_t id = (sdskv_database_id_t)(db);
    if(config->db_no_overwrite) {
        db->set_no_overwrite();
    }

    provider->name2id[std::string(config->db_name)] = id;
    provider->id2name[id] = std::string(config->db_name);
    provider->databases[id] = db;

    *db_id = id;

    return SDSKV_SUCCESS;
}

extern "C" int sdskv_provider_add_database(
        sdskv_provider_t provider,
        const char* db_name,
        const char* db_path,
        sdskv_db_type_t db_type,
        sdskv_compare_fn comp_fn,
        sdskv_database_id_t* db_id) {
    sdskv_config_t config = SDSKV_CONFIG_DEFAULT;
    config.db_name = db_name;
    config.db_path = db_path;
    config.db_type = db_type;
    config.db_comparison_fn = comp_fn;
    return sdskv_provider_attach_database(
            provider, &config, db_id);
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
        return SDSKV_SUCCESS;
    } else {
        return SDSKV_ERR_UNKNOWN_DB;
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

    return SDSKV_SUCCESS;
}

extern "C" int sdskv_provider_count_databases(
        sdskv_provider_t provider,
        uint64_t* num_db)
{
    *num_db = provider->databases.size();
    return SDSKV_SUCCESS;
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
    return SDSKV_SUCCESS;
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
        (sdskv_provider_t)margo_registered_data(mid, info->id);
    if(!svr_ctx) {
        fprintf(stderr, "Error: SDSKV could not find provider\n"); 
        out.ret = SDSKV_ERR_UNKNOWN_PR;
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }
    
    hret = margo_get_input(handle, &in);
    if(hret != HG_SUCCESS) {
        out.ret = SDSKV_ERR_MERCURY;
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    auto it = svr_ctx->databases.find(in.db_id);
    if(it == svr_ctx->databases.end()) {
        out.ret = SDSKV_ERR_UNKNOWN_DB;
        margo_respond(handle, &out);
        margo_free_input(handle, &in);
        margo_destroy(handle);
        return;
    }

    ds_bulk_t kdata(in.key.data, in.key.data+in.key.size);
    ds_bulk_t vdata(in.value.data, in.value.data+in.value.size);

    if(it->second->put(kdata, vdata)) {
        out.ret = SDSKV_SUCCESS;
    } else {
        out.ret = SDSKV_ERR_PUT;
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
        (sdskv_provider_t)margo_registered_data(mid, info->id);
    if(!svr_ctx) {
        fprintf(stderr, "Error: SDSKV could not find provider\n"); 
        out.ret = SDSKV_ERR_UNKNOWN_PR;
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    hret = margo_get_input(handle, &in);
    if(hret != HG_SUCCESS) {
        out.ret = SDSKV_ERR_MERCURY;
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    auto it = svr_ctx->databases.find(in.db_id);
    if(it == svr_ctx->databases.end()) {
        out.ret = SDSKV_ERR_UNKNOWN_DB;
        margo_respond(handle, &out);
        margo_free_input(handle, &in);
        margo_destroy(handle);
        return;
    }
    
    ds_bulk_t kdata(in.key.data, in.key.data+in.key.size);

    ds_bulk_t vdata;
    if(it->second->get(kdata, vdata)) {
        out.size = vdata.size();
        out.ret  = SDSKV_SUCCESS;
    } else {
        out.size = 0;
        out.ret  = SDSKV_ERR_UNKNOWN_KEY;
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
        (sdskv_provider_t)margo_registered_data(mid, info->id);
    if(!svr_ctx) {
        fprintf(stderr, "Error: SDSKV could not find provider\n"); 
        out.ret = SDSKV_ERR_UNKNOWN_PR;
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    hret = margo_get_input(handle, &in);
    if(hret != HG_SUCCESS) {
        out.ret = SDSKV_ERR_MERCURY;
        out.value.data = nullptr;
        out.value.size = 0;
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    auto it = svr_ctx->databases.find(in.db_id);
    if(it == svr_ctx->databases.end()) {
        out.ret = SDSKV_ERR_UNKNOWN_DB;
        out.value.data = nullptr;
        out.value.size = 0;
        margo_respond(handle, &out);
        margo_free_input(handle, &in);
        margo_destroy(handle);
        return;
    }
    
    ds_bulk_t kdata(in.key.data, in.key.data+in.key.size);

    ds_bulk_t vdata;
    if(it->second->get(kdata, vdata)) {
        if(vdata.size() <= in.vsize) {
            out.value.size = vdata.size();
            out.value.data = vdata.data();
            out.ret = SDSKV_SUCCESS;
        } else {
            out.value.size = 0;
            out.value.data = nullptr;
            out.ret = SDSKV_ERR_SIZE;
        }
    } else {
        out.value.size = 0;
        out.value.data = nullptr;
        out.ret   = SDSKV_ERR_UNKNOWN_KEY;
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
        (sdskv_provider_t)margo_registered_data(mid, info->id);
    if(!svr_ctx) {
        fprintf(stderr, "Error: SDSKV could not find provider\n"); 
        out.ret = SDSKV_ERR_UNKNOWN_PR;
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    hret = margo_get_input(handle, &in);
    if(hret != HG_SUCCESS) {
        out.ret = SDSKV_ERR_MERCURY;
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    auto it = svr_ctx->name2id.find(std::string(in.name));
    if(it == svr_ctx->name2id.end()) {
        out.ret = SDSKV_ERR_DB_NAME;
        margo_respond(handle, &out);
        margo_free_input(handle, &in);
        margo_destroy(handle);
        return;
    }

    out.db_id = it->second;
    out.ret  = SDSKV_SUCCESS;

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
        (sdskv_provider_t)margo_registered_data(mid, info->id);
    if(!svr_ctx) {
        fprintf(stderr, "Error: SDSKV could not find provider\n"); 
        out.ret = SDSKV_ERR_UNKNOWN_PR;
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    hret = margo_get_input(handle, &in);
    if(hret != HG_SUCCESS) {
        out.ret = SDSKV_ERR_MERCURY;
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    auto it = svr_ctx->databases.find(in.db_id);
    if(it == svr_ctx->databases.end()) {
        out.ret = SDSKV_ERR_UNKNOWN_DB;
        margo_respond(handle, &out);
        margo_free_input(handle, &in);
        margo_destroy(handle);
        return;
    }

    ds_bulk_t vdata(in.vsize);

    if(in.vsize > 0) {

        void *buffer = (void*)vdata.data();
        hg_size_t size = vdata.size();
        hret = margo_bulk_create(mid, 1, (void**)&buffer, &size,
                HG_BULK_WRITE_ONLY, &bulk_handle);
        if(hret != HG_SUCCESS) {
            out.ret = SDSKV_ERR_MERCURY;
            margo_respond(handle, &out);
            margo_free_input(handle, &in);
            margo_destroy(handle);
            return;
        }

        hret = margo_bulk_transfer(mid, HG_BULK_PULL, info->addr, in.handle, 0,
                bulk_handle, 0, vdata.size());
        if(hret != HG_SUCCESS) {
            out.ret = SDSKV_ERR_MERCURY;
            margo_respond(handle, &out);
            margo_free_input(handle, &in);
            margo_bulk_free(bulk_handle);
            margo_destroy(handle);
            return;
        }

        margo_bulk_free(bulk_handle);

    }

    ds_bulk_t kdata(in.key.data, in.key.data+in.key.size);

    auto b = it->second->put(kdata, vdata);

    if(b) {
        out.ret = SDSKV_SUCCESS;
    } else {
        out.ret = SDSKV_ERR_PUT;
    }

    margo_respond(handle, &out);
    margo_free_input(handle, &in);
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
        (sdskv_provider_t)margo_registered_data(mid, info->id);
    if(!svr_ctx) {
        fprintf(stderr, "Error: SDSKV could not find provider\n"); 
        out.ret = SDSKV_ERR_UNKNOWN_PR;
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    hret = margo_get_input(handle, &in);
    if(hret != HG_SUCCESS) {
        out.ret = SDSKV_ERR_MERCURY;
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    auto it = svr_ctx->databases.find(in.db_id);
    if(it == svr_ctx->databases.end()) {
        out.ret = SDSKV_ERR_UNKNOWN_DB;
        margo_respond(handle, &out);
        margo_free_input(handle, &in);
        margo_destroy(handle);
        return;
    }
    
    ds_bulk_t kdata(in.key.data, in.key.data+in.key.size);

    ds_bulk_t vdata;
    auto b = it->second->get(kdata, vdata);

    if(!b || vdata.size() > in.vsize) {
        out.size = 0;
        out.ret = SDSKV_ERR_SIZE;
        margo_respond(handle, &out);
        margo_free_input(handle, &in);
        margo_destroy(handle);
        return;
    }

    void *buffer = (void*)vdata.data();
    hg_size_t size = vdata.size();
    if(size > 0) {
        hret = margo_bulk_create(mid, 1, (void**)&buffer, &size,
                HG_BULK_READ_ONLY, &bulk_handle);
        if(hret != HG_SUCCESS) {
            out.size = 0;
            out.ret = SDSKV_ERR_MERCURY;
            margo_respond(handle, &out);
            margo_free_input(handle, &in);
            margo_destroy(handle);
            return;
        }

        hret = margo_bulk_transfer(mid, HG_BULK_PUSH, info->addr, in.handle, 0,
                bulk_handle, 0, vdata.size());
        if(hret != HG_SUCCESS) {
            out.size = 0;
            out.ret = SDSKV_ERR_MERCURY;
            margo_respond(handle, &out);
            margo_free_input(handle, &in);
            margo_bulk_free(bulk_handle);
            margo_destroy(handle);
            return;
        }

        margo_bulk_free(bulk_handle);
    }

    out.size = size;
    out.ret  = SDSKV_SUCCESS;

    margo_respond(handle, &out);
    margo_free_input(handle, &in);
    margo_destroy(handle);

    return;
}
DEFINE_MARGO_RPC_HANDLER(sdskv_bulk_get_ult)

static void sdskv_erase_ult(hg_handle_t handle)
{

    hg_return_t hret;
    erase_in_t in;
    erase_out_t out;

    margo_instance_id mid = margo_hg_handle_get_instance(handle);
    assert(mid);
    const struct hg_info* info = margo_get_info(handle);
    sdskv_provider_t svr_ctx = 
        (sdskv_provider_t)margo_registered_data(mid, info->id);
    if(!svr_ctx) {
        fprintf(stderr, "Error: SDSKV could not find provider\n"); 
        out.ret = SDSKV_ERR_UNKNOWN_PR;
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    hret = margo_get_input(handle, &in);
    if(hret != HG_SUCCESS) {
        out.ret = SDSKV_ERR_MERCURY;
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    auto it = svr_ctx->databases.find(in.db_id);
    if(it == svr_ctx->databases.end()) {
        out.ret = SDSKV_ERR_UNKNOWN_DB;
        margo_respond(handle, &out);
        margo_free_input(handle, &in);
        margo_destroy(handle);
        return;
    }
    
    ds_bulk_t kdata(in.key.data, in.key.data+in.key.size);

    if(it->second->erase(kdata)) {
        out.ret   = SDSKV_SUCCESS;
    } else {
        out.ret   = SDSKV_ERR_ERASE;
    }

    margo_respond(handle, &out);
    margo_free_input(handle, &in);
    margo_destroy(handle); 

    return;
}
DEFINE_MARGO_RPC_HANDLER(sdskv_erase_ult)

static void sdskv_exists_ult(hg_handle_t handle)
{

    hg_return_t hret;
    exists_in_t in;
    exists_out_t out;

    margo_instance_id mid = margo_hg_handle_get_instance(handle);
    assert(mid);
    const struct hg_info* info = margo_get_info(handle);
    sdskv_provider_t svr_ctx = 
        (sdskv_provider_t)margo_registered_data(mid, info->id);
    if(!svr_ctx) {
        fprintf(stderr, "Error: SDSKV could not find provider\n"); 
        out.ret = SDSKV_ERR_UNKNOWN_PR;
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    hret = margo_get_input(handle, &in);
    if(hret != HG_SUCCESS) {
        out.ret = SDSKV_ERR_MERCURY;
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    auto it = svr_ctx->databases.find(in.db_id);
    if(it == svr_ctx->databases.end()) {
        out.ret = SDSKV_ERR_UNKNOWN_DB;
        margo_respond(handle, &out);
        margo_free_input(handle, &in);
        margo_destroy(handle);
        return;
    }
    
    ds_bulk_t kdata(in.key.data, in.key.data+in.key.size);

    out.flag = it->second->exists(kdata) ? 1 : 0;
    out.ret  = SDSKV_SUCCESS;

    margo_respond(handle, &out);
    margo_free_input(handle, &in);
    margo_destroy(handle); 
    return;
}
DEFINE_MARGO_RPC_HANDLER(sdskv_exists_ult)

static void sdskv_list_keys_ult(hg_handle_t handle)
{

    hg_return_t hret;
    list_keys_in_t in;
    list_keys_out_t out;
    hg_bulk_t ksizes_local_bulk = HG_BULK_NULL;
    hg_bulk_t keys_local_bulk   = HG_BULK_NULL;

    out.ret     = SDSKV_SUCCESS;
    out.nkeys   = 0;

    /* get the provider handling this request */
    margo_instance_id mid = margo_hg_handle_get_instance(handle);
    assert(mid);
    const struct hg_info* info = margo_get_info(handle);
    sdskv_provider_t svr_ctx = 
        (sdskv_provider_t)margo_registered_data(mid, info->id);
    if(!svr_ctx) {
        std::cerr << "Error: SDSKV list_keys could not find provider" << std::endl;
        out.ret = SDSKV_ERR_UNKNOWN_PR;
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    /* get the input */
    hret = margo_get_input(handle, &in);
    if(hret != HG_SUCCESS) {
        std::cerr << "Error: SDSKV list_keys could not get RPC input" << std::endl;
        out.ret = SDSKV_ERR_MERCURY;
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    try {

        /* find the database targeted */
        auto it = svr_ctx->databases.find(in.db_id);
        if(it == svr_ctx->databases.end()) {
            std::cerr << "Error: SDSKV list_keys could not get database with id " << in.db_id << std::endl;
            throw SDSKV_ERR_UNKNOWN_DB;
        }
        auto db = it->second;

        /* create a bulk handle to receive and send key sizes from client */
        std::vector<hg_size_t> ksizes(in.max_keys);
        std::vector<void*> ksizes_addr(1);
        ksizes_addr[0] = (void*)ksizes.data();
        hg_size_t ksizes_bulk_size = ksizes.size()*sizeof(hg_size_t);
        hret = margo_bulk_create(mid, 1, ksizes_addr.data(), 
                &ksizes_bulk_size, HG_BULK_READWRITE, &ksizes_local_bulk);
        if(hret != HG_SUCCESS) {
            std::cerr << "Error: SDSKV list_keys could not create bulk handle (ksizes_local_bulk)" << std::endl;
            throw SDSKV_ERR_MERCURY;
        }


        /* receive the key sizes from the client */
        hg_addr_t origin_addr = info->addr;
        hret = margo_bulk_transfer(mid, HG_BULK_PULL, origin_addr,
                in.ksizes_bulk_handle, 0, ksizes_local_bulk, 0, ksizes_bulk_size);
        if(hret != HG_SUCCESS) {
            std::cerr << "Error: SDSKV list_keys could not issue bulk transfer " 
                << "(pull from in.ksizes_bulk_handle to ksizes_local_bulk)" << std::endl;
            throw SDSKV_ERR_MERCURY;
        }

        /* make a copy of the remote key sizes */
        std::vector<hg_size_t> remote_ksizes(ksizes.begin(), ksizes.end());

        /* get the keys from the underlying database */    
        ds_bulk_t start_kdata(in.start_key.data, in.start_key.data+in.start_key.size);
        ds_bulk_t prefix(in.prefix.data, in.prefix.data+in.prefix.size);
        auto keys = db->list_keys(start_kdata, in.max_keys, prefix);
        hg_size_t num_keys = std::min(keys.size(), in.max_keys);

        if(num_keys == 0) throw SDSKV_SUCCESS;

        /* create the array of actual sizes */
        std::vector<hg_size_t> true_ksizes(num_keys);
        hg_size_t keys_bulk_size = 0;
        for(unsigned i = 0; i < num_keys; i++) {
            true_ksizes[i] = keys[i].size();
            if(true_ksizes[i] > ksizes[i]) {
                // this key has a size that exceeds the allocated size on client
                throw SDSKV_ERR_SIZE;
            } else {
                ksizes[i] = true_ksizes[i];
                keys_bulk_size += ksizes[i];
            }
        }

        /* create an array of addresses pointing to keys */
        std::vector<void*> keys_addr(num_keys);
        for(unsigned i=0; i < num_keys; i++) {
            keys_addr[i] = (void*)(keys[i].data());
        }

        /* expose the keys for bulk transfer */
        hret = margo_bulk_create(mid, num_keys, keys_addr.data(),
                true_ksizes.data(), HG_BULK_READ_ONLY, &keys_local_bulk);
        if(hret != HG_SUCCESS) {
            std::cerr << "Error: SDSKV list_keys could not create bulk handle (keys_local_bulk)" << std::endl;
            throw SDSKV_ERR_MERCURY;
        }

        /* transfer the ksizes back to the client */
        hret = margo_bulk_transfer(mid, HG_BULK_PUSH, origin_addr, 
                in.ksizes_bulk_handle, 0, ksizes_local_bulk, 0, ksizes_bulk_size);
        if(hret != HG_SUCCESS) {
            std::cerr << "Error: SDSKV list_keys could not issue bulk transfer "
                << "(push from ksizes_local_bulk to in.ksizes_bulk_handle)" << std::endl;
            throw SDSKV_ERR_MERCURY;
        }

        /* transfer the keys to the client */
        uint64_t remote_offset = 0;
        uint64_t local_offset  = 0;
        for(unsigned i = 0; i < num_keys; i++) {

            if(true_ksizes[i] > 0) {
                hret = margo_bulk_transfer(mid, HG_BULK_PUSH, origin_addr,
                        in.keys_bulk_handle, remote_offset, keys_local_bulk, local_offset, true_ksizes[i]);
                if(hret != HG_SUCCESS) {
                    std::cerr << "Error: SDSKV list_keys could not issue bulk transfer (keys_local_bulk)" << std::endl;
                    throw SDSKV_ERR_MERCURY;
                }
            }

            remote_offset += remote_ksizes[i];
            local_offset  += true_ksizes[i];
        }

        out.nkeys = num_keys;
        out.ret = SDSKV_SUCCESS;

    } catch(int exc_no) {
        out.ret = exc_no;
    }

    margo_bulk_free(ksizes_local_bulk);
    margo_bulk_free(keys_local_bulk);
    margo_respond(handle, &out);
    margo_free_input(handle, &in);
    margo_destroy(handle); 

    return;
}
DEFINE_MARGO_RPC_HANDLER(sdskv_list_keys_ult)

static void sdskv_list_keyvals_ult(hg_handle_t handle)
{

    hg_return_t hret;
    list_keyvals_in_t in;
    list_keyvals_out_t out;
    hg_bulk_t ksizes_local_bulk = HG_BULK_NULL;
    hg_bulk_t keys_local_bulk   = HG_BULK_NULL;
    hg_bulk_t vsizes_local_bulk = HG_BULK_NULL;
    hg_bulk_t vals_local_bulk   = HG_BULK_NULL;

    out.ret     = SDSKV_SUCCESS;
    out.nkeys   = 0;

    /* get the provider handling this request */
    margo_instance_id mid = margo_hg_handle_get_instance(handle);
    assert(mid);
    const struct hg_info* info = margo_get_info(handle);
    sdskv_provider_t svr_ctx = 
        (sdskv_provider_t)margo_registered_data(mid, info->id);
    if(!svr_ctx) {
        std::cerr << "Error: SDSKV list_keyvals could not find provider" << std::endl;
        out.ret = SDSKV_ERR_UNKNOWN_PR;
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    /* get the input */
    hret = margo_get_input(handle, &in);
    if(hret != HG_SUCCESS) {
        std::cerr << "Error: SDSKV list_keyvals could not get RPC input" << std::endl;
        out.ret = SDSKV_ERR_MERCURY;
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    try {

        /* find the database targeted */
        auto it = svr_ctx->databases.find(in.db_id);
        if(it == svr_ctx->databases.end()) {
            std::cerr << "Error: SDSKV list_keyvals could not get database with id " << in.db_id << std::endl;
            throw SDSKV_ERR_UNKNOWN_DB;
        }
        auto db = it->second;

        /* create a bulk handle to receive and send key sizes from client */
        std::vector<hg_size_t> ksizes(in.max_keys);
        std::vector<void*> ksizes_addr(1);
        ksizes_addr[0] = (void*)ksizes.data();
        hg_size_t ksizes_bulk_size = ksizes.size()*sizeof(hg_size_t);
        hret = margo_bulk_create(mid, 1, ksizes_addr.data(), 
                &ksizes_bulk_size, HG_BULK_READWRITE, &ksizes_local_bulk);
        if(hret != HG_SUCCESS) {
            std::cerr << "Error: SDSKV list_keyvals could not create bulk handle (ksizes_local_bulk)" << std::endl;
            throw SDSKV_ERR_MERCURY;
        }

        /* create a bulk handle to receive and send value sizes from client */
        std::vector<hg_size_t> vsizes(in.max_keys);
        std::vector<void*> vsizes_addr(1);
        vsizes_addr[0] = (void*)vsizes.data();
        hg_size_t vsizes_bulk_size = vsizes.size()*sizeof(hg_size_t);
        hret = margo_bulk_create(mid, 1, vsizes_addr.data(), 
                &vsizes_bulk_size, HG_BULK_READWRITE, &vsizes_local_bulk);
        if(hret != HG_SUCCESS) {
            std::cerr << "Error: SDSKV list_keyvals could not create bulk handle (vsizes_local_bulk)" << std::endl;
            throw SDSKV_ERR_MERCURY;
        }

        /* receive the key sizes from the client */
        hg_addr_t origin_addr = info->addr;
        hret = margo_bulk_transfer(mid, HG_BULK_PULL, origin_addr,
                in.ksizes_bulk_handle, 0, ksizes_local_bulk, 0, ksizes_bulk_size);
        if(hret != HG_SUCCESS) {
            std::cerr << "Error: SDSKV list_keyvals could not issue bulk transfer " 
                << "(pull from in.ksizes_bulk_handle to ksizes_local_bulk)" << std::endl;
            throw SDSKV_ERR_MERCURY;
        }

        /* receive the values sizes from the client */
        hret = margo_bulk_transfer(mid, HG_BULK_PULL, origin_addr,
                in.vsizes_bulk_handle, 0, vsizes_local_bulk, 0, vsizes_bulk_size);
        if(hret != HG_SUCCESS) {
            std::cerr << "Error: SDSKV list_keyvals could not issue bulk transfer " 
                << "(pull from in.vsizes_bulk_handle to vsizes_local_bulk)" << std::endl;
            throw SDSKV_ERR_MERCURY;
        }

        /* make a copy of the remote key sizes and value sizes */
        std::vector<hg_size_t> remote_ksizes(ksizes.begin(), ksizes.end());
        std::vector<hg_size_t> remote_vsizes(vsizes.begin(), vsizes.end());

        /* get the keys and values from the underlying database */    
        ds_bulk_t start_kdata(in.start_key.data, in.start_key.data+in.start_key.size);
        ds_bulk_t prefix(in.prefix.data, in.prefix.data+in.prefix.size);
        auto keyvals = db->list_keyvals(start_kdata, in.max_keys, prefix);
        hg_size_t num_keys = std::min(keyvals.size(), in.max_keys);

        if(num_keys == 0) throw SDSKV_SUCCESS;

        /* create the array of actual key sizes */
        std::vector<hg_size_t> true_ksizes(num_keys);
        hg_size_t keys_bulk_size = 0;
        for(unsigned i = 0; i < num_keys; i++) {
            true_ksizes[i] = keyvals[i].first.size();
            if(true_ksizes[i] > ksizes[i]) {
                // this key has a size that exceeds the allocated size on client
                throw SDSKV_ERR_SIZE;
            } else {
                ksizes[i] = true_ksizes[i];
                keys_bulk_size += ksizes[i];
            }
        }

        /* create the array of actual value sizes */
        std::vector<hg_size_t> true_vsizes(num_keys);
        hg_size_t vals_bulk_size = 0;
        for(unsigned i = 0; i < num_keys; i++) {
            true_vsizes[i] = keyvals[i].second.size();
            if(true_vsizes[i] > vsizes[i]) {
                // this value has a size that exceeds the allocated size on client
                throw SDSKV_ERR_SIZE;
            } else {
                vsizes[i] = true_vsizes[i];
                vals_bulk_size += vsizes[i];
            }
        }

        /* create an array of addresses pointing to keys */
        std::vector<void*> keys_addr(num_keys);
        for(unsigned i=0; i < num_keys; i++) {
            keys_addr[i] = (void*)(keyvals[i].first.data());
        }

        /* create an array of addresses pointing to values */
        std::vector<void*> vals_addr(num_keys);
        for(unsigned i=0; i < num_keys; i++) {
            vals_addr[i] = (void*)(keyvals[i].second.data());
        }

        /* expose the keys for bulk transfer */
        hret = margo_bulk_create(mid, num_keys, keys_addr.data(),
                true_ksizes.data(), HG_BULK_READ_ONLY, &keys_local_bulk);
        if(hret != HG_SUCCESS) {
            std::cerr << "Error: SDSKV list_keyvals could not create bulk handle (keys_local_bulk)" << std::endl;
            throw SDSKV_ERR_MERCURY;
        }

        /* expose the values for bulk transfer */
        hret = margo_bulk_create(mid, num_keys, vals_addr.data(),
                true_vsizes.data(), HG_BULK_READ_ONLY, &vals_local_bulk);
        if(hret != HG_SUCCESS) {
            std::cerr << "Error: SDSKV list_keyvals could not create bulk handle (vals_local_bulk)" << std::endl;
            throw SDSKV_ERR_MERCURY;
        }

        /* transfer the ksizes back to the client */
        hret = margo_bulk_transfer(mid, HG_BULK_PUSH, origin_addr, 
                in.ksizes_bulk_handle, 0, ksizes_local_bulk, 0, ksizes_bulk_size);
        if(hret != HG_SUCCESS) {
            std::cerr << "Error: SDSKV list_keyvals could not issue bulk transfer "
                << "(push from ksizes_local_bulk to in.ksizes_bulk_handle)" << std::endl;
            throw SDSKV_ERR_MERCURY;
        }

        /* transfer the vsizes back to the client */
        hret = margo_bulk_transfer(mid, HG_BULK_PUSH, origin_addr, 
                in.vsizes_bulk_handle, 0, vsizes_local_bulk, 0, vsizes_bulk_size);
        if(hret != HG_SUCCESS) {
            std::cerr << "Error: SDSKV list_keyvals could not issue bulk transfer "
                << "(push from vsizes_local_bulk to in.vsizes_bulk_handle)" << std::endl;
            throw SDSKV_ERR_MERCURY;
        }

        uint64_t remote_offset = 0;
        uint64_t local_offset  = 0;

        /* transfer the keys to the client */
        for(unsigned i=0; i < num_keys; i++) {
            if(true_ksizes[i] > 0) {
                hret = margo_bulk_transfer(mid, HG_BULK_PUSH, origin_addr,
                        in.keys_bulk_handle, remote_offset, keys_local_bulk, local_offset, true_ksizes[i]);
                if(hret != HG_SUCCESS) {
                    std::cerr << "Error: SDSKV list_keyvals could not issue bulk transfer (keys_local_bulk)" << std::endl;
                    throw SDSKV_ERR_MERCURY;
                }
            }
            remote_offset += remote_ksizes[i];
            local_offset  += true_ksizes[i];
        }

        remote_offset = 0;
        local_offset  = 0;

        /* transfer the values to the client */
        for(unsigned i=0; i < num_keys; i++) {
            if(true_vsizes[i] > 0) {
                hret = margo_bulk_transfer(mid, HG_BULK_PUSH, origin_addr,
                        in.vals_bulk_handle, remote_offset, vals_local_bulk, local_offset, true_vsizes[i]);
                if(hret != HG_SUCCESS) {
                    std::cerr << "Error: SDSKV list_keyvals could not issue bulk transfer (vals_local_bulk)" << std::endl;
                    throw SDSKV_ERR_MERCURY;
                }
            }
            remote_offset += remote_vsizes[i];
            local_offset  += true_vsizes[i];
        }

        out.nkeys = num_keys;
        out.ret = SDSKV_SUCCESS;

    } catch(int exc_no) {
        out.ret = exc_no;
    }

    margo_bulk_free(ksizes_local_bulk);
    margo_bulk_free(keys_local_bulk);
    margo_bulk_free(vsizes_local_bulk);
    margo_bulk_free(vals_local_bulk);
    margo_respond(handle, &out);
    margo_free_input(handle, &in);
    margo_destroy(handle); 

    return;
}
DEFINE_MARGO_RPC_HANDLER(sdskv_list_keyvals_ult)

static void sdskv_migrate_keys_ult(hg_handle_t handle)
{
    hg_return_t hret;
    migrate_keys_in_t in;
    migrate_keys_out_t out;
    out.ret = SDSKV_SUCCESS;

    auto r0 = at_exit([&handle]() { margo_destroy(handle); });
    auto r1 = at_exit([&handle,&out]() { margo_respond(handle, &out); });

    /* get the provider handling this request */
    margo_instance_id mid = margo_hg_handle_get_instance(handle);
    const struct hg_info* info = margo_get_info(handle);
    sdskv_provider_t provider = 
        (sdskv_provider_t)margo_registered_data(mid, info->id);
    if(!provider) {
        out.ret = SDSKV_ERR_UNKNOWN_PR;
        return;
    }
    /* get the input */
    hret = margo_get_input(handle, &in);
    if(hret != HG_SUCCESS) {
        out.ret = SDSKV_ERR_MERCURY;
        return;
    }
    auto r2 = at_exit([&handle,&in]() { margo_free_input(handle, &in); });
    /* find the source database */
    auto it = provider->databases.find(in.source_db_id);
    if(it == provider->databases.end()) {
        out.ret = SDSKV_ERR_UNKNOWN_DB;
        return;
    }
    auto database = it->second;
    /* lookup the address of the target provider */
    hg_addr_t target_addr = HG_ADDR_NULL;
    hret = margo_addr_lookup(mid, in.target_addr, &target_addr);
    if(hret != HG_SUCCESS) {
        out.ret = SDSKV_ERR_MERCURY;
        return;
    }

    /* create the bulk buffer to receive the keys */
    char *buffer = (char*)malloc(in.bulk_size);
    auto r4 = at_exit([&buffer]() { free(buffer); });
    hg_size_t bulk_size = in.bulk_size;
    hg_bulk_t bulk_handle = HG_BULK_NULL;
    hret = margo_bulk_create(mid, 1, (void**)&buffer, &bulk_size,
            HG_BULK_WRITE_ONLY, &bulk_handle);
    if(hret != HG_SUCCESS) {
        out.ret = SDSKV_ERR_MERCURY;
        return;
    }
    auto r5 = at_exit([&bulk_handle]() { margo_bulk_free(bulk_handle); });

    /* issue a bulk pull */
    hret = margo_bulk_transfer(mid, HG_BULK_PULL, info->addr, in.keys_bulk, 0,
            bulk_handle, 0, in.bulk_size);
    if(hret != HG_SUCCESS) {
        out.ret = SDSKV_ERR_MERCURY;
        return;
    }

    /* remap the keys from the void* buffer */
    hg_size_t* seg_sizes = (hg_size_t*)buffer;
    char* packed_keys = buffer + in.num_keys*sizeof(hg_size_t);

    /* create a handle for a "put" RPC */
    hg_handle_t put_handle;
    hret = margo_create(mid, target_addr, provider->sdskv_put_id, &put_handle);
    if(hret != HG_SUCCESS) {
        out.ret = SDSKV_ERR_MERCURY;
        return;
    }
    auto r6 = at_exit([&put_handle]() { margo_destroy(put_handle); });
    /* iterate over the keys */
    size_t offset = 0;
    put_in_t put_in;
    put_out_t put_out;
    for(unsigned i=0; i < in.num_keys; i++) {
        /* find the key */
        char* key = packed_keys + offset;
        size_t size = seg_sizes[i]; 
        offset += size;

        ds_bulk_t kdata(key, key+size);
        ds_bulk_t vdata;
        auto b = database->get(kdata, vdata);
        if(!b) continue;

        /* issue a "put" for that key */
        put_in.db_id      = in.target_db_id;
        put_in.key.data   = (kv_ptr_t)kdata.data();
        put_in.key.size   = kdata.size();
        put_in.value.data = (kv_ptr_t)vdata.data();
        put_in.value.size = vdata.size();
        /* forward put call */
        hret = margo_provider_forward(in.target_provider_id, put_handle, &put_in);
        if(hret != HG_SUCCESS) {
            out.ret = SDSKV_ERR_MIGRATION;
            return;
        }
        /* get output of the put call */
        hret = margo_get_output(put_handle, &put_out);
        if(hret != HG_SUCCESS || put_out.ret != SDSKV_SUCCESS) {
            out.ret = SDSKV_ERR_MIGRATION;
            return;
        }
        margo_free_output(put_handle, &out);
        /* remove the key if needed */
        if(in.flag == SDSKV_REMOVE_ORIGINAL) {
            database->erase(kdata);
        }
    }
}
DEFINE_MARGO_RPC_HANDLER(sdskv_migrate_keys_ult)

static void sdskv_migrate_key_range_ult(hg_handle_t handle)
{
    hg_return_t hret;
    migrate_key_range_in_t in;
    migrate_keys_out_t out;

    /* get the provider handling this request */
    margo_instance_id mid = margo_hg_handle_get_instance(handle);
    const struct hg_info* info = margo_get_info(handle);
    sdskv_provider_t provider = 
        (sdskv_provider_t)margo_registered_data(mid, info->id);
    if(!provider) {
        out.ret = SDSKV_ERR_UNKNOWN_PR;
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }
    /* get the input */
    hret = margo_get_input(handle, &in);
    if(hret != HG_SUCCESS) {
        out.ret = SDSKV_ERR_MERCURY;
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }

    // TODO implement this operation
    out.ret = SDSKV_OP_NOT_IMPL;
    margo_respond(handle, &out);

    margo_free_input(handle, &in);
    margo_destroy(handle);
}
DEFINE_MARGO_RPC_HANDLER(sdskv_migrate_key_range_ult)

static void sdskv_migrate_keys_prefixed_ult(hg_handle_t handle)
{
    hg_return_t hret;
    migrate_keys_prefixed_in_t in;
    migrate_keys_out_t out;

    /* need to destroy the handle at exit */
    auto r0 = at_exit([&handle]() { margo_destroy(handle); });
    /* need to respond at exit */
    auto r1 = at_exit([&handle,&out]() { margo_respond(handle, &out); });

    /* get the provider handling this request */
    margo_instance_id mid = margo_hg_handle_get_instance(handle);
    const struct hg_info* info = margo_get_info(handle);
    sdskv_provider_t provider = 
        (sdskv_provider_t)margo_registered_data(mid, info->id);
    if(!provider) {
        out.ret = SDSKV_ERR_UNKNOWN_PR;
        return;
    }
    /* get the input */
    hret = margo_get_input(handle, &in);
    if(hret != HG_SUCCESS) {
        out.ret = SDSKV_ERR_MERCURY;
        return;
    }
    /* need to destroy the input at exit */
    auto r2 = at_exit([&handle,&in]() { margo_free_input(handle, &in); });
    /* find the source database */
    auto it = provider->databases.find(in.source_db_id);
    if(it == provider->databases.end()) {
        out.ret = SDSKV_ERR_UNKNOWN_DB;
        return;
    }
    auto database = it->second;
    /* lookup the address of the target provider */
    hg_addr_t target_addr = HG_ADDR_NULL;
    hret = margo_addr_lookup(mid, in.target_addr, &target_addr);
    if(hret != HG_SUCCESS) {
        out.ret = SDSKV_ERR_MERCURY;
        return;
    }
    /* need to destroy the address at exit */
    auto r3 = at_exit([&mid,&target_addr]() { margo_addr_free(mid, target_addr); });
    /* create a handle for a "put" RPC */
    hg_handle_t put_handle;
    hret = margo_create(mid, target_addr, provider->sdskv_put_id, &put_handle);
    if(hret != HG_SUCCESS) {
        out.ret = SDSKV_ERR_MERCURY;
        return;
    }
    /* need to destroy the handle at exist */
    auto r4 = at_exit([&put_handle]() { margo_destroy(put_handle); });
    /* iterate over the keys by packets of 64 */
    /* XXX make this number configurable */
    std::vector<std::pair<ds_bulk_t,ds_bulk_t>> batch;
    ds_bulk_t start_key;
    ds_bulk_t prefix(in.key_prefix.data, in.key_prefix.data + in.key_prefix.size);
    do {
        try {
            batch = database->list_keyvals(start_key, 64, prefix);
        } catch(int err) {
            out.ret = err;
            return;
        }
        if(batch.size() == 0) 
            break;
        /* issue a put for all the keys in this batch */
        put_in_t put_in;
        put_out_t put_out;
        for(auto& kv : batch) {
            put_in.db_id  = in.target_db_id;
            put_in.key.data   = (kv_ptr_t)kv.first.data();
            put_in.key.size   = kv.first.size();
            put_in.value.data = (kv_ptr_t)kv.second.data();
            put_in.value.size = kv.second.size();
            /* forward put call */
            hret = margo_provider_forward(in.target_provider_id, put_handle, &put_in);
            if(hret != HG_SUCCESS) {
                out.ret = SDSKV_ERR_MIGRATION;
                return;
            }
            /* get output of the put call */
            hret = margo_get_output(put_handle, &put_out);
            if(hret != HG_SUCCESS || put_out.ret != SDSKV_SUCCESS) {
                out.ret = SDSKV_ERR_MIGRATION;
                return;
            }
            margo_free_output(put_handle, &out);
            /* remove the key if needed */
            if(in.flag == SDSKV_REMOVE_ORIGINAL) {
                database->erase(kv.first);
            }
        }
        /* if original is removed, start_key can stay empty since we
           keep taking the beginning of the container, otherwise
           we need to update start_key. */
        if(in.flag != SDSKV_REMOVE_ORIGINAL) {
            start_key = std::move(batch.rbegin()->first);
        }
    } while(batch.size() == 64);
}
DEFINE_MARGO_RPC_HANDLER(sdskv_migrate_keys_prefixed_ult)

static void sdskv_migrate_all_keys_ult(hg_handle_t handle)
{
    hg_return_t hret;
    migrate_all_keys_in_t in;
    migrate_keys_out_t out;
    
    /* need to destroy the handle at exit */
    auto r0 = at_exit([&handle]() { margo_destroy(handle); });
    /* need to respond at exit */
    auto r1 = at_exit([&handle,&out]() { margo_respond(handle, &out); });

    /* get the provider handling this request */
    margo_instance_id mid = margo_hg_handle_get_instance(handle);
    const struct hg_info* info = margo_get_info(handle);
    sdskv_provider_t provider = 
        (sdskv_provider_t)margo_registered_data(mid, info->id);
    if(!provider) {
        out.ret = SDSKV_ERR_UNKNOWN_PR;
        return;
    }
    /* get the input */
    hret = margo_get_input(handle, &in);
    if(hret != HG_SUCCESS) {
        out.ret = SDSKV_ERR_MERCURY;
        return;
    }
    /* need to destroy the input at exit */
    auto r2 = at_exit([&handle,&in]() { margo_free_input(handle, &in); });
    /* find the source database */
    auto it = provider->databases.find(in.source_db_id);
    if(it == provider->databases.end()) {
        out.ret = SDSKV_ERR_UNKNOWN_DB;
        return;
    }
    auto database = it->second;
    /* lookup the address of the target provider */
    hg_addr_t target_addr = HG_ADDR_NULL;
    hret = margo_addr_lookup(mid, in.target_addr, &target_addr);
    if(hret != HG_SUCCESS) {
        out.ret = SDSKV_ERR_MERCURY;
        return;
    }
    /* need to destroy the address at exit */
    auto r3 = at_exit([&mid,&target_addr]() { margo_addr_free(mid, target_addr); });
    /* create a handle for a "put" RPC */
    hg_handle_t put_handle;
    hret = margo_create(mid, target_addr, provider->sdskv_put_id, &put_handle);
    if(hret != HG_SUCCESS) {
        out.ret = SDSKV_ERR_MERCURY;
        return;
    }
    /* need to destroy the handle at exist */
    auto r4 = at_exit([&put_handle]() { margo_destroy(put_handle); });
    /* iterate over the keys by packets of 64 */
    /* XXX make this number configurable */
    std::vector<std::pair<ds_bulk_t,ds_bulk_t>> batch;
    ds_bulk_t start_key;
    do {
        try {
            batch = database->list_keyvals(start_key, 64);
        } catch(int err) {
            out.ret = err;
            return;
        }
        if(batch.size() == 0) 
            break;
        /* issue a put for all the keys in this batch */
        put_in_t put_in;
        put_out_t put_out;
        for(auto& kv : batch) {
            put_in.db_id  = in.target_db_id;
            put_in.key.data   = (kv_ptr_t)kv.first.data();
            put_in.key.size   = kv.first.size();
            put_in.value.data = (kv_ptr_t)kv.second.data();
            put_in.value.size = kv.second.size();
            /* forward put call */
            hret = margo_provider_forward(in.target_provider_id, put_handle, &put_in);
            if(hret != HG_SUCCESS) {
                out.ret = SDSKV_ERR_MIGRATION;
                return;
            }
            /* get output of the put call */
            hret = margo_get_output(put_handle, &put_out);
            if(hret != HG_SUCCESS || put_out.ret != SDSKV_SUCCESS) {
                out.ret = SDSKV_ERR_MIGRATION;
                return;
            }
            margo_free_output(put_handle, &out);
            /* remove the key if needed */
            if(in.flag == SDSKV_REMOVE_ORIGINAL) {
                database->erase(kv.first);
            }
        }
        /* if original is removed, start_key can stay empty since we
           keep taking the beginning of the container, otherwise
           we need to update start_key. */
        if(in.flag != SDSKV_REMOVE_ORIGINAL) {
            start_key = std::move(batch.rbegin()->first);
        }
    } while(batch.size() == 64);

}
DEFINE_MARGO_RPC_HANDLER(sdskv_migrate_all_keys_ult)

static void sdskv_migrate_database_ult(hg_handle_t handle)
{
    hg_return_t hret;
    migrate_database_in_t in;
    migrate_database_out_t out;

    /* get the provider handling this request */
    margo_instance_id mid = margo_hg_handle_get_instance(handle);
    const struct hg_info* info = margo_get_info(handle);
    sdskv_provider_t provider = 
        (sdskv_provider_t)margo_registered_data(mid, info->id);
    if(!provider) {
        out.ret = SDSKV_ERR_UNKNOWN_PR;
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }
    /* get the input */
    hret = margo_get_input(handle, &in);
    if(hret != HG_SUCCESS) {
        out.ret = SDSKV_ERR_MERCURY;
        margo_respond(handle, &out);
        margo_destroy(handle);
        return;
    }
    
    // TODO implement this operation
    out.ret = SDSKV_OP_NOT_IMPL;
    out.db_id = 0;
    margo_respond(handle, &out);

    margo_free_input(handle, &in);
    margo_destroy(handle);
}
DEFINE_MARGO_RPC_HANDLER(sdskv_migrate_database_ult)

#if 0
static void __migrate_keys_ult(hg_handle_t handle)
{
    // TODO
}
DEFINE_MARGO_RPC_HANDLER(__migrate_keys_ult)

static void __migrate_key_range_ult(hg_handle_t handle)
{
    // TODO
}
DEFINE_MARGO_RPC_HANDLER(__migrate_key_range_ult)

static void __migrate_keys_prefixed_ult(hg_handle_t handle)
{
    // TODO
}
DEFINE_MARGO_RPC_HANDLER(__migrate_keys_prefixed_ult)

static void __migrate_all_keys_ult(hg_handle_t handle)
{
    // TODO
}
DEFINE_MARGO_RPC_HANDLER(__migrate_all_keys_ult)

static void __migrate_database_ult(hg_handle_t handle)
{
    // TODO
}
DEFINE_MARGO_RPC_HANDLER(__migrate_database_ult)
#endif

static void sdskv_server_finalize_cb(void *data)
{
    sdskv_provider_t svr_ctx = (sdskv_provider_t)data;
    assert(svr_ctx);

    sdskv_provider_remove_all_databases(svr_ctx);

    delete svr_ctx;

    return;
}

