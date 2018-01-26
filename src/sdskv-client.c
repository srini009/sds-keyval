#include "sdskv-client.h"
#include "sdskv-rpc-types.h"

#define MAX_RPC_MESSAGE_SIZE 4000 // in bytes

struct sdskv_client {
    margo_instance_id mid;

    hg_id_t sdskv_put_id;
    hg_id_t sdskv_bulk_put_id;
    hg_id_t sdskv_get_id;
    hg_id_t sdskv_length_id;
    hg_id_t sdskv_bulk_get_id;
    hg_id_t sdskv_open_id;
    hg_id_t sdskv_list_id;

    uint64_t num_provider_handles;
};

struct sdskv_provider_handle {
    sdskv_client_t client;
    hg_addr_t      addr;
    uint8_t        mplex_id;
    uint64_t       refcount;
};

static int sdskv_client_register(sdskv_client_t client, margo_instance_id mid)
{
    client->mid = mid;

    /* check if RPCs have already been registered */
    hg_bool_t flag;
    hg_id_t id;
    margo_registered_name(mid, "sdskv_put_rpc", &id, &flag);

    if(flag == HG_TRUE) { /* RPCs already registered */

        margo_registered_name(mid, "sdskv_put_rpc",      &client->sdskv_put_id,      &flag);
        margo_registered_name(mid, "sdskv_bulk_put_rpc", &client->sdskv_bulk_put_id, &flag);
        margo_registered_name(mid, "sdskv_get_rpc",      &client->sdskv_get_id,      &flag);
        margo_registered_name(mid, "sdskv_length_rpc",   &client->sdskv_length_id,   &flag);
        margo_registered_name(mid, "sdskv_bulk_get_rpc", &client->sdskv_bulk_get_id, &flag);
        margo_registered_name(mid, "sdskv_open_rpc",     &client->sdskv_open_id,     &flag);
        margo_registered_name(mid, "sdskv_list_rpc",     &client->sdskv_list_id,     &flag);

    } else {

        client->sdskv_put_id =
            MARGO_REGISTER(mid, "sdskv_put_rpc", put_in_t, put_out_t, NULL);
        client->sdskv_bulk_put_id =
            MARGO_REGISTER(mid, "sdskv_bulk_put_rpc", bulk_put_in_t, bulk_put_out_t, NULL);
        client->sdskv_get_id =
            MARGO_REGISTER(mid, "sdskv_get_rpc", get_in_t, get_out_t, NULL);
        client->sdskv_length_id =
            MARGO_REGISTER(mid, "sdskv_length_rpc", length_in_t, length_out_t, NULL);
        client->sdskv_bulk_get_id =
            MARGO_REGISTER(mid, "sdskv_bulk_get_rpc", bulk_get_in_t, bulk_get_out_t, NULL);
        client->sdskv_open_id =
            MARGO_REGISTER(mid, "sdskv_open_rpc", open_in_t, open_out_t, NULL);
        client->sdskv_list_id =
            MARGO_REGISTER(mid, "sdskv_list_rpc", list_in_t, list_out_t, NULL);
    }

    return 0;
}

int sdskv_client_init(margo_instance_id mid, sdskv_client_t* client)
{
    sdskv_client_t c = (sdskv_client_t)calloc(1, sizeof(*c));
    if(!c) return -1;

    c->num_provider_handles = 0;

    int ret = sdskv_client_register(c, mid);
    if(ret != 0) return ret;

    *client = c;
    return 0;
}

int sdskv_client_finalize(sdskv_client_t client)
{
    if(client->num_provider_handles != 0) {
        fprintf(stderr,
                "[SDSKV] Warning: %d provider handles not released before sdskv_client_finalize was called\n",
                client->num_provider_handles);
    }
    free(client);
    return 0;
}

int sdskv_provider_handle_create(
        sdskv_client_t client,
        hg_addr_t addr,
        uint8_t mplex_id,
        sdskv_provider_handle_t* handle)
{
    if(client == SDSKV_CLIENT_NULL) return -1;

    sdskv_provider_handle_t provider =
        (sdskv_provider_handle_t)calloc(1, sizeof(*provider));

    if(!provider) return -1;

    hg_return_t ret = margo_addr_dup(client->mid, addr, &(provider->addr));
    if(ret != HG_SUCCESS) {
        free(provider);
        return -1;
    }

    provider->client   = client;
    provider->mplex_id = mplex_id;
    provider->refcount = 1;

    client->num_provider_handles += 1;

    *handle = provider;
    return 0;
}

int sdskv_provider_handle_ref_incr(
        sdskv_provider_handle_t handle)
{
    if(handle == SDSKV_PROVIDER_HANDLE_NULL) return -1;
    handle->refcount += 1;
    return 0;
}

int sdskv_provider_release(sdskv_provider_handle_t handle)
{
    if(handle == SDSKV_PROVIDER_HANDLE_NULL) return -1;
    handle->refcount -= 1;
    if(handle->refcount == 0) {
        margo_addr_free(handle->client->mid, handle->addr);
        handle->client->num_provider_handles -= 1;
        free(handle);
    }
    return 0;
}

int sdskv_open(
        sdskv_provider_handle_t provider,
        const char* db_name,
        sdskv_database_id_t* db_id)
{
    hg_return_t hret;
    int ret;
    open_in_t in;
    open_out_t out;
    hg_handle_t handle;

    /* create handle */
    hret = margo_create(
            provider->client->mid,
            provider->addr,
            provider->client->sdskv_open_id,
            &handle);
    if(hret != HG_SUCCESS) return -1;

    hret = margo_set_target_id(handle, provider->mplex_id);

    if(hret != HG_SUCCESS) {
        margo_destroy(handle);
        return -1;
    }

    hret = margo_forward(handle, &in);
    if(hret != HG_SUCCESS) {
        margo_destroy(handle);
        return -1;
    }

    hret = margo_get_output(handle, &out);
    if(hret != HG_SUCCESS) {
        margo_destroy(handle);
        return -1;
    }

    ret = out.ret;
    *db_id = out.db_id;

    margo_free_output(handle, &out);
    margo_destroy(handle);

    return ret;
}

int sdskv_put(sdskv_provider_handle_t provider, 
        sdskv_database_id_t db_id,
        const void *key, hg_size_t ksize,
        const void *value, hg_size_t vsize)
{
    hg_return_t hret;
    int ret;
    hg_handle_t handle;

    hg_size_t msize = ksize + vsize + 2*sizeof(hg_size_t);

    if(msize <= MAX_RPC_MESSAGE_SIZE) {

        put_in_t in;
        put_out_t out;

        in.db_id = db_id;
        in.key   = (kv_data_t)key;
        in.ksize = ksize;
        in.value = (kv_data_t)value;
        in.vsize = vsize;

        /* create handle */
        hret = margo_create(
                provider->client->mid,
                provider->addr,
                provider->client->sdskv_put_id,
                &handle);
        if(hret != HG_SUCCESS) return -1;

        hret = margo_set_target_id(handle, provider->mplex_id);

        if(hret != HG_SUCCESS) {
            margo_destroy(handle);
            return -1;
        }

        hret = margo_forward(handle, &in);
        if(hret != HG_SUCCESS) {
            margo_destroy(handle);
            return -1;
        }

        hret = margo_get_output(handle, &out);
        if(hret != HG_SUCCESS) {
            margo_destroy(handle);
            return -1;
        }

        ret = out.ret;

        margo_free_output(handle, &out);

    } else {

        bulk_put_in_t in;
        bulk_put_out_t out;

        in.bulk.db_id = db_id;
        in.bulk.key   = (kv_data_t)key;
        in.bulk.ksize = ksize;
        in.bulk.vsize = vsize;

        hret = margo_bulk_create(provider->client->mid, 1, (void**)(&value), &in.bulk.vsize,
                                HG_BULK_READ_ONLY, &in.bulk.handle);
        if(ret != HG_SUCCESS) return -1;

        /* create handle */
        hret = margo_create(
                provider->client->mid,
                provider->addr,
                provider->client->sdskv_bulk_put_id,
                &handle);
        if(hret != HG_SUCCESS) {
            margo_bulk_free(in.bulk.handle);
            return -1;
        }

        hret = margo_set_target_id(handle, provider->mplex_id);

        if(hret != HG_SUCCESS) {
            margo_bulk_free(in.bulk.handle);
            margo_destroy(handle);
            return -1;
        }

        hret = margo_forward(handle, &in);
        if(hret != HG_SUCCESS) {
            margo_bulk_free(in.bulk.handle);
            margo_destroy(handle);
            return -1;
        }

        hret = margo_get_output(handle, &out);
        if(hret != HG_SUCCESS) {
            margo_bulk_free(in.bulk.handle);
            margo_destroy(handle);
            return -1;
        }

        ret = out.ret;
        margo_free_output(handle, &out);
        margo_bulk_free(in.bulk.handle);
    }

    margo_destroy(handle);
    return ret;
}

int sdskv_get(sdskv_provider_handle_t provider,
        sdskv_database_id_t db_id, 
        const void *key, hg_size_t ksize,
        void *value, hg_size_t* vsize)
{
    hg_return_t hret;
    hg_size_t size;
    hg_size_t msize;
    int ret;
    hg_handle_t handle;

    size = *(hg_size_t*)vsize;
    msize = size + sizeof(hg_size_t) + sizeof(hg_return_t);

    if (msize <= MAX_RPC_MESSAGE_SIZE) {

        get_in_t in;
        get_out_t out;

        in.db_id = db_id;
        in.key = (kv_data_t)key;
        in.ksize = ksize;
        in.vsize = size;

        /* create handle */
        hret = margo_create(
                provider->client->mid,
                provider->addr,
                provider->client->sdskv_get_id,
                &handle);
        if(hret != HG_SUCCESS) return -1;

        hret = margo_set_target_id(handle, provider->mplex_id);

        if(hret != HG_SUCCESS) {
            margo_destroy(handle);
            return -1;
        }

        hret = margo_forward(handle, &in);
        if(hret != HG_SUCCESS) {
            margo_destroy(handle);
            return -1;
        }

        hret = margo_get_output(handle, &out);
        if(hret != HG_SUCCESS) {
            margo_destroy(handle);
            return -1;
        }

        ret = out.ret;
        *vsize = (hg_size_t)out.vsize;

        if (out.vsize > 0) {
            memcpy(value, out.value, out.vsize);
        }

        margo_free_output(handle, &out);

    } else {

        bulk_get_in_t in;
        bulk_get_out_t out;

        in.bulk.db_id = db_id;
        in.bulk.key = (kv_data_t)key;
        in.bulk.ksize = ksize;
        in.bulk.vsize = size;

        hret = margo_bulk_create(provider->client->mid, 1, &value, &in.bulk.vsize,
                                HG_BULK_WRITE_ONLY, &in.bulk.handle);
        if(hret != HG_SUCCESS) return -1;

        /* create handle */
        hret = margo_create(
                provider->client->mid,
                provider->addr,
                provider->client->sdskv_bulk_get_id,
                &handle);
        if(hret != HG_SUCCESS) {
            margo_bulk_free(in.bulk.handle);
            return -1;
        }

        hret = margo_set_target_id(handle, provider->mplex_id);
        if(hret != HG_SUCCESS) {
            margo_bulk_free(in.bulk.handle);
            margo_destroy(handle);
            return -1;
        }

        hret = margo_forward(handle, &in);
        if(hret != HG_SUCCESS) {
            margo_bulk_free(in.bulk.handle);
            margo_destroy(handle);
            return -1;
        }

        hret = margo_get_output(handle, &out);
        if(hret != HG_SUCCESS) {
            margo_bulk_free(in.bulk.handle);
            margo_destroy(handle);
            return -1;
        }

        ret = out.ret;
        *vsize = (hg_size_t)out.size;

        margo_free_output(handle, &out);
        margo_bulk_free(in.bulk.handle);
    }

    margo_destroy(handle);
    return ret;
}

int sdskv_length(sdskv_provider_handle_t provider, 
        sdskv_database_id_t db_id, const void *key, 
        hg_size_t ksize, hg_size_t* vsize)
{
    hg_return_t hret;
    int ret;
    hg_handle_t handle;

    length_in_t in;
    length_out_t out;

    in.db_id = db_id;
    in.key   = (kv_data_t)key;
    in.ksize = ksize;

    /* create handle */
    hret = margo_create(
            provider->client->mid,
            provider->addr,
            provider->client->sdskv_length_id,
            &handle);
    if(hret != HG_SUCCESS) return -1;

    hret = margo_set_target_id(handle, provider->mplex_id);
    if(hret != HG_SUCCESS) {
        margo_destroy(handle);
        return -1;
    }

    hret = margo_forward(handle, &in);
    if(hret != HG_SUCCESS) {
        margo_destroy(handle);
        return -1;
    }

    hret = margo_get_output(handle, &out);
    if(hret != HG_SUCCESS) {
        margo_destroy(handle);
        return -1;
    }

    ret = out.ret;
    if(ret == 0) *vsize = out.size;

    margo_free_output(handle, &out);
    margo_destroy(handle);
    return ret;
}

int sdskv_list_keys(sdskv_provider_handle_t provider,
        sdskv_database_id_t db_id,  // db instance
        const void *start_key,  // we want keys strictly after this start_key
        hg_size_t start_ksize,  // size of the start_key
        void **keys,            // pointer to an array of void* pointers,
        //     this array has size *max_keys
        hg_size_t* ksizes,      // pointer to an array of hg_size_t sizes
        //    representing sizes allocated in
        //     keys for each key
        hg_size_t* max_keys)   // maximum number of keys requested
{
    list_in_t  in;
    list_out_t out;
    hg_return_t hret = HG_SUCCESS;
    hg_handle_t handle;
    int ret;
    int i;

    in.db_id = db_id;
    in.start_key = (kv_data_t) start_key;
    in.start_ksize = start_ksize;
    in.max_keys = *max_keys;

    /* create handle */
    hret = margo_create(
            provider->client->mid,
            provider->addr,
            provider->client->sdskv_list_id,
            &handle);
    if(hret != HG_SUCCESS) return -1;

    hret = margo_set_target_id(handle, provider->mplex_id);
    if(hret != HG_SUCCESS) {
        margo_destroy(handle);
        return -1;
    }

    hret = margo_forward(handle, &in);
    if(hret != HG_SUCCESS) {
        margo_destroy(handle);
        return -1;
    }

    hret = margo_get_output(handle, &out);
    if(hret != HG_SUCCESS) {
        margo_destroy(handle);
        return -1;
    }

    *max_keys = out.nkeys;
    ret = out.ret;

    for (i=0; i < out.nkeys; i++) {
        ksizes[i] = out.ksizes[i];
        memcpy(keys[i], out.keys[i], out.ksizes[i]);
    }
    margo_free_output(handle, &out);

    margo_destroy(handle);

    return ret;
}

int sdskv_list_keys_with_prefix(sdskv_provider_handle_t provider,
        sdskv_database_id_t db_id,  // db instance
        const void *start_key,  // we want keys strictly after this start_key
        hg_size_t start_ksize,  // size of the start_key
        const void *prefix,     // return only keys that begin with 'prefix'
        hg_size_t prefix_size,
        void **keys,            // pointer to an array of void* pointers,
        // this array has size *max_keys
        hg_size_t* ksizes,      // pointer to an array of hg_size_t sizes 
        // representing sizes allocated in
        // keys for each key
        hg_size_t* max_keys)   // maximum number of keys requested
{
    // TODO
}

int sdskv_shutdown_service(sdskv_client_t client, hg_addr_t addr)
{
    return margo_shutdown_remote_instance(client->mid, addr);
}
