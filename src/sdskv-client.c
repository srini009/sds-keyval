#include "sdskv-client.h"
#include "sdskv-rpc-types.h"

#define MAX_RPC_MESSAGE_SIZE 4000 // in bytes

struct sdskv_client {
    margo_instance_id mid;

    hg_id_t sdskv_put_id;
    hg_id_t sdskv_bulk_put_id;
    hg_id_t sdskv_get_id;
    hg_id_t sdskv_erase_id;
    hg_id_t sdskv_length_id;
    hg_id_t sdskv_bulk_get_id;
    hg_id_t sdskv_open_id;
    hg_id_t sdskv_list_keys_id;
    hg_id_t sdskv_list_keyvals_id;

    uint64_t num_provider_handles;
};

struct sdskv_provider_handle {
    sdskv_client_t client;
    hg_addr_t      addr;
    uint8_t        provider_id;
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
        margo_registered_name(mid, "sdskv_erase_rpc",    &client->sdskv_erase_id,    &flag);
        margo_registered_name(mid, "sdskv_length_rpc",   &client->sdskv_length_id,   &flag);
        margo_registered_name(mid, "sdskv_bulk_get_rpc", &client->sdskv_bulk_get_id, &flag);
        margo_registered_name(mid, "sdskv_open_rpc",     &client->sdskv_open_id,     &flag);
        margo_registered_name(mid, "sdskv_list_keys_rpc", &client->sdskv_list_keys_id, &flag);
        margo_registered_name(mid, "sdskv_list_keyvals_rpc", &client->sdskv_list_keyvals_id, &flag);

    } else {

        client->sdskv_put_id =
            MARGO_REGISTER(mid, "sdskv_put_rpc", put_in_t, put_out_t, NULL);
        client->sdskv_bulk_put_id =
            MARGO_REGISTER(mid, "sdskv_bulk_put_rpc", bulk_put_in_t, bulk_put_out_t, NULL);
        client->sdskv_get_id =
            MARGO_REGISTER(mid, "sdskv_get_rpc", get_in_t, get_out_t, NULL);
        client->sdskv_erase_id =
            MARGO_REGISTER(mid, "sdskv_erase_rpc", erase_in_t, erase_out_t, NULL);
        client->sdskv_length_id =
            MARGO_REGISTER(mid, "sdskv_length_rpc", length_in_t, length_out_t, NULL);
        client->sdskv_bulk_get_id =
            MARGO_REGISTER(mid, "sdskv_bulk_get_rpc", bulk_get_in_t, bulk_get_out_t, NULL);
        client->sdskv_open_id =
            MARGO_REGISTER(mid, "sdskv_open_rpc", open_in_t, open_out_t, NULL);
        client->sdskv_list_keys_id =
            MARGO_REGISTER(mid, "sdskv_list_keys_rpc", list_keys_in_t, list_keys_out_t, NULL);
        client->sdskv_list_keyvals_id =
            MARGO_REGISTER(mid, "sdskv_list_keyvals_rpc", list_keyvals_in_t, list_keyvals_out_t, NULL);
    }

    return SDSKV_SUCCESS;
}

int sdskv_client_init(margo_instance_id mid, sdskv_client_t* client)
{
    sdskv_client_t c = (sdskv_client_t)calloc(1, sizeof(*c));
    if(!c) return SDSKV_ERR_ALLOCATION;

    c->num_provider_handles = 0;

    int ret = sdskv_client_register(c, mid);
    if(ret != 0) return ret;

    *client = c;
    return SDSKV_SUCCESS;
}

int sdskv_client_finalize(sdskv_client_t client)
{
    if(client->num_provider_handles != 0) {
        fprintf(stderr,
                "[SDSKV] Warning: %d provider handles not released before sdskv_client_finalize was called\n",
                client->num_provider_handles);
    }
    free(client);
    return SDSKV_SUCCESS;
}

int sdskv_provider_handle_create(
        sdskv_client_t client,
        hg_addr_t addr,
        uint16_t provider_id,
        sdskv_provider_handle_t* handle)
{
    if(client == SDSKV_CLIENT_NULL) 
        return SDSKV_ERR_INVALID_ARG;

    sdskv_provider_handle_t provider =
        (sdskv_provider_handle_t)calloc(1, sizeof(*provider));

    if(!provider) return SDSKV_ERR_ALLOCATION;

    hg_return_t ret = margo_addr_dup(client->mid, addr, &(provider->addr));
    if(ret != HG_SUCCESS) {
        free(provider);
        return SDSKV_ERR_MERCURY;
    }

    provider->client      = client;
    provider->provider_id = provider_id;
    provider->refcount    = 1;

    client->num_provider_handles += 1;

    *handle = provider;
    return SDSKV_SUCCESS;
}

int sdskv_provider_handle_ref_incr(
        sdskv_provider_handle_t handle)
{
    if(handle == SDSKV_PROVIDER_HANDLE_NULL) return SDSKV_ERR_INVALID_ARG;
    handle->refcount += 1;
    return SDSKV_SUCCESS;
}

int sdskv_provider_handle_release(sdskv_provider_handle_t handle)
{
    if(handle == SDSKV_PROVIDER_HANDLE_NULL) return -1;
    handle->refcount -= 1;
    if(handle->refcount == 0) {
        margo_addr_free(handle->client->mid, handle->addr);
        handle->client->num_provider_handles -= 1;
        free(handle);
    }
    return SDSKV_SUCCESS;
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
    if(hret != HG_SUCCESS) return SDSKV_ERR_MERCURY;

    in.name = (char*)db_name;

    hret = margo_provider_forward(provider->provider_id, handle, &in);
    if(hret != HG_SUCCESS) {
        margo_destroy(handle);
        return SDSKV_ERR_MERCURY;
    }

    hret = margo_get_output(handle, &out);
    if(hret != HG_SUCCESS) {
        margo_destroy(handle);
        return SDSKV_ERR_MERCURY;
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
        in.key.data = (kv_ptr_t)key;
        in.key.size = ksize;
        in.value.data = (kv_ptr_t)value;
        in.value.size = vsize;

        /* create handle */
        hret = margo_create(
                provider->client->mid,
                provider->addr,
                provider->client->sdskv_put_id,
                &handle);
        if(hret != HG_SUCCESS) {
            fprintf(stderr,"[SDSKV] margo_create() failed in sdskv_put()\n");
            return SDSKV_ERR_MERCURY;
        }

        hret = margo_provider_forward(provider->provider_id, handle, &in);
        if(hret != HG_SUCCESS) {
            fprintf(stderr,"[SDSKV] margo_forward() failed in sdskv_put()\n");
            margo_destroy(handle);
            return SDSKV_ERR_MERCURY;
        }

        hret = margo_get_output(handle, &out);
        if(hret != HG_SUCCESS) {
            fprintf(stderr,"[SDSKV] margo_get_output() failed in sdskv_put()\n");
            margo_destroy(handle);
            return SDSKV_ERR_MERCURY;
        }

        ret = out.ret;

        margo_free_output(handle, &out);

    } else {

        bulk_put_in_t in;
        bulk_put_out_t out;

        in.db_id = db_id;
        in.key.data = (kv_ptr_t)key;
        in.key.size = ksize;
        in.vsize = vsize;

        hret = margo_bulk_create(provider->client->mid, 1, (void**)(&value), &in.vsize,
                                HG_BULK_READ_ONLY, &in.handle);
        if(hret != HG_SUCCESS) {
            fprintf(stderr,"[SDSKV] margo_bulk_create() failed in sdskv_put()\n");
            return SDSKV_ERR_MERCURY;
        }

        /* create handle */
        hret = margo_create(
                provider->client->mid,
                provider->addr,
                provider->client->sdskv_bulk_put_id,
                &handle);
        if(hret != HG_SUCCESS) {
            fprintf(stderr,"[SDSKV] margo_create() failed in sdskv_put()\n");
            margo_bulk_free(in.handle);
            return SDSKV_ERR_MERCURY;
        }

        hret = margo_provider_forward(provider->provider_id, handle, &in);
        if(hret != HG_SUCCESS) {
            fprintf(stderr,"[SDSKV] margo_forward() failed in sdskv_put()\n");
            margo_bulk_free(in.handle);
            margo_destroy(handle);
            return SDSKV_ERR_MERCURY;
        }

        hret = margo_get_output(handle, &out);
        if(hret != HG_SUCCESS) {
            fprintf(stderr,"[SDSKV] margo_get_output() failed in sdskv_put()\n");
            margo_bulk_free(in.handle);
            margo_destroy(handle);
            return SDSKV_ERR_MERCURY;
        }

        ret = out.ret;
        margo_free_output(handle, &out);
        margo_bulk_free(in.handle);
    }

    margo_destroy(handle);
    return SDSKV_SUCCESS;
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
        in.key.data = (kv_ptr_t)key;
        in.key.size = ksize;
        in.vsize = size;

        /* create handle */
        hret = margo_create(
                provider->client->mid,
                provider->addr,
                provider->client->sdskv_get_id,
                &handle);
        if(hret != HG_SUCCESS) return SDSKV_ERR_MERCURY;

        hret = margo_provider_forward(provider->provider_id, handle, &in);
        if(hret != HG_SUCCESS) {
            margo_destroy(handle);
            return SDSKV_ERR_MERCURY;
        }

        hret = margo_get_output(handle, &out);
        if(hret != HG_SUCCESS) {
            margo_destroy(handle);
            return SDSKV_ERR_MERCURY;
        }

        ret = out.ret;
        *vsize = (hg_size_t)out.value.size;

        if (out.value.size > 0) {
            memcpy(value, out.value.data, out.value.size);
        }

        margo_free_output(handle, &out);

    } else {

        bulk_get_in_t in;
        bulk_get_out_t out;

        in.db_id = db_id;
        in.key.data = (kv_ptr_t)key;
        in.key.size = ksize;
        in.vsize = size;

        hret = margo_bulk_create(provider->client->mid, 1, &value, &in.vsize,
                                HG_BULK_WRITE_ONLY, &in.handle);
        if(hret != HG_SUCCESS) return SDSKV_ERR_MERCURY;

        /* create handle */
        hret = margo_create(
                provider->client->mid,
                provider->addr,
                provider->client->sdskv_bulk_get_id,
                &handle);
        if(hret != HG_SUCCESS) {
            margo_bulk_free(in.handle);
            return SDSKV_ERR_MERCURY;
        }

        hret = margo_provider_forward(provider->provider_id, handle, &in);
        if(hret != HG_SUCCESS) {
            margo_bulk_free(in.handle);
            margo_destroy(handle);
            return SDSKV_ERR_MERCURY;
        }

        hret = margo_get_output(handle, &out);
        if(hret != HG_SUCCESS) {
            margo_bulk_free(in.handle);
            margo_destroy(handle);
            return SDSKV_ERR_MERCURY;
        }

        ret = out.ret;
        *vsize = (hg_size_t)out.size;

        margo_free_output(handle, &out);
        margo_bulk_free(in.handle);
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

    in.db_id    = db_id;
    in.key.data = (kv_ptr_t)key;
    in.key.size = ksize;

    /* create handle */
    hret = margo_create(
            provider->client->mid,
            provider->addr,
            provider->client->sdskv_length_id,
            &handle);
    if(hret != HG_SUCCESS) return SDSKV_ERR_MERCURY;

    hret = margo_provider_forward(provider->provider_id, handle, &in);
    if(hret != HG_SUCCESS) {
        margo_destroy(handle);
        return SDSKV_ERR_MERCURY;
    }

    hret = margo_get_output(handle, &out);
    if(hret != HG_SUCCESS) {
        margo_destroy(handle);
        return SDSKV_ERR_MERCURY;
    }

    ret = out.ret;
    if(ret == 0) *vsize = out.size;

    margo_free_output(handle, &out);
    margo_destroy(handle);
    return ret;
}

int sdskv_erase(sdskv_provider_handle_t provider,
        sdskv_database_id_t db_id, const void *key,
        hg_size_t ksize)
{
    hg_return_t hret;
    int ret;
    hg_handle_t handle;

    erase_in_t in;
    erase_out_t out;

    in.db_id = db_id;
    in.key.data   = (kv_ptr_t)key;
    in.key.size = ksize;

    /* create handle */
    hret = margo_create(
            provider->client->mid,
            provider->addr,
            provider->client->sdskv_erase_id,
            &handle);
    if(hret != HG_SUCCESS) return SDSKV_ERR_MERCURY;

    hret = margo_provider_forward(provider->provider_id, handle, &in);
    if(hret != HG_SUCCESS) {
        margo_destroy(handle);
        return SDSKV_ERR_MERCURY;
    }

    hret = margo_get_output(handle, &out);
    if(hret != HG_SUCCESS) {
        margo_destroy(handle);
        return SDSKV_ERR_MERCURY;
    }

    ret = out.ret;

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
    return sdskv_list_keys_with_prefix(provider,
            db_id,
            start_key,
            start_ksize,
            NULL,
            0,
            keys,
            ksizes,
            max_keys);
}

int sdskv_list_keys_with_prefix(sdskv_provider_handle_t provider,
        sdskv_database_id_t db_id,  // db instance
        const void *start_key,  // we want keys strictly after this start_key
        hg_size_t start_ksize,  // size of the start_key
        const void *prefix,
        hg_size_t prefix_size,
        void **keys,            // pointer to an array of void* pointers,
        //     this array has size *max_keys
        hg_size_t* ksizes,      // pointer to an array of hg_size_t sizes
        //    representing sizes allocated in
        //     keys for each key
        hg_size_t* max_keys)   // maximum number of keys requested
{
    list_keys_in_t  in;
    list_keys_out_t out;
    hg_return_t hret      = HG_SUCCESS;
    hg_handle_t handle    = HG_HANDLE_NULL;
    int ret = SDSKV_SUCCESS;
    int i;

    in.db_id = db_id;
    in.start_key.data = (kv_ptr_t) start_key;
    in.start_key.size = start_ksize;
    in.prefix.data = (char*)prefix;
    in.prefix.size = prefix_size;
    in.keys_bulk_handle   = HG_BULK_NULL;
    in.ksizes_bulk_handle = HG_BULK_NULL;
    in.max_keys = *max_keys;

    /* create bulk handle to expose the segments with key sizes */
    hg_size_t ksize_bulk_size = (*max_keys)*sizeof(*ksizes);
    void* buf_ptr[1] = { ksizes };
    hret = margo_bulk_create(provider->client->mid,
                             1, buf_ptr, &ksize_bulk_size,
                             HG_BULK_READWRITE,
                             &in.ksizes_bulk_handle);
    if(hret != HG_SUCCESS) {
        ret = SDSKV_ERR_MERCURY;
        goto finish;
    }

    /* create bulk handle to expose where the keys should be placed */
    hret = margo_bulk_create(provider->client->mid,
                             *max_keys, keys, ksizes,
                             HG_BULK_WRITE_ONLY,
                             &in.keys_bulk_handle);
    if(hret != HG_SUCCESS) {
        ret = SDSKV_ERR_MERCURY;
        goto finish;
    }


    /* create handle */
    hret = margo_create(
            provider->client->mid,
            provider->addr,
            provider->client->sdskv_list_keys_id,
            &handle);
    if(hret != HG_SUCCESS) {
        ret = SDSKV_ERR_MERCURY;
        goto finish;
    }

    /* forward to provider */
    hret = margo_provider_forward(provider->provider_id, handle, &in);
    if(hret != HG_SUCCESS) {
        ret = SDSKV_ERR_MERCURY;
        goto finish;
    }

    /* get the output from provider */
    hret = margo_get_output(handle, &out);
    if(hret != HG_SUCCESS) {
        ret = SDSKV_ERR_MERCURY;
        goto finish;
    }

    /* set return values */
    *max_keys = out.nkeys;
    ret = out.ret;

finish:
    /* free everything we created */
    margo_bulk_free(in.ksizes_bulk_handle);
    margo_bulk_free(in.keys_bulk_handle);
    margo_free_output(handle, &out);
    margo_destroy(handle);

    return ret;
}

int sdskv_list_keyvals(sdskv_provider_handle_t provider,
        sdskv_database_id_t db_id,  // db instance
        const void *start_key,  // we want keys strictly after this start_key
        hg_size_t start_ksize,  // size of the start_key
        void **keys,            // pointer to an array of void* pointers,
                                //     this array has size *max_keys
        hg_size_t* ksizes,      // pointer to an array of hg_size_t sizes
                                //    representing sizes allocated in
                                //     keys for each key
        void **values,          // pointer to an array of void* pointers,
                                //     this array has size *max_keys
        hg_size_t* vsizes,      // pointer to an array of hg_size_t sizes
                                //    representing sizes allocated in
                                //    values for each value
        hg_size_t* max_keys)    // maximum number of keys requested
{
    return sdskv_list_keyvals_with_prefix(
            provider,
            db_id,
            start_key,
            start_ksize,
            NULL,
            0,
            keys,
            ksizes,
            values,
            vsizes,
            max_keys);
}

int sdskv_list_keyvals_with_prefix(sdskv_provider_handle_t provider,
        sdskv_database_id_t db_id,  // db instance
        const void *start_key,  // we want keys strictly after this start_key
        hg_size_t start_ksize,  // size of the start_key
        const void* prefix,     // prefix of returned keys
        hg_size_t prefix_size,  // size of prefix
        void **keys,            // pointer to an array of void* pointers,
                                //     this array has size *max_keys
        hg_size_t* ksizes,      // pointer to an array of hg_size_t sizes
                                //    representing sizes allocated in
                                //     keys for each key
        void **values,          // pointer to an array of void* pointers,
                                //     this array has size *max_keys
        hg_size_t* vsizes,      // pointer to an array of hg_size_t sizes
                                //    representing sizes allocated in
                                //    values for each value
        hg_size_t* max_keys)    // maximum number of keys requested
{
    list_keyvals_in_t  in;
    list_keyvals_out_t out;
    hg_return_t hret      = HG_SUCCESS;
    hg_handle_t handle    = HG_HANDLE_NULL;
    int ret = SDSKV_SUCCESS;
    int i;

    in.db_id = db_id;
    in.start_key.data = (kv_ptr_t) start_key;
    in.start_key.size = start_ksize;
    in.prefix.data = (char*)prefix;
    in.prefix.size = prefix_size;
    in.max_keys = *max_keys;
    in.keys_bulk_handle   = HG_BULK_NULL;
    in.ksizes_bulk_handle = HG_BULK_NULL;
    in.vals_bulk_handle   = HG_BULK_NULL;
    in.vsizes_bulk_handle = HG_BULK_NULL;

    /* create bulk handle to expose the segments with key sizes */
    hg_size_t ksize_bulk_size = (*max_keys)*sizeof(*ksizes);
    void* ksizes_buf_ptr[1] = { ksizes };
    hret = margo_bulk_create(provider->client->mid,
                             1, ksizes_buf_ptr, &ksize_bulk_size,
                             HG_BULK_READWRITE,
                             &in.ksizes_bulk_handle);
    if(hret != HG_SUCCESS) {
        ret = SDSKV_ERR_MERCURY;
        goto finish;
    }

    /* create bulk handle to expose the segments with value sizes */
    hg_size_t vsize_bulk_size = (*max_keys)*sizeof(*vsizes);
    void* vsizes_buf_ptr[1] = { vsizes };
    hret = margo_bulk_create(provider->client->mid,
                             1, vsizes_buf_ptr, &ksize_bulk_size,
                             HG_BULK_READWRITE,
                             &in.vsizes_bulk_handle);
    if(hret != HG_SUCCESS) {
        ret = SDSKV_ERR_MERCURY;
        goto finish;
    }

    /* create bulk handle to expose where the keys should be placed */
    hret = margo_bulk_create(provider->client->mid,
                             *max_keys, keys, ksizes,
                             HG_BULK_WRITE_ONLY,
                             &in.keys_bulk_handle);
    if(hret != HG_SUCCESS) {
        ret = SDSKV_ERR_MERCURY;
        goto finish;
    }

    /* create bulk handle to expose where the keys should be placed */
    hret = margo_bulk_create(provider->client->mid,
                             *max_keys, values, vsizes,
                             HG_BULK_WRITE_ONLY,
                             &in.vals_bulk_handle);
    if(hret != HG_SUCCESS) {
        ret = SDSKV_ERR_MERCURY;
        goto finish;
    }

    /* create handle */
    hret = margo_create(
            provider->client->mid,
            provider->addr,
            provider->client->sdskv_list_keyvals_id,
            &handle);
    if(hret != HG_SUCCESS) {
        ret = SDSKV_ERR_MERCURY;
        goto finish;
    }

    /* forward to provider */
    hret = margo_provider_forward(provider->provider_id, handle, &in);
    if(hret != HG_SUCCESS) {
        ret = SDSKV_ERR_MERCURY;
        goto finish;
    }

    /* get the output from provider */
    hret = margo_get_output(handle, &out);
    if(hret != HG_SUCCESS) {
        ret = SDSKV_ERR_MERCURY;
        goto finish;
    }

    /* set return values */
    *max_keys = out.nkeys;
    ret = out.ret;

finish:
    /* free everything we created */
    margo_bulk_free(in.ksizes_bulk_handle);
    margo_bulk_free(in.keys_bulk_handle);
    margo_bulk_free(in.vsizes_bulk_handle);
    margo_bulk_free(in.vals_bulk_handle);
    margo_free_output(handle, &out);
    margo_destroy(handle);

    return ret;
}

int sdskv_shutdown_service(sdskv_client_t client, hg_addr_t addr)
{
    return margo_shutdown_remote_instance(client->mid, addr);
}
