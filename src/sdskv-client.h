#ifndef sds_keyval_client_h
#define sds_keyval_client_h

#include <margo.h>
#include <stdint.h>
#include <sdskv-common.h>

#if defined(__cplusplus)
extern "C" {
#endif

typedef struct sdskv_client* sdskv_client_t;
#define SDSKV_CLIENT_NULL ((sdskv_client_t)NULL)

typedef struct sdskv_provider_handle *sdskv_provider_handle_t;
#define SDSKV_PROVIDER_HANDLE_NULL ((sdskv_provider_handle_t)NULL)

int sdskv_client_init(margo_instance_id mid, sdskv_client_t* client);

int sdskv_client_finalize(sdskv_client_t client);

int sdskv_provider_handle_create(
        sdskv_client_t client,
        hg_addr_t addr,
        uint8_t mplex_id,
        sdskv_provider_handle_t* handle);

int sdskv_provider_handle_ref_incr(
        sdskv_provider_handle_t handle);

int sdskv_provider_handle_release(sdskv_provider_handle_t handle);

int sdskv_open(
        sdskv_provider_handle_t provider,
        const char* db_name,
        sdskv_database_id_t* db_id);

int sdskv_put(sdskv_provider_handle_t provider, 
        sdskv_database_id_t db_id,
        const void *key, hg_size_t ksize,
        const void *value, hg_size_t vsize);

int sdskv_get(sdskv_provider_handle_t provider,
        sdskv_database_id_t db_id, 
        const void *key, hg_size_t ksize,
        void *value, hg_size_t* vsize);

int sdskv_length(sdskv_provider_handle_t db, 
        sdskv_database_id_t db_id, const void *key, 
        hg_size_t ksize, hg_size_t* vsize);

int sdskv_list_keys(sdskv_provider_handle_t provider,
        sdskv_database_id_t db_id,  // db instance
        const void *start_key,  // we want keys strictly after this start_key
        hg_size_t start_ksize,  // size of the start_key
        void **keys,            // pointer to an array of void* pointers,
                                //     this array has size *max_keys
        hg_size_t* ksizes,      // pointer to an array of hg_size_t sizes
                                //    representing sizes allocated in
                                //     keys for each key
        hg_size_t* max_keys);   // maximum number of keys requested

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
        hg_size_t* max_keys);   // maximum number of keys requested

/**
 * Shuts down a remote SDSKV service (given an address).
 * This will shutdown all the providers on the target address.
 * 
 * @param [in] client SDSKV client
 * @param [in] addr address of the server 
 * @returns 0 on success, -1 on failure 
 */
int sdskv_shutdown_service(
        sdskv_client_t client, hg_addr_t addr);

#if defined(__cplusplus)
}
#endif

#endif 
