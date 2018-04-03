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

/**
 * @brief Creates a SDSKV client.
 *
 * @param[in] mid Margo instance
 * @param[out] client SDSKV client
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_client_init(margo_instance_id mid, sdskv_client_t* client);

/**
 * @brief Finalizes a SDSKV client.
 *
 * @param[in] client SDSKV client to finalize
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_client_finalize(sdskv_client_t client);

/**
 * @brief Creates a SDSKV provider handle.
 *
 * @param[in] client SDSKV client responsible for the provider handle
 * @param[in] addr Mercury address of the provider
 * @param[in] provider_id id of the provider
 * @param[in] handle provider handle
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_provider_handle_create(
        sdskv_client_t client,
        hg_addr_t addr,
        uint16_t provider_id,
        sdskv_provider_handle_t* handle);

/**
 * @brief Increments the reference counter of a provider handle.
 *
 * @param handle provider handle
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_provider_handle_ref_incr(
        sdskv_provider_handle_t handle);

/**
 * @brief Releases the provider handle. This will decrement the
 * reference counter, and free the provider handle if the reference
 * counter reaches 0.
 *
 * @param[in] handle provider handle to release.
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_provider_handle_release(sdskv_provider_handle_t handle);

/**
 * @brief Opens a database. This effectively contacts the provider
 * pointed to by the provider handle and request the database id
 * associated with the database name.
 *
 * @param[in] provider provider handle
 * @param[in] db_name name of the database to lookup
 * @param[out] db_id database id
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_open(
        sdskv_provider_handle_t provider,
        const char* db_name,
        sdskv_database_id_t* db_id);

/**
 * @brief Puts a key/value pair into the database.
 *
 * @param provider provider handle managing the database
 * @param db_id targeted database id
 * @param key key to store
 * @param ksize size (in bytes) of the key
 * @param value value to store
 * @param vsize size (in bytes) of the value
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_put(sdskv_provider_handle_t provider, 
        sdskv_database_id_t db_id,
        const void *key, hg_size_t ksize,
        const void *value, hg_size_t vsize);

/**
 * @brief Gets the value associated with a given key.
 * vsize needs to be set to the current size of the allocated
 * value buffer. After a succesful call to sdskv_get, vsize
 * will hold the actual size of the key. Note that the size
 * of a value can get obtained using sdskv_length.
 *
 * @param[in] provider provider handle
 * @param[in] db_id database id of the target database
 * @param[in] key key to lookup
 * @param[in] ksize size of the key
 * @param[out] value pointer to buffer to put the value
 * @param[inout] vsize size of the value
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_get(sdskv_provider_handle_t provider,
        sdskv_database_id_t db_id, 
        const void *key, hg_size_t ksize,
        void *value, hg_size_t* vsize);

/**
 * @brief Gets the length of a value associated with a given key.
 *
 * @param[in] handle provider handle
 * @param[in] db_id database id
 * @param[in] key key to lookup
 * @param[in] ksize size of the key
 * @param[out] vsize size of the associated value
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_length(sdskv_provider_handle_t handle, 
        sdskv_database_id_t db_id, const void *key, 
        hg_size_t ksize, hg_size_t* vsize);

/**
 * @brief Erases the key/value pair pointed by the given key.
 *
 * @param handle provider handle
 * @param db_id database id
 * @param key key to erase
 * @param ksize size of the key
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_erase(sdskv_provider_handle_t handle,
        sdskv_database_id_t db_id, const void *key,
        hg_size_t ksize);

/**
 * Lists at most max_keys keys starting strictly after start_key,
 * whether start_key is effectively in the database or not. "strictly after"
 * must be understood in the sens of the custom comparison function set
 * for the target database, if such comparison function has been set.
 * Passing NULL as start_key allows one to start listing from the beginning
 * of the database.
 * keys must be an array of max_keys void* elements, each element keys[i]
 * being a preallocated buffer of size ksizes[i]. ksizes must be an array
 * of sizes of the preallocated buffers. After a successful call, max_keys
 * is set to the actual number of keys retrieved, ksizes[i] is set to the
 * actual size of the i-th key, and keys[i] contains the i-th key. The call
 * will fail if at least one of the preallocated size is too small to hold
 * the key.
 *
 * @param[in] provider provider handle
 * @param[in] db_id database id
 * @param[in] start_key starting key
 * @param[in] start_ksize size of the starting key
 * @param[out] keys array of buffers to hold returned keys
 * @param[inout] ksizes array of key sizes
 * @param[inout] max_keys max keys requested
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_list_keys(
        sdskv_provider_handle_t provider,
        sdskv_database_id_t db_id,
        const void *start_key,
        hg_size_t start_ksize,
        void **keys,
        hg_size_t* ksizes,
        hg_size_t* max_keys);

/**
 * @brief Same as sdskv_list_keys, but provides a prefix
 * that returned keys must start with. Note that "prefix" is understood
 * in the sens of the bytes making up the key, not in the sens of any
 * comparison function set by the user on the provider side.
 *
 * @param[in] provider provider handle
 * @param[in] db_id database id
 * @param[in] start_key starting key
 * @param[in] start_ksize size of the starting key
 * @param[in] prefix prefix that returned keys must match
 * @param[in] prefix_size size of the prefix
 * @param[out] keys array of buffers to hold returned keys
 * @param[inout] ksizes array of key sizes
 * @param[inout] max_keys max keys requested
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_list_keys_with_prefix(
        sdskv_provider_handle_t provider,
        sdskv_database_id_t db_id,
        const void *start_key,
        hg_size_t start_ksize,
        const void *prefix,
        hg_size_t prefix_size,
        void **keys,
        hg_size_t* ksizes, 
        hg_size_t* max_keys);

/**
 * @brief Same as sdskv_list_keys but returns also the values.
 * The values array must be an array of max_keys sizes, with
 * each element values[i] being a buffer of size vsizes[i].
 * After a successful call, values[i] will hold the i-th value
 * and vsizes[i] will be set to the actual size of the i-th value.
 *
 * @param[in] provider provider handle
 * @param[in] db_id database id
 * @param[in] start_key starting key
 * @param[in] start_ksize size of the starting key
 * @param[out] keys array of buffers to hold returned keys
 * @param[inout] ksizes array of key sizes
 * @param[inout] values array of buffers to hold returned values
 * @param[inout] vsizes array of value sizes
 * @param[inout] max_keys max keys requested
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_list_keyvals(
        sdskv_provider_handle_t provider,
        sdskv_database_id_t db_id,
        const void *start_key,
        hg_size_t start_ksize,
        void **keys,
        hg_size_t* ksizes,
        void **values,
        hg_size_t* vsizes,
        hg_size_t* max_items);

/**
 * @brief Same as sdskv_list_keyvals but lets the user provide
 * a prefix that returned keys must satisfy.
 *
 * @param[in] provider provider handle
 * @param[in] db_id database id
 * @param[in] start_key starting key
 * @param[in] start_ksize size of the starting key
 * @param[in] prefix prefix that returned keys must match
 * @param[in] prefix_size size of the prefix
 * @param[out] keys array of buffers to hold returned keys
 * @param[inout] ksizes array of key sizes
 * @param[inout] values array of buffers to hold returned values
 * @param[inout] vsizes array of value sizes
 * @param[inout] max_keys max keys requested
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_list_keyvals_with_prefix(
        sdskv_provider_handle_t provider,
        sdskv_database_id_t db_id,
        const void *start_key,
        hg_size_t start_ksize,
        const void *prefix,
        hg_size_t prefix_size,
        void **keys,
        hg_size_t* ksizes,
        void **values,
        hg_size_t* vsizes,
        hg_size_t* max_items);

/**
 * Shuts down a remote SDSKV service (given an address).
 * This will shutdown all the providers on the target address.
 * 
 * @param [in] client SDSKV client
 * @param [in] addr address of the server 
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_shutdown_service(
        sdskv_client_t client, hg_addr_t addr);

#if defined(__cplusplus)
}
#endif

#endif 
