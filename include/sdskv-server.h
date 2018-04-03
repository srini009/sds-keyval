/*
 * (C) 2018 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#ifndef __SDSKV_SERVER_H
#define __SDSKV_SERVER_H

#include <margo.h>
#include <sdskv-common.h>

#ifdef __cplusplus
extern "C" {
#endif

#define SDSKV_ABT_POOL_DEFAULT ABT_POOL_NULL
#define SDSKV_PROVIDER_ID_DEFAULT 0
#define SDSKV_PROVIDER_IGNORE NULL
#define SDSKV_COMPARE_DEFAULT NULL

typedef struct sdskv_server_context_t* sdskv_provider_t;
typedef int (*sdskv_compare_fn)(const void*, size_t, const void*, size_t);

/**
 * @brief Creates a new provider.
 *
 * @param[in] mid Margo instance
 * @param[in] provider_id provider id
 * @param[in] pool Argobots pool
 * @param[out] provider provider handle
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_provider_register(
        margo_instance_id mid,
        uint16_t provider_id,
        ABT_pool pool,
        sdskv_provider_t* provider);

/**
 * Makes the provider start managing a database.
 *
 * @param[in] provider provider
 * @param[in] db_name name of the database
 * @param[in] db_type type of database
 * @param[in] comp_fn comparison function for the database
 * @param[out] db_id resulting id identifying the database
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_provider_add_database(
        sdskv_provider_t provider,
        const char* db_name,
        sdskv_db_type_t db_type,
        sdskv_compare_fn comp_fn,
        sdskv_database_id_t* sb_id);

/**
 * Makes the provider stop managing a database and deletes the
 * database. This will effectively destroy the database if it is
 * not backed up by a file.
 *
 * @param provider provider
 * @param db_id id of the database to remove
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_provider_remove_database(
        sdskv_provider_t provider,
        sdskv_database_id_t db_id);

/**
 * Removes all the databases associated with a provider.
 *
 * @param provider provider
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_provider_remove_all_databases(
        sdskv_provider_t provider);

/**
 * Returns the number of databases that this provider manages.
 *
 * @param provider provider
 * @param num_db resulting number of databases
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_provider_count_databases(
        sdskv_provider_t provider,
        uint64_t* num_db);

/**
 * List the database ids of the databases managed by this provider.
 * The databases array must be pre-allocated with at least enough
 * space to hold all the ids (use sdskv_provider_count_databases
 * to know how many databases are managed).
 *
 * @param[in] provider provider
 * @param[out] databases resulting database ids
 *
 * @return SDSKV_SUCCESS or error code defined in sdskv-common.h
 */
int sdskv_provider_list_databases(
        sdskv_provider_t provider,
        sdskv_database_id_t* databases);

#ifdef __cplusplus
}
#endif

#endif /* __BAKE_SERVER_H */
