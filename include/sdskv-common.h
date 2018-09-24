#ifndef SDSKV_COMMON_H 
#define SDSKV_COMMON_H


#if defined(__cplusplus)
extern "C" {
#endif

typedef enum sdskv_db_type_t 
{
    KVDB_MAP = 0,       /* Datastore implementation using std::map   */
    KVDB_BWTREE,    /* Datastore implementation using a BwTree   */
    KVDB_LEVELDB,   /* Datastore implementation using LevelDB    */
    KVDB_BERKELEYDB /* Datasotre implementation using BerkeleyDB */
} sdskv_db_type_t;

typedef uint64_t sdskv_database_id_t;
#define SDSKV_DATABASE_ID_INVALID 0

#define SDSKV_KEEP_ORIGINAL    0 /* for migration operations, keep original */
#define SDSKV_REMOVE_ORIGINAL  1 /* for migration operations, remove the origin after migrating */

#define SDSKV_SUCCESS          0 /* Success */
#define SDSKV_ERR_ALLOCATION  -1 /* Error allocating something */
#define SDSKV_ERR_INVALID_ARG -2 /* An argument is invalid */
#define SDSKV_ERR_MERCURY     -3 /* An error happened calling a Mercury function */
#define SDSKV_ERR_DB_CREATE   -4 /* Could not create database */
#define SDSKV_ERR_DB_NAME     -5 /* Invalid database name */
#define SDSKV_ERR_UNKNOWN_DB  -6 /* Database refered to by id is not known to provider */
#define SDSKV_ERR_UNKNOWN_PR  -7 /* Mplex id could not be matched with a provider */
#define SDSKV_ERR_PUT         -8 /* Could not put into the database */
#define SDSKV_ERR_UNKNOWN_KEY -9 /* Key requested does not exist */
#define SDSKV_ERR_SIZE        -10 /* Client did not allocate enough for the requested data */
#define SDSKV_ERR_ERASE       -11 /* Could not erase the given key */
#define SDSKV_ERR_MIGRATION   -12 /* Error during data migration */
#define SDSKV_OP_NOT_IMPL     -13 /* Operation not implemented for this backend */
#define SDSKV_ERR_COMP_FUNC   -14 /* Comparison function does not exist */
#define SDSKV_ERR_REMI        -15 /* REMI-related error */
#define SDSKV_ERR_ARGOBOTS    -16 /* Argobots related error */

#if defined(__cplusplus)
}
#endif

#endif 
