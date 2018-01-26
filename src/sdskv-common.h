#ifndef SDSKV_COMMON_H 
#define SDSKV_COMMON_H


#if defined(__cplusplus)
extern "C" {
#endif

typedef enum sdskv_db_type_t 
{
    KVDB_BWTREE,
    KVDB_LEVELDB,
    KVDB_BERKELEYDB
} sdskv_db_type_t;

typedef uint64_t sdskv_database_id_t;
#define SDSKV_DATABASE_ID_INVALID 0

#define SDSKV_SUCCESS 0
#define SDSKV_ERROR  -1

#if defined(__cplusplus)
}
#endif

#endif 
