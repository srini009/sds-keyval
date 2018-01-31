#ifndef datastore_factory_h
#define datastore_factory_h
#include <string>

#ifdef SDSKV
#include "sdskv-common.h"
#else
#include "sds-keyval.h"
#endif
#include "datastore.h"

#include "map_datastore.h"

#ifdef USE_BWTREE
#include "bwtree_datastore.h"
#endif

#ifdef USE_BDB
#include "berkeleydb_datastore.h"
#endif

#ifdef USE_LEVELDB
#include "leveldb_datastore.h"
#endif

class datastore_factory {

    static AbstractDataStore* create_map_datastore(const std::string& name) {
        auto db = new MapDataStore();
        db->createDatabase(name);
        return db;
    }

    static AbstractDataStore* create_bwtree_datastore(const std::string& name) {
#ifdef USE_BWTREE
        auto db = new BwTreeDataStore();
        db->createDatabase(name);
        return db;
#else
        return nullptr;
#endif
    }

    static AbstractDataStore* create_berkeleydb_datastore(const std::string& name) {
#ifdef USE_BDB
        auto db = new BerkeleyDBDataStore();
        db->createDatabase(name);
        return db;
#else
        return nullptr;
#endif
    }

    static AbstractDataStore* create_leveldb_datastore(const std::string& name) {
#ifdef USE_LEVELDB
        auto db = new LevelDBDataStore();
        db->createDatabase(name);
        return db;
#else
        return nullptr;
#endif
    }

    public:

#ifdef SDSKV
    static AbstractDataStore* create_datastore(sdskv_db_type_t type, const std::string& name)
#else
    static AbstractDataStore* create_datastore(kv_db_type_t type, const std::string& name="db")
#endif
    {
        switch(type) {
            case KVDB_MAP:
                return create_map_datastore(name);
            case KVDB_BWTREE:
                return create_bwtree_datastore(name);
            case KVDB_LEVELDB:
                return create_leveldb_datastore(name);
            case KVDB_BERKELEYDB:
                return create_berkeleydb_datastore(name);
        }
        return nullptr;
    };

};

#endif
