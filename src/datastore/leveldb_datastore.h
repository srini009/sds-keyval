// Copyright (c) 2017, Los Alamos National Security, LLC.
// All rights reserved.
#ifndef ldb_datastore_h
#define ldb_datastore_h

#include "kv-config.h"
#include <leveldb/db.h>
#include <leveldb/comparator.h>
#include <leveldb/env.h>
#include "sdskv-common.h"
#include "datastore/datastore.h"


// may want to implement some caching for persistent stores like LevelDB
class LevelDBDataStore : public AbstractDataStore {
    private:

        class LevelDBDataStoreComparator : public leveldb::Comparator {
            private:
                LevelDBDataStore* _store;

            public:
                LevelDBDataStoreComparator(LevelDBDataStore* store)
                    : _store(store) {}

                int Compare(const leveldb::Slice& a, const leveldb::Slice& b) const {
                    if(_store->_less) {
                        return _store->_less((const void*)a.data(), a.size(), (const void*)b.data(), b.size());
                    } else {
                        return a.compare(b);
                    }
                }

                // Ignore the following methods for now:
                const char* Name() const { return "LevelDBDataStoreComparator"; }
                void FindShortestSeparator(std::string*, const leveldb::Slice&) const {}
                void FindShortSuccessor(std::string*) const {}
        };

    public:
        LevelDBDataStore();
        LevelDBDataStore(Duplicates duplicates, bool eraseOnGet, bool debug);
        virtual ~LevelDBDataStore();
        virtual bool openDatabase(const std::string& db_name, const std::string& path);
        virtual bool put(const ds_bulk_t &key, const ds_bulk_t &data);
        virtual bool get(const ds_bulk_t &key, ds_bulk_t &data);
        virtual bool get(const ds_bulk_t &key, std::vector<ds_bulk_t> &data);
        virtual bool exists(const ds_bulk_t &key);
        virtual bool erase(const ds_bulk_t &key);
        virtual void set_in_memory(bool enable); // not supported, a no-op
        virtual void set_comparison_function(comparator_fn less);
        virtual void set_no_overwrite() {
            _no_overwrite = true;
        }
    protected:
        virtual std::vector<ds_bulk_t> vlist_keys(
                const ds_bulk_t &start, size_t count, const ds_bulk_t &prefix) const;
        virtual std::vector<std::pair<ds_bulk_t,ds_bulk_t>> vlist_keyvals(
                const ds_bulk_t &start_key, size_t count, const ds_bulk_t &prefix) const;
        virtual std::vector<ds_bulk_t> vlist_key_range(
                const ds_bulk_t &lower_bound, const ds_bulk_t &upper_bound, size_t max_keys) const;
        virtual std::vector<std::pair<ds_bulk_t,ds_bulk_t>> vlist_keyval_range(
                const ds_bulk_t &lower_bound, const ds_bulk_t& upper_bound, size_t max_keys) const;
        leveldb::DB *_dbm = NULL;
    private:
        std::string toString(const ds_bulk_t &key);
        ds_bulk_t fromString(const std::string &keystr);
        AbstractDataStore::comparator_fn _less;
        LevelDBDataStoreComparator _keycmp;
        bool _no_overwrite = false;
};

#endif // ldb_datastore_h
