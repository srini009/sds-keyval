// Copyright (c) 2017, Los Alamos National Security, LLC.
// All rights reserved.
#ifndef bdb_datastore_h
#define bdb_datastore_h

#include "kv-config.h"
#include "datastore/datastore.h"
#include <db_cxx.h>
#include <dbstl_map.h>
#include "sdskv-common.h"

// may want to implement some caching for persistent stores like BerkeleyDB
class BerkeleyDBDataStore : public AbstractDataStore {

    private:
        struct DbWrapper {
            Db _db;
            AbstractDataStore::comparator_fn _less;

            template<typename ... T>
            DbWrapper(T&&... args) :
            _db(std::forward<T>(args)...), _less(nullptr) {}
        };

        static int compkeys(Db *db, const Dbt *dbt1, const Dbt *dbt2, size_t *locp);

    public:
        BerkeleyDBDataStore();
        BerkeleyDBDataStore(Duplicates duplicates, bool eraseOnGet, bool debug);
        virtual ~BerkeleyDBDataStore();
        virtual bool openDatabase(const std::string& db_name, const std::string& path) override;
        virtual bool put(const void* key, size_t ksize, const void* value, size_t vsize) override;
        virtual bool put_multi(size_t num_items,
                               const void* const* keys,
                               const size_t* ksizes,
                               const void* const* values,
                               const size_t* vsizes) override;
        /* virtual bool put(const ds_bulk_t &key, const ds_bulk_t &data) override;
        virtual bool put(ds_bulk_t &&key, ds_bulk_t &&data) override; */
        virtual bool get(const ds_bulk_t &key, ds_bulk_t &data) override;
        virtual bool get(const ds_bulk_t &key, std::vector<ds_bulk_t> &data) override;
        virtual bool exists(const void* key, size_t ksize) const override;
        virtual bool erase(const ds_bulk_t &key) override;
        virtual void set_in_memory(bool enable) override; // enable/disable in-memory mode
        virtual void set_comparison_function(const std::string& name, comparator_fn less) override;
        virtual void set_no_overwrite() override {
            _no_overwrite = true;
        }
        virtual void sync() override;
#ifdef USE_REMI
        virtual remi_fileset_t create_and_populate_fileset() const override;
#endif
    protected:
        virtual std::vector<ds_bulk_t> vlist_keys(
                const ds_bulk_t &start, size_t count, const ds_bulk_t &prefix) const override;
        virtual std::vector<std::pair<ds_bulk_t,ds_bulk_t>> vlist_keyvals(
                const ds_bulk_t &start_key, size_t count, const ds_bulk_t &) const override;
        virtual std::vector<ds_bulk_t> vlist_key_range(
                const ds_bulk_t &lower_bound, const ds_bulk_t &upper_bound, size_t max_keys) const override;
        virtual std::vector<std::pair<ds_bulk_t,ds_bulk_t>> vlist_keyval_range(
                const ds_bulk_t &lower_bound, const ds_bulk_t& upper_bound, size_t max_keys) const override;
        DbEnv *_dbenv = nullptr;
        Db *_dbm = nullptr;
        DbWrapper* _wrapper = nullptr;
};

#endif // bdb_datastore_h
