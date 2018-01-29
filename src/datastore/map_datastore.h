// Copyright (c) 2017, Los Alamos National Security, LLC.
// All rights reserved.
#ifndef map_datastore_h
#define map_datastore_h

#include <map>

#include "kv-config.h"
#include "bulk.h"
#include "datastore/datastore.h"

class MapDataStore : public AbstractDataStore {
    public:

        MapDataStore()
            : AbstractDataStore() {}

        MapDataStore(Duplicates duplicates, bool eraseOnGet, bool debug)
            : AbstractDataStore(duplicates, eraseOnGet, debug) {}

        ~MapDataStore() = default;

        virtual void createDatabase(std::string db_name) {
            _map.clear();
        }

        virtual bool put(const ds_bulk_t &key, const ds_bulk_t &data) {
            if(_duplicates == Duplicates::IGNORE && _map.count(key)) {
                return true;
            }
            _map.insert(std::make_pair(key,data));
            return true;
        }

        virtual bool get(const ds_bulk_t &key, ds_bulk_t &data) {
            auto it = _map.find(key);
            if(it == _map.end()) return false;
            data = it->second;
            return true;
        }

        virtual bool get(const ds_bulk_t &key, std::vector<ds_bulk_t>& values) {
            values.clear();
            values.resize(1);
            return get(key, values[0]);
        }

        virtual void set_in_memory(bool enable) {
            _in_memory = enable;
        }

        virtual std::vector<ds_bulk_t> list(const ds_bulk_t &start_key, size_t count) {
            std::vector<ds_bulk_t> result;
            auto it = _map.lower_bound(start_key);
            while(it != _map.end() && it->first == start_key) {
                it++;
            }
            auto lastkey = start_key;
            for(size_t i=0; it != _map.end() && i < count; it++) {
                if(it->first != lastkey) {
                    result.push_back(lastkey);
                    i += 1;
                    lastkey = it->first;
                }
            }
            return result;
        }

    private:
        std::map<ds_bulk_t, ds_bulk_t> _map;
};

#endif
