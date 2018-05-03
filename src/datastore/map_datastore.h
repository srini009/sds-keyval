// Copyright (c) 2017, Los Alamos National Security, LLC.
// All rights reserved.
#ifndef map_datastore_h
#define map_datastore_h

#include <map>
#include "kv-config.h"
#include "bulk.h"
#include "datastore/datastore.h"

class MapDataStore : public AbstractDataStore {

    private:

        struct keycmp {
            MapDataStore* _store;
            keycmp(MapDataStore* store)
                : _store(store) {}
            bool operator()(const ds_bulk_t& a, const ds_bulk_t& b) const {
                if(_store->_less)
                    return _store->_less((const void*)a.data(),
                            a.size(), (const void*)b.data(), b.size()) < 0;
                else
                    return std::less<ds_bulk_t>()(a,b);
            }
        };

    public:

        MapDataStore()
            : AbstractDataStore(), _less(nullptr), _map(keycmp(this)) {}

        MapDataStore(Duplicates duplicates, bool eraseOnGet, bool debug)
            : AbstractDataStore(duplicates, eraseOnGet, debug), _less(nullptr), _map(keycmp(this)) {}

        ~MapDataStore() = default;

        virtual void createDatabase(const std::string& db_name, const std::string& path) {
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

        virtual bool erase(const ds_bulk_t &key) {
            bool b = _map.find(key) != _map.end();
            _map.erase(key);
            return b;
        }

        virtual void set_in_memory(bool enable) {
            _in_memory = enable;
        }

        virtual void set_comparison_function(comparator_fn less) {
           _less = less; 
        }

    protected:

        virtual std::vector<ds_bulk_t> vlist_keys(
                const ds_bulk_t &start_key, size_t count, const ds_bulk_t &prefix) {
            std::vector<ds_bulk_t> result;
            decltype(_map.begin()) it;
            if(start_key.size() > 0) {
                it = _map.lower_bound(start_key);
                while(it != _map.end() && it->first == start_key) it++;
            } else {
                it = _map.begin();
            }
            while(result.size() < count && it != _map.end()) {
                const auto& p = *it;
                if(prefix.size() > p.first.size()) continue;
                int c = std::memcmp(prefix.data(), p.first.data(), prefix.size());
                if(c == 0) {
                    result.push_back(p.first);
                } else if(c < 0) {
                    break; // we have exceeded prefix
                }
                it++;
            }
            return result;
        }

        virtual std::vector<std::pair<ds_bulk_t,ds_bulk_t>> vlist_keyvals(
                const ds_bulk_t &start_key, size_t count, const ds_bulk_t &prefix) {
            std::vector<std::pair<ds_bulk_t,ds_bulk_t>> result;
            decltype(_map.begin()) it;
            if(start_key.size() > 0) {
                it = _map.lower_bound(start_key);
                while(it != _map.end() && it->first == start_key) it++;
            } else {
                it = _map.begin();
            }
            while(result.size() < count && it != _map.end()) {
                const auto& p = *it;
                if(prefix.size() > p.first.size()) continue;
                int c = std::memcmp(prefix.data(), p.first.data(), prefix.size());
                if(c == 0) {
                    result.push_back(p);
                } else if(c < 0) {
                    break; // we have exceeded prefix
                }
                it++;
            }
            return result;
        }

    private:
        AbstractDataStore::comparator_fn _less;
        std::map<ds_bulk_t, ds_bulk_t, keycmp> _map;
};

#endif
