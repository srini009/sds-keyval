// Copyright (c) 2017, Los Alamos National Security, LLC.
// All rights reserved.
#include "leveldb_datastore.h"
#include "kv-config.h"
#include <chrono>
#include <iostream>
#include <boost/filesystem.hpp>

using namespace std::chrono;

LevelDBDataStore::LevelDBDataStore() :
  AbstractDataStore(Duplicates::IGNORE, false, false), _less(nullptr), _keycmp(this) {
  _dbm = NULL;
};

LevelDBDataStore::LevelDBDataStore(Duplicates duplicates, bool eraseOnGet, bool debug) :
  AbstractDataStore(duplicates, eraseOnGet, debug), _less(nullptr), _keycmp(this) {
  _dbm = NULL;
};
  
std::string LevelDBDataStore::toString(const ds_bulk_t &bulk_val) {
  std::string str_val(bulk_val.begin(), bulk_val.end());
  return str_val;
};

ds_bulk_t LevelDBDataStore::fromString(const std::string &str_val) {
  ds_bulk_t bulk_val(str_val.begin(), str_val.end());
  return bulk_val;
};

LevelDBDataStore::~LevelDBDataStore() {
  delete _dbm;
  //leveldb::Env::Shutdown(); // Riak version only
};

void LevelDBDataStore::createDatabase(std::string db_name) {
  leveldb::Options options;
  leveldb::Status status;
  
  // db_name assumed to include the full path (e.g. /var/data/db.dat)
  boost::filesystem::path p(db_name);
  std::string basepath = p.parent_path().string();
  if (!basepath.empty()) {
    boost::filesystem::create_directories(basepath);
  }
  options.comparator = &_keycmp;
  options.create_if_missing = true;
  status = leveldb::DB::Open(options, db_name, &_dbm);
  
  if (!status.ok()) {
    // error
    std::cerr << "LevelDBDataStore::createDatabase: LevelDB error on Open = " << status.ToString() << std::endl;
  }
  assert(status.ok()); // fall over
  
  // debugging support?
};

void LevelDBDataStore::set_comparison_function(comparator_fn less) {
   _less = less; 
}

bool LevelDBDataStore::put(const ds_bulk_t &key, const ds_bulk_t &data) {
  leveldb::Status status;
  bool success = false;
  
  high_resolution_clock::time_point start = high_resolution_clock::now();
  // IGNORE case deals with redundant puts (where key/value is the same). In LevelDB a
  // redundant put simply overwrites previous value which is fine when key/value is the same.
  if (_duplicates == Duplicates::IGNORE) {
    status = _dbm->Put(leveldb::WriteOptions(), toString(key), toString(data));
    if (status.ok()) {
      success = true;
    }
    else {
      std::cerr << "LevelDBDataStore::put: LevelDB error on Put = " << status.ToString() << std::endl;
    }
  }
  else if (_duplicates == Duplicates::ALLOW) {
    std::cerr << "LevelDBDataStore::put: Duplicates::ALLOW set, LevelDB does not support duplicates" << std::endl;
  }
  else {
    std::cerr << "LevelDBDataStore::put: Unexpected Duplicates option = " << int32_t(_duplicates) << std::endl;
  }
  uint64_t elapsed = duration_cast<microseconds>(high_resolution_clock::now()-start).count();
  std::cout << "LevelDBDataStore::put time = " << elapsed << " microseconds" << std::endl;

  return success;
};

bool LevelDBDataStore::erase(const ds_bulk_t &key) {
    leveldb::Status status;
    status = _dbm->Delete(leveldb::WriteOptions(), toString(key));
    return status.ok();
}

bool LevelDBDataStore::get(const ds_bulk_t &key, ds_bulk_t &data) {
  leveldb::Status status;
  bool success = false;

  high_resolution_clock::time_point start = high_resolution_clock::now();
  data.clear();
  std::string value;
  status = _dbm->Get(leveldb::ReadOptions(), toString(key), &value);
  if (status.ok()) {
    data = fromString(value);
    success = true;
  }
  else if (!status.IsNotFound()) {
    std::cerr << "LevelDBDataStore::get: LevelDB error on Get = " << status.ToString() << std::endl;
  }
  uint64_t elapsed = duration_cast<microseconds>(high_resolution_clock::now()-start).count();
  std::cout << "LevelDBDataStore::get time = " << elapsed << " microseconds" << std::endl;

  return success;
};

bool LevelDBDataStore::get(const ds_bulk_t &key, std::vector<ds_bulk_t> &data) {
  bool success = false;

  data.clear();
  ds_bulk_t value;
  if (get(key, value)) {
    data.push_back(value);
    success = true;
  }
  
  return success;
};

void LevelDBDataStore::set_in_memory(bool enable)
{};

std::vector<ds_bulk_t> LevelDBDataStore::vlist_keys(const ds_bulk_t &start, size_t count, const ds_bulk_t &prefix)
{
    std::vector<ds_bulk_t> keys;

    leveldb::Iterator *it = _dbm->NewIterator(leveldb::ReadOptions());
    leveldb::Slice start_slice(start.data(), start.size());

    int c = 0;

    if (start.size() > 0) {
        it->Seek(start_slice);
        /* we treat 'start' the way RADOS treats it: excluding it from returned
         * keys. LevelDB treats start inclusively, so skip over it if we found
         * an exact match */
        if ( start.size() == it->key().size() &&
                (memcmp(it->key().data(), start.data(), start.size()) == 0))
            it->Next();
    } else {
        it->SeekToFirst();
    }
    /* note: iterator initialized above, not in for loop */
    for (; it->Valid() && keys.size() < count; it->Next() ) {
        ds_bulk_t k(it->key().size());
        memcpy(k.data(), it->key().data(), it->key().size() );
        c = std::memcmp(prefix.data(), k.data(), prefix.size());
        if(c == 0) {
            keys.push_back(std::move(k));
        } else if(c < 0) {
            break;
        }
    }
    delete it;
    return keys;
}

std::vector<std::pair<ds_bulk_t,ds_bulk_t>> LevelDBDataStore::vlist_keyvals(const ds_bulk_t &start, size_t count, const ds_bulk_t &prefix)
{
    std::vector<std::pair<ds_bulk_t,ds_bulk_t>> result;

    leveldb::Iterator *it = _dbm->NewIterator(leveldb::ReadOptions());
    leveldb::Slice start_slice(start.data(), start.size());

    int c = 0;

    if (start.size() > 0) {
        it->Seek(start_slice);
        /* we treat 'start' the way RADOS treats it: excluding it from returned
         * keys. LevelDB treats start inclusively, so skip over it if we found
         * an exact match */
        if ( start.size() == it->key().size() &&
                (memcmp(it->key().data(), start.data(), start.size()) == 0))
            it->Next();
    } else {
        it->SeekToFirst();
    }
    /* note: iterator initialized above, not in for loop */
    for (; it->Valid() && result.size() < count; it->Next() ) {
        ds_bulk_t k(it->key().size());
        ds_bulk_t v(it->value().size());
        memcpy(k.data(), it->key().data(), it->key().size());
        memcpy(v.data(), it->value().data(), it->value().size());

        c = std::memcmp(prefix.data(), k.data(), prefix.size());
        if(c == 0) {
            result.push_back(std::make_pair(std::move(k), std::move(v)));
        } else if(c < 0) {
            break;
        }
    }
    delete it;
    return result;
}
/*
{
    std::vector<std::pair<ds_bulk_t,ds_bulk_t>> keyvals;

    leveldb::Iterator *it = _dbm->NewIterator(leveldb::ReadOptions());
    size_t i=0;
    for (it->SeekToFirst(); it->Valid(); it->Next() ) {
        ds_bulk_t k(it->key().size());
        ds_bulk_t v(it->value().size());
        memcpy(k.data(), it->key().data(), it->key().size() );
        memcpy(v.data(), it->value().data(), it->value().size() );
        keyvals.push_back(std::make_pair(std::move(k), std::move(v)));
        if (i++ > count) break;
    }
    delete it;
    return keyvals;
}
*/
