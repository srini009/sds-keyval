// Copyright (c) 2017, Los Alamos National Security, LLC.
// All rights reserved.
#ifndef ldb_datastore_h
#define ldb_datastore_h

#include "kv-config.h"
#include <leveldb/db.h>
#include <leveldb/env.h>
#include "datastore/datastore.h"

// may want to implement some caching for persistent stores like LevelDB
class LevelDBDataStore : public AbstractDataStore {
public:
  LevelDBDataStore();
  LevelDBDataStore(Duplicates duplicates, bool eraseOnGet, bool debug);
  virtual ~LevelDBDataStore();
  virtual void createDatabase(std::string db_name);
  virtual bool put(const ds_bulk_t &key, const ds_bulk_t &data);
  virtual bool get(const ds_bulk_t &key, ds_bulk_t &data);
  virtual bool get(const ds_bulk_t &key, std::vector<ds_bulk_t> &data);
  virtual bool erase(const ds_bulk_t &key);
  virtual void set_in_memory(bool enable); // not supported, a no-op
  virtual void set_comparison_function(comparator_fn less);
protected:
  virtual std::vector<ds_bulk_t> vlist_keys(const ds_bulk_t &start, size_t count, const ds_bulk_t &prefix);
  virtual std::vector<std::pair<ds_bulk_t,ds_bulk_t>> vlist_keyvals(const ds_bulk_t &start_key, size_t count, const ds_bulk_t &prefix);
  leveldb::DB *_dbm = NULL;
private:
  std::string toString(const ds_bulk_t &key);
  ds_bulk_t fromString(const std::string &keystr);
};

#endif // ldb_datastore_h
