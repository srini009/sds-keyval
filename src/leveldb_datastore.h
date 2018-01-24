// Copyright (c) 2017, Los Alamos National Security, LLC.
// All rights reserved.
#ifndef ldb_datastore_h
#define ldb_datastore_h

#include "kv-config.h"
#include "sds-keyval.h"
#include <leveldb/db.h>
#include <leveldb/env.h>

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
  virtual void set_in_memory(bool enable); // not supported, a no-op
  virtual std::vector<ds_bulk_t> list(const ds_bulk_t &start, size_t count);
protected:
  leveldb::DB *_dbm = NULL;
private:
  std::string toString(ds_bulk_t &key);
  ds_bulk_t fromString(std::string &keystr);
};

#endif // ldb_datastore_h
