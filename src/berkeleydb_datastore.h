// Copyright (c) 2017, Los Alamos National Security, LLC.
// All rights reserved.
#ifndef bdb_datastore_h
#define bdb_datastore_h

#include "kv-config.h"
#include "sds-keyval.h"
#include <db_cxx.h>
#include <dbstl_map.h>

// may want to implement some caching for persistent stores like BerkeleyDB
class BerkeleyDBDataStore : public AbstractDataStore {
public:
  BerkeleyDBDataStore();
  BerkeleyDBDataStore(Duplicates duplicates, bool eraseOnGet, bool debug);
  virtual ~BerkeleyDBDataStore();
  virtual void createDatabase(std::string db_name);
  virtual bool put(const ds_bulk_t &key, const ds_bulk_t &data);
  virtual bool get(const ds_bulk_t &key, ds_bulk_t &data);
  virtual bool get(const ds_bulk_t &key, std::vector<ds_bulk_t> &data);
  virtual void set_in_memory(bool enable); // enable/disable in-memory mode
  virtual std::vector<ds_bulk_t> list(const ds_bulk_t &start, size_t count);
protected:
  DbEnv *_dbenv = NULL;
  Db *_dbm = NULL;
};

#endif // bdb_datastore_h