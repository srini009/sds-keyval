// Copyright (c) 2017, Los Alamos National Security, LLC.
// All rights reserved.
#ifndef bwtree_datastore_h
#define bwtree_datastore_h

#include "kv-config.h"
#include "bwtree.h"
#include "datastore.h"

using namespace wangziqi2013::bwtree;

class BwTreeDataStore : public AbstractDataStore {
public:
  BwTreeDataStore();
  BwTreeDataStore(Duplicates duplicates, bool eraseOnGet, bool debug);
  virtual ~BwTreeDataStore();
  virtual void createDatabase(std::string db_name);
  virtual bool put(const ds_bulk_t &key, const ds_bulk_t &data);
  virtual bool get(const ds_bulk_t &key, ds_bulk_t &data);
  virtual bool get(const ds_bulk_t &key, std::vector<ds_bulk_t> &data);
  virtual void set_in_memory(bool enable); // a no-op
  virtual std::vector<ds_bulk_t> list(const ds_bulk_t &start, size_t count);
protected:
  BwTree<ds_bulk_t, ds_bulk_t, 
	 ds_bulk_less, ds_bulk_equal, ds_bulk_hash,
	 ds_bulk_equal, ds_bulk_hash> *_tree = NULL;
};

#endif // bwtree_datastore_h
