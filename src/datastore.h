// Copyright (c) 2017, Los Alamos National Security, LLC.
// All rights reserved.
#include "bwtree.h"
#include "sds-keyval.h"

#include <boost/functional/hash.hpp>
#include <vector>
#include <leveldb/db.h>
#include <leveldb/env.h>
#include <db_cxx.h>
#include <dbstl_map.h>

using namespace wangziqi2013::bwtree;

#ifndef datastore_h
#define datastore_h

enum class Duplicates : int {ALLOW, IGNORE};

// implementation is std::vector<char> specific
// typedef is for convenience
typedef std::vector<char> ds_bulk_t;

struct my_hash {
  size_t operator()(const ds_bulk_t &v) const {
    size_t hash = 0;
    boost::hash_range(hash, v.begin(), v.end());
    return hash;
  }
};

struct my_equal_to {
  bool operator()(const ds_bulk_t &v1, const ds_bulk_t &v2) const {
    return (v1 == v2);
  }
};

class AbstractDataStore {
public:
  AbstractDataStore();
  AbstractDataStore(Duplicates duplicates, bool eraseOnGet, bool debug);
  virtual ~AbstractDataStore();
  virtual void createDatabase(std::string db_name)=0;
  virtual bool put(const kv_key_t &key, ds_bulk_t &data)=0;
  virtual bool get(const kv_key_t &key, ds_bulk_t &data)=0;
  virtual bool get(const kv_key_t &key, std::vector<ds_bulk_t> &data)=0;
protected:
  Duplicates _duplicates;
  bool _eraseOnGet;
  bool _debug;
};

class BwTreeDataStore : public AbstractDataStore {
public:
  BwTreeDataStore();
  BwTreeDataStore(Duplicates duplicates, bool eraseOnGet, bool debug);
  virtual ~BwTreeDataStore();
  virtual void createDatabase(std::string db_name);
  virtual bool put(const kv_key_t &key, ds_bulk_t &data);
  virtual bool get(const kv_key_t &key, ds_bulk_t &data);
  virtual bool get(const kv_key_t &key, std::vector<ds_bulk_t> &data);
protected:
  BwTree<kv_key_t, ds_bulk_t, std::less<kv_key_t>,
	 std::equal_to<kv_key_t>, std::hash<kv_key_t>,
	 my_equal_to/*ds_bulk_t*/, my_hash/*ds_bulk_t*/> *_tree = NULL;
};

// may want to implement some caching for persistent stores like LevelDB
class LevelDBDataStore : public AbstractDataStore {
public:
  LevelDBDataStore();
  LevelDBDataStore(Duplicates duplicates, bool eraseOnGet, bool debug);
  virtual ~LevelDBDataStore();
  virtual void createDatabase(std::string db_name);
  virtual bool put(const kv_key_t &key, ds_bulk_t &data);
  virtual bool get(const kv_key_t &key, ds_bulk_t &data);
  virtual bool get(const kv_key_t &key, std::vector<ds_bulk_t> &data);
protected:
  leveldb::DB *_dbm = NULL;
private:
  std::string key2string(const kv_key_t &key);
  kv_key_t string2key(std::string &keystr);
};

#endif // datastore_h
