// Copyright (c) 2017, Los Alamos National Security, LLC.
// All rights reserved.
#include "datastore.h"

AbstractDataStore::AbstractDataStore() {
  _duplicates = Duplicates::IGNORE;
  _eraseOnGet = false;
  _debug = false;
};

AbstractDataStore::AbstractDataStore(Duplicates duplicates, bool eraseOnGet, bool debug) {
  _duplicates = duplicates;
  _eraseOnGet = eraseOnGet;
  _debug = debug;
};

AbstractDataStore::~AbstractDataStore()
{};

BwTreeDataStore::BwTreeDataStore() :
  AbstractDataStore() {
  _tree = NULL;
};

BwTreeDataStore::BwTreeDataStore(Duplicates duplicates, bool eraseOnGet, bool debug) :
  AbstractDataStore(duplicates, eraseOnGet, debug) {
  _tree = NULL;
};
  
BwTreeDataStore::~BwTreeDataStore() {
  // deleting BwTree can cause core dump
  delete _tree;
};

void BwTreeDataStore::createDatabase(std::string db_name) {
  _tree = new BwTree<kv_key_t, ds_bulk_t, std::less<kv_key_t>,
		     std::equal_to<kv_key_t>, std::hash<kv_key_t>,
		     my_equal_to/*ds_bulk_t*/, my_hash/*ds_bulk_t*/>();
  if (_debug) {
    _tree->SetDebugLogging(1);
  }
  else {
    _tree->SetDebugLogging(0);
  }
  _tree->UpdateThreadLocal(1);
  _tree->AssignGCID(0);
};

bool BwTreeDataStore::put(const kv_key_t &key, ds_bulk_t &data) {
  std::vector<ds_bulk_t> values;
  bool success = false;
  
  _tree->GetValue(key, values);
  bool duplicate_key = (values.size() != 0);

  if (duplicate_key) {
    if (_duplicates == Duplicates::IGNORE) {
      // silently ignore
      success = true;
    }
    else { // Duplicates::ALLOW (default)
      success = _tree->Insert(key, data);
    }
  }
  else {
    success = _tree->Insert(key, data);
  }
  
  return success;
};

bool BwTreeDataStore::get(const kv_key_t &key, ds_bulk_t &data) {
  std::vector<ds_bulk_t> values;
  bool success = false;

  _tree->GetValue(key, values);
  if (values.size() == 1) {
    data = values.front();
    success = true;
  }
  else if (values.size() > 1) {
    // this should only happen if duplicates are allowed
    if (_duplicates == Duplicates::ALLOW) {
      data = values.front(); // caller is asking for just 1
      success = true;
    }
  }

  return success;
};

bool BwTreeDataStore::get(const kv_key_t &key, std::vector<ds_bulk_t> &data) {
  std::vector<ds_bulk_t> values;
  bool success = false;

  _tree->GetValue(key, values);
  if (values.size() > 1) {
    // this should only happen if duplicates are allowed
    if (_duplicates == Duplicates::ALLOW) {
      data = values;
      success = true;
    }
  }
  else {
    data = values;
    success = true;
  }
  
  return success;
};


LevelDBDataStore::LevelDBDataStore() :
  AbstractDataStore() {
  _dbm = NULL;
};

LevelDBDataStore::LevelDBDataStore(Duplicates duplicates, bool eraseOnGet, bool debug) :
  AbstractDataStore(duplicates, eraseOnGet, debug) {
  _dbm = NULL;
};
  
std::string LevelDBDataStore::key2string(const kv_key_t &key) {
  kv_key_t k = key; // grab a copy to work with
  char *c = reinterpret_cast<char *>(&k);
  std::string keystr(c, sizeof(k));
  return keystr;
};

kv_key_t LevelDBDataStore::string2key(std::string &keystr) {
  kv_key_t *key = reinterpret_cast<kv_key_t*>(&(keystr[0]));
  return *key;
};

LevelDBDataStore::~LevelDBDataStore() {
  // deleting LevelDB can cause core dump
  delete _dbm;
};

void LevelDBDataStore::createDatabase(std::string db_name) {
  leveldb::Options options;
  leveldb::Status status;
  
  // db_name assumed to include the full path (e.g. /var/data/db.dat)
  options.create_if_missing = true;
  status = leveldb::DB::Open(options, db_name, &_dbm);
  
  if (!status.ok()) {
    // error
    std::cerr << "LevelDBDataStore::createDatabase: LevelDB error on Open = " << status.ToString() << std::endl;
  }
  assert(status.ok()); // fall over
  
  // debugging support?
};

bool LevelDBDataStore::put(const kv_key_t &key, ds_bulk_t &data) {
  leveldb::Status status;
  bool success = false;
  
  std::string value;
  status = _dbm->Get(leveldb::ReadOptions(), key2string(key), &value);
  bool duplicate_key = false;
  if (status.ok()) {
    duplicate_key = true;
  }
  else if (!status.IsNotFound()) {
    std::cerr << "LevelDBDataStore::put: LevelDB error on Get = " << status.ToString() << std::endl;
    return false; // give up and return
  }

  bool insert_key = true;
  if (duplicate_key) {
    insert_key = false;
    if (_duplicates == Duplicates::IGNORE) {
      // silently ignore
      success = true;
    }
    else {
      std::cerr << "LevelDBDataStore::put: duplicate key " <<  key
		<< ", duplicates not supported" << std::endl;
    }
  }
  
  if (insert_key) {
    std::string datastr(data.begin(), data.end());
    status = _dbm->Put(leveldb::WriteOptions(), key2string(key), datastr);
    if (status.ok()) {
      success = true;
    }
    else {
      std::cerr << "LevelDBDataStore::put: LevelDB error on Put = " << status.ToString() << std::endl;
    }
  }

  return success;
};

bool LevelDBDataStore::get(const kv_key_t &key, ds_bulk_t &data) {
  leveldb::Status status;
  bool success = false;

  data.clear();
  std::string value;
  status = _dbm->Get(leveldb::ReadOptions(), key2string(key), &value);
  if (status.ok()) {
    data = ds_bulk_t(value.begin(), value.end());
    success = true;
  }
  else if (!status.IsNotFound()) {
    std::cerr << "LevelDBDataStore::get: LevelDB error on Get = " << status.ToString() << std::endl;
  }

  return success;
};

bool LevelDBDataStore::get(const kv_key_t &key, std::vector<ds_bulk_t> &data) {
  bool success = false;

  data.clear();
  ds_bulk_t value;
  if (get(key, value)) {
    data.push_back(value);
    success = true;
  }
  
  return success;
};
