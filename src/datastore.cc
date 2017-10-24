// Copyright (c) 2017, Los Alamos National Security, LLC.
// All rights reserved.
#include "datastore.h"
#include <boost/filesystem.hpp>

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
  AbstractDataStore(Duplicates::IGNORE, false, false) {
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
  AbstractDataStore(Duplicates::IGNORE, false, false) {
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

BerkeleyDBDataStore::BerkeleyDBDataStore() :
  AbstractDataStore(Duplicates::IGNORE, false, false) {
  _dbm = NULL;
  _dbenv = NULL;
};

BerkeleyDBDataStore::BerkeleyDBDataStore(Duplicates duplicates, bool eraseOnGet, bool debug) :
  AbstractDataStore(duplicates, eraseOnGet, debug) {
  _dbm = NULL;
  _dbenv = NULL;
};
  
ds_bulk_t BerkeleyDBDataStore::key2ds_bulk(const kv_key_t &key) {
  ds_bulk_t keydata(sizeof(kv_key_t), 0);
  uint64_t *p = reinterpret_cast<uint64_t*>(&(keydata[0]));
  *p = key;
  return keydata;
};

kv_key_t BerkeleyDBDataStore::ds_bulk2key(ds_bulk_t &keydata) {
  kv_key_t *key = reinterpret_cast<kv_key_t*>(&(keydata[0]));
  return *key;
};

BerkeleyDBDataStore::~BerkeleyDBDataStore() {
  delete _dbm;
  delete _dbenv;
};

void BerkeleyDBDataStore::createDatabase(std::string db_name) {
  int status = 0;

  // db_name assumed to include the full path (e.g. /var/data/db.dat)
  boost::filesystem::path path(db_name);
  std::string basepath = path.parent_path().string();
  std::string dbname = path.filename().string();
  if (!basepath.empty()) {
    boost::filesystem::create_directories(basepath);
  }

  // initialize the environment
  uint32_t flags=
    DB_CREATE      | // Create the environment if it does not exist
    DB_PRIVATE     |
    DB_RECOVER     | // Run normal recovery.
    //DB_INIT_LOCK | // Initialize the locking subsystem
    DB_INIT_LOG    | // Initialize the logging subsystem
    DB_INIT_TXN    | // Initialize the transactional subsystem. This
    DB_AUTO_COMMIT |
    //DB_THREAD    | // Cause the environment to be free-threaded
    DB_INIT_MPOOL;   // Initialize the memory pool (in-memory cache)

  try {
    // create and open the environment
    uint32_t flag = DB_CXX_NO_EXCEPTIONS;
    int scratch_size = 1; // what's this?
    _dbenv = new DbEnv(flag);
    _dbenv->set_lk_detect(DB_LOCK_MINWRITE);

    _dbenv->set_error_stream(&std::cerr);
    _dbenv->set_cachesize(scratch_size, 0, 0);
    _dbenv->open(basepath.c_str(), flags, 0644);
  }
  catch (DbException &e) {
    std::cerr << "BerkeleyDBDataStore::createDatabase: BerkeleyDB error on environment open = " 
	      << e.what() << std::endl;
    status = 1; // failure
  }
  
  if (status == 0) {
    _dbm = new Db(_dbenv, DB_CXX_NO_EXCEPTIONS);

    uint32_t flags = DB_CREATE; // Allow database creation
    status = _dbm->open(NULL, dbname.c_str(), NULL, DB_HASH, flags, 0);
    if (status != 0) { // is this the right test for error?
      std::cerr << "BerkeleyDBDataStore::createDatabase: BerkeleyDB error on DB open" << std::endl;
    }
  }
  assert(status == 0); // fall over
  
  // debugging support?
};

bool BerkeleyDBDataStore::put(const kv_key_t &key, ds_bulk_t &data) {
  int status = 0;
  bool success = false;
  
  ds_bulk_t keydata = key2ds_bulk(key);
  Dbt db_key(&(keydata[0]), uint32_t(keydata.size()));
  Dbt get_data;
  status = _dbm->get(NULL, &db_key, &get_data, 0);

  bool duplicate_key = false;
  if (status != DB_NOTFOUND && status != DB_KEYEMPTY) {
    duplicate_key = true;
  }
  bool insert_key = true;
  if (duplicate_key) {
    insert_key = false;
    if (_duplicates == Duplicates::IGNORE) {
      // silently ignore
      success = true;
    }
    else {
      std::cerr << "BerkeleyDBDataStore::put: duplicate key " <<  key
		<< ", duplicates not supported" << std::endl;
    }
  }
  
  if (insert_key) {
    Dbt put_data(&(data[0]), uint32_t(data.size()));
    uint32_t flags = DB_NOOVERWRITE;
    status = _dbm->put(NULL, &db_key, &put_data, flags);
    if (status == 0) { // is this the right test for success?
      success = true;
    }
    else {
      std::cerr << "BerkeleyDBDataStore::put: BerkeleyDB error on put = " << status << std::endl;
    }
  }

  return success;
};

bool BerkeleyDBDataStore::get(const kv_key_t &key, ds_bulk_t &data) {
  int status = 0;
  bool success = false;

  data.clear();

  ds_bulk_t keydata = key2ds_bulk(key);
  Dbt db_key(&(keydata[0]), uint32_t(keydata.size()));
  Dbt db_data;
  status = _dbm->get(NULL, &db_key, &db_data, 0);

  if (status != DB_NOTFOUND && status != DB_KEYEMPTY) {
    data.resize(db_data.get_size(), 0);
    memcpy(&(data[0]), db_data.get_data(), db_data.get_size());
    success = true;
  }
  else {
    std::cerr << "BerkeleyDBDataStore::get: BerkeleyDB error on Get = " << status << std::endl;
  }

  return success;
};

bool BerkeleyDBDataStore::get(const kv_key_t &key, std::vector<ds_bulk_t> &data) {
  bool success = false;

  data.clear();
  ds_bulk_t value;
  if (get(key, value)) {
    data.push_back(value);
    success = true;
  }
  
  return success;
};
