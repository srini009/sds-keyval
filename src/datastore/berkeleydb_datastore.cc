// Copyright (c) 2017, Los Alamos National Security, LLC.
// All rights reserved.
#include "berkeleydb_datastore.h"
#include "kv-config.h"
#include <chrono>
#include <iostream>
#include <boost/filesystem.hpp>

using namespace std::chrono;

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
  uint32_t flags = 0;
  if (_in_memory) {
    // not sure if we want all of these for in_memory
    flags =
      DB_CREATE      | // Create the environment if it does not exist
      DB_PRIVATE     |
      DB_RECOVER     | // Run normal recovery.
      DB_INIT_LOCK   | // Initialize the locking subsystem
      DB_INIT_LOG    | // Initialize the logging subsystem
      DB_INIT_TXN    | // Initialize the transactional subsystem. This
      DB_THREAD      | // Cause the environment to be free-threaded
      DB_AUTO_COMMIT |
      DB_INIT_MPOOL;   // Initialize the memory pool (in-memory cache)
  }
  else {
    flags =
      DB_CREATE      | // Create the environment if it does not exist
      DB_PRIVATE     |
      DB_RECOVER     | // Run normal recovery.
      DB_INIT_LOCK   | // Initialize the locking subsystem
      DB_INIT_LOG    | // Initialize the logging subsystem
      DB_INIT_TXN    | // Initialize the transactional subsystem. This
      DB_THREAD      | // Cause the environment to be free-threaded
      DB_AUTO_COMMIT |
      DB_INIT_MPOOL;   // Initialize the memory pool (in-memory cache)
  }

  try {
    // create and open the environment
    uint32_t flag = DB_CXX_NO_EXCEPTIONS;
    int scratch_size = 1; // 1GB cache
    _dbenv = new DbEnv(flag);
    _dbenv->set_error_stream(&std::cerr);
    _dbenv->set_cachesize(scratch_size, 0, 0);
    if (_in_memory) {
      _dbenv->log_set_config(DB_LOG_IN_MEMORY, 1);
      _dbenv->set_lg_bsize(scratch_size * 1024 * 1024 * 1024); // in GB
      _dbenv->open(NULL, flags, 0);
    }
    else {
      _dbenv->set_lk_detect(DB_LOCK_MINWRITE);
      _dbenv->open(basepath.c_str(), flags, 0644);
    }
  }
  catch (DbException &e) {
    std::cerr << "BerkeleyDBDataStore::createDatabase: BerkeleyDB error on environment open = " 
	      << e.what() << std::endl;
    status = 1; // failure
  }
  
  if (status == 0) {
    _dbm = new Db(_dbenv, DB_CXX_NO_EXCEPTIONS);

    if (_duplicates == Duplicates::ALLOW) {
      _dbm->set_flags(DB_DUP); // Allow duplicate keys
    }
  
    uint32_t flags = DB_CREATE | DB_AUTO_COMMIT | DB_THREAD; // Allow database creation
    if (_in_memory) {
      status = _dbm->open(NULL, // txn pointer
			  NULL, // NULL for in-memory DB
			  NULL, // logical DB name
			  DB_HASH, // DB type (e.g. BTREE, HASH)
			  flags,
			  0);
      if (status == 0) {
	DbMpoolFile *mpf = _dbm->get_mpf();
	mpf->set_flags(DB_MPOOL_NOFILE, 1);
      }
    }
    else {
      status = _dbm->open(NULL, // txn pointer
			  dbname.c_str(), // file name
			  NULL, // logical DB name
			  DB_HASH, // DB type (e.g. BTREE, HASH)
			  flags,
			  0);
    }
    if (status != 0) { // is this the right test for error?
      std::cerr << "BerkeleyDBDataStore::createDatabase: BerkeleyDB error on DB open" << std::endl;
    }
  }
  assert(status == 0); // fall over

  // debugging support?
};

bool BerkeleyDBDataStore::put(const ds_bulk_t &key, const ds_bulk_t &data) {
  int status = 0;
  bool success = false;
  
  // IGNORE case deals with redundant puts (where key/value is the same). In BerkeleyDB a
  // redundant may overwrite previous value which is fine when key/value is the same.
  // ALLOW case deals with actual duplicates (where key is the same but value is different).
  // This option might be used when eraseOnGet is set (e.g. ParSplice hotpoint use case).
  if (_duplicates == Duplicates::IGNORE || _duplicates == Duplicates::ALLOW) {
    Dbt db_key((void*)&(key[0]), uint32_t(key.size()));
    Dbt db_data((void*)&(data[0]), uint32_t(data.size()));
    db_key.set_flags(DB_DBT_USERMEM);
    db_data.set_flags(DB_DBT_USERMEM);
    uint32_t flags = DB_NOOVERWRITE; // to simply overwrite value, don't use this flag
    status = _dbm->put(NULL, &db_key, &db_data, flags);
    if (status == 0 || 
	(_duplicates == Duplicates::IGNORE && status == DB_KEYEXIST)) {
      success = true;
    }
    else {
      std::cerr << "BerkeleyDBDataStore::put: BerkeleyDB error on put = " << status << std::endl;
    }
  }
  else {
    std::cerr << "BerkeleyDBDataStore::put: Unexpected Duplicates option = " << int32_t(_duplicates) << std::endl;
  }

  return success;
};

// In the case where Duplicates::ALLOW, this will return the first
// value found using key.
bool BerkeleyDBDataStore::get(const ds_bulk_t &key, ds_bulk_t &data) {
  int status = 0;
  bool success = false;

  data.clear();

  Dbt db_key((void*)&(key[0]), uint32_t(key.size()));
  Dbt db_data;
  db_key.set_flags(DB_DBT_USERMEM);
  db_data.set_flags(DB_DBT_REALLOC);
  status = _dbm->get(NULL, &db_key, &db_data, 0);

  if (status != DB_NOTFOUND && status != DB_KEYEMPTY && db_data.get_size() > 0) {
    data.resize(db_data.get_size(), 0);
    memcpy(&(data[0]), db_data.get_data(), db_data.get_size());
    free(db_data.get_data());
    success = true;
  }
  else {
    std::cerr << "BerkeleyDBDataStore::get: BerkeleyDB error on Get = " << status << std::endl;
  }
  
  if (success && _eraseOnGet) {
    status = _dbm->del(NULL, &db_key, 0);
    if (status != 0) {
      success = false;
      std::cerr << "BerkeleyDBDataStore::get: BerkeleyDB error on delete (eraseOnGet) = " << status << std::endl;
    }
  }

  return success;
};

// TODO: To return more than 1 value (when Duplicates::ALLOW), this code should
// use the c_get interface.
bool BerkeleyDBDataStore::get(const ds_bulk_t &key, std::vector<ds_bulk_t> &data) {
  bool success = false;

  data.clear();
  ds_bulk_t value;
  if (get(key, value)) {
    data.push_back(value);
    success = true;
  }
  
  return success;
};

void BerkeleyDBDataStore::BerkeleyDBDataStore::set_in_memory(bool enable) {
  _in_memory = enable;
};

std::vector<ds_bulk_t> BerkeleyDBDataStore::BerkeleyDBDataStore::list(const ds_bulk_t &start, size_t count)
{
    std::vector<ds_bulk_t> keys;
    Dbc * cursorp;
    Dbt key, data;
    _dbm->cursor(NULL, &cursorp, 0);
    while (cursorp->get(&key, &data, DB_NEXT) == 0) {
	ds_bulk_t k(key.get_size() );
	memcpy(k.data(), key.get_data(), key.get_size() );
	/* I hope this is a deep copy! */
	keys.push_back(std::move(k));
    }
    cursorp->close();
    return keys;
}
