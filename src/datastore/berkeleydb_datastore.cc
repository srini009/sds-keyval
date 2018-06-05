// Copyright (c) 2017, Los Alamos National Security, LLC.
// All rights reserved.
#include "berkeleydb_datastore.h"
#include "fs_util.h"
#include "kv-config.h"
#include <sstream>
#include <chrono>
#include <cstring>
#include <iostream>

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
//  delete _dbm;
  delete _wrapper;
  delete _dbenv;
};

void BerkeleyDBDataStore::createDatabase(const std::string& db_name, const std::string& db_path) {
  int status = 0;

  if (!db_path.empty()) {
    mkdirs(db_path.c_str());
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
      _dbenv->open(db_path.c_str(), flags, 0644);
    }
  }
  catch (DbException &e) {
    std::cerr << "BerkeleyDBDataStore::createDatabase: BerkeleyDB error on environment open = " 
	      << e.what() << std::endl;
    status = 1; // failure
  }
  
  if (status == 0) {
    _wrapper = new DbWrapper(_dbenv, DB_CXX_NO_EXCEPTIONS);
    _dbm = &(_wrapper->_db);

    if (_duplicates == Duplicates::ALLOW) {
      _dbm->set_flags(DB_DUP); // Allow duplicate keys
    }

    _dbm->set_bt_compare(&(BerkeleyDBDataStore::compkeys));
  
    uint32_t flags = DB_CREATE | DB_AUTO_COMMIT | DB_THREAD; // Allow database creation
    if (_in_memory) {
      status = _dbm->open(NULL, // txn pointer
			  NULL, // NULL for in-memory DB
			  NULL, // logical DB name
			  DB_BTREE, // DB type (e.g. BTREE, HASH)
			  flags,
			  0);
      if (status == 0) {
	DbMpoolFile *mpf = _dbm->get_mpf();
	mpf->set_flags(DB_MPOOL_NOFILE, 1);
      }
    }
    else {
      status = _dbm->open(NULL, // txn pointer
			  db_name.c_str(), // file name
			  NULL, // logical DB name
			  DB_BTREE, // DB type (e.g. BTREE, HASH)
			  flags,
			  0);
    }
    if (status != 0) { // is this the right test for error?
      std::cerr << "BerkeleyDBDataStore::createDatabase: BerkeleyDB error on DB open" << std::endl;
      std::cerr << "status = " << status << std::endl;
    }
  }
  assert(status == 0); // fall over

  // debugging support?
};

void BerkeleyDBDataStore::set_comparison_function(comparator_fn less) {
    _wrapper->_less = less;
}

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

bool BerkeleyDBDataStore::erase(const ds_bulk_t &key) {
    Dbt db_key((void*)key.data(), key.size());
    int status = _dbm->del(NULL, &db_key, 0);
    return status == 0;
} 

// In the case where Duplicates::ALLOW, this will return the first
// value found using key.
bool BerkeleyDBDataStore::get(const ds_bulk_t &key, ds_bulk_t &data) {
  int status = 0;
  bool success = false;

  data.clear();

  Dbt db_key((void*)&(key[0]), uint32_t(key.size()));
  db_key.set_ulen(uint32_t(key.size()));
  Dbt db_data;
  db_key.set_flags(DB_DBT_USERMEM);
  db_data.set_flags(DB_DBT_MALLOC);
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

void BerkeleyDBDataStore::set_in_memory(bool enable) {
  _in_memory = enable;
};

std::vector<ds_bulk_t> BerkeleyDBDataStore::vlist_keys(const ds_bulk_t &start, size_t count, const ds_bulk_t &prefix)
{
    std::vector<ds_bulk_t> keys;
    Dbc * cursorp;
    Dbt key, data;
    int ret;
    _dbm->cursor(NULL, &cursorp, 0);

    /* 'start' is like RADOS: not inclusive  */
    if (start.size()) {
	    key.set_size(start.size());
	    key.set_data((void *)start.data());
	    ret = cursorp->get(&key, &data, DB_SET_RANGE);
	    if (ret != 0) {
	        cursorp->close();
	        return keys;
	    }
    } else {
        ret = cursorp->get(&key, &data, DB_FIRST);
        if (ret != 0) {
            cursorp->close();
            return keys;
        }
    }

	ds_bulk_t k((char*)key.get_data(), ((char*)key.get_data())+key.get_size());
	/* SET_RANGE will return the smallest key greater than or equal to the
	 * requested key, but we want strictly greater than */
    int c = 0;
	if (k != start) {
        c = std::memcmp(prefix.data(), k.data(), prefix.size());
        if(c == 0) {
            keys.push_back(std::move(k));
        }
    }
    while (keys.size() < count && c >= 0) {
	    ret = cursorp->get(&key, &data, DB_NEXT);
	    if (ret !=0 ) break;
        
        ds_bulk_t k((char*)key.get_data(), ((char*)key.get_data())+key.get_size());
        c = std::memcmp(prefix.data(), k.data(), prefix.size());
        if(c == 0) {
            keys.push_back(std::move(k));
        }
    }
    cursorp->close();
    return keys;
}

std::vector<std::pair<ds_bulk_t,ds_bulk_t>> BerkeleyDBDataStore::vlist_keyvals(const ds_bulk_t &start, size_t count, const ds_bulk_t &prefix)
{
    std::vector<std::pair<ds_bulk_t,ds_bulk_t>> result;
    Dbc * cursorp;
    Dbt key, data;
    int ret;
    _dbm->cursor(NULL, &cursorp, 0);

    /* 'start' is like RADOS: not inclusive  */
    if (start.size()) {
	    key.set_size(start.size());
	    key.set_data((void *)start.data());
	    ret = cursorp->get(&key, &data, DB_SET_RANGE);
	    if (ret != 0) {
	        cursorp->close();
	        return result;
	    }
    } else {
        ret = cursorp->get(&key, &data, DB_FIRST);
        if (ret != 0) {
            cursorp->close();
            return result;
        }
    }

	ds_bulk_t k((char*)key.get_data(), ((char*)key.get_data())+key.get_size());
    ds_bulk_t v((char*)data.get_data(), ((char*)data.get_data())+data.get_size());

	/* SET_RANGE will return the smallest key greater than or equal to the
	 * requested key, but we want strictly greater than */
    int c = 0;
	if (k != start) {
        c = std::memcmp(prefix.data(), k.data(), prefix.size());
        if(c == 0) {
            result.push_back(std::make_pair(std::move(k),std::move(v)));
        }
    }
    while (result.size() < count && c >= 0) {
	    ret = cursorp->get(&key, &data, DB_NEXT);
	    if (ret !=0 ) break;
        
        ds_bulk_t k((char*)key.get_data(), ((char*)key.get_data())+key.get_size());
        ds_bulk_t v((char*)data.get_data(), ((char*)data.get_data())+data.get_size());

        c = std::memcmp(prefix.data(), k.data(), prefix.size());
        if(c == 0) {
            result.push_back(std::make_pair(std::move(k), std::move(v)));
        }
    }
    cursorp->close();
    return result;
}

int BerkeleyDBDataStore::compkeys(Db *db, const Dbt *dbt1, const Dbt *dbt2, size_t *locp) {
    DbWrapper* _wrapper = (DbWrapper*)(((char*)db) - offsetof(BerkeleyDBDataStore::DbWrapper, _db));
    if(_wrapper->_less) {
        return (_wrapper->_less)(dbt1->get_data(), dbt1->get_size(), 
                dbt2->get_data(), dbt2->get_size());
    } else {
        size_t s = dbt1->get_size() > dbt2->get_size() ? dbt2->get_size() : dbt1->get_size();
        int c = std::memcmp(dbt1->get_data(), dbt2->get_data(), s);
        if(c != 0) return c;
        if(dbt1->get_size() < dbt2->get_size()) return -1;
        if(dbt1->get_size() > dbt2->get_size()) return 1;
        return 0;
    }
}
