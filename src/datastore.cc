// Copyright (c) 2017, Los Alamos National Security, LLC.
// All rights reserved.
#include "bwtree.h"

#include <vector>

using namespace wangziqi2013::bwtree;

#ifndef datastore_cc
#define datastore_cc

template <typename KeyType, typename ValueType> class AbstractDataStore {
public:
  virtual void createDatabase(std::string homeDir, std::string baseName)=0;
  virtual bool put(const KeyType &key, ValueType &data)=0;
  virtual bool get(const KeyType &key, ValueType &data)=0;
  virtual bool get(const KeyType &key, std::vector<ValueType> &data)=0;
};

template <typename KeyType,
          typename ValueType,
          typename KeyComparator = std::less<KeyType>,
          typename KeyEqualityChecker = std::equal_to<KeyType>,
          typename KeyHashFunc = std::hash<KeyType>,
          typename ValueEqualityChecker = std::equal_to<ValueType>,
          typename ValueHashFunc = std::hash<ValueType>>
class BwTreeDataStore : public AbstractDataStore<KeyType, ValueType> {
public:
  BwTreeDataStore(Duplicates duplicates, bool eraseOnGet, bool debug) {
    _duplicates = duplicates; 
    _eraseOnGet = eraseOnGet;
    _debug = debug;
    _tree = NULL;
  };
  
  virtual ~BwTreeDataStore() {
    delete _tree;
  };

  virtual void createDatabase(std::string homeDir, std::string baseName) {
    _tree = new BwTree<KeyType, ValueType,
		       KeyComparator, KeyEqualityChecker, KeyHashFunc,
		       ValueEqualityChecker, ValueHashFunc>();
    if (_debug) {
      _tree->SetDebugLogging(1);
    }
    else {
      _tree->SetDebugLogging(0);
    }
    _tree->UpdateThreadLocal(1);
    _tree->AssignGCID(0);
  };

  virtual bool put(const KeyType &key, ValueType &data) {
    std::vector<ValueType> values;
    bool success = false;
  
    _tree->GetValue(key, values);
    bool duplicate_key = (values.size() != 0);

    if (duplicate_key) {
      if (_duplicates == Duplicates::IGNORE) {
	success = true;
      }
      else { // Duplicates::ALLOW
	success = _tree->Insert(key, data);
      }
    }
    else {
      success = _tree->Insert(key, data);
    }
  
    return success;
  };

  virtual bool get(const KeyType &key, ValueType &data) {
    std::vector<ValueType> values;
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

  virtual bool get(const KeyType &key, std::vector<ValueType> &data) {
    std::vector<ValueType> values;
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

protected:
  BwTree<KeyType, ValueType,
	 KeyComparator, KeyEqualityChecker, KeyHashFunc,
	 ValueEqualityChecker, ValueHashFunc> *_tree = NULL;
  Duplicates _duplicates;
  bool _eraseOnGet;
  bool _debug;
};

#endif // datastore_cc
