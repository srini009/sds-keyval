// Copyright (c) 2017, Los Alamos National Security, LLC.
// All rights reserved.
#include "datastore.h"
#include "kv-config.h"
#include <chrono>
#include <iostream>

#include <boost/filesystem.hpp>

using namespace std::chrono;

AbstractDataStore::AbstractDataStore() {
  _duplicates = Duplicates::IGNORE;
  _eraseOnGet = false;
  _debug = false;
  _in_memory = false;
};

AbstractDataStore::AbstractDataStore(Duplicates duplicates, bool eraseOnGet, bool debug) {
  _duplicates = duplicates;
  _eraseOnGet = eraseOnGet;
  _debug = debug;
  _in_memory = false;
};

AbstractDataStore::~AbstractDataStore()
{};

