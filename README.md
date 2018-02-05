# SDSKV (SDS Key/Val)

## Installation

SDSKV can easily be installed using Spack:

`spack install sdskeyval`

This will install SDSKV (and any required dependencies). 
Available backends will be _Map_ (in-memory C++ std::map, useful for testing)
and BwTree (deprecated). To enable the BerkeleyDB and LevelDB backends,
ass `+bdb` and `+leveldb` respectively. For example:

`spack install sdskeyval+bdb+leveldb`

## Architecture

List most mochi services, SDSKV relies on a client/provider architecture.
A provider, identified by its _address_ and _multiplex id_, manages one or more
databases, referenced externally by their database id.

## Starting a daemon

SDSKV ships with a default daemon program that can setup providers and
databases. This daemon can be started as follows:

`sdskv-server-daemon [OPTIONS] <listen_addr> <db name 1>[:map|:bwt|:bdb|:ldb] <db name 2>[:map|:bwt|:bdb|:ldb] ...`

For example:

`sdskv-server-daemon tcp://localhost:1234 foo:bdb bar`

listen_addr is the address at which to listen; database names should be provided in the form
_name:type_ where _type_ is _map_ (std::map), _bwt_ (BwTree), _bdb_ (Berkeley DB), or _ldb_ (LevelDB).

For database that are persistent like BerkeleyDB or LevelDB, the name should be a path to the
file where the database will be put (this file should not exist).

The following additional options are accepted:

* `-f` provides the name of the file in which to write the address of the daemon.
* `-m` provides the mode (providers or databases).

The providers mode indicates that, if multiple SDSKV databases are used (as above),
these databases should be managed by multiple providers, accessible through 
different multiplex ids 1, 2, ... N where N is the number of databases
to manage. The targets mode indicates that a single provider should be used to
manage all the databases. This provider will be accessible at multiplex id 1.

## Client API

The client API is available in _sdskv-client.h_.
The codes in the _test_ folder illustrate how to use it.

## Provider API

The server-side API is available in _sdskv-server.h_.
The code of the daemon (_src/sdskv-server-daemon.c_) can be used as an example.

### Custom key comparison function

It is possible to specify a custom function for comparing/sorting keys
when creating a provider. A comparison function must have the following prototype:

`int (*)(const void* key1, size_t keysize1, const void* key2, size_t keysize2)`

Its return value must be < 0 if key1 < key2, 0 if key1 = key2, > 0 if key1 > key2.
It must define a total order of the key space.
