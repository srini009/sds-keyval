# SDSKV (SDS Key/Val)

## Installation

SDSKV can easily be installed using Spack:

`spack install sdskeyval`

This will install SDSKV (and any required dependencies). 
Available backends will be _Map_ (in-memory C++ std::map, useful for testing)
and BwTree (deprecated). To enable the BerkeleyDB and LevelDB backends,
ass `+bdb` and `+leveldb` respectively. For example:

`spack install sdskeyval+bdb+leveldb`

Note that if you are using a system boost path in spack (in your
packages.yaml) rather than letting spack build boost, then you must
install libboost-system-dev and libboost-filesystem-dev packages on
your system.

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

## C++ API

An object-oriented C++ API is available in `sdskv-client.hpp` and `sdskv-server.hpp`.
On the client side this API provides the `client`, `provider_handle`, and `database` objects.
Examples of usage of these objects can be found in the `test/sdskv-cxx-test.cc`.
On the server side, this API provides a `provider` object.

## Benchmark

SDSKV can be compiled with `--enable-benchmark` (or `+benchmark` in Spack). In this case,
SDSKV requires the JsonCPP and MPI dependencies (when compiling manually, use `CXX=mpicxx` in
your configure step, for example), and it will build and install the `sdskv-benchmark` program.

This program is an MPI program that reads a JSON file describing a series of access patterns.
Rank 0 of this MPI program acts as an SDSKV server. Other ranks act as clients, all executing
this access pattern.

The following is an example of a JSON file.

```json
{
	"protocol" : "tcp",
	"seed" : 0,
	"server" : {
		"use-progress-thread" : false,
		"rpc-thread-count" : 0,
		"database" : {
			"type" : "map",
			"name" : "benchmark-db",
			"path" : "/dev/shm"
		}
	},
	"benchmarks" : [
	{
		"type" : "put",
		"repetitions" : 10,
		"num-entries" : 30,
		"key-sizes" : [ 8, 32 ],
		"val-sizes" : [ 24, 48 ],
		"erase-on-teardown" : true
	},
	{
		"type" : "get",
		"repetitions" : 10,
		"num-entries" : 30,
		"key-sizes" : 64,
		"val-sizes" : 128,
		"erase-on-teardown" : true
	}
	]
}
```

The JSON file starts with the protocol to use, and a seed for the random-number generator (RNG).
The actual seed used on each rank will actually be a function of this global seed and the rank of
the client. The RNG will be reset with this seed after each benchmark.

The `server` field sets up the provider and the database. Database types can be `map`, `ldb`, or `bdb`.
Then follows the `benchmarks` entry, which is a list of benchmarks to execute. Each benchmark is composed
of three steps. A *setup* phase, an *execution* phase, and a *teardown* phase. The setup phase may for
example store a bunch of keys in the database that the execution phase will read by (in the case of a
*get* benchmark, for example). The teardown phase will usually remove all the keys that were written
during the benchmark, if "erase-on-teardown" is set to `true`.

Each benchmark entry has a `type` (which may be `put`, `put-multi`, `get`, `get-multi`, `length`,
`length-multi`, `erase`, and `erase-multi`), and a number of repetitions. The benchmark will be
executed as many times as requested (without resetting the RNG in between repetitions). Taking the
example of the `put` benchmark above, each repetition will put 30 key/value pairs into the database.
The key size will be chosen randomly in a uniform manner in the interval `[8, 32 [` (32 excluded).
The value size will be chosen randomly in a uniform manner in `[24, 48 [` (48 excluded). Note that
you may also set a specific size instead of a range.

An MPI barrier between clients is executed in between each benchmark and in between the setup,
execution, and teardown phases, so that the execution phase is always executed at the same time
on all the clients. Once all the repetitions are done for a given benchmark entry, the program
will report statistics on the timings: average time, variance, standard deviation, mininum, maximum,
median, first and third quartiles. Note that these times are for a repetition, not for single operations
within a repetition. To get the timing of each individual operation, it is then necessary to divide
the times by the number of key/value pairs involved in the benchmark.
