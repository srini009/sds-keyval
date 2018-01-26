ACLOCAL_AMFLAGS="-Im4"

SERVER_LIBS=@SERVER_LIBS@
CLIENT_LiBS=@CLIENT_LIBS@

AM_CPPFLAGS = -I${srcdir}/src

bin_PROGRAMS = bin/sdskv-server-daemon \
	       bin/sdskv-shutdown \
	       test/sdskv-client-test

bin_sdskv_server_daemon_SOURCES = src/sdskv-server-daemon.c
bin_sdskv_server_daemon_DEPENDENCIES = lib/libsdskv-server.la
bin_sdskv_server_daemon_LDFLAGS = -Llib -lsdskv-server
bin_sdskv_server_daemon_LDADD = ${LIBS} -lsdskv-server ${SERVER_LIBS}

bin_sdskv_shutdown_SOURCES = src/sdskv-shutdown.c
bin_sdskv_shutdown_DEPENDENCIES = lib/libsdskv-client.la
bin_sdskv_shutdown_LDFLAGS = -Llib -lsdskv-client
bin_sdskv_shutdown_LDADD = ${LIBS} -lsdskv-client

lib_LTLIBRARIES = lib/libkvclient.la \
                  lib/libkvserver.la \
                  lib/libkvgroupclient.la \
                  lib/libkvgroupserver.la \
		  lib/libsdskv-client.la \
		  lib/libsdskv-server.la

lib_libsdskv_client_la_SOURCES = src/sdskv-client.c

lib_libkvclient_la_SOURCES = src/kv-client.c

lib_libkvgroupclient_la_SOURCES = src/kvgroup-client.cc

lib_libkvserver_la_SOURCES = src/kv-server.cc \
			     src/datastore/datastore.cc

lib_libsdskv_server_la_SOURCES = src/sdskv-server.cc \
				 src/datastore/datastore.cc

if BUILD_BWTREE
lib_libkvserver_la_SOURCES += src/BwTree/src/bwtree.cpp \
			      src/datastore/bwtree_datastore.cc

lib_libsdskv_server_la_SOURCES += src/BwTree/src/bwtree.cpp \
				  src/datastore/bwtree_datastore.cc
endif

if BUILD_BDB
lib_libkvserver_la_SOURCES += src/datastore/berkeleydb_datastore.cc

lib_libsdskv_server_la_SOURCES += src/datastore/berkeleydb_datastore.cc
endif

if BUILD_LEVELDB
lib_libkvserver_la_SOURCES += src/datastore/leveldb_datastore.cc

lib_libsdskv_server_la_SOURCES += src/datastore/leveldb_datastore.cc
endif


include_HEADERS = src/sds-keyval.h \
		  src/sds-keyval-group.h

noinst_HEADERS = src/datastore/datastore.h \
		 src/datastore/bwtree_datastore.h \
		 src/datastore/leveldb_datastore.h \
		 src/datastore/berkeleydb_datastore.h \
		 src/datastore/datastore_factory.h \
		 src/keyval-internal.h \
		 src/BwTree/src/bwtree.h \
		 src/BwTree/src/atomic_stack.h\
		 src/BwTree/src/bloom_filter.h \
		 src/BwTree/src/sorted_small_set.h


lib_libkvgroupserver_la_SOURCES = src/kvgroup-server.cc

check_PROGRAMS = test/sdskv-client-test

TESTS = test/basic.sh

TESTS_ENVIRONMENT = TIMEOUT="$(TIMEOUT)" \
		    MKTEMP="$(MKTEMP)"

test_sdskv_client_test_SOURCES = test/sdskv-client-test.cc
test_sdskv_client_test_DEPENDENCIES = lib/libsdskv-client.la
test_sdskv_client_test_LDFLAGS = -Llib -lsdskv-client

#############################################################
## tests bellow correspond to old tests (see old-test folder)
#############################################################

#bin_PROGRAMS = test/bench-client \
#	       test/test-client \
#	       test/test-server

#test_bench_client_SOURCES = test/bench-client.cc
#test_bench_client_DEPENDENCIES = lib/libkvclient.la
#test_bench_client_LDFLAGS = -Llib -lkvclient
#test_bench_client_LDADD = ${LIBS} -lkvclient

#check_PROGRAMS = test/test-client \
#		test/test-server \
#		test/bench-client \
#		test/test-mpi \
#		test/test-mpi-group
#

#TESTS = test/test-client \
#        test/test-server \
#	test/bench-client \
#	test/test-mpi \
#	test/test-mpi-group

#test_test_client_SOURCES = test/test-client.cc
#test_test_client_DEPENDENCIES = lib/libkvclient.la
#test_test_client_LDFLAGS = -Llib -lkvclient

#test_test_server_SOURCES = test/test-server.cc
#test_test_server_DEPENDENCIES = lib/libkvserver.la
#test_test_server_LDFLAGS = -Llib -lkvserver ${SERVER_LIBS}

#test_test_mpi_SOURCES = test/test-mpi.cc


#test_test_mpi_DEPENDENCIES = lib/libkvserver.la lib/libkvclient.la
#test_test_mpi_LDFLAGS = -Llib -lkvclient -lkvserver ${SERVER_LIBS}

#test_test_mpi_group_SOURCES = test/test-mpi-group.cc
#test_test_mpi_group_DEPENDENCIES = lib/libkvgroupserver.la lib/libkvgroupclient.la lib/libkvserver.la lib/libkvclient.la
#test_test_mpi_group_LDFLAGS = -Llib -lkvgroupserver -lkvgroupclient -lkvclient -lkvserver ${SERVER_LIBS} ${GROUP_LIBS}

pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = maint/sdskv-server.pc \
		 maint/sdskv-client.pc



