ACLOCAL_AMFLAGS="-Im4"

#bin_PROGRAMS = kv-client\
#		kv-server

lib_LTLIBRARIES = libkvclient.la \
                  libkvserver.la

libkvclient_la_SOURCES = src/kv-client.c

libkvserver_la_SOURCES = src/kv-server.cc

include_HEADERS = src/sds-keyval.h

check_PROGRAMS = test/test-client \
		test/test-server

TESTS = test/test-client \
        test/test-server

test_test_client_CPPFLAGS = ${CPPFLAGS} -I${srcdir}/src
test_test_client_LDADD =  ${LIBS} -lkvclient

test_test_server_CPPFLAGS = ${CPPFLAGS} -I${srcdir}/src
test_test_server_LDADD =  ${LIBS} -lkvserver
