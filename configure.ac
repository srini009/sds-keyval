#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.69])
AC_INIT([sds-keyval], [0.1], [robl@mcs.anl.gov])
AM_INIT_AUTOMAKE([1.13.4 -Wall -Werror foreign subdir-objects silent-rules])
AM_SILENT_RULES([yes])
AC_CONFIG_MACRO_DIR([m4])
AC_CONFIG_SRCDIR([src/sds-keyval.h])
AC_CONFIG_HEADERS([src/kv-config.h])
LT_PREREQ([2.2])
# LT_INIT needs to know about AR
AM_PROG_AR
LT_INIT([dlopen])

# Checks for programs.
AC_PROG_CC
# subdir objects needs a bit of help
AM_PROG_CC_C_O
AC_PROG_CXX
AC_PROG_INSTALL

# Checks for libraries.
# Checks for header files.

# Checks for typedefs, structures, and compiler characteristics.
AC_C_INLINE
AC_TYPE_INT64_T
AC_TYPE_SIZE_T
AC_TYPE_SSIZE_T

# Checks for library functions.
AC_FUNC_MALLOC
AC_FUNC_REALLOC

AX_CXX_COMPILE_STDCXX_14

#pkg-config and libraries supporting that
PKG_PROG_PKG_CONFIG
if test "x$PKG_CONFIG" == "x"; then
   AC_MSG_ERROR([Could not find pkg-config utility!])
fi

# coreutils checks for OSX
AC_ARG_VAR([TIMEOUT], timeout program)
AC_ARG_VAR([MKTEMP], mktemp program)

if test -z "$TIMEOUT" ; then
    AC_CHECK_PROGS(TIMEOUT, [timeout gtimeout])
    if test -z "$TIMEOUT" ; then
        AC_MSG_ERROR([Could not find timeout command (can optionally provide via the TIMEOUT variable)])
    fi
else
    AC_SUBST([TIMEOUT], ["$TIMEOUT"])
fi

if test -z "$MKTEMP" ; then
    AC_CHECK_PROGS(MKTEMP, [mktemp gmktemp])
    if test -z "$MKTEMP" ; then
        AC_MSG_ERROR([Could not find mktemp command (can optionally provide via the MKTEMP variable)])
    fi
else
    AC_SUBST([MKTEMP], ["$MKTEMP"])
fi

PKG_CHECK_MODULES([MARGO],[margo],[],
      AC_MSG_ERROR([Could not find working margo installation!]) )
LIBS="$MARGO_LIBS $LIBS"
CPPFLAGS="$MARGO_CFLAGS $CPPFLAGS"
CFLAGS="$MARGO_CFLAGS $CFLAGS"

PKG_CHECK_MODULES([SSG], [ssg], [],
		  AC_MSG_ERROR([Could not find ssg]) )
CFLAGS="$SSG_CFLAGS $CFLAGS"
SERVER_LIBS="$SSG_LIBS $SERVER_LIBS"

PKG_CHECK_MODULES([CHPLACEMENT], [ch-placement], [],
		  AC_MSG_ERROR([Could not find ch-placement]) )
CFLAGS="$CHPLACEMENT_CFLAGS $CFLAGS"
SERVER_LIBS="$CHPLAEMENT_LIBS $SERVER_LIBS"
GROUP_LIBS="$CHPLACEMENT_LIBS $CLIENT_LIBS"

# todo: proper configure checks for boost
SERVER_LIBS="$SERVER_LIBS -lboost_filesystem -lboost_system"

# we have three possible backends for our datastore.  If none are selected,
# then nothing will initialize the class and well that doesn't make any sense
# anyway.  Set BwTree as a default option as it's self-contained to the tree.
# Other backends can be selected

bwtree_backend=yes
leveldb_backend=no
berkelydb_backend=no

AC_ARG_ENABLE([berkeleydb],
	      AS_HELP_STRING([--enable-berkeleydb], [Select "berkeleydb" as
			      storage backend (default is no)]),
	      [berkelydb_backend=${enableval}]
)
AC_ARG_ENABLE([leveldb],
	      AS_HELP_STRING([--enable-leveldb], [Select "leveldb" as storage
			      backend (default is no)]),
	      [leveldb_backend=${enableval}]
)
AC_ARG_ENABLE([bwtree],
	     AS_HELP_STRING([--enable-bwtree], [Enable BwTree as server
			     backend.  (default is no) ]),
	     [bwtree_backend=${enableval}], [bwtree_backend=no]
)


if test "x${berkelydb_backend}" == xyes ; then
	AC_LANG_PUSH([C++])
	AC_CHECK_HEADERS([db_cxx.h], ,
			 AC_ERROR("Could not find Berkely DB C++ headers"))
	AC_CHECK_HEADERS([dbstl_map.h],,
			 AC_ERROR("Could not find Berkely DB STL headers"))
	AC_DEFINE([USE_BDB], 1, [Use Berkely DB backend])
	SERVER_LIBS="${SERVER_LIBS} -ldb_cxx -ldb_stl"
	AC_LANG_POP
fi

if test "x${leveldb_backend}" == xyes ; then
	# spack provides a pkgconfig file for leveldb but other leveldb
	# packages probably do not provide one
	AC_CHECK_HEADERS([leveldb/c.h], ,
			 AC_ERROR("Could not find leveldb headers"))
	AC_DEFINE([USE_LEVELDB], 1, [use leveldb backend])
	SERVER_LIBS="${SERVER_LIBS} -lleveldb"
fi

if test "x${bwtree_backend}" == xyes ; then
	AC_DEFINE([USE_BWTREE], 1, [use BwTree backend])
	AC_MSG_WARN([BwTree backend is deprecated])
	CPPFLAGS="-I${srcdir}/src/BwTree/src ${CPPFLAGS}"
	CXXFLAGS="-pthread -g -Wall -mcx16 -Wno-invalid-offsetof ${CXXFLAGS}"
fi

AM_CONDITIONAL([BUILD_BDB], [test "x${berkelydb_backend}" == xyes])
AM_CONDITIONAL([BUILD_LEVELDB], [test "x${leveldb_backend}" == xyes])
AM_CONDITIONAL([BUILD_BWTREE], [test "x${bwtree_backend}" == xyes])

AC_SUBST(SERVER_LIBS)
AC_SUBST(GROUP_LIBS)
AC_CONFIG_FILES([Makefile maint/sdskv-client.pc maint/sdskv-server.pc])
AC_OUTPUT

