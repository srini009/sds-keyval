#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.69])
AC_INIT([sds-keyval], [0.1], [robl@mcs.anl.gov])
AM_INIT_AUTOMAKE([1.14.1 -Wall -Werror foreign subdir-objects silent-rules])
AM_SILENT_RULES([yes])
AC_CONFIG_MACRO_DIR([m4])
AC_CONFIG_SRCDIR([src/sds-keyval.h])
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

#pkg-config and libraries supporting that
PKG_PROG_PKG_CONFIG
if test "x$PKG_CONFIG" == "x"; then
   AC_MSG_ERROR([Could not find pkg-config utility!])
fi

PKG_CHECK_MODULES([MARGO],[margo],[],
      AC_MSG_ERROR([Could not find working margo installation!]) )
LIBS="$MARGO_LIBS $LIBS"
CPPFLAGS="$MARGO_CFLAGS $CPPFLAGS"
CFLAGS="$MARGO_CFLAGS $CFLAGS"

PKG_CHECK_MODULES([LEVELDB],[leveldb],[],
      AC_MSG_ERROR([Could not find working leveldb installation!]) )
LIBS="$LEVELDB_LIBS $LIBS"
CPPFLAGS="$LEVELDB_CFLAGS $CPPFLAGS"
CFLAGS="$LEVELDB_CFLAGS $CFLAGS"

# Checks for typedefs, structures, and compiler characteristics.
AC_C_INLINE
AC_TYPE_INT64_T
AC_TYPE_SIZE_T
AC_TYPE_SSIZE_T

AC_CHECK_HEADER([iostream],
  [AC_DEFINE([HAVE_CXX_STDHEADERS], [1], [Standard C++ Headers])])

# Checks for library functions.
AC_FUNC_MALLOC
AC_FUNC_REALLOC

PKG_CHECK_MODULES([MARGO],[margo],[],
      AC_MSG_ERROR([Could not find working margo installation!]) )
LIBS="$MARGO_LIBS $LIBS"
CPPFLAGS="$MARGO_CFLAGS $CPPFLAGS"
CFLAGS="$MARGO_CFLAGS $CFLAGS"


bwtree_backend=yes
AC_ARG_ENABLE([bwtree],
	     AS_HELP_STRING([--disable-bwtree], [Disable BwTree as server backend.  Will use Berkely DB if disabled (default is no) ]),
	     [bwtree_backend=${enableval}], [bwtree_backend=yes]
)

if test "x${bwtree_backend}" != xyes ; then
	AC_DEFINE([DISABLE_BWTREE], 1, [do not use BwTree backend])
	AC_CHECK_HEADERS([db.h], ,
			 AC_ERROR("Could not find alternate backend") )
	LIBS="${LIBS} -ldb"
fi

AC_CONFIG_FILES([Makefile maint/kv-client.pc maint/kv-server.pc])
AC_OUTPUT

