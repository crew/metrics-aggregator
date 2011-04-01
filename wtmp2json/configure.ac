#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.64])
AC_INIT([wtmp2json], [0.1], [lee@ccs.neu.edu])
AC_CONFIG_SRCDIR([main.c])
AM_INIT_AUTOMAKE
# AC_CONFIG_HEADERS([])

# Checks for programs.
AC_PROG_CC

# Checks for libraries.

# Checks for header files.
AC_CHECK_HEADERS([string.h unistd.h utmp.h])

# Checks for typedefs, structures, and compiler characteristics.

# Checks for library functions.
AC_CHECK_FUNCS([utmpname])

AC_CONFIG_FILES([Makefile])
AC_OUTPUT
