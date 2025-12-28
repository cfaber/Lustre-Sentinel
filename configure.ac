# Copyright (C) 2025 Colin Faber <cfaber@thelustrecollective.com>
#
# This file is part of Lustre Sentinel.
#
# Lustre Sentinel is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Lustre Sentinel is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Lustre Sentinel.  If not, see <https://www.gnu.org/licenses/>.

AC_INIT([lsentineld], [1.0], [cfaber@thelustrecollective.com])
AM_INIT_AUTOMAKE([-Wall -Werror foreign])
AC_CONFIG_SRCDIR([src/lsentineld.c])
AC_CONFIG_HEADERS([config.h])

# Checks for programs
AC_PROG_CC
AC_PROG_INSTALL

# Checks for libraries
AC_CHECK_LIB([lustreapi], [llapi_fid2path], [], [
  AC_MSG_ERROR([Could not find liblustreapi])
])
AC_CHECK_LIB([duckdb], [duckdb_open], [], [
  AC_MSG_ERROR([Could not find libduckdb])
])
AC_CHECK_LIB([cfs], [libcfs_nid2str_r], [AC_DEFINE([HAVE_LIBCFS_LIBCFS_H], [1], [Define if libcfs_nid2str_r is available])])

# Checks for header files
AC_CHECK_HEADERS([liblustreapi.h duckdb.h], [], [
  AC_MSG_ERROR([Required headers liblustreapi.h or duckdb.h not found])
])

# Checks for library functions
AC_CHECK_FUNCS([strtok_r setlocale], [], [
  AC_MSG_ERROR([Required function not found])
])

AC_CONFIG_FILES([Makefile src/Makefile])
AC_OUTPUT
