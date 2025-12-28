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

bin_PROGRAMS = lsentineld sentinel

lsentineld_SOURCES = lsentineld.c
lsentineld_LDADD = $(LUSTRE_LIBS) $(DUCKDB_LIBS)

sentinel_SOURCES = sentinel.c
sentinel_LDADD = ${LUSTRE_LIBS} $(DUCKDB_LIBS)

AM_CFLAGS = -Wall -Wextra
dist_man1_MANS = lsentineld.1 sentinel.1
