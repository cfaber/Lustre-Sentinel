/*
 * lsentineld.c - Lustre changelog consumer and DuckDB storage daemon
 *
 * This program consumes Lustre filesystem changelogs, resolves file paths,
 * and stores audit data in a DuckDB database. It supports daemon mode for
 * continuous monitoring, consumer registration, and report generation.
 *
 * Usage: lsentineld <fsname-MDTnumber> [options]
 *
 * Options:
 *   -h, --help: Display help message and exit
 *   -d, --daemon: Run in daemon mode, polling changelog at dynamic intervals (1-10 seconds)
 *   --consumer <name>: Specify consumer name (required for running, optional for registration)
 *   --db <path>: Path to DuckDB database (required for running and reporting)
 *   --register-consumer: Register a changelog consumer (requires only MDT, optional mask and name)
 *   --audit-mask <full|partial>: Audit mask for registration (default: partial)
 *   --report <period>: Generate report (e.g., daily, weekly; requires --db)
 *   -c, --config <file>: Load config from file
 *   etc...
 *
 * Examples:
 * - Register consumer: lsentineld lustre-MDT0000 --register-consumer [--audit-mask full] [--consumer myname]
 * - Run daemon: lsentineld lustre-MDT0000 --db /path/db.duckdb --consumer cl1 --daemon
 * - Generate report: lsentineld --db /path/db.duckdb --report daily
 *
 * Author: Colin Faber <cfaber@thelustrecollective.com>
 * Date: December 28, 2025
 *
 * Copyright (C) 2025 Colin Faber <cfaber@thelustrecollective.com>
 *
 * This file is part of Lustre Sentinel.
 *
 * Lustre Sentinel is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Lustre Sentinel is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Lustre Sentinel.  If not, see <https://www.gnu.org/licenses/>.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <lustre/lustreapi.h>
// #include <lustre/lustre_user.h>
#include <linux/lustre/lustre_user.h>
#ifdef HAVE_LIBCFS_LIBCFS_H
#include <libcfs/libcfs.h>
#endif
#include <duckdb.h>
#include <locale.h>
#include <unistd.h>
#include <signal.h>
#include <stdbool.h>
#include <mntent.h>
#include <inttypes.h>
#include <errno.h>
#include <ctype.h>
#include <dirent.h>
#include <regex.h>

#define MAX_PATH	16384
#define MAX_NID		128
#define MAX_TYPE	32
#define MAX_RECORDS	1000
#define CACHE_SIZE	1000
#define CACHE_TIMEOUT	300
#define MIN_INTERVAL	1
#define MAX_INTERVAL	10
#define VERSION		"1.0"
#define DEFAULT_CONFIG  "/usr/share/sentinel/default.conf"

static int get_full_path(duckdb_connection conn, const char *mount_point, const char *fid,
                  char *path, size_t path_len, const char *name_buf, const char *pfid);
static const char *fmt_int64(long long val);
static int parse_rotation_duration(const char *duration, long *seconds);
static int generate_next_db_path(const char *db_template, const char *current_path,
                                char *next_path, size_t path_len, time_t *latest_path_mtime);

static int create_indexes(duckdb_connection conn);

static volatile bool keep_running = true;
static int verbose = 0;
static int phelp = 0;

struct changelog_data {
	char operation_type[MAX_TYPE];
	char operation_time[32];
	char client_nid[MAX_NID];
	char uid[32];
	char gid[32];
	char full_path[MAX_PATH];
};

struct fid_cache_entry {
	char fid[64];
	char path[MAX_PATH];
	time_t timestamp;
};

struct fid_cache_entry fid_cache[CACHE_SIZE];
int cache_initialized = 0;

static duckdb_database db;
static duckdb_connection conn;
static bool db_opened = false;
static bool conn_connected = false;

struct my_changelog_ext_rename {
    struct lu_fid cr_sfid;
    struct lu_fid cr_spfid;
    uint32_t cr_snamelen;
};

/* llapi_changelog_rec2str - Convert changelog record type to string representation
 * @type: Changelog record type enum
 *
 * Returns a string corresponding to the given changelog record type.
 */

const char *
llapi_changelog_rec2str(enum changelog_rec_type type)
{
	switch (type) {
		case CL_MARK:     return "MARK";
		case CL_CREATE:   return "CREAT";
		case CL_MKDIR:    return "MKDIR";
		case CL_HARDLINK: return "HLINK";
		case CL_SOFTLINK: return "SLINK";
		case CL_MKNOD:    return "MKNOD";
		case CL_UNLINK:   return "UNLNK";
		case CL_RMDIR:    return "RMDIR";
		case CL_RENAME:   return "RENME";
		case CL_EXT:      return "RNMTO";
		case CL_OPEN:     return "OPEN";
		case CL_CLOSE:    return "CLOSE";
		case CL_LAYOUT:   return "LYOUT";
		case CL_TRUNC:    return "TRUNC";
		case CL_SETATTR:  return "SATTR";
		case CL_XATTR:    return "XATTR";
		case CL_HSM:      return "HSM";
		case CL_MTIME:    return "MTIME";
		case CL_CTIME:    return "CTIME";
		case CL_ATIME:    return "ATIME";
		case CL_MIGRATE:  return "MIGRT";
		case CL_FLRW:     return "FLRW";
		case CL_RESYNC:   return "RESYNC";
		case CL_GETXATTR: return "GXATR";
		case CL_DN_OPEN:  return "NOPEN";
		default:          return "UNKNOWN";
	}
}

/* llapi_fid2str - Convert Lustre FID to string representation
 * @fid: Pointer to the Lustre FID structure
 * @buf: Buffer to store the string representation
 * @len: Length of the buffer
 *
 * Formats the FID into a string in the format [seq:oid:ver] and stores it in buf.
 * Returns the number of characters written to the buffer.
 */
int
llapi_fid2str(const struct lu_fid *fid, char *buf, int len)
{
	return snprintf(buf, len, "[0x%llx:0x%x:0x%x]", fid->f_seq, fid->f_oid, fid->f_ver);
}


/* handle_signal - Handle termination signals for graceful shutdown
 * @sig: Signal number received
 *
 * Sets the keep_running flag to false to initiate program termination.
 */
void
handle_signal(int sig)
{
    (void)sig;
    if (verbose >= 1)
        printf("Received signal %d, stopping...\n", sig);
    keep_running = false;

    // Ensure database resources are released
    if (conn_connected) {
        if (verbose >= 2)
            printf("Disconnecting from DuckDB\n");
        duckdb_disconnect(&conn);
        conn_connected = false;
    }
    if (db_opened) {
        if (verbose >= 2)
            printf("Closing DuckDB database\n");
        duckdb_close(&db);
        db_opened = false;
    }
}

/* is_valid_utf8 - Validate if a string is valid UTF-8
 * @str: String to validate
 *
 * Checks if the input string is a valid UTF-8 encoded string.
 * Returns 1 if valid, 0 otherwise.
 */
int
is_valid_utf8(const char *str)
{
	if (!str)
		return 0;
	for (size_t i = 0; str[i]; i++) {
		if ((str[i] & 0x80) == 0)
			continue;
		else if ((str[i] & 0xE0) == 0xC0) {
			if ((str[++i] & 0xC0) != 0x80)
				return 0;
		} else if ((str[i] & 0xF0) == 0xE0) {
			if ((str[++i] & 0xC0) != 0x80 || (str[++i] & 0xC0) != 0x80)
				return 0;
		} else if ((str[i] & 0xF8) == 0xF0) {
			if ((str[++i] & 0xC0) != 0x80 || (str[++i] & 0xC0) != 0x80 ||
			    (str[++i] & 0xC0) != 0x80)
				return 0;
		} else {
			return 0;
		}
	}
	return 1;
}

/* utf8len - Calculate the length of a UTF-8 string in characters
 * @str: Input string to measure
 *
 * Counts the number of UTF-8 characters (not bytes) in the string.
 * Returns the character count.
 */
size_t
utf8len(const char *str)
{
	size_t len = 0;

	for (size_t i = 0; str[i]; i++) {
		if ((str[i] & 0xC0) != 0x80)
			len++;
	}
	return len;
}

/* utf8_strncpy - Copy a UTF-8 string with boundary checking
 * @dest: Destination buffer
 * @src: Source string
 * @dest_size: Size of the destination buffer
 *
 * Copies the source UTF-8 string to the destination, ensuring proper handling of
 * UTF-8 sequences and null-termination within the destination buffer size.
 */
void
utf8_strncpy(char *dest, const char *src, size_t dest_size)
{
	if (!src || !dest || dest_size == 0)
		return;
	size_t i = 0, j = 0;

	while (src[i] && j < dest_size - 1) {
		dest[j++] = src[i++];
		if ((src[i-1] & 0xC0) == 0xC0 || (src[i-1] & 0xE0) == 0xE0 ||
		    (src[i-1] & 0xF0) == 0xF0) {
			while ((src[i] & 0xC0) == 0x80 && j < dest_size - 1) {
				dest[j++] = src[i++];
			}
		}
	}
	dest[j] = '\0';
	if (!is_valid_utf8(dest))
		dest[0] = '\0';
}

/* sql_escape - Escape single quotes in a string for SQL queries
 * @str: Input string to escape
 * @buf: Output buffer for escaped string
 * @len: Size of the output buffer
 *
 * Escapes single quotes in the input string by doubling them for safe SQL usage.
 */
void
sql_escape(const char *str, char *buf, size_t len)
{
	size_t i = 0, j = 0;

	while (str[i] && j < len - 1) {
		if (str[i] == '\'') {
			if (j < len - 2) {
				buf[j++] = '\'';
				buf[j++] = '\'';
			} else {
				break;
			}
		} else {
			buf[j++] = str[i];
		}
		i++;
	}
	buf[j] = '\0';
}

/* init_fid_cache - Initialize the FID-to-path cache
 *
 * Initializes the FID cache by setting all entries to empty with zero timestamps.
 */
void
init_fid_cache(void)
{
	if (verbose >= 2)
		printf("Initializing FID cache\n");
	for (int i = 0; i < CACHE_SIZE; i++) {
		fid_cache[i].fid[0] = '\0';
		fid_cache[i].path[0] = '\0';
		fid_cache[i].timestamp = 0;
	}
	cache_initialized = 1;
}

/* hash_fid - Compute a hash value for a FID string
 * @fid: FID string to hash
 *
 * Generates a hash value for the FID using a simple string hashing algorithm.
 * Returns the hash value modulo CACHE_SIZE.
 */
unsigned int
hash_fid(const char *fid)
{
	unsigned int hash = 0;

	while (*fid) {
		hash = (hash * 31) + *fid++;
	}
	return hash % CACHE_SIZE;
}

/* get_cached_path - Retrieve a path from the FID cache
 * @fid: FID to look up
 * @path: Buffer to store the retrieved path
 * @path_len: Size of the path buffer
 *
 * Checks the cache for a valid path corresponding to the FID within the cache timeout.
 * Returns 1 if a valid path is found, 0 otherwise.
 */
int
get_cached_path(const char *fid, char *path, size_t path_len)
{
	if (!cache_initialized)
		init_fid_cache();

	unsigned int index = hash_fid(fid);
	time_t now = time(NULL);

	for (int i = 0; i < CACHE_SIZE; i++) {
		unsigned int probe = (index + i) % CACHE_SIZE;

		if (fid_cache[probe].fid[0] == '\0') {
			if (verbose >= 7)
				printf("Cache miss for FID %s (empty slot)\n", fid);
			return 0;
		}
		if (strcmp(fid_cache[probe].fid, fid) == 0) {
			if (now - fid_cache[probe].timestamp < CACHE_TIMEOUT) {
				utf8_strncpy(path, fid_cache[probe].path, path_len);
				if (verbose >= 5) {
					printf("Cache hit for FID %s: %s\n", fid, path ? path : "(empty)");
				}
				return is_valid_utf8(path);
			} else {
				if (verbose >= 5)
					printf("Cache entry expired for FID %s\n", fid);
				fid_cache[probe].fid[0] = '\0';
				return 0;
			}
		}
	}
	if (verbose >= 7)
		printf("Cache miss for FID %s (no match)\n", fid);
	return 0;
}

/* add_to_cache - Add a FID-to-path mapping to the cache
 * @fid: FID to cache
 * @path: Path to associate with the FID
 *
 * Adds or updates a FID-to-path mapping in the cache, replacing the oldest entry if necessary.
 */
void
add_to_cache(const char *fid, const char *path)
{
	if (!is_valid_utf8(path))
		return;

	if (verbose >= 5)
		printf("Adding to cache: FID %s -> %s\n", fid, path);

	unsigned int index = hash_fid(fid);
	time_t now = time(NULL);

	for (int i = 0; i < CACHE_SIZE; i++) {
		unsigned int probe = (index + i) % CACHE_SIZE;

		if (fid_cache[probe].fid[0] == '\0' ||
		    strcmp(fid_cache[probe].fid, fid) == 0 ||
		    now - fid_cache[probe].timestamp >= CACHE_TIMEOUT) {
			strncpy(fid_cache[probe].fid, fid, sizeof(fid_cache[probe].fid) - 1);
			utf8_strncpy(fid_cache[probe].path, path, sizeof(fid_cache[probe].path) - 1);
			fid_cache[probe].timestamp = now;
			if (verbose >= 10)
				printf("Cache insert at index %u: FID %s, path %s, ts %ld\n",
				       probe, fid_cache[probe].fid, fid_cache[probe].path,
				       fid_cache[probe].timestamp);
			return;
		}
	}
	int oldest = 0;
	time_t oldest_time = fid_cache[0].timestamp;

	for (int i = 1; i < CACHE_SIZE; i++) {
		if (fid_cache[i].timestamp < oldest_time) {
			oldest = i;
			oldest_time = fid_cache[i].timestamp;
		}
	}
	if (verbose >= 7)
		printf("Evicting oldest cache entry at %d for new FID %s\n", oldest, fid);
	strncpy(fid_cache[oldest].fid, fid, sizeof(fid_cache[oldest].fid) - 1);
	utf8_strncpy(fid_cache[oldest].path, path, sizeof(fid_cache[oldest].path) - 1);
	fid_cache[oldest].timestamp = now;
	if (verbose >= 10)
		printf("Cache eviction insert at index %u: FID %s, path %s, ts %ld\n",
		       oldest, fid_cache[oldest].fid, fid_cache[oldest].path,
		       fid_cache[oldest].timestamp);
}

/* get_mount_point - Retrieve the mount point for a Lustre filesystem
 * @mdtname: MDT name (e.g., lustre-MDT0000)
 *
 * Reads /proc/mounts to find the mount point for the given Lustre MDT.
 * Returns a dynamically allocated string with the mount point or NULL on failure.
 */
char *
get_mount_point(const char *mdtname)
{
	if (verbose >= 2){
		printf("Getting mount point for %s\n", mdtname);
	}

	char *fsname;
	char *sep;
	
	fsname = strdup(mdtname);
	if (!fsname) {
	    if (verbose >= 1) {
	        fprintf(stderr, "Error duplicating mdtname: %s\n", strerror(errno));
	    }

	    return NULL;
	}

	sep = strchr(fsname, '-');

	if (sep) {
	    *sep = '\0';
	}

	FILE *fp = fopen("/proc/mounts", "r");

	if (!fp) {
		if (verbose >= 1)
			fprintf(stderr, "Error opening /proc/mounts: %s\n", strerror(errno));
		free(fsname);
		return NULL;
	}

	char line[1024];
	char *mount_point = NULL;
	char search[256];

	snprintf(search, sizeof(search), "%s", fsname);  /* No leading '/' */

	while (fgets(line, sizeof(line), fp)) {
		char dev[256], mnt[256], type[32];

		if (sscanf(line, "%255s %255s %31s", dev, mnt, type) != 3)
			continue;
		if (strcmp(type, "lustre") != 0)
			continue;
		if (strstr(dev, search)) {
			mount_point = strdup(mnt);
			if (verbose >= 3)
				printf("Found mount point: %s\n", mount_point);
			break;
		}
	}

	fclose(fp);
	free(fsname);
	return mount_point;
}

/* get_full_path - Resolve the full path for a FID
 * @conn: DuckDB connection handle
 * @mount_point: Filesystem mount point
 * @fid: FID to resolve
 * @path: Buffer to store the resolved path
 * @path_len: Size of the path buffer
 * @name_buf: Name from changelog for fallback path construction (optional)
 * @pfid: Parent FID for fallback path construction (optional)
 *
 * Resolves the full path for a FID using parent FID and name, Lustre API, or database lookup.
 * Caches the result and returns 0 on success, negative error code on failure.
 */
int
get_full_path(duckdb_connection conn, const char *mount_point, const char *fid,
              char *path, size_t path_len, const char *name_buf, const char *pfid)
{
    if (verbose >= 7)
        printf("Getting full path for FID %s under mount %s\n", fid, mount_point);
    if (get_cached_path(fid, path, path_len))
        return 0;

    // Try parent FID and name first if provided
    if (name_buf && name_buf[0] != '\0' && pfid && strncmp(fid, "[0x0:0x0:0x0]", 13) != 0) {
        char parent_path[MAX_PATH];
        int prc = get_full_path(conn, mount_point, pfid, parent_path, sizeof(parent_path), NULL, NULL);
        if (prc == 0 && strncmp(parent_path, "unknown", 7) != 0) {
            size_t parent_len = strnlen(parent_path, MAX_PATH - 1);
            size_t separator_len = (strcmp(parent_path, "/") == 0) ? 0 : 1;
            size_t name_len = strnlen(name_buf, MAX_PATH - parent_len - separator_len - 1);
            size_t total_len = parent_len + separator_len + name_len + 1;

            if (total_len <= MAX_PATH) {
                snprintf(path, path_len, "%s%s%s",
                         parent_path, (strcmp(parent_path, "/") == 0 ? "" : "/"), name_buf);
                path[path_len - 1] = '\0';
                if (verbose >= 5)
                    printf("Constructed path for FID %s: %s\n", fid, path);
                add_to_cache(fid, path);
                return 0;
            }
        }
    }

    // Fallback to llapi_fid2path
    int rc;
    long long recno = -1;
    int linkno = 0;

    rc = llapi_fid2path(mount_point, fid, path, path_len, &recno, &linkno);
    if (verbose >= 7)
        printf("Calling llapi_fid2path: mount_point=%s, fid=%s, path_len=%zu, recno=%lld, linkno=%d, rc=%d\n",
               mount_point, fid, path_len, recno, linkno, rc);

    if (rc < 0) {
        if (verbose >= 2)
            fprintf(stderr, "Error getting path for FID %s from filesystem: %d (%s)\n",
                    fid, rc, strerror(-rc));

        // Retry once after a short delay
        if (strncmp(fid, "[0x0:0x0:0x0]", 13) != 0) {
            if (verbose >= 2)
                fprintf(stderr, "Retrying llapi_fid2path for FID %s\n", fid);
            sleep(1);
            rc = llapi_fid2path(mount_point, fid, path, path_len, &recno, &linkno);
            if (rc == 0) {
                if (verbose >= 5)
                    printf("Retry succeeded for FID %s: %s\n", fid, path);
                add_to_cache(fid, path);
                return 0;
            }
        }
    }

    if (rc == 0) {
        // Normalize to absolute path with leading '/'
        char temp[MAX_PATH];
        strncpy(temp, path, sizeof(temp) - 1);
        temp[sizeof(temp) - 1] = '\0';
        if (strcmp(temp, ".") == 0) {
            strcpy(path, "/");
        } else if (temp[0] != '/') {
            size_t temp_len = strlen(temp);
            if (temp_len < path_len - 1) {
                // Enough space: full copy
                path[0] = '/';
                memcpy(path + 1, temp, temp_len);
                path[1 + temp_len] = '\0';
            } else {
                // Truncate to fit (rare case)
                path[0] = '/';
                memcpy(path + 1, temp, path_len - 2);
                path[path_len - 1] = '\0';
            }
        }
    
        if (verbose >= 5)
            printf("Fetched path via API: %s\n", path);
    
        add_to_cache(fid, path);
        return 0;
    }
    
    // Fallback to DB lookup as last resort
    char escaped_fid[128];
    sql_escape(fid, escaped_fid, sizeof(escaped_fid));
    char query[256];
    snprintf(query, sizeof(query), "SELECT current_path FROM files WHERE fid = '%s'", escaped_fid);
    if (verbose >= 3)
        printf("Executing SQL for path fallback: %s\n", query);
    duckdb_result result;

    if (duckdb_query(conn, query, &result) == DuckDBSuccess) {
        if (duckdb_row_count(&result) > 0) {
            char *db_path = duckdb_value_varchar(&result, 0, 0);
            if (db_path && db_path[0] != '\0' && strcmp(db_path, "unknown (invalid FID)") != 0) {
                utf8_strncpy(path, db_path, path_len);
                if (verbose >= 5)
                    printf("Fetched path via DB: %s\n", path);
                add_to_cache(fid, path);
                duckdb_destroy_result(&result);
                duckdb_free(db_path);
                return 0;
            }
            duckdb_free(db_path);
        }
        duckdb_destroy_result(&result);
    } else {
        const char *err = duckdb_result_error(&result);
        if (verbose >= 1)
            fprintf(stderr, "Error querying DB for FID %s: %s\n", fid, err ? err : "unknown");
        duckdb_destroy_result(&result);
    }

    // Set unknown if all else fails
    strncpy(path, "unknown (invalid FID)", path_len - 1);
    path[path_len - 1] = '\0';
    return rc;
}

/* get_last_rec - Retrieve the last processed changelog record number
 * @conn: DuckDB connection handle
 *
 * Queries the metadata table for the last processed changelog record number.
 * Returns the record number or 0 if not found or on error.
 */
static long long get_last_rec(duckdb_connection conn)
{
	if (verbose >= 3)
		printf("Querying last processed record\n");

	duckdb_prepared_statement stmt;
	duckdb_result result;

	if (duckdb_prepare(conn, "SELECT value FROM metadata WHERE key = 'last_rec'",
			   &stmt) != DuckDBSuccess) {
		if (verbose >= 1)
			fprintf(stderr, "Error preparing last_rec query\n");
		return 0;
	}

	if (duckdb_execute_prepared(stmt, &result) != DuckDBSuccess) {
		const char *err = duckdb_result_error(&result);
		if (verbose >= 1)
			fprintf(stderr, "Error querying last_rec: %s\n",
				err ? err : "unknown");
		duckdb_destroy_result(&result);
		duckdb_destroy_prepare(&stmt);
		return 0;
	}

	char *val = duckdb_value_varchar(&result, 0, 0);
	long long last = val ? strtoll(val, NULL, 10) : 0;

	if (verbose >= 5)
		printf("Last record: %lld\n", last);
	duckdb_destroy_result(&result);
	duckdb_destroy_prepare(&stmt);
	return last;
}

/* update_last_rec - Update the last processed changelog record number
 * @conn: DuckDB connection handle
 * @new_rec: New record number to store
 *
 * Updates the metadata table with the new last processed changelog record number.
 */
static void update_last_rec(duckdb_connection conn, long long new_rec)
{
	if (verbose >= 3)
		printf("Updating last_rec to %lld\n", new_rec);

	duckdb_prepared_statement stmt;
	duckdb_result result;

	if (duckdb_prepare(conn,
			   "UPDATE metadata SET value = ? WHERE key = 'last_rec'",
			   &stmt) != DuckDBSuccess) {
		if (verbose >= 1)
			fprintf(stderr, "Error preparing last_rec update\n");
		return;
	}

	if (duckdb_bind_varchar(stmt, 1, fmt_int64(new_rec)) != DuckDBSuccess) {
		if (verbose >= 1)
			fprintf(stderr, "Error binding parameter for last_rec update\n");
		duckdb_destroy_prepare(&stmt);
		return;
	}

	if (duckdb_execute_prepared(stmt, &result) != DuckDBSuccess) {
		const char *err = duckdb_result_error(&result);
		if (verbose >= 1)
			fprintf(stderr, "Error updating last_rec: %s\n",
				err ? err : "unknown");
		duckdb_destroy_result(&result);
	}
	duckdb_destroy_result(&result);
	duckdb_destroy_prepare(&stmt);
}

/*
 * fmt_int64 - Format a long long integer as a string
 * @val: The integer to format
 *
 * Return: Pointer to static buffer containing the formatted string
 */
static const char *fmt_int64(long long val)
{
	static char buf[32];

	snprintf(buf, sizeof(buf), "%lld", val);
	return buf;
}

/* append_int64 - Append a 64-bit integer to a DuckDB appender
 * @appender: DuckDB appender handle
 * @value: Integer value to append
 * @rec_index: Record index for error reporting
 * @field: Field name for error reporting
 *
 * Appends a 64-bit integer to the appender.
 * Returns 0 on success, -1 on failure.
 */
static int append_int64(duckdb_appender appender, int64_t value,
                        long long rec_index, const char *field)
{
    if (duckdb_append_int64(appender, value) != DuckDBSuccess) {
        if (verbose >= 1)
            fprintf(stderr, "Append failed for %s in record %lld: %s\n",
                    field, rec_index, duckdb_appender_error(appender));
        return -1;
    }
    return 0;
}

/* append_varchar - Append a string to a DuckDB appender
 * @appender: DuckDB appender handle
 * @value: String value to append
 * @rec_index: Record index for error reporting
 * @field: Field name for error reporting
 *
 * Appends a string to the appender, handling NULL values.
 * Returns 0 on success, -1 on failure.
 */
static int append_varchar(duckdb_appender appender, const char *value,
                          long long rec_index, const char *field)
{
    if (!value) {
        if (verbose >= 1)
            fprintf(stderr,
                    "Null value for field %s in record %lld\n",
                    field, rec_index);
        return -1;
    }
    if (duckdb_append_varchar(appender, value) != DuckDBSuccess) {
        if (verbose >= 1)
            fprintf(stderr,
                    "Failed to append field %s (value: %s) in record %lld: %s\n",
                    field, value, rec_index,
                    duckdb_appender_error(appender));
        return -1;
    }
    if (verbose >= 5)
        printf("Appended field %s (value: %s) for record %lld\n",
               field, value, rec_index);
    return 0;
}

/* append_timestamp - Append a timestamp to a DuckDB appender
 * @appender: DuckDB appender handle
 * @value: Timestamp value to append
 * @rec_index: Record index for error reporting
 * @field: Field name for error reporting
 *
 * Appends a timestamp to the appender.
 * Returns 0 on success, -1 on failure.
 */
static int append_timestamp(duckdb_appender appender, duckdb_timestamp value,
                            long long rec_index, const char *field)
{
    if (duckdb_append_timestamp(appender, value) != DuckDBSuccess) {
        if (verbose >= 1)
            fprintf(stderr, "Append failed for %s in record %lld: %s\n",
                    field, rec_index, duckdb_appender_error(appender));
        return -1;
    }
    return 0;
}

/* invalidate_cache - Invalidate a FID-to-path cache entry
 * @fid: FID string to invalidate
 *
 * Removes the cache entry for the specified FID, ensuring outdated paths are not reused.
 * Searches the cache using the FID's hash and linear probing, clearing the matching entry
 * by setting its FID, path, and timestamp to zero/empty. Logs the invalidation if verbose
 * level is 5 or higher. If the cache is not initialized, it is initialized before proceeding.
 */
void
invalidate_cache(const char *fid)
{
    if (!cache_initialized)
        init_fid_cache();

    unsigned int index = hash_fid(fid);

    for (int i = 0; i < CACHE_SIZE; i++) {
        unsigned int probe = (index + i) % CACHE_SIZE;
        if (strcmp(fid_cache[probe].fid, fid) == 0) {
            fid_cache[probe].fid[0] = '\0';
            fid_cache[probe].path[0] = '\0';
            fid_cache[probe].timestamp = 0;
            if (verbose >= 5)
                printf("Invalidated cache for FID %s at index %u\n", fid, probe);
            break;
        }
    }
}

/* insert_record - Insert a changelog record into the database
 * @conn: DuckDB connection handle
 * @appender: DuckDB appender for changelog table
 * @rec: Changelog record to insert
 * @ext_rec: Extended rename record (if applicable)
 * @mount_point: Filesystem mount point
 * @max_rec: Pointer to store the maximum record index processed
 * @affected_fid: Buffer to store the affected FID
 * @fid_len: Size of the affected_fid buffer
 * @new_path: Buffer to store the new path for rename operations
 * @path_len: Size of the new_path buffer
 * @files_appender: DuckDB appender for files table
 *
 * Inserts a changelog record into the database, handling path resolution and
 * file table updates. For renames, uses cr_sfid as tfid, extracts new name
 * from the record, and invalidates caches for both cr_sfid and cr_spfid.
 * The full_path stores only the directory path, excluding the file name.
 * Returns 0 on success, -1 on failure.
 */
static int
insert_record(duckdb_connection conn, duckdb_appender appender,
              struct changelog_rec *rec, struct changelog_ext_rename *ext_rec,
              const char *mount_point, long long *max_rec, char *affected_fid,
              size_t fid_len, char *new_path, size_t path_len,
              duckdb_appender files_appender)
{
    /* Lustre changelog time: top 34 bits are seconds, bottom 30 bits are nanoseconds */
    uint64_t sec = rec->cr_time >> 30;
    uint32_t nsec = rec->cr_time & 0x3FFFFFFF;
    struct tm *tm = gmtime((time_t *)&sec);
    if (!tm) {
        if (verbose >= 1)
            fprintf(stderr, "Error converting time for record %lld\n",
                    rec->cr_index);
        return -1;
    }

    duckdb_timestamp_struct ts;
    ts.date.year = tm->tm_year + 1900;
    ts.date.month = tm->tm_mon + 1;
    ts.date.day = tm->tm_mday;
    ts.time.hour = tm->tm_hour;
    ts.time.min = tm->tm_min;
    ts.time.sec = tm->tm_sec;
    ts.time.micros = nsec / 1000; /* Convert nanoseconds to microseconds */
    duckdb_timestamp timestamp = duckdb_to_timestamp(ts);

    char operation_type[MAX_TYPE];
    const char *typestr = llapi_changelog_rec2str(rec->cr_type);
    if (!typestr) {
        if (verbose >= 1)
            fprintf(stderr, "Unknown operation type %d for record %lld\n",
                    rec->cr_type, rec->cr_index);
        strcpy(operation_type, "UNKNOWN");
    } else {
        strncpy(operation_type, typestr, sizeof(operation_type) - 1);
        operation_type[sizeof(operation_type) - 1] = '\0';
    }

    /* For renames, use cr_sfid from ext_rec as the target FID */
    char tfidstr[64];
    if (rec->cr_type == CL_RENAME && ext_rec && (rec->cr_flags & CLF_RENAME)) {
        llapi_fid2str(&ext_rec->cr_sfid, tfidstr, sizeof(tfidstr));
    } else {
        llapi_fid2str(&rec->cr_tfid, tfidstr, sizeof(tfidstr));
    }

    char pfidstr[64];
    llapi_fid2str(&rec->cr_pfid, pfidstr, sizeof(pfidstr));

    char name_buf[MAX_PATH];
    size_t namelen = rec->cr_namelen;
    if (namelen > sizeof(name_buf) - 1) {
        if (verbose >= 1)
            fprintf(stderr, "Record %lld: Name length %zu exceeds buffer\n",
                    rec->cr_index, namelen);
        namelen = sizeof(name_buf) - 1;
    }
    if (namelen > 0) {
        memcpy(name_buf, changelog_rec_name(rec), namelen);
        name_buf[namelen] = '\0';
    } else {
        name_buf[0] = '\0';
    }
    if (!is_valid_utf8(name_buf)) {
        if (verbose >= 1)
            fprintf(stderr, "Record %lld: Invalid UTF-8 name '%s'\n",
                    rec->cr_index, name_buf);
        name_buf[0] = '\0';
    }

    /* Handle renames... */
    char new_name_buf[MAX_PATH];
    char target_parent_fidstr[64];
    if (rec->cr_flags & CLF_RENAME && ext_rec) {
        llapi_fid2str(&ext_rec->cr_sfid, target_parent_fidstr, sizeof(target_parent_fidstr));

        /* Invalidate cache for the renamed file and source parent FID */
        invalidate_cache(target_parent_fidstr);

        /* Extract the new name with multiple offset attempts */
        const char *old_name = changelog_rec_name(rec);
        const char *new_name = old_name + rec->cr_namelen + 1;
        size_t new_namelen = strnlen(new_name, MAX_PATH - 1);
        size_t total_record_size = changelog_rec_size(rec);
        size_t expected_offset = (new_name + new_namelen + 1) - (const char *)rec;
        if (new_namelen > 0 && new_namelen < sizeof(new_name_buf) && expected_offset <= total_record_size) {
            memcpy(new_name_buf, new_name, new_namelen);
            new_name_buf[new_namelen] = '\0';
        } else {
            /* Try alternative offset (no extra null terminator) */
            new_name = old_name + rec->cr_namelen;
            new_namelen = strnlen(new_name, MAX_PATH - 1);
            expected_offset = (new_name + new_namelen + 1) - (const char *)rec;
            if (new_namelen > 0 && new_namelen < sizeof(new_name_buf) && expected_offset <= total_record_size) {
                memcpy(new_name_buf, new_name, new_namelen);
                new_name_buf[new_namelen] = '\0';
            } else {
                if (verbose >= 1)
                    fprintf(stderr, "Record %lld: Invalid new name length %zu or offset %zu (total size %zu)\n",
                            rec->cr_index, new_namelen, expected_offset, total_record_size);
                strcpy(new_name_buf, "unknown");
            }
        }
        if (!is_valid_utf8(new_name_buf)) {
            if (verbose >= 1)
                fprintf(stderr, "Record %lld: Invalid UTF-8 new name '%s'\n",
                        rec->cr_index, new_name_buf);
            strcpy(new_name_buf, "unknown");
        }
    } else {
        new_name_buf[0] = '\0';
        target_parent_fidstr[0] = '\0';
    }

    char full_path[MAX_PATH];
    full_path[0] = '\0';
    char complete_path[MAX_PATH]; // Store full path with name for caching

    if (rec->cr_flags & CLF_RENAME && ext_rec) {
        char old_parent_path[MAX_PATH];
        int rc = get_full_path(conn, mount_point, pfidstr, old_parent_path, sizeof(old_parent_path), name_buf, NULL);
        if (rc < 0) {
            if (verbose >= 1)
                fprintf(stderr, "Record %lld: Failed to resolve old parent path for pfid %s\n",
                        rec->cr_index, pfidstr);
            strcpy(old_parent_path, "unknown");
        }

        char target_parent_path[MAX_PATH];
        rc = get_full_path(conn, mount_point, target_parent_fidstr, target_parent_path, sizeof(target_parent_path), new_name_buf, NULL);
        if (rc < 0) {
            if (verbose >= 1)
                fprintf(stderr, "Record %lld: Failed to resolve target parent path for fid %s\n",
                        rec->cr_index, target_parent_fidstr);
            strcpy(target_parent_path, "unknown");
        }

        char old_full_path[MAX_PATH];
        char new_full_path[MAX_PATH];

        size_t old_parent_len = strnlen(old_parent_path, MAX_PATH);
        size_t name_len = strnlen(name_buf, MAX_PATH);
        size_t target_parent_len = strnlen(target_parent_path, MAX_PATH);
        size_t new_name_len = strnlen(new_name_buf, MAX_PATH);
        size_t separator_len = (strcmp(target_parent_path, "/") == 0) ? 0 : 1;

        // Construct old full path
        if (old_parent_len + separator_len + name_len + 1 <= MAX_PATH) {
            snprintf(old_full_path, sizeof(old_full_path), "%s%s%s",
                     old_parent_path, (strcmp(old_parent_path, "/") == 0 ? "" : "/"), name_buf);
        } else {
            strcpy(old_full_path, "unknown (path too long)");
        }

        // Construct new full path (with name for caching)
        if (target_parent_len + separator_len + new_name_len + 1 <= MAX_PATH) {
            snprintf(new_full_path, sizeof(new_full_path), "%s%s%s",
                     target_parent_path, (strcmp(target_parent_path, "/") == 0 ? "" : "/"), new_name_buf);
        } else {
            strcpy(new_full_path, "unknown (path too long)");
        }

        // Set full_path to directory only
        strncpy(full_path, target_parent_path, sizeof(full_path) - 1);
        full_path[sizeof(full_path) - 1] = '\0';
        if (strncmp(full_path, "unknown", 7) != 0 && strcmp(full_path, "/") != 0 && full_path[0] != '\0') {
            char *last_slash = strrchr(full_path, '/');
            if (last_slash && last_slash != full_path) {
                *last_slash = '\0'; // Remove trailing file name if present
            }
        }
        if (verbose >= 5)
            fprintf(stderr, "Record %lld: Constructed full_path (directory): %s\n", rec->cr_index, full_path);

        // Store complete path for caching
        strncpy(complete_path, new_full_path, sizeof(complete_path) - 1);
        complete_path[sizeof(complete_path) - 1] = '\0';
    } else {
        int rc = get_full_path(conn, mount_point, tfidstr, full_path, sizeof(full_path), name_buf, pfidstr);
        strncpy(complete_path, full_path, sizeof(complete_path) - 1); // Store complete path for caching
        complete_path[sizeof(complete_path) - 1] = '\0';
        if (rc < 0 && (rec->cr_type == CL_UNLINK || rec->cr_type == CL_RMDIR || rec->cr_type == CL_CLOSE)) {
            char parent_path[MAX_PATH];
            int prc = get_full_path(conn, mount_point, pfidstr, parent_path, sizeof(parent_path), name_buf, NULL);
            if (prc != 0 || strncmp(parent_path, "unknown", 7) == 0) {
                if (verbose >= 1)
                    fprintf(stderr, "Record %lld: Failed to resolve parent path for pfid %s\n",
                            rec->cr_index, pfidstr);
                strcpy(full_path, "unknown (invalid parent FID)");
                strcpy(complete_path, full_path);
            } else if (name_buf[0] == '\0' && rec->cr_type != CL_CLOSE) {
                if (verbose >= 5)
                    fprintf(stderr, "Record %lld: No name in changelog for %s\n",
                            rec->cr_index, operation_type);
                strcpy(full_path, "unknown (no name)");
                strcpy(complete_path, full_path);
            } else {
                size_t parent_len = strnlen(parent_path, MAX_PATH - 1);
                size_t separator_len = (strcmp(parent_path, "/") == 0) ? 0 : 1;
                size_t name_len = strnlen(name_buf, MAX_PATH - parent_len - separator_len - 1);
                size_t total_len = parent_len + separator_len + name_len + 1;

                if (total_len > MAX_PATH) {
                    if (verbose >= 1)
                        fprintf(stderr, "Record %lld: Path too long (%zu bytes) for %s/%s\n",
                                rec->cr_index, total_len, parent_path, name_buf);
                    strcpy(full_path, "unknown (path too long)");
                    strcpy(complete_path, full_path);
                } else {
                    snprintf(complete_path, sizeof(complete_path), "%s%s%s",
                             parent_path, (strcmp(parent_path, "/") == 0 ? "" : "/"), name_buf);
                    complete_path[sizeof(complete_path) - 1] = '\0';
                    strncpy(full_path, parent_path, sizeof(full_path) - 1);
                    full_path[sizeof(full_path) - 1] = '\0';
                    if (strncmp(full_path, "unknown", 7) != 0 && strcmp(full_path, "/") != 0 && full_path[0] != '\0') {
                        char *last_slash = strrchr(full_path, '/');
                        if (last_slash && last_slash != full_path) {
                            *last_slash = '\0'; // Remove trailing file name if present
                        }
                    }
                    if (verbose >= 5)
                        fprintf(stderr, "Record %lld: Constructed full_path (directory): %s\n",
                                rec->cr_index, full_path);
                }
            }
        } else if (rc == 0 && strncmp(full_path, "unknown", 7) != 0 && strcmp(full_path, "/") != 0 && full_path[0] != '\0') {
            char *last_slash = strrchr(full_path, '/');
            if (last_slash && last_slash != full_path) {
                *last_slash = '\0'; // Remove trailing file name
            }
            if (verbose >= 5)
                fprintf(stderr, "Record %lld: Modified full_path (directory): %s\n",
                        rec->cr_index, full_path);
        }
        if (strncmp(complete_path, "unknown", 7) != 0) {
            add_to_cache(tfidstr, complete_path);
        }
    }

    /* Resolve name from full_path if not provided in changelog, except for CLOSE */
    if (name_buf[0] == '\0' && rec->cr_type != CL_CLOSE) {
        if (strcmp(full_path, "/") == 0) {
            strcpy(name_buf, "root");
            if (verbose >= 5)
                fprintf(stderr, "Record %lld: No name in changelog, using 'root' for path %s\n",
                        rec->cr_index, full_path);
        } else if (strncmp(full_path, "unknown", 7) != 0) {
            char *basename = strrchr(complete_path, '/'); // Use complete_path for basename
            if (basename && *(basename + 1) != '\0') {
                utf8_strncpy(name_buf, basename + 1, sizeof(name_buf));
                if (verbose >= 5)
                    fprintf(stderr, "Record %lld: No name in changelog, extracted '%s' from path %s\n",
                            rec->cr_index, name_buf, complete_path);
            } else {
                strcpy(name_buf, "unknown");
                if (verbose >= 5)
                    fprintf(stderr, "Record %lld: No name in changelog, unable to extract basename from path %s\n",
                            rec->cr_index, complete_path);
            }
        } else {
            strcpy(name_buf, "unknown");
            if (verbose >= 5)
                fprintf(stderr, "Record %lld: No name in changelog, path is %s\n",
                        rec->cr_index, full_path);
        }
    } else if (rec->cr_type == CL_CLOSE) {
        strcpy(name_buf, "");
        if (verbose >= 5)
            fprintf(stderr, "Record %lld: Setting name to empty for CLOSE operation\n",
                    rec->cr_index);
    }

    const char *name = name_buf;

    if (full_path[0] == '\0') {
        if (verbose >= 1)
            fprintf(stderr, "Record %lld: Empty full_path, setting to unknown\n",
                    rec->cr_index);
        strcpy(full_path, "unknown (invalid FID)");
    }

    char job_id[MAX_NID] = "";
    if (rec->cr_flags & CLF_JOBID) {
        struct changelog_ext_jobid *jobid_struct = changelog_rec_jobid(rec);
        if (jobid_struct) {
            strncpy(job_id, jobid_struct->cr_jobid, sizeof(job_id) - 1);
            job_id[sizeof(job_id) - 1] = '\0';
            if (!is_valid_utf8(job_id)) {
                if (verbose >= 1)
                    fprintf(stderr, "Invalid UTF-8 job ID '%s' for record %lld\n",
                            job_id, rec->cr_index);
                job_id[0] = '\0';
            } else {
                if (verbose >= 5)
                    fprintf(stderr, "Record %lld: job_id=%s\n",
                            rec->cr_index, job_id);
            }
        }
    }

    char uid_buf[32];
    char gid_buf[32];
    char nid_buf[MAX_NID];
    const char *uid = "unknown";
    const char *gid = "unknown";
    const char *client_nid = "";

    if (verbose >= 5)
        fprintf(stderr, "Record %lld: cr_flags=0x%x\n", rec->cr_index, rec->cr_flags);

    if (rec->cr_flags & CLF_EXTRA_FLAGS) {
        struct changelog_ext_extra_flags *ef = changelog_rec_extra_flags(rec);
        __u64 extra_flags = ef->cr_extra_flags;

        if (verbose >= 5)
            fprintf(stderr, "Record %lld: extra_flags=0x%llx\n", rec->cr_index, extra_flags);

        if (extra_flags & CLFE_UIDGID) {
            struct changelog_ext_uidgid *uidgid = changelog_rec_uidgid(rec);
            if (uidgid) {
                if (verbose >= 5)
                    fprintf(stderr, "Record %lld: UID=%llu, GID=%llu\n",
                            rec->cr_index, uidgid->cr_uid, uidgid->cr_gid);
                if (uidgid->cr_uid <= 0xFFFFFFFFFFFFFFFFULL && uidgid->cr_gid <= 0xFFFFFFFFFFFFFFFFULL) {
                    snprintf(uid_buf, sizeof(uid_buf), "%llu", uidgid->cr_uid);
                    snprintf(gid_buf, sizeof(gid_buf), "%llu", uidgid->cr_gid);
                    uid = uid_buf;
                    gid = gid_buf;
                } else {
                    if (verbose >= 1)
                        fprintf(stderr, "Record %lld: Out-of-range UID=%llu or GID=%llu\n",
                                rec->cr_index, uidgid->cr_uid, uidgid->cr_gid);
                }
            } else {
                if (verbose >= 1)
                    fprintf(stderr, "Record %lld: changelog_rec_uidgid returned NULL\n", rec->cr_index);
            }
        }

        if (extra_flags & CLFE_NID) {
            struct changelog_ext_nid *nid_ext = changelog_rec_nid(rec);
            if (nid_ext) {
                snprintf(nid_buf, sizeof(nid_buf), "0x%llx", nid_ext->cr_nid);
                client_nid = nid_buf;
                if (verbose >= 5)
                    fprintf(stderr, "Record %lld: NID=%s\n", rec->cr_index, client_nid);
            }
        }
    } else if (verbose >= 5) {
        fprintf(stderr, "Record %lld: No extra flags (cr_flags=0x%x)\n", rec->cr_index, rec->cr_flags);
    }

    /* Begin row for changelog appender */
    if (duckdb_appender_begin_row(appender) != DuckDBSuccess) {
        if (verbose >= 1)
            fprintf(stderr, "Error beginning row for changelog record %lld: %s\n",
                    rec->cr_index, duckdb_appender_error(appender));
        return -1;
    }

    /* Use helper functions */
    if (append_int64(appender, rec->cr_index, rec->cr_index, "id") < 0)
        return -1;
    if (append_varchar(appender, operation_type, rec->cr_index, "operation_type") < 0)
        return -1;
    if (append_timestamp(appender, timestamp, rec->cr_index, "operation_time") < 0)
        return -1;
    if (append_int64(appender, rec->cr_flags, rec->cr_index, "flags") < 0)
        return -1;
    if (append_varchar(appender, tfidstr, rec->cr_index, "tfid") < 0)
        return -1;
    if (append_varchar(appender, pfidstr, rec->cr_index, "pfid") < 0)
        return -1;
    if (append_varchar(appender, name, rec->cr_index, "name") < 0)
        return -1;
    if (append_varchar(appender, uid, rec->cr_index, "uid") < 0)
        return -1;
    if (append_varchar(appender, gid, rec->cr_index, "gid") < 0)
        return -1;
    if (append_varchar(appender, client_nid, rec->cr_index, "client_nid") < 0)
        return -1;
    if (append_varchar(appender, job_id, rec->cr_index, "job_id") < 0)
        return -1;
    if (append_varchar(appender, full_path, rec->cr_index, "full_path") < 0)
        return -1;

    if (duckdb_appender_end_row(appender) != DuckDBSuccess) {
        if (verbose >= 1)
            fprintf(stderr, "Error ending row for changelog record %lld: %s\n",
                    rec->cr_index, duckdb_appender_error(appender));
        return -1;
    }

    char operation_time_str[64];
    snprintf(operation_time_str, sizeof(operation_time_str),
             "%04d-%02d-%02dT%02d:%02d:%02d.%06u",
             tm->tm_year + 1900, tm->tm_mon + 1, tm->tm_mday,
             tm->tm_hour, tm->tm_min, tm->tm_sec, nsec / 1000);

    /* Compute clean current_path for files table */
    char current_path_for_files[MAX_PATH];
    if (rec->cr_flags & CLF_RENAME && ext_rec) {
        char target_parent_path[MAX_PATH];
        int rc = get_full_path(conn, mount_point, target_parent_fidstr, target_parent_path, sizeof(target_parent_path), new_name_buf, NULL);
        if (rc < 0) {
            if (verbose >= 1)
                fprintf(stderr, "Record %lld: Failed to resolve target parent path for fid %s\n",
                        rec->cr_index, target_parent_fidstr);
            strcpy(target_parent_path, "unknown");
        }

        size_t target_parent_len = strnlen(target_parent_path, MAX_PATH);
        size_t new_name_len = strnlen(new_name_buf, MAX_PATH);
        size_t separator_len = (strcmp(target_parent_path, "/") == 0) ? 0 : 1;
        size_t total_len = target_parent_len + separator_len + new_name_len + 1;

        if (total_len > MAX_PATH) {
            if (verbose >= 1)
                fprintf(stderr, "Record %lld: Current path too long (%zu bytes) for %s/%s\n",
                        rec->cr_index, total_len, target_parent_path, new_name_buf);
            strcpy(current_path_for_files, "unknown (path too long)");
        } else {
            snprintf(current_path_for_files, sizeof(current_path_for_files), "%s%s%s",
                     target_parent_path, (strcmp(target_parent_path, "/") == 0 ? "" : "/"), new_name_buf);
            current_path_for_files[sizeof(current_path_for_files) - 1] = '\0';
            if (verbose >= 5)
                fprintf(stderr, "Record %lld: Constructed current_path_for_files: %s\n",
                        rec->cr_index, current_path_for_files);
        }
        if (strncmp(current_path_for_files, "unknown", 7) != 0) {
            add_to_cache(tfidstr, current_path_for_files);
        }
    } else {
        strncpy(current_path_for_files, complete_path, sizeof(current_path_for_files) - 1);
        current_path_for_files[sizeof(current_path_for_files) - 1] = '\0';
        if (strncmp(complete_path, "unknown", 7) != 0) {
            add_to_cache(tfidstr, complete_path);
        }
    }

    /* Begin row for files appender */
    if (rec->cr_type != CL_MARK) {
        if (duckdb_appender_begin_row(files_appender) != DuckDBSuccess) {
            if (verbose >= 1)
                fprintf(stderr, "Error beginning row for files record %lld: %s\n",
                        rec->cr_index, duckdb_appender_error(files_appender));
            return -1;
        }
    }

    if (rec->cr_type == CL_UNLINK || rec->cr_type == CL_RMDIR) {
        if (duckdb_append_varchar(files_appender, tfidstr) != DuckDBSuccess ||
            duckdb_append_varchar(files_appender, current_path_for_files) != DuckDBSuccess ||
            duckdb_append_bool(files_appender, true) != DuckDBSuccess ||
            duckdb_append_varchar(files_appender, operation_time_str) != DuckDBSuccess ||
            duckdb_append_varchar(files_appender, operation_type) != DuckDBSuccess ||
            duckdb_append_null(files_appender) != DuckDBSuccess) {
            if (verbose >= 1)
                fprintf(stderr, "Append failed for files table in record %lld: %s\n",
                        rec->cr_index, duckdb_appender_error(files_appender));
            return -1;
        }
    } else if (rec->cr_type == CL_RENAME && ext_rec) {
        if (duckdb_append_varchar(files_appender, tfidstr) != DuckDBSuccess ||
            duckdb_append_varchar(files_appender, current_path_for_files) != DuckDBSuccess ||
            duckdb_append_bool(files_appender, false) != DuckDBSuccess ||
            duckdb_append_varchar(files_appender, operation_time_str) != DuckDBSuccess ||
            duckdb_append_varchar(files_appender, operation_type) != DuckDBSuccess ||
            duckdb_append_null(files_appender) != DuckDBSuccess) {
            if (verbose >= 1)
                fprintf(stderr, "Append failed for files table in record %lld: %s\n",
                        rec->cr_index, duckdb_appender_error(files_appender));
            return -1;
        }
    } else if (rec->cr_type != CL_MARK) {
        if (duckdb_append_varchar(files_appender, tfidstr) != DuckDBSuccess ||
            duckdb_append_varchar(files_appender, current_path_for_files) != DuckDBSuccess ||
            duckdb_append_bool(files_appender, false) != DuckDBSuccess ||
            duckdb_append_varchar(files_appender, operation_time_str) != DuckDBSuccess ||
            duckdb_append_varchar(files_appender, operation_type) != DuckDBSuccess ||
            duckdb_append_null(files_appender) != DuckDBSuccess) {
            if (verbose >= 1)
                fprintf(stderr, "Append failed for files table in record %lld: %s\n",
                        rec->cr_index, duckdb_appender_error(files_appender));
            return -1;
        }
    }

    if (rec->cr_type != CL_MARK) {
        if (duckdb_appender_end_row(files_appender) != DuckDBSuccess) {
            if (verbose >= 1)
                fprintf(stderr, "Error ending files row for record %lld: %s\n",
                        rec->cr_index, duckdb_appender_error(files_appender));
            return -1;
        }
    }

    *max_rec = rec->cr_index;
    snprintf(affected_fid, fid_len, "%s", tfidstr);
    if (rec->cr_flags & CLF_RENAME && ext_rec) {
        strncpy(new_path, new_name_buf, path_len);
        new_path[path_len - 1] = '\0';
    } else {
        new_path[0] = '\0';
    }

    return 0;
}

/* process_changelog - Process Lustre changelog records
 * @mdtname: MDT name (e.g., lustre-MDT0000)
 * @conn: DuckDB connection handle
 * @mount_point: Filesystem mount point
 * @start_rec: Starting changelog record number
 * @consumer: Changelog consumer name
 * @dry_run: If true, simulate without committing changes
 *
 * Processes changelog records from the specified starting point, storing them in the database.
 * Returns the number of records processed or -1 on error.
 */
static int
process_changelog(const char *mdtname, duckdb_connection conn,
                  const char *mount_point, long long start_rec,
                  const char *consumer, bool dry_run, bool noclear, int daemon_mode)
{
    void *clh = NULL;
    struct changelog_rec *rec = NULL;
    duckdb_appender appender = NULL;
    duckdb_result result;
    duckdb_appender files_appender = NULL;
    int rc = 0;
    int count = 0;
    long long max_rec = start_rec;
    long long last_rec = get_last_rec(conn);

    if (verbose >= 2)
        printf("Starting changelog processing from rec %lld for consumer %s\n",
               start_rec, consumer);

    /* Create temp table for batch file updates */
    const char *create_temp_files = "CREATE TEMP TABLE temp_files (fid VARCHAR, current_path VARCHAR, is_deleted BOOLEAN, last_operation_time VARCHAR, last_operation_type VARCHAR, extended_attributes VARCHAR)";
    if (verbose >= 3)
        printf("Executing SQL: %s\n", create_temp_files);

    if (duckdb_query(conn, create_temp_files, &result) != DuckDBSuccess) {
        const char *err = duckdb_result_error(&result);
        if (verbose >= 1)
            fprintf(stderr, "Error creating temp_files table: %s\n", err ? err : "unknown");
        duckdb_destroy_result(&result);
        return -1;
    }
    duckdb_destroy_result(&result);

    /* Create appenders */
    if (duckdb_appender_create(conn, NULL, "changelog", &appender) != DuckDBSuccess) {
        if (verbose >= 1)
            fprintf(stderr, "Error creating changelog appender\n");
        rc = -1;
        goto out;
    }

    if (duckdb_appender_create(conn, NULL, "temp_files", &files_appender) != DuckDBSuccess) {
        if (verbose >= 1) {
            const char *err = duckdb_appender_error(files_appender);
            fprintf(stderr, "Error creating files appender: %s\n",
                    err ? err : "unknown");
        }
        rc = -1;
        goto out;
    }
    rc = llapi_changelog_start(&clh, 
		    CHANGELOG_FLAG_JOBID |
		    CHANGELOG_FLAG_EXTRA_FLAGS |
		    (daemon_mode && 0 ? CHANGELOG_FLAG_FOLLOW : 0), //FIXME - this should be handled differently, and until then we'll disable FOLLOW
		    mdtname, start_rec + 1);
    if (rc < 0) {
        fprintf(stderr, "Error starting changelog: %d (%s)\n", rc, strerror(-rc));
        goto out;
    }

    rc = llapi_changelog_set_xflags(clh,
		    CHANGELOG_EXTRA_FLAG_UIDGID |
		    CHANGELOG_EXTRA_FLAG_NID |
		    CHANGELOG_EXTRA_FLAG_OMODE |
		    CHANGELOG_EXTRA_FLAG_XATTR
	   );

    if(rc < 0){
	fprintf(stderr, "Error setting changelog xflags: %d\n", rc);
	goto out;
    }

    while (keep_running) {
        rc = llapi_changelog_recv(clh, &rec);
        if (rc != 0)
            break;  /* No more records or error */

        struct changelog_ext_rename *ext = NULL;
        if (rec->cr_type == CL_RENAME || rec->cr_type == CL_EXT)
            ext = changelog_rec_rename(rec);
        char affected_fid[64];
        char new_path[MAX_PATH];

        rc = insert_record(conn, appender, rec, ext, mount_point, &max_rec,
                           affected_fid, sizeof(affected_fid), new_path,
                           sizeof(new_path), files_appender);
        if (rc < 0) {
            if (verbose >= 1)
                fprintf(stderr, "Error inserting record %lld\n", rec->cr_index);
            if (rec) {
                llapi_changelog_free(&rec);
                rec = NULL;
            }
            break;
        }
        count++;

        if (rec) {
            llapi_changelog_free(&rec);
            rec = NULL;
        }
    }

    if (!dry_run && max_rec > last_rec) {
        /* Flush appenders before creating indexes */
        if (verbose >= 6)
            printf("Flushing DuckDB appender\n");

        if (appender && duckdb_appender_flush(appender) != DuckDBSuccess) {
            if (verbose >= 1)
                fprintf(stderr, "Error flushing changelog appender: %s\n",
                        duckdb_appender_error(appender));
            rc = -1;
            goto out;
        }

        if (verbose >= 6)
            printf("Flushing DuckDB files appender\n");

        if (files_appender && duckdb_appender_flush(files_appender) != DuckDBSuccess) {
            if (verbose >= 1)
                fprintf(stderr, "Error flushing files appender: %s\n",
                        duckdb_appender_error(files_appender));
            rc = -1;
            goto out;
        }

        /* Create indexes after appending data */
        if (verbose >= 2)
            printf("Creating indexes for performance\n");
        if (create_indexes(conn) != 0) {
            if (verbose >= 1)
                fprintf(stderr, "Error creating indexes\n");
            rc = -1;
            goto out;
        }

        /* Merge temp into files */
        const char *merge = "INSERT INTO files SELECT * FROM temp_files ON CONFLICT (fid) DO UPDATE SET current_path = EXCLUDED.current_path, is_deleted = EXCLUDED.is_deleted, last_operation_time = EXCLUDED.last_operation_time, last_operation_type = EXCLUDED.last_operation_type, extended_attributes = EXCLUDED.extended_attributes";
        if (verbose >= 3)
            printf("Executing SQL: %s\n", merge);
        if (duckdb_query(conn, merge, &result) != DuckDBSuccess) {
            const char *err = duckdb_result_error(&result);
            if (verbose >= 1)
                fprintf(stderr, "Error merging temp_files: %s\n", err ? err : "unknown");
            duckdb_destroy_result(&result);
            rc = -1;
            goto out;
        }
        duckdb_destroy_result(&result);
        update_last_rec(conn, max_rec);


	if(!noclear){
	        /* Clear processed changelog records */
	        rc = llapi_changelog_clear(mdtname, consumer, max_rec);
	        if (rc < 0) {
	            fprintf(stderr, "Error clearing changelog for %s up to %lld: %d (%s)\n",
	                    consumer, max_rec, rc, strerror(-rc));
	            goto out;
	        }
	        if (verbose >= 3)
	            printf("Cleared changelog for %s up to record %lld\n",
	                   consumer, max_rec);
	} else if (verbose >= 2){
		printf("Skipping changelog clear for %s at record %lld\n",
				consumer, max_rec);
	}
    } else if (verbose >= 2) {
        printf("Skipping clear for %s because max_rec <= last_rec\n", consumer);
    }

    /* Drop temp_files table */
    const char *drop_temp_files = "DROP TABLE IF EXISTS temp_files";
    if (verbose >= 3)
        printf("Executing SQL: %s\n", drop_temp_files);
    if (duckdb_query(conn, drop_temp_files, &result) != DuckDBSuccess) {
        const char *err = duckdb_result_error(&result);
        if (verbose >= 1)
            fprintf(stderr, "Error dropping temp_files table: %s\n", err ? err : "unknown");
        duckdb_destroy_result(&result);
        rc = -1;
        goto out;
    }
    duckdb_destroy_result(&result);

out:
    /* Flush appenders before destroying */
    if (appender) {
        if (verbose >= 6)
            printf("Flushing DuckDB appender\n");

        if (duckdb_appender_flush(appender) != DuckDBSuccess) {
            if (verbose >= 1)
                fprintf(stderr, "Error flushing changelog appender on cleanup: %s\n",
                        duckdb_appender_error(appender));
            rc = -1;
        }

        if (verbose >= 6)
            printf("Destroying DuckDB appender\n");

        duckdb_appender_destroy(&appender);
        appender = NULL;
    }
    if (files_appender) {
        if (verbose >= 6)
            printf("Flushing DuckDB files appender\n");

        if (duckdb_appender_flush(files_appender) != DuckDBSuccess) {
            if (verbose >= 1)
                fprintf(stderr, "Error flushing files appender on cleanup: %s\n",
                        duckdb_appender_error(files_appender));
            rc = -1;
        }

        if (verbose >= 6)
            printf("Destroying DuckDB files appender\n");

        duckdb_appender_destroy(&files_appender);
        files_appender = NULL;
    }
    if (clh)
        llapi_changelog_fini(&clh);
    if (rec)
        llapi_changelog_free(&rec);
    return rc < 0 ? -1 : count;
}


/* get_polling_interval - Determine the polling interval for daemon mode
 * @record_count: Number of records processed in the last cycle
 *
 * Calculates the polling interval based on the number of records processed.
 * Returns the interval in seconds (between MIN_INTERVAL and MAX_INTERVAL).
 */
int
get_polling_interval(int record_count)
{
	if (record_count > 100)
		return MIN_INTERVAL;
	else
		return MAX_INTERVAL;
}

/* parse_config - Parse configuration file for program settings
 * @file: Path to configuration file
 * @fsname: Filesystem name (output)
 * @daemon_mode: Daemon mode flag (output)
 * @consumer_name: Consumer name (output)
 * @audit_mask: Audit mask (output)
 * @db_template: Database file name template (output)
 * @register_consumer: Register consumer flag (output)
 * @rotation_seconds: Rotation interval in seconds (output)
 * @db_path: Database path (output)
 *
 * Reads key-value pairs from the config file and updates provided pointers.
 */
int parse_config(const char *file, char **fsname, int *daemon_mode, char **consumer_name,
                 char **audit_mask, char **db_template, int *register_consumer,
                 long *rotation_seconds, char **db_path) {
    FILE *fp = fopen(file, "r");
    if (!fp) {
        fprintf(stderr, "Error opening config file %s: %s\n", file, strerror(errno));
        return -1;
    }

    bool has_error = false;
    char line[256];
    while (fgets(line, sizeof(line), fp)) {
        line[strcspn(line, "\n")] = '\0';
        if (line[0] == '\0' || line[0] == '#')
            continue;

        char *key = strtok(line, "=");
        char *value = strtok(NULL, "=");
        if (!key || !value) {
            fprintf(stderr, "Invalid line in config: %s\n", line);
            has_error = true;
            continue;
        }

        while (*key && isspace(*key)) key++;
        while (*value && isspace(*value)) value++;
        char *end = key + strlen(key) - 1;
        while (end > key && isspace(*end)) *end-- = '\0';
        end = value + strlen(value) - 1;
        while (end > value && isspace(*end)) *end-- = '\0';

        if (strcmp(key, "fsname") == 0) {
            if (*fsname) free(*fsname);
            *fsname = strdup(value);
            if (!*fsname) {
                fprintf(stderr, "Memory allocation failed for fsname\n");
                has_error = true;
            }
        } else if (strcmp(key, "daemon") == 0) {
            if (strcmp(value, "0") != 0 && strcmp(value, "1") != 0) {
                fprintf(stderr, "Invalid value for daemon: %s (must be 0 or 1)\n", value);
                has_error = true;
            } else {
                *daemon_mode = atoi(value);
            }
        } else if (strcmp(key, "consumer") == 0) {
            if (*consumer_name) free(*consumer_name);
            *consumer_name = strdup(value);
            if (!*consumer_name) {
                fprintf(stderr, "Memory allocation failed for consumer\n");
                has_error = true;
            }
        } else if (strcmp(key, "mask") == 0) {
            if (strcmp(value, "full") != 0 && strcmp(value, "partial") != 0) {
                fprintf(stderr, "Invalid value for mask: %s (must be full or partial)\n", value);
                has_error = true;
            } else {
                if (*audit_mask) free(*audit_mask);
                *audit_mask = strdup(value);
                if (!*audit_mask) {
                    fprintf(stderr, "Memory allocation failed for mask\n");
                    has_error = true;
                }
            }
        } else if (strcmp(key, "db") == 0) {
            if (*db_template) free(*db_template);
            *db_template = strdup(value);
            if (!*db_template) {
                fprintf(stderr, "Memory allocation failed for db\n");
                has_error = true;
            }
        } else if (strcmp(key, "register") == 0) {
            if (strcmp(value, "0") != 0 && strcmp(value, "1") != 0) {
                fprintf(stderr, "Invalid value for register: %s (must be 0 or 1)\n", value);
                has_error = true;
            } else {
                *register_consumer = atoi(value);
            }
        } else if (strcmp(key, "rotate") == 0) {
            if (parse_rotation_duration(value, rotation_seconds) < 0) {
                fprintf(stderr, "Invalid value for rotate: %s\n", value);
                has_error = true;
            }
        } else {
            fprintf(stderr, "Unknown config key: %s\n", key);
            has_error = true;
        }
    }
    fclose(fp);
    if (has_error) {
        if (*fsname) { free(*fsname); *fsname = NULL; }
        if (*consumer_name) { free(*consumer_name); *consumer_name = NULL; }
        if (*audit_mask) { free(*audit_mask); *audit_mask = NULL; }
        if (*db_template) { free(*db_template); *db_template = NULL; }
        *daemon_mode = 0;
        *register_consumer = 0;
        *rotation_seconds = 0;
        if (*db_path) { free(*db_path); *db_path = NULL; }
    }
    fprintf(stderr, "parse_config returning %d\n", has_error ? -1 : 0);
    return has_error ? -1 : 0;
}

/* create_indexes - Create indexes on changelog and files tables for performance
 * @conn: DuckDB connection handle
 *
 * Creates indexes on the changelog and files tables to optimize query performance.
 * Indexes are created on operation_type, name, and full_path for the changelog table,
 * and on fid and current_path for the files table.
 * Returns 0 on success, -1 on failure.
 */
static int create_indexes(duckdb_connection conn)
{
	const char *index_changelog_op = "CREATE INDEX IF NOT EXISTS idx_changelog_op ON changelog (operation_type)";
	const char *index_changelog_name = "CREATE INDEX IF NOT EXISTS idx_changelog_name ON changelog (name)";
	const char *index_changelog_path = "CREATE INDEX IF NOT EXISTS idx_changelog_path ON changelog (full_path)";
	const char *index_files_fid = "CREATE INDEX IF NOT EXISTS idx_files_fid ON files (fid)";
	const char *index_files_path = "CREATE INDEX IF NOT EXISTS idx_files_path ON files (current_path)";
	duckdb_result result;

	if (verbose >= 3)
		printf("Creating indexes for changelog and files tables\n");

	if (duckdb_query(conn, index_changelog_op, &result) != DuckDBSuccess) {
		const char *err = duckdb_result_error(&result);
		if (verbose >= 1)
			fprintf(stderr, "Error creating index idx_changelog_op: %s\n",
				err ? err : "unknown");
		duckdb_destroy_result(&result);
		return -1;
	}
	duckdb_destroy_result(&result);

	if (duckdb_query(conn, index_changelog_name, &result) != DuckDBSuccess) {
		const char *err = duckdb_result_error(&result);
		if (verbose >= 1)
			fprintf(stderr, "Error creating index idx_changelog_name: %s\n",
				err ? err : "unknown");
		duckdb_destroy_result(&result);
		return -1;
	}
	duckdb_destroy_result(&result);

	if (duckdb_query(conn, index_changelog_path, &result) != DuckDBSuccess) {
		const char *err = duckdb_result_error(&result);
		if (verbose >= 1)
			fprintf(stderr, "Error creating index idx_changelog_path: %s\n",
				err ? err : "unknown");
		duckdb_destroy_result(&result);
		return -1;
	}
	duckdb_destroy_result(&result);

	if (duckdb_query(conn, index_files_fid, &result) != DuckDBSuccess) {
		const char *err = duckdb_result_error(&result);
		if (verbose >= 1)
			fprintf(stderr, "Error creating index idx_files_fid: %s\n",
				err ? err : "unknown");
		duckdb_destroy_result(&result);
		return -1;
	}
	duckdb_destroy_result(&result);

	if (duckdb_query(conn, index_files_path, &result) != DuckDBSuccess) {
		const char *err = duckdb_result_error(&result);
		if (verbose >= 1)
			fprintf(stderr, "Error creating index idx_files_path: %s\n",
				err ? err : "unknown");
		duckdb_destroy_result(&result);
		return -1;
	}
	duckdb_destroy_result(&result);

	return 0;
}

/*
 * print_help - Print usage information and options for Lustre Sentinel
 * @prog: Program name (argv[0])
 */
void
print_help(const char *prog)
{
    printf("Lustre Sentinel daemon version %s\n\n", VERSION);
    printf("Usage: %s <fsname-MDTnumber> [options]\n", prog);
    printf("Options:\n");
    printf("  -h, --help                Display this help message\n");
    printf("  -d, --daemon              Run in daemon mode\n");
    printf("  -C, --consumer <name>     Changelog consumer name (default: sentinel)\n");
    printf("  -D, --db <path>           Path to DuckDB database (required unless -c or -R)\n");
    printf("  -R, --register            Register a changelog consumer\n");
    printf("  -m, --mask <full|partial> Audit mask (default: partial with -r)\n");
    printf("  -c, --config <file>       Load configuration from file\n");
    printf("  -v, --verbose <level>     Verbose level (1=simple, 10=everything)\n");
    printf("  -n, --dry-run             Simulate without committing (uses in-memory DB)\n");
    printf("  -r, --rotate <duration>   Rotate database file after duration (e.g., 3600s, 1h)\n");
    if (verbose > 1) {
	printf("\nAdvanced Debug features\n");
        printf("  --no-clear-changelog      Avoid clearning changelog records for the consumer (debugging)\n");
    }
    printf("\nConfiguration file format (for -c):\n");
    printf("  Default configuration file: %s\n", DEFAULT_CONFIG);
    printf("  Key-value pairs (e.g., 'fsname=lustre-MDT0000', 'db=duck.db')\n");
    printf("  Supported keys: fsname, db, consumer, daemon, mask, register, rotate\n");
    printf("  Example:\n");
    printf("    fsname=lustre-MDT0000\n");
    printf("    db=duck.db\n");
    printf("    consumer=cl1\n");
    printf("    rotate=1h\n");
    printf("\nDatabase rotation (with --rotate or rotate key):\n");
    printf("  Database file (e.g., duck.db) is renamed with sequence numbers\n");
    printf("  (e.g., duck.db.0, duck.db.1) based on db template\n");
    if (verbose > 1) {
        printf("\nAdvanced details:\n");
        printf("  - Cache: Hash table of size %d with linear probing\n", CACHE_SIZE);
        printf("  - Cache timeout: %d seconds\n", CACHE_TIMEOUT);
        printf("  - Daemon polling: Min %d, Max %d seconds\n",
               MIN_INTERVAL, MAX_INTERVAL);
        printf("  - Changelog types: CREAT, MKDIR, UNLNK, RMDIR, RENME, RNMTO,\n");
        printf("    CLOSE, TRUNC, SATTR, XATTR, MTIME, CTIME, etc.\n");
        printf("  - Database: changelog and files tables track operations\n");
        printf("  - Reports: SQL-based, supports daily/weekly periods\n");
        printf("  - Rotation units: s (seconds), m (minutes), h (hours), d (days)\n");
    }
    printf("\nVisit https://github.com/cfaber/Lustre-Sentinel for more information\n\n");
}

/*
 * parse_rotation_duration - Parse a duration string into seconds
 * @duration: String containing duration (e.g., "3600s", "1h")
 * @seconds: Pointer to store the parsed seconds
 *
 * Return: 0 on success, -1 on invalid duration
 */
static int parse_rotation_duration(const char *duration, long *seconds)
{
	char *endptr;
	const char *unit;

	*seconds = strtol(duration, &endptr, 10);
	if (*seconds <= 0 || endptr == duration)
		return -1;

	unit = endptr;
	if (*unit == '\0')
		return -1;

	switch (*unit) {
	case 's':
		break;
	case 'm':
		*seconds *= 60;
		break;
	case 'h':
		*seconds *= 3600;
		break;
	case 'd':
		*seconds *= 86400;
		break;
	default:
		return -1;
	}
	return 0;
}

/* generate_next_db_path - Generate the next available database file name
 * @db_template: Template for database file name (e.g., "sentinel_%02d.db")
 * @current_path: Current database path (for directory extraction)
 * @next_path: Buffer to store the next available file path
 * @path_len: Size of next_path buffer
 * @latest_path_mtime: If not NULL, sets to the mtime of the latest file found
 *
 * Scans the directory of current_path for files matching db_template,
 * finds the highest sequence number, and generates the next available name.
 *
 * Return: 0 on success, -errno on failure
 */
static int generate_next_db_path(const char *db_template, const char *current_path,
                                 char *next_path, size_t path_len, time_t *latest_path_mtime)
{
    char *dir_path;
    char *slash;
    struct dirent **namelist;
    int n, max_seq = -1;
    char sscanf_format[PATH_MAX];
    char *percent, *type_start;
    time_t latest_mtime = 0;
    char latest_file[PATH_MAX] = {0};

    if (!db_template || !*db_template) {
        if (verbose >= 1)
            fprintf(stderr, "Invalid db_template: null or empty\n");
        return -EINVAL;
    }

    // Use current_path if valid, otherwise fall back to db_template
    const char *path_to_use = (current_path && *current_path) ? current_path : db_template;
    dir_path = strdup(path_to_use);
    if (!dir_path) {
        if (verbose >= 1)
            fprintf(stderr, "Error duplicating path: %s\n", strerror(errno));
        return -ENOMEM;
    }

    slash = strrchr(dir_path, '/');
    if (slash)
        *slash = '\0';
    else
        strcpy(dir_path, ".");

    if (verbose >= 5)
        printf("Scanning directory: %s\n", dir_path);

    // Create sscanf format by escaping special characters in db_template
    char *basename = strrchr(db_template, '/');
    basename = basename ? basename + 1 : (char *)db_template;
    snprintf(sscanf_format, sizeof(sscanf_format), "%s", basename);
    percent = strstr(sscanf_format, "%");
    if (!percent) {
        if (verbose >= 1)
            fprintf(stderr, "Invalid db_template: no sequence placeholder\n");
        free(dir_path);
        return -EINVAL;
    }

    type_start = percent + 1;
    while (isdigit(*type_start))
        type_start++;
    if (*type_start != 'd') {
        if (verbose >= 1)
            fprintf(stderr, "Unsupported format in db_template: %s\n", sscanf_format);
        free(dir_path);
        return -EINVAL;
    }

    memmove(percent + 2, type_start, strlen(type_start) + 1);
    percent[0] = '%';
    percent[1] = 'd';

    if (verbose >= 5)
        printf("sscanf format: %s\n", sscanf_format);

    n = scandir(dir_path, &namelist, NULL, alphasort);
    if (n < 0) {
        if (verbose >= 1)
            fprintf(stderr, "Error scanning directory %s: %s\n", dir_path, strerror(errno));
        free(dir_path);
        return -errno;
    }

    for (int i = 0; i < n; i++) {
        int seq;
        if (sscanf(namelist[i]->d_name, sscanf_format, &seq) == 1) {
            if (verbose >= 5)
                printf("Found matching file: %s, seq: %d\n", namelist[i]->d_name, seq);
            if (seq > max_seq) {
                max_seq = seq;
                snprintf(latest_file, sizeof(latest_file), "%s/%s", dir_path, namelist[i]->d_name);
                struct stat st;
                if (stat(latest_file, &st) == 0) {
                    latest_mtime = st.st_mtime;
                }
            }
        }
        free(namelist[i]);
    }
    free(namelist);
    free(dir_path);

    if (latest_path_mtime) {
        *latest_path_mtime = latest_mtime;
        if (verbose >= 5 && latest_mtime > 0)
            printf("Latest file %s has mtime %ld\n", latest_file, latest_mtime);
    }

    // Ensure the directory exists
    char *dir_end = strrchr(db_template, '/');
    if (dir_end) {
        char dir[PATH_MAX];
        strncpy(dir, db_template, dir_end - db_template);
        dir[dir_end - db_template] = '\0';
        if (mkdir(dir, 0755) && errno != EEXIST) {
            if (verbose >= 1)
                fprintf(stderr, "Error creating directory %s: %s\n", dir, strerror(errno));
            return -errno;
        }
    }

    snprintf(next_path, path_len, db_template, max_seq + 1);
    if (verbose >= 3)
        printf("Generated next database path: %s\n", next_path);

    if (strlen(next_path) == 0) {
        if (verbose >= 1)
            fprintf(stderr, "Generated empty database path\n");
        return -EINVAL;
    }

    return 0;
}


/* init_db - Initialize a database if needed
 * @duckdb_connection - DuckDB database connection
 * @last_rec - the last record from the previous database
 */
static int init_db(duckdb_connection conn, long long last_rec)
{
    duckdb_result result;
    const char *create_table = "CREATE TABLE IF NOT EXISTS changelog ("
                               "id BIGINT, "
                               "operation_type STRING, "
                               "operation_time TIMESTAMP, "
                               "flags INTEGER, "
                               "tfid STRING, "
                               "pfid STRING, "
                               "name STRING, "
                               "uid STRING, "
                               "gid STRING, "
                               "client_nid STRING, "
                               "job_id STRING, "
                               "full_path STRING)";
    if (verbose >= 3)
        printf("Executing SQL: %s\n", create_table);
    if (duckdb_query(conn, create_table, &result) != DuckDBSuccess) {
        const char *err = duckdb_result_error(&result);
        fprintf(stderr, "Error creating changelog table: %s\n", err ? err : "unknown");
        duckdb_destroy_result(&result);
        return -1;
    }
    duckdb_destroy_result(&result);

    const char *create_metadata = "CREATE TABLE IF NOT EXISTS metadata (key STRING PRIMARY KEY, value STRING)";
    if (verbose >= 3)
        printf("Executing SQL: %s\n", create_metadata);
    if (duckdb_query(conn, create_metadata, &result) != DuckDBSuccess) {
        const char *err = duckdb_result_error(&result);
        fprintf(stderr, "Error creating metadata table: %s\n", err ? err : "unknown");
        duckdb_destroy_result(&result);
        return -1;
    }
    duckdb_destroy_result(&result);

    const char *create_files = "CREATE TABLE IF NOT EXISTS files (fid VARCHAR PRIMARY KEY, current_path VARCHAR, is_deleted BOOLEAN, last_operation_time VARCHAR, last_operation_type VARCHAR, extended_attributes VARCHAR)";
    if (verbose >= 3)
        printf("Executing SQL: %s\n", create_files);
    if (duckdb_query(conn, create_files, &result) != DuckDBSuccess) {
        const char *err = duckdb_result_error(&result);
        fprintf(stderr, "Error creating files table: %s\n", err ? err : "unknown");
        duckdb_destroy_result(&result);
        return -1;
    }
    duckdb_destroy_result(&result);

    char insert_default[128];
    snprintf(insert_default, sizeof(insert_default),
             "INSERT OR IGNORE INTO metadata (key, value) VALUES ('last_rec', '%lld')",
             last_rec);
    if (verbose >= 3)
        printf("Executing SQL: %s\n", insert_default);
    if (duckdb_query(conn, insert_default, &result) != DuckDBSuccess) {
        const char *err = duckdb_result_error(&result);
        fprintf(stderr, "Error inserting default last_rec: %s\n", err ? err : "unknown");
        duckdb_destroy_result(&result);
        return -1;
    }
    duckdb_destroy_result(&result);

    if (verbose >= 2)
        printf("Database initialization completed successfully\n");
    return 0;
}

/* rotate_database - Rotate the database file by tearing down and restarting DuckDB
 * @db: DuckDB database handle
 * @conn: DuckDB connection handle
 * @db_path: Current database path (updated on rotation)
 * @db_template: Template for database file name
 * @rotation_seconds: Rotation interval in seconds
 * @db_opened: Flag indicating if database is open
 * @conn_connected: Flag indicating if connection is active
 * @dry_run: If true, use in-memory database
 * @last_rec: Last processed changelog record number (to resume scanning)
 *
 * Rotates the database by stopping scanning, closing the current database,
 * opening a new one, and preparing to resume scanning.
 *
 * Return: 0 on success, -errno on failure
 */
static int rotate_database(duckdb_database *db, duckdb_connection *conn,
                          char **db_path, const char *db_template,
                          long rotation_seconds, bool *db_opened,
                          bool *conn_connected, bool dry_run,
                          long long *last_rec, int last_record_count) {
    static time_t last_rotation = 0;
    time_t now = time(NULL);

    // Initialize last_rotation on first call without rotating
    if (last_rotation == 0) {
        last_rotation = now;
        return 0;
    }
    if (*db_path && rotation_seconds > 0 && now - last_rotation >= rotation_seconds && last_record_count > 0) {
        // Save last_rec before closing database
        if (*conn_connected) {
            *last_rec = get_last_rec(*conn);
            if (verbose >= 3)
                printf("Saved last_rec: %lld\n", *last_rec);
        }

        // Perform rotation only if not dry-run
	if (!dry_run){
            char new_path[PATH_MAX];
            int rc = generate_next_db_path(db_template, *db_path, new_path, sizeof(new_path), NULL);

            if (rc < 0) {
                if (verbose >= 1)
                    fprintf(stderr, "Error generating next database path: %s\n", strerror(-rc));
                return rc;
            }

            // Close current database
            if (*conn_connected) {
                duckdb_disconnect(conn);
                *conn_connected = false;
            }
            if (*db_opened) {
                duckdb_close(db);
                *db_opened = false;
            }

            // Update db_path
            free(*db_path);
            *db_path = strdup(new_path);
            if (!*db_path) {
                if (verbose >= 1)
                    fprintf(stderr, "Error duplicating db_path: %s\n", strerror(errno));
                return -ENOMEM;
            }
            if (verbose >= 1)
                printf("Rotated database to %s\n", *db_path);

            // Reopen new database
            if (duckdb_open(*db_path, db) != DuckDBSuccess) {
                if (verbose >= 1)
                    fprintf(stderr, "Error opening database %s\n", *db_path);
                return -EIO;
            }
            *db_opened = true;

            if (duckdb_connect(*db, conn) != DuckDBSuccess) {
                if (verbose >= 1)
                    fprintf(stderr, "Error connecting to DuckDB\n");
                duckdb_close(db);
                *db_opened = false;
                return -EIO;
            }
            *conn_connected = true;

            if (init_db(*conn, *last_rec) != 0) {
                if (verbose >= 1)
                    fprintf(stderr, "Error initializing new database\n");
                duckdb_disconnect(conn);
                *conn_connected = false;
                duckdb_close(db);
                *db_opened = false;
                return -EIO;
            }

            // Restore last_rec
            if (*last_rec > 0) {
                update_last_rec(*conn, *last_rec);
                if (verbose >= 3)
                    printf("Restored last_rec: %lld in new database\n", *last_rec);
            }
        }
        last_rotation = now;
    }

    keep_running = true;
    if (verbose >= 3)
        printf("Database rotation completed successfully\n");
    return 0;
}

/* scandir_for_latest_db - Find the latest existing database file matching the template
 * @db_template: Template for database file name (e.g., "sentinel_%02d.db")
 * @latest_path: Buffer to store the path of the latest file
 * @path_len: Size of latest_path buffer
 * @latest_mtime: Pointer to store the mtime of the latest file found
 *
 * Scans the directory of db_template for files matching the template and returns the path
 * with the highest sequence number. Does not generate a new path.
 *
 * Return: 0 on success, -errno on failure
 */
static int scandir_for_latest_db(const char *db_template, char *latest_path, size_t path_len, time_t *latest_mtime) {
    char *dir_path;
    char *slash;
    struct dirent **namelist;
    int n, max_seq = -1;
    char sscanf_format[PATH_MAX];
    char *percent, *type_start;
    time_t mtime = 0;

    if (!db_template || !*db_template) {
        if (verbose >= 1)
            fprintf(stderr, "Invalid db_template: null or empty\n");
        return -EINVAL;
    }

    dir_path = strdup(db_template);
    if (!dir_path) {
        if (verbose >= 1)
            fprintf(stderr, "Error duplicating path: %s\n", strerror(errno));
        return -ENOMEM;
    }

    slash = strrchr(dir_path, '/');
    if (slash)
        *slash = '\0';
    else
        strcpy(dir_path, ".");

    if (verbose >= 5)
        printf("Scanning directory: %s\n", dir_path);

    snprintf(sscanf_format, sizeof(sscanf_format), "%s", strrchr(db_template, '/') ? strrchr(db_template, '/') + 1 : db_template);
    percent = strstr(sscanf_format, "%");
    if (!percent) {
        if (verbose >= 1)
            fprintf(stderr, "Invalid db_template: no sequence placeholder\n");
        free(dir_path);
        return -EINVAL;
    }

    type_start = percent + 1;
    while (isdigit(*type_start))
        type_start++;
    if (*type_start != 'd') {
        if (verbose >= 1)
            fprintf(stderr, "Unsupported format in db_template: %s\n", sscanf_format);
        free(dir_path);
        return -EINVAL;
    }

    memmove(percent + 2, type_start, strlen(type_start) + 1);
    percent[0] = '%';
    percent[1] = 'd';

    if (verbose >= 5)
        printf("sscanf format: %s\n", sscanf_format);

    n = scandir(dir_path, &namelist, NULL, alphasort);
    if (n < 0) {
        if (verbose >= 1)
            fprintf(stderr, "Error scanning directory %s: %s\n", dir_path, strerror(errno));
        free(dir_path);
        return -errno;
    }

    for (int i = 0; i < n; i++) {
        int seq;
        if (sscanf(namelist[i]->d_name, sscanf_format, &seq) == 1) {
            if (verbose >= 5)
                printf("Found matching file: %s, seq: %d\n", namelist[i]->d_name, seq);
            if (seq > max_seq) {
                max_seq = seq;
                snprintf(latest_path, path_len, "%s/%s", dir_path, namelist[i]->d_name);
                struct stat st;
                if (stat(latest_path, &st) == 0) {
                    mtime = st.st_mtime;
                }
            }
        }
        free(namelist[i]);
    }
    free(namelist);
    free(dir_path);

    if (latest_mtime)
        *latest_mtime = mtime;
    if (verbose >= 5 && mtime > 0)
        printf("Latest file %s has mtime %ld\n", latest_path, mtime);

    return 0;
}

/* Apparently this function does stuff */
int
main(int argc, char *argv[])
{
    setlocale(LC_ALL, "");
    bool use_default_config = true;

    char *fsname = NULL;
    char *mount_point = NULL;
    char *config_file = NULL;
    int daemon_mode = 0;
    char *consumer_name = NULL;
    char *audit_mask = NULL;
    char *db_path = NULL;
    int register_consumer = 0;
    int ret = 0;
    bool db_opened = false;
    bool conn_connected = false;
    duckdb_database db;
    duckdb_connection conn;
    bool dry_run = false;
    char *db_template = NULL;
    long rotation_seconds = 0;
    int last_record_count = 0;
    long long last_rec = 0;
    time_t now = time(NULL);
    bool noclear = false;
    bool config_provided = false;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-v") == 0 || strcmp(argv[i], "--verbose") == 0) {
            if (i + 1 < argc && isdigit(argv[i + 1][0])) {
                i++;
                verbose = atoi(argv[i]);
                if (verbose < 1 || verbose > 10)
                    verbose = 0;
            } else {
                verbose = 1;
            }
        } else if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
            phelp = 1;
        } else if (strcmp(argv[i], "-d") == 0 || strcmp(argv[i], "--daemon") == 0) {
            daemon_mode = 1;
        } else if (strcmp(argv[i], "-R") == 0 || strcmp(argv[i], "--register") == 0) {
            register_consumer = 1;
        } else if (strcmp(argv[i], "-C") == 0 || strcmp(argv[i], "--consumer") == 0) {
            consumer_name = strdup(argv[++i]);
        } else if (strcmp(argv[i], "-m") == 0 || strcmp(argv[i], "--mask") == 0) {
            audit_mask = strdup(argv[++i]);
        } else if (strcmp(argv[i], "-D") == 0 || strcmp(argv[i], "--db") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "Missing database path after -D/--db\n");
                print_help(argv[0]);
                return 1;
            }
            db_template = strdup(argv[++i]);
            if (!db_template) {
                fprintf(stderr, "Error duplicating db_template: %s\n", strerror(errno));
                return 1;
            }
        } else if (strcmp(argv[i], "-r") == 0 || strcmp(argv[i], "--rotate") == 0) {
            if (i + 1 < argc) {
                if (parse_rotation_duration(argv[++i], &rotation_seconds) != 0) {
                    fprintf(stderr, "Invalid rotation duration: %s\n", argv[i]);
                    print_help(argv[0]);
                    return 1;
                }
            } else {
                fprintf(stderr, "Missing rotation duration\n");
                print_help(argv[0]);
                return 1;
            }
        } else if (strcmp(argv[i], "-c") == 0 || strcmp(argv[i], "--config") == 0) {
            config_file = strdup(argv[++i]);
	    config_provided = true;
        } else if (strcmp(argv[i], "-n") == 0 || strcmp(argv[i], "--dry-run") == 0) {
            dry_run = true;
	    if (verbose >= 1)
            	printf("Dry run mode!\n");
        } else if (!fsname) {
            fsname = strdup(argv[i]);
	} else if (strcmp(argv[i], "--no-clear-changelog") == 0) {
	    noclear = true;
	    if (verbose >= 1)
		printf("Will NOT clear changelogs!\n");
        } else {
            fprintf(stderr, "Unknown argument: %s\n", argv[i]);
            print_help(argv[0]);
            return 1;
        }

	use_default_config = config_provided ? false : use_default_config;
    }

    if (phelp) {
        print_help(argv[0]);
        return 0;
    }

    if (geteuid() != 0 && !dry_run) {
        fprintf(stderr, "ERROR: This program must be run as root.\n");
	ret = 1;
	goto cleanup;
    }

    /* Check and parse default config file if no -c is provided and file exists */
    if (use_default_config && access(DEFAULT_CONFIG, R_OK) == 0) {
        if (verbose >= 1)
            printf("Loading default configuration from %s\n", DEFAULT_CONFIG);
        if (parse_config(DEFAULT_CONFIG, &fsname, &daemon_mode, &consumer_name,
                         &audit_mask, &db_template, &register_consumer,
                         &rotation_seconds, &db_path) < 0) {
            if (verbose >= 1)
                fprintf(stderr, "Warning: Failed to parse default config file %s, ignoring\n", DEFAULT_CONFIG);

            /* Clear any partially set values to avoid issues */
            free(fsname); fsname = NULL;
            free(consumer_name); consumer_name = NULL;
            free(audit_mask); audit_mask = NULL;
            free(db_template); db_template = NULL;
            daemon_mode = 0;
            register_consumer = 0;
            rotation_seconds = 0;
            free(db_path); db_path = NULL;
        }
    }

    if (config_file && parse_config(config_file, &fsname, &daemon_mode, &consumer_name,
                     &audit_mask, &db_template, &register_consumer,
                     &rotation_seconds, &db_path) < 0) {
        ret = -1;
        goto cleanup;
    }

    /* Set initial db_path based on template if rotation is enabled */
    if (db_template && rotation_seconds > 0 && !db_path) {
        time_t latest_mtime = 0;
        char latest_path[PATH_MAX] = {0};
        int rc = scandir_for_latest_db(db_template, latest_path, sizeof(latest_path), &latest_mtime);

        if (rc < 0) {
            fprintf(stderr, "Error scanning for existing database: %s\n", strerror(-rc));
            ret = -1;
            goto cleanup;
        }

	/* FIXME I'm not sure what the right move is here, basically if rotation is
	 * enabled we won't be able to prevent creation of zero record database files
	 * as we haven't tried processing changelogs yet. 
	 * This case only occurs when running without daemon_mode and is designed to
	 * automatically rotate out old database records.
	 */
        if (!daemon_mode && (now - latest_mtime <= rotation_seconds) ) {
            db_path = strdup(latest_path);
            if (verbose >= 3)
                printf("Reusing existing database: %s (ttl: %ld)\n", db_path, rotation_seconds - (now - latest_mtime));
        } else {
            char new_path[PATH_MAX];
            rc = generate_next_db_path(db_template, latest_path[0] ? latest_path : db_template, new_path, sizeof(new_path), NULL);
            if (rc < 0) {
                fprintf(stderr, "Error generating new database path: %s\n", strerror(-rc));
                ret = -1;
                goto cleanup;
            }
            db_path = strdup(new_path);
            if (verbose >= 3)
                printf("Generated new database path: %s\n", db_path);
        }
        if (!db_path) {
            fprintf(stderr, "Error duplicating db_path: %s\n", strerror(errno));
            ret = -1;
            goto cleanup;
        }
    } else if (db_template) {
        db_path = strdup(db_template);
        if (!db_path) {
            fprintf(stderr, "Error duplicating db_path: %s\n", strerror(errno));
            ret = -1;
            goto cleanup;
        }
    }

    /* Default consumer if not set */
    if (!consumer_name)
        consumer_name = strdup("sentinel");

    if (!fsname) {
        fprintf(stderr, "Missing fsname-MDTnumber\n");
        print_help(argv[0]);
        ret = -1;
        goto cleanup;
    }

    mount_point = get_mount_point(fsname);
    if (!mount_point) {
        fprintf(stderr, "Cannot find mount point for filesystem of %s\n", fsname);
        ret = -1;
        goto cleanup;
    }

    char ver_cmd[] = "lctl --version";
    FILE *ver_fp = popen(ver_cmd, "r");
    if (!ver_fp) {
        if (verbose >= 1)
            fprintf(stderr, "Error executing lctl --version: %s\n",
                    strerror(errno));
        ret = -1;
        goto cleanup;
    }

    char ver_output[256];
    if (fgets(ver_output, sizeof(ver_output), ver_fp) == NULL) {
        pclose(ver_fp);
        fprintf(stderr, "Error reading lctl version\n");
        ret = -1;
        goto cleanup;
    }
    pclose(ver_fp);

    float lctl_version = 0.0;
    if (sscanf(ver_output, "lctl %f", &lctl_version) != 1) {
        fprintf(stderr, "Error parsing lctl version\n");
        ret = -1;
        goto cleanup;
    }
    bool supports_name = (lctl_version >= 2.16);

    if (register_consumer) {
        if (!audit_mask)
            audit_mask = strdup("partial");
        if (strcmp(audit_mask, "full") != 0 && strcmp(audit_mask, "partial") != 0) {
            fprintf(stderr, "Invalid -m/--mask, must be 'full' or 'partial'\n");
            free(audit_mask);
            ret = -1;
            goto cleanup;
        }
        char mask_str[1024];
        if (strcmp(audit_mask, "full") == 0) {
            strncpy(mask_str, "+CREAT +MKDIR +HLINK +SLINK +MKNOD +UNLNK +RMDIR +RENME +RNMTO +TRUNC +SATTR +XATTR +MTIME +CTIME +ATIME +MIGRT +GXATR", sizeof(mask_str) - 1);
        } else {
            strncpy(mask_str, "+CREAT +MKDIR +UNLNK +RMDIR +RENME +RNMTO +CLOSE +TRUNC +SATTR +XATTR +MTIME +CTIME", sizeof(mask_str) - 1);
        }
        free(audit_mask);
        audit_mask = NULL;

        if (verbose >= 1)
            printf("Setting changelog mask: %s\n", mask_str);

        if (verbose >= 1)
            printf("Registering consumer\n");

        char cmd[1024];

	if (strlen(fsname) + strlen(mask_str) + 40 >= sizeof(cmd)) {
	    fprintf(stderr, "Command buffer too small for fsname and mask\n");
	    ret = -1;
	    goto cleanup;
	}

	volatile size_t cmd_size = sizeof(cmd);
	snprintf(cmd, cmd_size, "lctl --device %s changelog_register -m \"%s\"", fsname, mask_str);
 
         if (supports_name && consumer_name) {
            char temp[1024];

	    if (strlen(cmd) + strlen(consumer_name) + 5 >= sizeof(temp)) {
	        fprintf(stderr, "Command buffer too small for consumer name\n");
	        ret = -1;
	        goto cleanup;
	    }

	    volatile size_t temp_size = sizeof(temp);
            snprintf(temp, temp_size, "%s -n %s", cmd, consumer_name);
            strcpy(cmd, temp);
         }

        if (verbose >= 10)
            printf("Registration command: %s\n", cmd);
        if (dry_run) {
            printf("Dry-run: would register consumer with command: %s\n", cmd);
        } else {
            FILE *reg_fp = popen(cmd, "r");
            if (!reg_fp) {
                fprintf(stderr, "Error registering changelog user\n");
                ret = -1;
                goto cleanup;
            }
            char reg_output[256] = {0};
            if (fgets(reg_output, sizeof(reg_output), reg_fp) == NULL) {
                pclose(reg_fp);
                fprintf(stderr, "No output from registration command\n");
                ret = -1;
                goto cleanup;
            }
            int status = pclose(reg_fp);
            if (status != 0) {
                fprintf(stderr, "Registration command failed with exit code %d\n", WEXITSTATUS(status));
                ret = -1;
                goto cleanup;
            }
            reg_output[strcspn(reg_output, "\n")] = '\0';  /* Trim newline */

            char id[32];
            if (sscanf(reg_output, "%31s", id) == 1) {
                free(consumer_name);
                consumer_name = strdup(id);
            } else {
                fprintf(stderr, "Error parsing consumer ID from output: %s\n", reg_output);
                ret = -1;
                goto cleanup;
            }

            printf("Registered as %s\n", consumer_name);
        }

        ret = 0;
        goto cleanup;
    }

    if (!db_path) {
        ret = 0;
        goto cleanup;
    }

    // Open DB
    if (dry_run) {
        if (verbose >= 1)
            printf("Dry-run mode: using in-memory database\n");
        if (duckdb_open(NULL, &db) != DuckDBSuccess) {
            fprintf(stderr, "Error opening in-memory DuckDB database\n");
            ret = -1;
            goto cleanup;
        }
        db_opened = true;
    } else {
        if (verbose >= 1)
            printf("Opening DuckDB database at %s\n", db_path);
        if (duckdb_open(db_path, &db) != DuckDBSuccess) {
            fprintf(stderr, "Error opening DuckDB database at %s\n", db_path);
            ret = -1;
            goto cleanup;
        }
        db_opened = true;
    }

    if (duckdb_connect(db, &conn) != DuckDBSuccess) {
        fprintf(stderr, "Error connecting to DuckDB\n");
        ret = -1;
        goto cleanup;
    }
    conn_connected = true;

    if (init_db(conn, last_rec) != 0) {
        fprintf(stderr, "Error initializing database\n");
        ret = -1;
        goto cleanup;
    }

    last_rec = get_last_rec(conn);

    if (verbose >= 2)
        printf("Database opened and initialized successfully\n");

    if (daemon_mode) {
        // Setup signals
        if (verbose >= 1)
            printf("Setting up signals for daemon mode\n");
        signal(SIGINT, handle_signal);
        signal(SIGTERM, handle_signal);
    }

    while (keep_running) {
        if (rotation_seconds > 0) {
            // Rotate database if needed
            if (rotate_database(&db, &conn, &db_path, db_template,
                                rotation_seconds, &db_opened, &conn_connected,
                                dry_run, &last_rec, last_record_count) != 0) {
                fprintf(stderr, "Error rotating database\n");
                ret = -1;
                goto cleanup;
            }
        }

        int record_count = process_changelog(fsname, conn, mount_point,
                                            last_rec, consumer_name,
                                            dry_run, noclear, daemon_mode);
        if (record_count >= 0) {
            if (verbose)
                printf("Processed %d changelog records\n", record_count);
            last_record_count = record_count;
            last_rec = get_last_rec(conn);
        } else {
            fprintf(stderr, "Error processing changelog\n");
            ret = -1;
            goto cleanup;
        }

        if (!daemon_mode)
            break;

        int interval = get_polling_interval(last_record_count);
        if (verbose >= 1)
            printf("Sleeping for %d seconds...\n", interval);
        sleep(interval);
    }

cleanup:
    if (conn_connected) {
        if (verbose >= 2)
            printf("Disconnecting from DuckDB\n");
        duckdb_disconnect(&conn);
        conn_connected = false;
    }

    if (db_opened) {
        if (verbose >= 2)
            printf("Closing DuckDB database\n");
        duckdb_close(&db);
        db_opened = false;
    }

    free(fsname);
    free(consumer_name);
    free(audit_mask);
    free(db_path);
    free(config_file);
    free(mount_point);
    free(db_template);

    return ret & 0xFF;
}

