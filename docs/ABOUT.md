# Lustre Sentinel Design Document

## 1. Introduction

### 1.1 Purpose
Lustre Sentinel is a monitoring and querying tool designed to track changes in a Lustre filesystem by consuming its changelog. It processes changelog records, resolves file paths, stores audit data in a DuckDB database, and supports reporting on filesystem activities. The tool consists of two main components:
- **lsentineld**: The daemon or single-shot consumer that ingests Lustre changelogs and populates the database.
- **sentinel**: A client tool that queries the databases to reconstruct the operation history for a specific file (identified by FID or path).

The primary goals are:
- Audit filesystem operations (e.g., creates, deletes, renames) in real-time or batch.
- Maintain a cache for efficient path resolution using File Identifiers (FIDs).
- Store structured data for querying and reporting.
- Handle UTF-8 validation and error-prone path resolutions gracefully.
- Provide historical queries for file operations across multiple databases.

Version: 1.0 (as of December 28, 2025)

### 1.2 Scope
- Supports Lustre changelog consumption via `lustre/lustreapi.h` (and optionally `libcfs/libcfs.h` for NID handling).
- Uses DuckDB for embedded, lightweight SQL storage.
- Handles common operations like creates, deletes, renames, and attribute changes.
- Limited to MDT (Metadata Target) changelogs.
- Includes a query client (`sentinel`) for file history reconstruction, supporting outputs in text, YAML, JSON, CSV, or fancy formats.
- No support for installing additional packages; relies on pre-installed libraries.

### 1.3 Dependencies
- **Libraries**: 
  - Lustre API (`lustre/lustreapi.h` and `lustre/lustre_user.h`) for changelog access, FID/path operations, and user-level interactions.
  - DuckDB (`duckdb.h`) for database management.
  - Standard C libraries: `stdio.h`, `stdlib.h`, `string.h`, `time.h`, `locale.h`, `unistd.h`, `signal.h`, `stdbool.h`, `mntent.h`, `inttypes.h`, `errno.h`, `ctype.h`, `dirent.h`, `regex.h`.
- **Environment**: Assumes a Linux system with Lustre mounted and accessible via `/proc/mounts`. Build requires Autoconf, Automake, Libtool, and a C compiler (e.g., GCC).
- **No Internet Access**: All operations are local; no external fetches.
- **Optional**: `libcfs/libcfs.h` for advanced NID string conversion (detected via configure).

## 2. System Overview

### 2.1 Architecture
The system comprises two standalone C programs:
- **lsentineld (Consumer/Daemon)**: Single-threaded process for changelog ingestion.
- **sentinel (Query Client)**: Tool for querying stored data to build file histories.

High-level components shared or used across both:
- **Command-Line Interface**: Parses arguments and configuration files (key=value format).
- **Changelog Consumer** (lsentineld): Interfaces with Lustre to fetch and process records.
- **FID Cache** (lsentineld): In-memory array-based cache for FID-to-path mappings to optimize resolutions.
- **Path Resolver** (both): Uses Lustre API or database fallback to get full paths; includes UTF-8 validation.
- **Database Manager** (both): Handles DuckDB connections, schema creation, insertions, and queries.
- **Signal Handler** (lsentineld): For graceful shutdown in daemon mode.
- **Reporting Module** (lsentineld): Generates summary reports based on time periods.
- **History Query Module** (sentinel): Reconstructs and sorts file operation histories from multiple databases.

The programs can operate in:
- **Single-Shot Mode** (lsentineld): Process available changelogs once.
- **Daemon Mode** (lsentineld): Continuously poll and process with adaptive sleep intervals (1-10 seconds based on activity).
- **Dry-Run Mode** (lsentineld): Simulate processing using an in-memory database without committing changes.
- **Query Modes** (sentinel): Supports looping over databases or attaching them for queries; handles read-only access and skips locked databases.

Data flow for lsentineld:
1. Parse config/args.
2. Open DuckDB database (file or in-memory).
3. Initialize cache and retrieve last processed record.
4. Fetch changelogs from Lustre.
5. Process records: Resolve paths, insert into DB, update cache.
6. Clear processed changelogs (unless dry-run or noclear).
7. Generate reports if requested.
8. Loop in daemon mode or exit.

Data flow for sentinel:
1. Parse args (e.g., FID/path, DB paths/glob/template, output format).
2. Resolve path to FID if needed (using Lustre API).
3. Query databases (loop or attach method) for matching records.
4. Sort and output history in specified format (text, JSON, YAML, CSV, fancy).
5. Handle verbosity and edge cases like no records found.

### 2.2 Key Data Structures
- **changelog_data** (lsentineld): Temporary struct for record fields (operation_type, time, client_nid, uid, gid, full_path).
- **fid_cache_entry** (lsentineld): Array of CACHE_SIZE (1000) entries with FID, path, and timestamp for time-based eviction.
- **history_list** (sentinel): Dynamic list for storing and sorting query results (operation details including paths).
- **Database Tables** (shared):
  - `changelog`: Stores raw records (id, operation_type, operation_time, flags, tfid, pfid, name, uid, gid, client_nid, full_path).
  - `metadata`: Key-value store (e.g., 'last_rec' for last processed changelog index).
  - `files`: Tracks file states (fid, current_path, is_deleted, last_operation_time, last_operation_type, extended_attributes).
  - Temporary `temp_files` for bulk upserts during processing.

## 3. Core Components

### 3.1 FID Cache (lsentineld)
- **Purpose**: Avoid repeated expensive path lookups via Lustre API.
- **Implementation**: Fixed-size array (1000) with simple hashing (multiplicative on FID string) and linear probing.
  - Eviction: Time-based (300-second timeout) or overwrite oldest on full.
- **Operations**:
  - `init_fid_cache()`: Clears cache.
  - `get_cached_path()`: Probes for valid, non-expired entry.
  - `add_to_cache()`: Inserts or updates, validates UTF-8.
- **Limitations**: No resizing; assumes CACHE_SIZE is sufficient.

### 3.2 Path Resolution (Shared)
- **Primary Method**: `llapi_fid2path()` using mount point.
- **Fallbacks**:
  - Query `files` table in DB for current_path.
  - For deletes/renames: Construct path from parent FID + name (e.g., via `construct_full_path()` or `split_full_path()` in sentinel).
- **UTF-8 Handling**: Validates strings; discards invalid ones, replaces with "unknown".
  - Functions: `is_valid_utf8()`, `utf8len()`, `utf8_strncpy()`.
- **Mount Point Detection**: Parses `/proc/mounts` for Lustre devices matching fsname.
- **Additional in sentinel**: `resolve_path_to_fid()` using Lustre API or database queries.

### 3.3 Changelog Processing (lsentineld)
- **Entry Point**: `process_changelog()`.
- **Modes**:
  - **Follow (Daemon)**: Real-time recv, process one-by-one.
  - **Single-Shot**: Collect all records first, pre-cache deletes in reverse order, then process forward.
- **Special Handling**:
  - Renames: Pair CL_RENAME with following CL_EXT record using custom struct `my_changelog_ext_rename`.
  - Deletes: Pre-resolve paths if API fails (using parent + name).
- **Insertion**:
  - Use DuckDB appender for efficient bulk inserts.
  - Bulk upsert into `files` via temporary table.
- **Clearing**: `llapi_changelog_clear()` after processing (skipped in dry-run).
- **Polling Interval**: Adaptive (1s if >100 records, else 10s).
- **Rotation**: Supports database rotation based on duration (e.g., via `generate_next_db_path()`).

### 3.4 Database Operations (Shared)
- **Schema Creation**: On startup, create tables if missing (e.g., `create_indexes()` for performance).
- **Appenders**: For high-performance inserts without SQL parsing.
- **Queries**:
  - Get/update last_rec.
  - Fallback path lookups (e.g., `get_full_path()`).
  - Recent records display (LIMIT 10).
  - File history queries (e.g., `query_loop()` or `query_attach()` in sentinel).
- **Transactions**: Implicit via appender flush; no explicit commits.
- **Multi-DB Support (sentinel)**: Handles directories, globs, templates, or explicit file lists; skips locked databases.

### 3.5 Reporting (lsentineld)
- **Function**: `generate_report()`.
- **Queries**: Aggregate counts by operation_type over interval (daily/weekly).
- **Output**: Simple stdout (e.g., "CREAT: 42").

### 3.6 Configuration Parsing (Shared)
- **Sources**: Command-line args or config file (key=value format).
- **Keys** (lsentineld): fsname, daemon, consumer, mask, db, register, report, verbose, dry-run, noclear, config.
- **Keys** (sentinel): db-path, db-template, db-glob, db-files, format (text/yaml/json/csv/fancy), method (attach/loop), verbose.

## 4. Functionality Details

### 4.1 Record Processing (lsentineld: `insert_record()`)
- Extracts time, type, FIDs, name, jobid (client_nid), uid/gid.
- Resolves full_path based on type (e.g., "old_path -> new_path" for renames).
- Appends to changelog and temp_files.
- Updates max_rec for clearing.

### 4.2 History Querying (sentinel)
- Resolves input (FID or path) using Lustre API.
- Queries across databases, collects records into `history_list`.
- Sorts by time (`sort_history_list()`).
- Outputs in various formats (e.g., `output_text()`, `output_json()`).
- Handles edge cases like no records, invalid paths, or memory allocation failures.

### 4.3 Error Handling (Shared)
- Verbose levels (0+): Logs vary from errors (1) to detailed cache ops (higher).
- Graceful shutdown on SIGINT/TERM in daemon.
- Fallbacks for path resolution; "unknown" placeholders.
- DuckDB errors reported via `duckdb_result_error()`.
- Skips locked databases in sentinel.

### 4.4 Security Considerations
- Escapes single quotes in SQL (but uses appenders mostly).
- Validates UTF-8 to prevent invalid data.
- Runs as user; assumes permissions for Lustre and DB.

## 5. Configuration and Usage

### 5.1 Command-Line Options (lsentineld)
- `<fsname-MDTnumber>`: Required (e.g., "lustrefs-MDT0000").
- `-d/--daemon`: Continuous mode.
- `--consumer <name>`: Changelog consumer (default: "cl1" or auto-generated).
- `--db <path>`: DuckDB file.
- `--register-consumer`: Register consumer with mask.
- `--audit-mask <full|partial>`: Audit events (full includes more like access times; default: partial).
- `--report <daily|weekly>`: Generate report.
- `-c/--config <file>`: Load from file (default: /usr/share/sentinel/default.conf if present).
- `-v/--verbose`: Increase verbosity (repeat for more).
- `--dry-run`: Simulate without writing.
- `-h/--help`: Usage.

### 5.2 Command-Line Options (sentinel)
- `--fid <fid> | --path <path>`: File to query (required).
- `--db-path <dir>`: Directory to search for databases.
- `--db-template <tmpl>`: Database file template (e.g., "sentinel_%02d.db").
- `--db-glob <glob>`: Glob pattern for database files (e.g., "*.db").
- `--db-files <file1> [<file2>...]`: List of database files.
- `--format <text|yaml|json|csv|fancy>`: Output format (default: text).
- `--method <attach|loop>`: Query method (default: loop).
- `-v/--verbose`: Increase verbosity (repeat for more).
- `-h/--help`: Usage.

### 5.3 Example Usage
- Register (lsentineld): `lsentineld lustrefs-MDT0000 --register-consumer --audit-mask full --consumer myconsumer`
- Run Daemon (lsentineld): `lsentineld lustrefs-MDT0000 --db /path/audit.db --consumer cl1 --daemon --verbose`
- Report (lsentineld): `lsentineld --db /path/audit.db --report daily`
- Query History (sentinel): `sentinel --path /lustre/file.txt --db-path /path/to/dbs --format json --verbose`

## 6. Limitations and Future Work
- **Scalability**: Fixed cache size; no distributed processing or multi-threading.
- **Error Recovery**: No retry on Lustre errors; assumes reliable API.
- **Extensions**: Basic reports and queries; could add advanced analytics or visualizations.
- **Testing**: Assumes valid Lustre setup; includes test scripts (e.g., simulate_activity.sh, test-sentinel.sh) but no unit tests in code.
- **Potential Improvements**: Add indexing on DB tables, support multiple MDTs, configurable cache size/timeout, enhanced output formats in sentinel.
