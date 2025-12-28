Lustre Sentinel Test Suite Help Documentation
Overview
The Lustre Sentinel Test Suite is a comprehensive Bash script designed to validate the functionality of the Lustre Sentinel tool, a file system monitoring and querying solution developed by Colin Faber <cfaber@thelustrecollective.com> of The Lustre Collective (TLC). The test suite verifies the behavior of the lsentineld daemon and the sentinel client, which interact with a Lustre file system to monitor and query file system events. It leverages the lustre-tests package to set up a temporary, single-node Lustre file system on the local machine for testing purposes. The script requires root privileges and specific dependencies, including the DuckDB CLI and Lustre Sentinel binaries.
This document provides a detailed guide to the test suite, including its purpose, requirements, usage, available tests, and the single-node Lustre setup process.
Purpose
The Lustre Sentinel Test Suite ensures that Lustre Sentinel functions correctly by testing:

Compilation of the lsentineld daemon and sentinel client.
Registration of changelog consumers with the Lustre file system.
Processing of file system events (e.g., file creation, deletion, renaming, symlinks).
Configuration file handling and database operations, including rotation.
Querying file system history by FID or path in various output formats.
Edge cases, such as invalid configurations, UTF-8 paths, and database collection.

The suite is designed for developers and system administrators to verify Lustre Sentinel’s reliability in a controlled, single-node Lustre environment.
Requirements
To run the Lustre Sentinel Test Suite, the following prerequisites must be met:

Root Privileges: The script requires root or sudo privileges for Lustre file system operations, such as mounting and changelog consumer registration.
Lustre File System and lustre-tests Package: 
A Lustre file system must be installed, with the lustre-tests package providing scripts like llmount.sh and llmountcleanup.sh.
These scripts are typically located in /usr/lib64/lustre/tests/ (or a custom path specified by $LUSTRE_TESTS_DIR).
The lustre-tests package is used to set up a temporary, single-node Lustre file system on the local machine.


DuckDB CLI: The DuckDB command-line interface must be installed and accessible at /usr/local/bin/duckdb (or a custom path specified in $DUCKDB_CLI).
Compiler and Libraries: gcc and required headers/libraries (lustreapi, duckdb, pthread, libm) must be available to compile lsentineld.c and sentinel.c.
Lustre Sentinel Binaries: The source files lsentineld.c and sentinel.c must be available for compilation, or the compiled binaries (lsentineld and sentinel) must be present.
Lustre Sentinel Source: Obtain the Lustre Sentinel tool from: https://github.com/cfaber/Lustre-Sentinel

Installation and Setup

Install Dependencies:

Install the Lustre file system and ensure the lustre-tests package is available.
Install DuckDB CLI and verify it is accessible at /usr/local/bin/duckdb (or update $DUCKDB_CLI).
Install gcc and required libraries: lustreapi, duckdb, pthread, and libm.


Obtain Lustre Sentinel:

Download the Lustre Sentinel source code from: https://github.com/cfaber/Lustre-Sentinel
Place lsentineld.c and sentinel.c in the src/ directory for compilation.


Set Up Environment:

If lustre-tests scripts are not in /usr/lib64/lustre/tests/, set the LUSTRE_TESTS_DIR environment variable to the correct path.
Ensure the test suite script (test-sentinel.sh) has executable permissions: chmod +x test-sentinel.sh.


Run as Root:

Execute the script with sudo or as the root user due to Lustre operations and file system mounting requirements.



Single-Node Lustre Setup
The test suite uses the lustre-tests package to create a temporary, single-node Lustre file system on the local machine. This setup is managed by the following scripts:

llmount.sh: Configures a minimal Lustre file system with a single Metadata Target (MDT) named lustre-MDT0000 and mounts it at /mnt/lustre (default mount point). It uses local storage (often loopback devices) to simulate a Lustre environment without requiring a full cluster.
llmountcleanup.sh: Unmounts the file system and cleans up resources, ensuring no residual configurations remain after testing.

Setup Process

The setup_lustre function:
Checks if the Lustre file system is already set up to avoid redundant operations.
Executes llmount.sh to mount the file system.
Verifies the mount point (/mnt/lustre) exists in the system’s mount table.
Creates a test directory (/mnt/lustre/lsentinel_test.<timestamp>) for file system operations.
Waits briefly to ensure the file system is stable before proceeding.



Teardown Process

The cleanup function:
Terminates any running lsentineld processes.
Removes temporary files, directories, and compiled binaries.
Executes llmountcleanup.sh to unmount the Lustre file system, unless --no-lustre-setup is specified.
Ensures no resources are left behind, maintaining a clean test environment.



This single-node setup is lightweight, making it ideal for testing without the complexity of a multi-node Lustre cluster.
Usage
The test suite is executed via the command line with various options to control its behavior. The general syntax is:
sudo ./test-sentinel.sh [options]

Options

--all: Run all available tests in the default mode (sequential unless --random is specified).
--sequential: Run tests in sequential order (default).
--random: Run tests in random order.
--test <test_name>: Run a single specified test (e.g., --test client_fid).
--help: Display the help message and exit.
--no-lustre-setup: Skip Lustre file system setup and teardown (useful if Lustre is already configured).
--no-cleanup: Skip cleanup steps to preserve test artifacts for debugging.
--list-tests: List all available tests with descriptions and exit.
--consumer-id <id>: Specify a custom consumer ID for changelog registration (default: test_sentinel).

Example Commands

Run all tests sequentially:
sudo ./test-sentinel.sh --all --sequential


Run a single test:
sudo ./test-sentinel.sh --test client_fid


List all available tests:
./test-sentinel.sh --list-tests


Run tests with a custom consumer ID and no cleanup:
sudo ./test-sentinel.sh --all --consumer-id my_consumer --no-cleanup



Available Tests
The test suite includes a comprehensive set of tests to validate Lustre Sentinel’s functionality. Below is a list of all tests with their descriptions:



Test Name
Description



daemon_compile
Verifies that lsentineld.c compiles successfully.


client_compile
Verifies that sentinel.c compiles successfully.


help
Checks if the help message is displayed correctly.


register_full_mask
Tests changelog consumer registration with a full event mask.


register_partial_mask
Tests changelog consumer registration with a partial event mask.


dry_run_register
Verifies dry-run mode for consumer registration.


parse_config
Ensures configuration file is parsed and database is created.


process_create
Tests changelog processing for file creation events.


process_delete
Tests changelog processing for file deletion events.


process_rename
Tests changelog processing for file renaming events.


daemon_mode
Verifies daemon mode with file creation and signal handling.


cache
Checks if FID-to-path cache is used during processing.


verbose_levels
Ensures higher verbosity levels produce more output.


database_rotation
Tests database file rotation and record consistency.


database_rotation_as_daemon
Tests database file rotation in daemon mode.


invalid_config
Verifies error handling for invalid configuration files.


process_symlink
Tests changelog processing for symlink creation events.


mount_point_failure
Verifies error handling when mount point detection fails.


sql_escape
Tests SQL escaping of special characters in filenames.


client_fid
Tests querying history by FID from a single database.


client_path
Tests resolving a path to FID and querying history.


output_formats
Verifies output in JSON, YAML, and CSV formats.


mount_integration
Tests live path resolution with mount point (requires root).


config_client
Tests loading client options from a configuration file.


multi_db_client
Tests querying across multiple rotated databases.


edge_utf8_path
Tests handling of UTF-8 paths in queries.


invalid_search
Tests error handling for invalid FID or path queries.


verbose_client
Tests verbosity levels for the client, ensuring progressive log output.


summary_accuracy
Tests summary generation for file existence and name changes.


collect_db_files_glob
Tests database collection with a glob pattern.


collect_db_files_template
Tests database collection with a template pattern.


collect_db_files_explicit
Tests database collection with an explicit file list.


collect_db_files_invalid_dir
Tests error handling for invalid directory in database collection.


collect_db_files_no_match
Tests error handling when no files match the database glob pattern.


output_file_success
Tests successful output redirection to a file.


output_file_failure
Tests error handling when the output file cannot be opened.


default_db_collection
Tests default database collection from the current directory.


query_method_loop
Tests querying with the loop method for multiple/single databases.


query_method_attach
Tests querying with the attach method for multiple/single databases.


query_method_invalid
Tests error handling for an invalid query method.


verbose_repeated
Tests handling of repeated verbosity flags, ensuring the last value wins.


To list these tests with their descriptions, run:
./test-sentinel.sh --list-tests

Test Execution Flow

Initialization:

Verifies the presence of the DuckDB CLI and lustre-tests scripts.
Sets up temporary directories (/tmp/lsentinel_test.<timestamp>) and configuration files.
If not skipped (--no-lustre-setup), mounts a single-node Lustre file system using llmount.sh.


Test Selection:

If --test <test_name> is specified, runs only the specified test.
If --all is used, runs all tests in the specified mode (--sequential or --random).
Otherwise, runs all tests sequentially.


Test Execution:

Tests are executed in the specified order, performing actions like compiling binaries, creating files, and querying databases.
Tests involving Lustre operations use the temporary directory /mnt/lustre/lsentinel_test.<timestamp>.
Results are recorded as pass or fail using the pass_test or fail_test functions.


Reporting:

Generates a report listing passed and failed tests, with failure reasons for debugging.
If tests fail, cleanup is skipped, and artifacts are preserved in /tmp/lsentinel_test.<timestamp>.


Cleanup:

Unless --no-cleanup is specified, removes temporary files, directories, and binaries.
Unmounts the Lustre file system using llmountcleanup.sh if it was set up.



Key Features Tested

Compilation: Ensures lsentineld and sentinel compile correctly.
Changelog Processing: Validates processing of file system events (create, delete, rename, symlink) into a DuckDB database.
Consumer Registration: Tests registration with full or partial event masks.
Database Management: Verifies database creation, rotation, and multi-database querying.
Client Queries: Ensures querying by FID or path, with support for text, CSV, JSON, and YAML outputs.
Error Handling: Tests handling of invalid configurations, mount failures, and invalid queries.
UTF-8 Support: Verifies correct handling of non-ASCII filenames.
Verbosity: Ensures increasing verbosity levels produce more detailed logs.
Output Redirection: Tests writing query results to files and handling write failures.

Troubleshooting

Lustre Setup Fails:

Ensure llmount.sh and llmountcleanup.sh are in $LUSTRE_TESTS_DIR (default: /usr/lib64/lustre/tests/).
Verify root privileges with sudo.
Check that Lustre kernel modules and services are running.
Ensure no conflicting Lustre file system is mounted at /mnt/lustre.


DuckDB CLI Not Found:

Install DuckDB and verify its path (/usr/local/bin/duckdb or update $DUCKDB_CLI).


Compilation Errors:

Ensure gcc and libraries (lustreapi, duckdb, pthread, libm) are installed.
Verify lsentineld.c and sentinel.c are in the src/ directory.


Test Failures:

Check the test report for failure reasons.
Inspect log files in /tmp/lsentinel_test.<timestamp>.
Use --no-cleanup to preserve artifacts for debugging.


Permission Issues:

Run the script as root or with sudo.
Ensure write permissions for /tmp and /mnt/lustre.



Notes

Temporary Files: Artifacts are stored in /tmp/lsentinel_test.<timestamp> and cleaned up unless --no-cleanup is used.
Lustre Mount Point: The default mount point is /mnt/lustre. Ensure it is not in use by other processes.
Consumer ID: The default consumer ID is test_sentinel. Use --consumer-id to specify a custom ID.
Verbose Output: Use -v <level> with lsentineld or sentinel to increase log verbosity for debugging.
Single-Node Setup: The lustre-tests package creates a lightweight, loopback-based Lustre file system, ideal for testing without a full cluster.

Contact and Support
For issues with Lustre Sentinel or the test suite, contact The Lustre Collective or refer to the official repository at https://github.com/cfaber/Lustre-Sentinel
For Lustre-related support, consult the Lustre documentation or community forums.
