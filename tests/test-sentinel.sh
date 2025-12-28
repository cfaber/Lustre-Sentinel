#!/bin/bash

# Lustre Sentinel Test Suite
# This script sets up a temporary Lustre filesystem using llmount.sh,
# compiles and tests the lsentinel program.
# Assumptions:
# - Lustre source/tests are available (llmount.sh in $LUSTRE_TESTS_DIR or default /usr/lib64/lustre/tests/)
# - duckdb CLI is installed for DB verification (duckdb command)
# - gcc and necessary headers/libs for compiling lsentinel.c (lustreapi, duckdb)
# - Run as root or with sudo for mounting and Lustre ops

# Default paths (configurable)
DUCKDB_CLI="/usr/local/bin/duckdb"

# Lustre config
FS_NAME="lustre"
MDT_NAME="${FS_NAME}-MDT0000"
CONSUMER_NAME="test_sentinel"
LUSTRE_TESTS_DIR="${LUSTRE_TESTS_DIR:-/usr/lib64/lustre/tests}"

# Temporary dirs
TEST_DIR="/tmp/lsentinel_test.$(date +%s)"
MOUNT_POINT="/mnt/${FS_NAME}"
LUSTRE_TEST_DIR="/lsentinel_test.$(date +%s)"
LUSTRE_TEST_DIR_FP="${MOUNT_POINT}${LUSTRE_TEST_DIR}"

# Sentinel config
DB_PATH="${TEST_DIR}/sentinel.db"
CONFIG_FILE="${TEST_DIR}/sentinel.conf"
DAEMON_BIN="${TEST_DIR}/lsentineld"
CLIENT_BIN="${TEST_DIR}/sentinel"
DAEMON_COMPILE_FLAGS="-o ${TEST_DIR}/lsentineld src/lsentineld.c -llustreapi -lduckdb -lpthread -lm"
CLIENT_COMPILE_FLAGS="-o ${TEST_DIR}/sentinel src/sentinel.c -llustreapi -lduckdb -lpthread -lm"

# Global vars
FAILED_TESTS=()
PASSED_TESTS=()
SETUP_DONE=0
CLEANUP_DONE=0

# Test config
MODE="sequential"
SELECTED_TESTS=()
RUN_ALL=0
SINGLE_TEST=""



##################
# Test functions #
##################

test_client_compile() {
    compile_sentinel_client
    pass_test "client_compile"
}

test_daemon_compile() {
    compile_sentinel_daemon
    pass_test "daemon_compile"
}


test_help() {
    local output=$($DAEMON_BIN -h)
    if [[ "$output" != *"Lustre Sentinel daemon version"* ]]; then
        fail_test "help" "Help message not displayed"
    else
        pass_test "help"
    fi
}

test_register_full_mask() {
    setup_lustre
    local consumer_full="${CONSUMER_ID}_full"
    local output=$($DAEMON_BIN "$MDT_NAME" -R -m full -C "$consumer_full" 2>&1)
    if [ $? -ne 0 ]; then
        fail_test "register_full_mask" "Registration failed"
        return
    fi

    local registered_name=$(echo "$output" | grep "Registered as" | awk '{print $3}')
    if [ -z "$registered_name" ]; then
        fail_test "register_full_mask" "Could not parse registered name"
        return
    fi

    local mask=$(get_mask "$registered_name")
    local expected_types=(CREAT MKDIR HLINK SLINK MKNOD UNLNK RMDIR RENME RNMTO TRUNC SATTR XATTR MTIME CTIME ATIME MIGRT GXATR)
    for typ in "${expected_types[@]}"; do
        if [[ ! "$mask" == *"$typ"* ]]; then
            fail_test "register_full_mask" "Mask does not contain $typ: $mask"
            return
        fi
    done
    pass_test "register_full_mask"
    CONSUMER_ID=$registered_name
}

test_register_partial_mask() {
    setup_lustre
    local output=$($DAEMON_BIN "$MDT_NAME" -R -m partial -C "$CONSUMER_ID" 2>&1)
    if [ $? -ne 0 ]; then
        fail_test "register_partial_mask" "Registration failed"
        return
    fi

    local registered_name=$(echo "$output" | grep "Registered as" | awk '{print $3}')
    if [ -z "$registered_name" ]; then
        fail_test "register_partial_mask" "Could not parse registered name"
        return
    fi
    
    local mask=$(get_mask "$registered_name")
    local expected_types=(CREAT MKDIR UNLNK RMDIR RENME RNMTO CLOSE TRUNC SATTR XATTR MTIME CTIME)
    for typ in "${expected_types[@]}"; do
        if [[ ! "$mask" == *"$typ"* ]]; then
            fail_test "register_partial_mask" "Mask does not contain $typ: $mask"
            return
        fi
    done
    pass_test "register_partial_mask"
    CONSUMER_ID=$registered_name
}

test_dry_run_register() {
    setup_lustre
    local output=$($DAEMON_BIN "$MDT_NAME" -R -m partial -C "$CONSUMER_ID" -n)
    if [[ "$output" != *"Dry-run: would register"* ]]; then
        fail_test "dry_run_register" "Dry-run not simulating registration"
    else
        pass_test "dry_run_register"
    fi
}

test_parse_config() {
    generate_config
    # Run with config, check if uses db
    $DAEMON_BIN -c "$CONFIG_FILE" -D "$DB_PATH"  # No fsname, but config has it
    if [ ! -f "$DB_PATH" ]; then
        fail_test "parse_config" "Config not parsed, DB not created"
    else
        pass_test "parse_config"
    fi
}

test_process_create() {
    setup_lustre
    register_cl
    if [ -z "$CONSUMER_ID" ]; then
        fail_test "process_create" "Could not parse registered name"
        return
    fi
    touch "$LUSTRE_TEST_DIR_FP/testfile"
    sleep 2
    $DAEMON_BIN "$MDT_NAME" -D "$DB_PATH" -C "$CONSUMER_ID"
    if ! verify_db_record "SELECT operation_type FROM changelog WHERE name = 'testfile' AND full_path = '$LUSTRE_TEST_DIR'" "CREAT"; then
        fail_test "process_create" "Create operation not recorded"
    else
        pass_test "process_create"
    fi
}

test_process_delete() {
    setup_lustre
    register_cl
    if [ -z "$CONSUMER_ID" ]; then
        fail_test "process_delete" "Could not parse registered name"
        return
    fi
    touch "$LUSTRE_TEST_DIR_FP/deltest"
    sleep 2
    $DAEMON_BIN "$MDT_NAME" -D "$DB_PATH" -C "$CONSUMER_ID"
    rm "$LUSTRE_TEST_DIR_FP/deltest"
    sleep 2
    $DAEMON_BIN "$MDT_NAME" -D "$DB_PATH" -C "$CONSUMER_ID"
    if ! verify_db_record "SELECT operation_type FROM changelog WHERE name = 'deltest' and full_path = '$LUSTRE_TEST_DIR'" "UNLNK"; then
        fail_test "process_delete" "Delete operation not recorded"
    else
        pass_test "process_delete"
    fi
}

test_process_rename() {
    setup_lustre
    register_cl
    if [ -z "$CONSUMER_ID" ]; then
        fail_test "process_rename" "Could not parse registered name"
        return
    fi
    rm -f "$DB_PATH"  # Clear database to avoid old records
    touch "$LUSTRE_TEST_DIR_FP/oldname"
    sleep 2  # Increase sleep to ensure changelog generation
    $DAEMON_BIN "$MDT_NAME" -D "$DB_PATH" -C "$CONSUMER_ID" 
    mv "$LUSTRE_TEST_DIR_FP/oldname" "$LUSTRE_TEST_DIR_FP/newname"
    sleep 2
    $DAEMON_BIN "$MDT_NAME" -D "$DB_PATH" -C "$CONSUMER_ID"
    # Flexible client
    if ! verify_db_record "SELECT operation_type FROM changelog WHERE operation_type = 'RENME' AND name = 'newname' AND full_path = '$LUSTRE_TEST_DIR'" "RENME"; then
        fail_test "process_rename" "Rename operation not recorded correctly"
    else
        pass_test "process_rename"
    fi
}

test_daemon_mode() {
    setup_lustre
    register_cl
    if [ -z "$CONSUMER_ID" ]; then
        fail_test "daemon_mode" "Could not parse registered name"
        return
    fi
    $DAEMON_BIN "$MDT_NAME" -d -D "$DB_PATH" -C "$CONSUMER_ID" -R -m partial &
    local pid=$!
    sleep 2
    touch "$LUSTRE_TEST_DIR_FP/daemon_test"
    sleep 2
    kill -TERM $pid
    wait $pid
    if [ $? -ne 0 ]; then
        fail_test "daemon_mode" "Daemon did not handle signal properly"
        return
    fi
    $DAEMON_BIN "$MDT_NAME" -D "$DB_PATH" -C "$CONSUMER_ID"
    if ! verify_db_record "SELECT operation_type FROM changelog WHERE name = 'daemon_test' AND full_path = '$LUSTRE_TEST_DIR'" "CREAT"; then
        fail_test "daemon_mode" "Create operation not recorded"
    else
        pass_test "daemon_mode"
    fi
}

test_cache() {
    setup_lustre
    register_cl
    if [ -z "$CONSUMER_ID" ]; then
        fail_test "cache" "Could not parse registered name"
        return
    fi
    touch "$LUSTRE_TEST_DIR_FP/cachetest"
    sleep 2
    $DAEMON_BIN "$MDT_NAME" -D "$DB_PATH" -C "$CONSUMER_ID" -v 5 >> cache.log
    if ! grep -q "Adding to cache" cache.log; then
        fail_test "cache" "Cache not used/added"
    else
        pass_test "cache"
    fi
    rm -f cache.log
}

test_verbose_levels() {
    local output1=$($DAEMON_BIN -h -v)
    local output10=$($DAEMON_BIN -h -v 10)
    if [[ ${#output10} -le ${#output1} ]]; then
        fail_test "verbose_levels" "Higher verbose not producing more output"
    else
        pass_test "verbose_levels"
    fi
}

test_database_rotation() {
    setup_lustre
    register_cl
    if [ -z "$CONSUMER_ID" ]; then
        fail_test "database_rotation" "Could not parse registered name"
        return
    fi
    local db_template="${TEST_DIR}/sentinel_%02d.db"
    mkdir -p "$TEST_DIR" || {
        fail_test "database_rotation" "Failed to create test directory $TEST_DIR"
        return
    }
    rm -f "${TEST_DIR}/sentinel_*.db"  # Clean up previous databases
    lfs changelog_clear $MDT_NAME $CONSUMER_ID 0  # Clear changelog

    log "Running first lsentineld with 15s rotation"
    touch "$LUSTRE_TEST_DIR_FP/rotate_test1"
    rm -f "$LUSTRE_TEST_DIR_FP/rotate_test1"
    sleep 2
    $DAEMON_BIN "$MDT_NAME" -D "$db_template" -C "$CONSUMER_ID" -r 15s -v 10 > "${TEST_DIR}/rotation1.log" 2>&1
    if [ $? -ne 0 ]; then
        fail_test "database_rotation" "First run failed, check ${TEST_DIR}/rotation1.log:\n$(cat ${TEST_DIR}/rotation1.log)"
        return
    fi
    sleep 15

    log "Running second lsentineld with 15s rotation"
    touch "$LUSTRE_TEST_DIR_FP/rotate_test2"
    rm -f "$LUSTRE_TEST_DIR_FP/rotate_test2"
    sleep 2
    $DAEMON_BIN "$MDT_NAME" -D "$db_template" -C "$CONSUMER_ID" -r 15s -v 10 > "${TEST_DIR}/rotation2.log" 2>&1
    if [ $? -ne 0 ]; then
        fail_test "database_rotation" "Second run failed, check ${TEST_DIR}/rotation2.log:\n$(cat ${TEST_DIR}/rotation2.log)"
        return
    fi

    # Verify database files
    db_files=($(ls -1 ${TEST_DIR}/sentinel_*.db 2>/dev/null))
    if [ ${#db_files[@]} -ne 2 ]; then
        fail_test "database_rotation" "Expected exactly 2 database files, found ${#db_files[@]}: ${db_files[*]}"
        return
    fi

    # Dump databases for debugging
    for db in "${db_files[@]}"; do
        log "Dumping $db"
        $DUCKDB_CLI "$db" -c ".dump" > "${TEST_DIR}/$(basename $db .db).dump"
    done

    # Verify rotate_test1 in any database
    found_test1=0
    for db in "${db_files[@]}"; do
        count=$($DUCKDB_CLI "$db" -csv -c "SELECT COUNT(*) FROM changelog WHERE name = 'rotate_test1' AND full_path = '$LUSTRE_TEST_DIR' AND operation_type = 'CREAT'" | tail -n 1)
        if [ "$count" -eq 1 ]; then
            found_test1=1
            break
        fi
    done
    if [ $found_test1 -ne 1 ]; then
        fail_test "database_rotation" "rotate_test1 not found in any database, check ${TEST_DIR}/rotation1.log:\n$(cat ${TEST_DIR}/rotation1.log)"
        return
    fi

    # Verify rotate_test2 in any database
    found_test2=0
    for db in "${db_files[@]}"; do
        count=$($DUCKDB_CLI "$db" -csv -c "SELECT COUNT(*) FROM changelog WHERE name = 'rotate_test2' AND full_path = '$LUSTRE_TEST_DIR' AND operation_type = 'CREAT'" | tail -n 1)
        if [ "$count" -eq 1 ]; then
            found_test2=1
            break
        fi
    done
    if [ $found_test2 -ne 1 ]; then
        fail_test "database_rotation" "rotate_test2 not found in any database, check ${TEST_DIR}/rotation2.log:\n$(cat ${TEST_DIR}/rotation2.log)"
        return
    fi

    pass_test "database_rotation"
}

test_database_rotation() {
    setup_lustre
    register_cl
    if [ -z "$CONSUMER_ID" ]; then
        fail_test "database_rotation" "Could not parse registered name"
        return
    fi
    local db_template="${TEST_DIR}/sentinel_%02d.db"
    mkdir -p "$TEST_DIR" || {
        fail_test "database_rotation" "Failed to create test directory $TEST_DIR"
        return
    }
    rm -f "${TEST_DIR}/sentinel_*.db"  # Clean up previous databases
    lfs changelog_clear $MDT_NAME $CONSUMER_ID 0  # Clear changelog
    log "Creating rotate_test1"
    touch "$LUSTRE_TEST_DIR_FP/rotate_test1"
    sleep 2  # Increased sleep for changelog generation
    log "Running first lsentineld with 5s rotation"
    $DAEMON_BIN "$MDT_NAME" -D "$db_template" -C "$CONSUMER_ID" -r 5s -v 10 > "${TEST_DIR}/rotation1.log" 2>&1
    if [ $? -ne 0 ]; then
        fail_test "database_rotation" "First run failed, check ${TEST_DIR}/rotation1.log:\n$(cat ${TEST_DIR}/rotation1.log)"
        return
    fi
    sleep 10  # Increased sleep for rotation
    log "Dumping sentinel_00.db"
    $DUCKDB_CLI "${TEST_DIR}/sentinel_00.db" -c "SELECT * FROM changelog" > "${TEST_DIR}/sentinel_00.dump" 2>&1
    log "Creating rotate_test2"
    touch "$LUSTRE_TEST_DIR_FP/rotate_test2"
    sleep 2
    log "Running second lsentineld with 5s rotation"
    $DAEMON_BIN "$MDT_NAME" -D "$db_template" -C "$CONSUMER_ID" -r 5s -v 10 > "${TEST_DIR}/rotation2.log" 2>&1
    if [ $? -ne 0 ]; then
        fail_test "database_rotation" "Second run failed, check ${TEST_DIR}/rotation2.log:\n$(cat ${TEST_DIR}/rotation2.log)"
        return
    fi
    log "Dumping sentinel_01.db"
    $DUCKDB_CLI "${TEST_DIR}/sentinel_01.db" -c "SELECT * FROM changelog" > "${TEST_DIR}/sentinel_01.dump" 2>&1
    if [ ! -f "${TEST_DIR}/sentinel_00.db" ] || [ ! -f "${TEST_DIR}/sentinel_01.db" ]; then
        fail_test "database_rotation" "Database files not rotated correctly"
        return
    fi

    local count=$($DUCKDB_CLI "${TEST_DIR}/sentinel_00.db" -csv -c "SELECT COUNT(*) FROM changelog WHERE name = 'rotate_test1' AND operation_type = 'CREAT' AND full_path = '$LUSTRE_TEST_DIR'" | tail -n 1)
    if [ "$count" -ne 1 ]; then
        fail_test "database_rotation" "First rotation did not record initial operation (count=$count), check ${TEST_DIR}/sentinel_00.dump"
        return
    fi
    count=$($DUCKDB_CLI "${TEST_DIR}/sentinel_01.db" -csv -c "SELECT COUNT(*) FROM changelog WHERE name = 'rotate_test2' AND operation_type = 'CREAT' AND full_path = '$LUSTRE_TEST_DIR'" | tail -n 1)
    if [ "$count" -ne 1 ]; then
        fail_test "database_rotation" "Second rotation did not record subsequent operation (count=$count), check ${TEST_DIR}/sentinel_01.dump"
        return
    fi
    pass_test "database_rotation"
}

test_database_rotation_as_daemon() {
    setup_lustre
    register_cl
    if [ -z "$CONSUMER_ID" ]; then
        fail_test "database_rotation_as_daemon" "Could not parse registered name"
        return
    fi
    local db_template="${TEST_DIR}/sentinel_r2_%02d.db"
    mkdir -p "$TEST_DIR" || {
        fail_test "database_rotation_as_daemon" "Failed to create test directory $TEST_DIR"
        return
    }
    rm -f "${TEST_DIR}/sentinel_*.db"  # Clean up previous databases
    lfs changelog_clear $MDT_NAME $CONSUMER_ID 0

    log "Starting lsentineld in daemon mode with 15s rotation"
    $DAEMON_BIN "$MDT_NAME" -d -D "$db_template" -C "$CONSUMER_ID" -r 15s -v 10 > "${TEST_DIR}/rotation_r2.log" 2>&1 &
    local pid=$!
    sleep 2  # Allow daemon to start

    log "Creating rotate_r2_test1"
    touch "$LUSTRE_TEST_DIR_FP/rotate_r2_test1"
    sleep 15  # Wait for changelog processing and rotation

    log "Creating rotate_r2_test2"
    touch "$LUSTRE_TEST_DIR_FP/rotate_r2_test2"
    sleep 15  # Wait for changelog processing and rotation

    log "Terminating lsentineld"
    kill -TERM $pid
    wait $pid
    if [ $? -ne 0 ]; then
        fail_test "database_rotation_as_daemon" "Daemon did not terminate properly, check ${TEST_DIR}/rotation_r2.log"
        return
    fi

    # Verify database files
    db_files=($(ls -1 ${TEST_DIR}/sentinel_r2_*.db 2>/dev/null))
    if [ ${#db_files[@]} -ne 2 ]; then
        fail_test "database_rotation_as_daemon" "Expected exactly 2 database files, found ${#db_files[@]}: ${db_files[*]}"
        return
    fi

    # Dump databases for debugging
    for db in "${db_files[@]}"; do
        log "Dumping $db"
        $DUCKDB_CLI "$db" -c ".dump" > "${TEST_DIR}/$(basename $db .db).dump"
    done

    # Verify rotate_test1 in any database
    found_test1=0
    for db in "${db_files[@]}"; do
        count=$($DUCKDB_CLI "$db" -csv -c "SELECT COUNT(*) FROM changelog WHERE name = 'rotate_r2_test1' AND operation_type = 'CREAT' AND full_path = '$LUSTRE_TEST_DIR'" | tail -n 1)
        if [ "$count" -ne 0 ]; then
            found_test1=1
            break
        fi
    done
    if [ $found_test1 -ne 1 ]; then
        fail_test "database_rotation_as_daemon" "rotate_r2_test1 not found in any database"
        return
    fi

    # Verify rotate_test2 in any database
    found_test2=0
    for db in "${db_files[@]}"; do
        count=$($DUCKDB_CLI "$db" -csv -c "SELECT COUNT(*) FROM changelog WHERE name = 'rotate_r2_test2' AND operation_type = 'CREAT' AND full_path = '$LUSTRE_TEST_DIR'" | tail -n 1)
        if [ "$count" -ne 0 ]; then
            found_test2=1
            break
        fi
    done
    if [ $found_test2 -ne 1 ]; then
        fail_test "database_rotation_as_daemon" "rotate_r2_test2 not found in any database"
        return
    fi

    pass_test "database_rotation_as_daemon"
}

test_invalid_config() {
    generate_invalid_config
    # Run command and redirect output to a file to preserve exit status
    $DAEMON_BIN -c "${TEST_DIR}/invalid.conf" > "${TEST_DIR}/output.log" 2>&1
    local exit_status=$?
    # Read output from file
    local output=$(cat "${TEST_DIR}/output.log")
    if [ $exit_status -eq 0 ]; then
        fail_test "invalid_config" "Program did not fail on invalid configuration (exit status: $exit_status)"
        return
    fi
    if ! echo "$output" | grep -q "Invalid"; then
        fail_test "invalid_config" "No error message for invalid configuration"
        return
    fi
    pass_test "invalid_config"
    rm -f "${TEST_DIR}/output.log"  # Clean up
}

test_cache_timeout() {
    setup_lustre
    register_cl
    if [ -z "$CONSUMER_ID" ]; then
        fail_test "cache_timeout" "Could not parse registered name"
        return
    fi
    touch "$LUSTRE_TEST_DIR_FP/cache_timeout_test"
    sleep 2
    $DAEMON_BIN "$MDT_NAME" -D "$DB_PATH" -C "$CONSUMER_ID" -v 5 >> cache_timeout.log
    sleep 310
    touch "$LUSTRE_TEST_DIR_FP/cache_timeout_test"  # Trigger new changelog for same FID (e.g., MTIME)
    sleep 2
    $DAEMON_BIN "$MDT_NAME" -D "$DB_PATH" -C "$CONSUMER_ID" -v 5 >> cache_timeout.log
    if ! grep -q "Cache entry expired" cache_timeout.log; then
        fail_test "cache_timeout" "Cache timeout not detected"
    else
        pass_test "cache_timeout"
    fi
    rm -f cache_timeout.log
}

test_process_symlink() {
    setup_lustre
    register_cl
    if [ -z "$CONSUMER_ID" ]; then
        fail_test "process_symlink" "Could not parse registered name"
        return
    fi
    ln -s "$LUSTRE_TEST_DIR_FP/target" "$LUSTRE_TEST_DIR_FP/symlink_test"
    sleep 2
    $DAEMON_BIN "$MDT_NAME" -D "$DB_PATH" -C "$CONSUMER_ID"
    if ! verify_db_record "SELECT operation_type FROM changelog WHERE name = 'symlink_test' AND full_path = '$LUSTRE_TEST_DIR'" "SLINK"; then
        fail_test "process_symlink" "Symlink creation not recorded"
    else
        pass_test "process_symlink"
    fi
}

test_mount_point_failure() {
    $DAEMON_BIN "invalid-MDT0000" -D "$DB_PATH" > "${TEST_DIR}/output.log" 2>&1
    local exit_status=$?
    local output=$(cat "${TEST_DIR}/output.log")
    if [ $exit_status -eq 0 ]; then
        fail_test "mount_point_failure" "Program did not fail when mount point detection failed (exit status: $exit_status)"
        return
    fi
    if ! echo "$output" | grep -q "Cannot find mount point"; then
        fail_test "mount_point_failure" "No error message for mount point detection failure"
        return
    fi
    pass_test "mount_point_failure"
    rm -f "${TEST_DIR}/output.log"
}

test_sql_escape() {
    setup_lustre
    register_cl
    if [ -z "$CONSUMER_ID" ]; then
        fail_test "sql_escape" "Could not parse registered name"
        return
    fi
    touch "$LUSTRE_TEST_DIR_FP/test'file"
    sleep 2
    $DAEMON_BIN "$MDT_NAME" -D "$DB_PATH" -C "$CONSUMER_ID"
    if ! verify_db_record "SELECT name FROM changelog WHERE name = 'test''file' AND full_path = '$LUSTRE_TEST_DIR'" "test'file"; then
        fail_test "sql_escape" "SQL escaping of single quotes failed"
    else
        pass_test "sql_escape"
    fi
}




##
## sentinel client tests
##

test_client_fid() {
    setup_lustre
    register_cl
    if [ -z "$CONSUMER_ID" ]; then
        fail_test "client_fid" "Could not register consumer"
        return
    fi
    if [ ! -x "$CLIENT_BIN" ]; then
        fail_test "client_fid" "Client binary not found at $CLIENT_BIN"
        return
    fi
    rm -f "$DB_PATH"  # Clear database
    touch "$LUSTRE_TEST_DIR_FP/client_fid_test"
    sleep 2  # Increased sleep time
    log "Running lsentineld to process create operation"
    $DAEMON_BIN "$MDT_NAME" -D "$DB_PATH" -C "$CONSUMER_ID" -v 5 > "${TEST_DIR}/client_fid.log" 2>&1
    local fid=$($DUCKDB_CLI -line "$DB_PATH" -c "SELECT fid FROM files WHERE current_path = '$LUSTRE_TEST_DIR/client_fid_test'" | grep fid | sed -e 's/\s*fid = //g')
    if [ -z "$fid" ] || [[ ! "$fid" =~ ^\[0x ]]; then
        fail_test "client_fid" "Could not resolve FID for test file, check ${TEST_DIR}/client_fid.log $fid"
        return
    fi
    log "Executing: $CLIENT_BIN --db $DB_PATH -f csv $fid"
    local output=$("$CLIENT_BIN" --db "$DB_PATH" -f csv "$fid" 2>&1)
    if ! echo "$output" | grep -q "File Created.*client_fid_test"; then
        fail_test "client_fid" "FID client did not return expected create record, output: $output"
    else
        pass_test "client_fid"
    fi
}

test_client_path() {
    setup_lustre
    register_cl
    if [ -z "$CONSUMER_ID" ]; then
        fail_test "client_path" "Could not register consumer"
        return
    fi
    if [ ! -x "$CLIENT_BIN" ]; then
        fail_test "client_path" "Client binary not found at $CLIENT_BIN"
        return
    fi
    rm -f "$DB_PATH"  # Clear database
    local test_path="$LUSTRE_TEST_DIR_FP/client_path_test"
    mkdir -p "$(dirname "$test_path")"
    touch "$test_path"
    sleep 2
    log "Running lsentineld to process create operation"
    $DAEMON_BIN "$MDT_NAME" -D "$DB_PATH" -C "$CONSUMER_ID" -v 5 > "${TEST_DIR}/client_path.log" 2>&1
    log "Executing: $CLIENT_BIN -m $MOUNT_POINT --db $DB_PATH -f text $test_path"
    local output=$("$CLIENT_BIN" -m $MOUNT_POINT --db "$DB_PATH" -f text "$test_path" 2>&1)


    # Check for "File Created" instead of "CREAT" and correct path
    if ! echo "$output" | grep -q "Current Full Path:.*$LUSTRE_TEST_DIR/client_path_test" || ! echo "$output" | grep -q "File Created"; then
        fail_test "client_path" "Path client failed to resolve or extract history, output: $output"
    else
        # Skip normalization check if paths are already normalized
        pass_test "client_path"
    fi
}

test_invalid_search() {
    if [ ! -x "$CLIENT_BIN" ]; then
        fail_test "invalid_search" "Client binary not found at $CLIENT_BIN"
        return
    fi
    log "Executing: $CLIENT_BIN --db $DB_PATH invalid_fid"
    "$CLIENT_BIN" --db "$DB_PATH" invalid_fid 
    local exit_status=$?
    if [ $exit_status -eq 0 ]; then
        fail_test "invalid_search" "Did not handle invalid search term gracefully, output: $output"
    else
        pass_test "invalid_search"
    fi
}

test_output_formats() {
    local fid="[0x200000401:0x1:0x0]"  # Dummy FID; assume DB has records or mock
    # Generate text output as baseline
    local text_out=$("$CLIENT_BIN" --db "$DB_PATH" -f text "$fid")
    local record_count=$(echo "$text_out" | grep -c "CREAT\|OPEN")  # Count ops
    # Test JSON
    local json_out=$("$CLIENT_BIN" --db "$DB_PATH" -f json "$fid")
    if command -v jq >/dev/null; then
        local json_count=$(echo "$json_out" | jq '.records | length')
        if [ "$json_count" -ne "$record_count" ]; then
            fail_test "output_formats" "JSON record count mismatch: $json_count vs $record_count"
            return
        fi
    fi
    # Test YAML/CSV similarly (use yq for YAML if available, or grep for CSV)
    local csv_out=$("$CLIENT_BIN" --db "$DB_PATH" -f csv "$fid")
    local csv_lines=$(echo "$csv_out" | wc -l)
    if [ "$csv_lines" -le 1 ]; then  # Header + records
        fail_test "output_formats" "CSV output empty or malformed"
    else
        pass_test "output_formats"
    fi
    
}

test_mount_integration() {
    if [ "$EUID" -ne 0 ]; then
        skip_test "mount_integration" "Requires root for -m option"
        return
    fi
    setup_lustre
    # --- Start of changes ---
    if [ -z "$CONSUMER_ID" ]; then
        fail_test "mount_integration" "Could not register consumer"
        return
    fi
    if [ ! -x "$CLIENT_BIN" ]; then
        fail_test "mount_integration" "Client binary not found at $CLIENT_BIN"
        return
    fi
    rm -f "$DB_PATH"  # Clear database
    local test_file="$LUSTRE_TEST_DIR_FP/mount_test_file"
    touch "$test_file"
    sleep 1
    log "Running lsentineld to process create operation"
    $DAEMON_BIN "$MDT_NAME" -D "$DB_PATH" -C "$CONSUMER_ID" -v 5 > "${TEST_DIR}/mount_integration.log" 2>&1
    local fid=$($DUCKDB_CLI -line "$DB_PATH" -c "SELECT fid FROM files WHERE current_path = '$LUSTRE_TEST_DIR/mount_test_file'" | grep fid | sed -e 's/\s*fid = //g')
    if [ -z "$fid" ] || [[ ! "$fid" =~ ^\[0x ]]; then
        fail_test "mount_integration" "Could not resolve FID for test file, check ${TEST_DIR}/mount_integration.log $fid"
        return
    fi
    log "Executing: $CLIENT_BIN --db $DB_PATH -m $MOUNT_POINT -f csv $fid"
    local output=$("$CLIENT_BIN" --db "$DB_PATH" -m "$MOUNT_POINT" -f csv "$fid" 2>&1)
    if ! echo "$output" | grep -q "current_full_path,$MOUNT_POINT" && echo "$output" | grep -q "file_exists,false"; then
        fail_test "mount_integration" "Mount point not integrated or file existence check failed, output: $output"
    else
        pass_test "mount_integration"
    fi
    # --- End of changes ---
}

test_config_client() {
    if [ "$EUID" -ne 0 ]; then
        skip_test "config_client" "Requires root for mount operations"
        return
    fi
    setup_lustre
    register_cl
    if [ -z "$CONSUMER_ID" ]; then
        fail_test "config_client" "Could not register consumer"
        return
    fi
    if [ ! -x "$CLIENT_BIN" ]; then
        fail_test "config_client" "Client binary not found at $CLIENT_BIN"
        return
    fi
    rm -f "$DB_PATH"  # Clear database
    local test_file="$LUSTRE_TEST_DIR_FP/config_test_file"
    touch "$test_file"
    sleep 1
    log "Running lsentineld to process create operation"
    $DAEMON_BIN "$MDT_NAME" -D "$DB_PATH" -C "$CONSUMER_ID" -v 5 > "${TEST_DIR}/config_client.log" 2>&1
    local fid=$($DUCKDB_CLI -line "$DB_PATH" -c "SELECT fid FROM files WHERE current_path = '$LUSTRE_TEST_DIR/config_test_file'" | grep fid | sed -e 's/\s*fid = //g')
    if [ -z "$fid" ] || [[ ! "$fid" =~ ^\[0x ]]; then
        fail_test "config_client" "Could not resolve FID for test file, check ${TEST_DIR}/config_client.log $fid"
        return
    fi
    local config="${TEST_DIR}/client.conf"
    cat > "$config" <<EOF
db=$DB_PATH
format=json
method=attach
mount=$MOUNT_POINT
verbose=3
EOF
    log "Executing: $CLIENT_BIN -c $config $fid"
    local output=$("$CLIENT_BIN" -c "$config" "$fid" 2>&1)
    if ! echo "$output" | grep -q 'format: json' || ! echo "$output" | grep -q "Attach query"; then
        fail_test "config_client" "Config options not applied (format/method)"
    else
        pass_test "config_client"
    fi
}

test_multi_db_client() {
    if [ "$EUID" -ne 0 ]; then
        skip_test "multi_db_client" "Requires root for mount operations"
        return
    fi
    setup_lustre
    register_cl
    if [ -z "$CONSUMER_ID" ]; then
        fail_test "multi_db_client" "Could not register consumer"
        return
    fi
    if [ ! -x "$CLIENT_BIN" ]; then
        fail_test "multi_db_client" "Client binary not found at $CLIENT_BIN"
        return
    fi
    local db_template="${TEST_DIR}/multi_%02d.db"
    rm -f "${TEST_DIR}/multi_*.db"  # Clean up previous databases
    lfs changelog_clear $MDT_NAME $CONSUMER_ID 0  # Clear changelog
    log "Creating multi_test1"
    touch "$LUSTRE_TEST_DIR_FP/multi_test1"
    sleep 2
    log "Running lsentineld with 5s rotation for first file"
    $DAEMON_BIN "$MDT_NAME" -D "$db_template" -C "$CONSUMER_ID" -r 5s -v 10 > "${TEST_DIR}/multi_db1.log" 2>&1
    if [ $? -ne 0 ]; then
        fail_test "multi_db_client" "First run failed, check ${TEST_DIR}/multi_db1.log:\n$(cat ${TEST_DIR}/multi_db1.log)"
        return
    fi
    sleep 10
    log "Creating multi_test2"
    touch "$LUSTRE_TEST_DIR_FP/multi_test2"
    sleep 2
    log "Running lsentineld with 5s rotation for second file"
    $DAEMON_BIN "$MDT_NAME" -D "$db_template" -C "$CONSUMER_ID" -r 5s -v 10 > "${TEST_DIR}/multi_db2.log" 2>&1
    if [ $? -ne 0 ]; then
        fail_test "multi_db_client" "Second run failed, check ${TEST_DIR}/multi_db2.log:\n$(cat ${TEST_DIR}/multi_db2.log)"
        return
    fi
    sleep 10
    local fid=$($DUCKDB_CLI -line "${TEST_DIR}/multi_00.db" -c "SELECT fid FROM files WHERE current_path = '$LUSTRE_TEST_DIR/multi_test1'" | grep fid | sed -e 's/\s*fid = //g')
    if [ -z "$fid" ] || [[ ! "$fid" =~ ^\[0x ]]; then
        fail_test "multi_db_client" "Could not resolve FID for multi_test1, check ${TEST_DIR}/multi_db1.log"
        return
    fi
    log "Executing: $CLIENT_BIN --db $db_template -f csv $fid"
    local output=$("$CLIENT_BIN" --db "$db_template" -f csv "$fid" 2>&1)
    local total_records=$(echo "$output" | grep -c "File Created.*multi_test1")
    if [ "$total_records" -ge 1 ]; then
        pass_test "multi_db_client"
    else
        fail_test "multi_db_client" "Did not aggregate records from multiple DBs"
    fi
}

test_edge_utf8_path() {
    setup_lustre
    register_cl
    if [ -z "$CONSUMER_ID" ]; then
        fail_test "edge_utf8_path" "Could not parse registered name"
        return
    fi
    local utf8_path="$LUSTRE_TEST_DIR_FP/test_π_file"
    touch "$utf8_path"
    sleep 2
    $DAEMON_BIN "$MDT_NAME" -D "$DB_PATH" -C "$CONSUMER_ID"
    local output=$("$CLIENT_BIN" -m "$MOUNT_POINT" --db "$DB_PATH" -f text "$utf8_path")
    if echo "$output" | grep -q "π"; then
        pass_test "edge_utf8_path"
    else
        fail_test "edge_utf8_path" "UTF-8 path not handled correctly"
    fi
}

test_verbose_client() {
    local out_v1=$("$CLIENT_BIN" --db "$DB_PATH" -m "$MOUNT_POINT" -v 1 "[0x200000401:0x1:0x0]" 2>&1)
    local out_v5=$("$CLIENT_BIN" --db "$DB_PATH" -m "$MOUNT_POINT" -v 5 "[0x200000401:0x1:0x0]" 2>&1)
    local lines_v1=$(echo "$out_v1" | wc -l)
    local lines_v5=$(echo "$out_v5" | wc -l)
    if [ "$lines_v5" -le "$lines_v1" ]; then
        fail_test "verbose_client" "Higher verbosity did not produce more logs (v1: $lines_v1 lines, v5: $lines_v5 lines)"
    else
        pass_test "verbose_client"
    fi
}

test_summary_accuracy() {
    setup_lustre
    register_cl
    if [ -z "$CONSUMER_ID" ]; then
        fail_test "summary_accuracy" "Could not register consumer"
        return
    fi
    if [ ! -x "$CLIENT_BIN" ]; then
        fail_test "summary_accuracy" "Client binary not found at $CLIENT_BIN"
        return
    fi
    rm -f "$DB_PATH"  # Clear database
    local test_file="$LUSTRE_TEST_DIR_FP/summary_test"
    touch "$test_file"
    sleep 2
    log "Running lsentineld to process create operation"
    $DAEMON_BIN "$MDT_NAME" -D "$DB_PATH" -C "$CONSUMER_ID" -v 5 > "${TEST_DIR}/summary.log" 2>&1
    log "Executing: $CLIENT_BIN --db $DB_PATH -m $MOUNT_POINT -f csv $test_file"
    local output=$("$CLIENT_BIN" --db "$DB_PATH" -m "$MOUNT_POINT" -f csv "$test_file" 2>&1)
    if echo "$output" | grep -q "file_exists,true" && echo "$output" | grep -q "file_name_changed,false"; then
        mv "$test_file" "$LUSTRE_TEST_DIR_FP/summary_test_new"
        sleep 2
        $DAEMON_BIN "$MDT_NAME" -D "$DB_PATH" -C "$CONSUMER_ID" -v 5 >> "${TEST_DIR}/summary.log" 2>&1
        log "Executing: $CLIENT_BIN --db $DB_PATH -m $MOUNT_POINT -f csv $LUSTRE_TEST_DIR/summary_test_new"
        local new_out=$("$CLIENT_BIN" --db "$DB_PATH" -m "$MOUNT_POINT" -f csv "$LUSTRE_TEST_DIR/summary_test_new" 2>&1)
        if echo "$new_out" | grep -q "file_name_changed,true"; then
            pass_test "summary_accuracy"
        else
            fail_test "summary_accuracy" "Summary did not detect name/parent changes"
        fi
    else
        fail_test "summary_accuracy" "Initial summary inaccurate"
    fi
}


test_collect_db_files_glob() {
    setup_lustre
    register_cl
    if [ -z "$CONSUMER_ID" ]; then
        fail_test "collect_db_files_glob" "Could not register consumer"
        return
    fi
    if [ ! -x "$CLIENT_BIN" ]; then
        fail_test "collect_db_files_glob" "Client binary not found at $CLIENT_BIN"
        return
    fi
    local db_glob="${TEST_DIR}/*.db"
    rm -vf "${TEST_DIR}/*.db"  # Clean up previous databases
    # Create test database files with valid content
    create_test_db "${TEST_DIR}/test1.db" || {
        fail_test "collect_db_files_glob" "Failed to create test1.db"
        return
    }
    create_test_db "${TEST_DIR}/test2.db" || {
        fail_test "collect_db_files_glob" "Failed to create test2.db"
        return
    }
    touch "${TEST_DIR}/other.txt"  # Non-matching file
    sleep 2
    local temp_output="${TEST_DIR}/collect_db_files_glob.out"
    log "Executing: $CLIENT_BIN --db $db_glob -f csv [0x200000401:0x1:0x0]"
    "$CLIENT_BIN" --db "$db_glob" -f csv "[0x200000401:0x1:0x0]" -v 10 > "$temp_output" 2>&1
    local exit_status=$?
    local output=$(cat "$temp_output")
    rm -f "$temp_output"
    if [ $exit_status -ne 0 ]; then
        fail_test "collect_db_files_glob" "Command failed with exit status $exit_status"
        return
    fi
    if ! echo "$output" | grep -q "Collected [0-9]* database files"; then
        fail_test "collect_db_files_glob" "Did not collect expected database files with glob $db_glob"
        return
    fi
    # Verify specific files were collected
    if ! echo "$output" | grep -q "Added file: ${TEST_DIR}/test1.db" || ! echo "$output" | grep -q "Added file: ${TEST_DIR}/test2.db"; then
        fail_test "collect_db_files_glob" "Did not collect correct files test1.db and test2.db"
        return
    fi
    if echo "$output" | grep -q "Added file: ${TEST_DIR}/other.txt"; then
        fail_test "collect_db_files_glob" "Incorrectly collected non-matching file other.txt"
        return
    fi
    # Verify that the query succeeded (indicating valid DBs)
    if ! echo "$output" | grep -q "File Created.*testfile"; then
        fail_test "collect_db_files_glob" "Query failed to return expected records"
        return
    fi
    pass_test "collect_db_files_glob"
}

test_collect_db_files_template() {
    setup_lustre
    register_cl
    if [ -z "$CONSUMER_ID" ]; then
        fail_test "collect_db_files_template" "Could not register consumer"
        return
    fi
    if [ ! -x "$CLIENT_BIN" ]; then
        fail_test "collect_db_files_template" "Client binary not found at $CLIENT_BIN"
        return
    fi
    local db_template="${TEST_DIR}/sentinel_%02d.db"
    rm -f "${TEST_DIR}/sentinel_*.db"  # Clean up previous databases
    # Create test database files with valid content
    create_test_db "${TEST_DIR}/sentinel_00.db" || {
        fail_test "collect_db_files_template" "Failed to create sentinel_00.db"
        return
    }
    create_test_db "${TEST_DIR}/sentinel_01.db" || {
        fail_test "collect_db_files_template" "Failed to create sentinel_01.db"
        return
    }
    touch "${TEST_DIR}/test.db"  # Non-matching file
    sleep 2
    local temp_output="${TEST_DIR}/collect_db_files_template.out"
    log "Executing: $CLIENT_BIN --db $db_template -f csv [0x200000401:0x1:0x0]"
    "$CLIENT_BIN" --db "$db_template" -f csv "[0x200000401:0x1:0x0]" -v 10 > "$temp_output" 2>&1
    local exit_status=$?
    local output=$(cat "$temp_output")
    rm -f "$temp_output"
    if [ $exit_status -ne 0 ]; then
        fail_test "collect_db_files_template" "Command failed with exit status $exit_status"
        return
    fi
    if ! echo "$output" | grep -q "Collected [1-2] database files"; then
        fail_test "collect_db_files_template" "Did not collect expected database files with template $db_template"
        return
    fi
    # Verify specific files were collected
    if ! echo "$output" | grep -q "Added file: ${TEST_DIR}/sentinel_00.db" || ! echo "$output" | grep -q "Added file: ${TEST_DIR}/sentinel_01.db"; then
        fail_test "collect_db_files_template" "Did not collect correct files sentinel_00.db and sentinel_01.db"
        return
    fi
    if echo "$output" | grep -q "Added file: ${TEST_DIR}/test.db"; then
        fail_test "collect_db_files_template" "Incorrectly collected non-matching file test.db"
        return
    fi
    # Verify that the query succeeded
    if ! echo "$output" | grep -q "File Created.*testfile"; then
        fail_test "collect_db_files_template" "Query failed to return expected records"
        return
    fi
    pass_test "collect_db_files_template"
}

test_collect_db_files_explicit() {
    setup_lustre
    register_cl
    if [ -z "$CONSUMER_ID" ]; then
        fail_test "collect_db_files_explicit" "Could not register consumer"
        return
    fi
    if [ ! -x "$CLIENT_BIN" ]; then
        fail_test "collect_db_files_explicit" "Client binary not found at $CLIENT_BIN"
        return
    fi
    local db_files="${TEST_DIR}/explicit1.db ${TEST_DIR}/explicit2.db"
    rm -f "${TEST_DIR}/explicit*.db"  # Clean up previous databases
    # Create test database files with valid content
    create_test_db "${TEST_DIR}/explicit1.db" || {
        fail_test "collect_db_files_explicit" "Failed to create explicit1.db"
        return
    }
    create_test_db "${TEST_DIR}/explicit2.db" || {
        fail_test "collect_db_files_explicit" "Failed to create explicit2.db"
        return
    }
    touch "${TEST_DIR}/other.db"  # Non-matching file
    sleep 2
    local temp_output="${TEST_DIR}/collect_db_files_explicit.out"
    log "Executing: $CLIENT_BIN --db \"$db_files\" -f csv [0x200000401:0x1:0x0]"
    "$CLIENT_BIN" --db "$db_files" -f csv "[0x200000401:0x1:0x0]" -v 10 > "$temp_output" 2>&1
    local exit_status=$?
    local output=$(cat "$temp_output")
    rm -f "$temp_output"
    if [ $exit_status -ne 0 ]; then
        fail_test "collect_db_files_explicit" "Command failed with exit status $exit_status"
        return
    fi
    if ! echo "$output" | grep -q "Collected 2 database files"; then
        fail_test "collect_db_files_explicit" "Did not collect exactly 2 database files with explicit list"
        return
    fi
    # Verify specific files were collected
    if ! echo "$output" | grep -q "Added file: ${TEST_DIR}/explicit1.db" || ! echo "$output" | grep -q "Added file: ${TEST_DIR}/explicit2.db"; then
        fail_test "collect_db_files_explicit" "Did not collect correct files explicit1.db and explicit2.db"
        return
    fi
    if echo "$output" | grep -q "Added file: ${TEST_DIR}/other.db"; then
        fail_test "collect_db_files_explicit" "Incorrectly collected non-matching file other.db"
        return
    fi
    # Verify that the query succeeded
    if ! echo "$output" | grep -q "File Created.*testfile"; then
        fail_test "collect_db_files_explicit" "Query failed to return expected records"
        return
    fi
    pass_test "collect_db_files_explicit"
}

test_collect_db_files_invalid_dir() {
    setup_lustre
    register_cl
    if [ -z "$CONSUMER_ID" ]; then
        fail_test "collect_db_files_invalid_dir" "Could not register consumer"
        return
    fi
    if [ ! -x "$CLIENT_BIN" ]; then
        fail_test "collect_db_files_invalid_dir" "Client binary not found at $CLIENT_BIN"
        return
    fi
    local invalid_dir="/nonexistent_dir_$(date +%s)"
    local temp_output="${TEST_DIR}/collect_db_files_invalid_dir.out"
    log "Executing: $CLIENT_BIN --db $invalid_dir/*.db -f text [0x200000401:0x1:0x0]"
    "$CLIENT_BIN" --db "$invalid_dir/*.db" -f text "[0x200000401:0x1:0x0]" -v 10 > "$temp_output" 2>&1
    local exit_status=$?
    local output=$(cat "$temp_output")
    rm -f "$temp_output"
    if [ $exit_status -eq 0 ]; then
        fail_test "collect_db_files_invalid_dir" "Did not fail on invalid directory $invalid_dir"
        return
    fi
    if ! echo "$output" | grep -q "Invalid directory for glob pattern: $invalid_dir"; then
        fail_test "collect_db_files_invalid_dir" "No error message for invalid directory"
        return
    fi
    pass_test "collect_db_files_invalid_dir"
}

test_collect_db_files_no_match() {
    setup_lustre
    register_cl
    if [ -z "$CONSUMER_ID" ]; then
        fail_test "collect_db_files_no_match" "Could not register consumer"
        return
    fi
    if [ ! -x "$CLIENT_BIN" ]; then
        fail_test "collect_db_files_no_match" "Client binary not found at $CLIENT_BIN"
        return
    fi
    local db_glob="${TEST_DIR}/*.nodb"
    rm -f "${TEST_DIR}/*.db" "${TEST_DIR}/*.nodb"  # Clean up
    touch "${TEST_DIR}/other.txt"  # Create non-matching file
    local temp_output="${TEST_DIR}/collect_db_files_no_match.out"
    log "Executing: $CLIENT_BIN --db $db_glob -f csv [0x200000401:0x1:0x0]"
    "$CLIENT_BIN" --db "$db_glob" -f csv "[0x200000401:0x1:0x0]" -v 10 > "$temp_output" 2>&1
    local exit_status=$?
    local output=$(cat "$temp_output")
    rm -f "$temp_output"
    if [ $exit_status -eq 0 ]; then
        fail_test "collect_db_files_no_match" "Did not fail when no files matched glob $db_glob"
        return
    fi
    if ! echo "$output" | grep -q "No database files found"; then
        fail_test "collect_db_files_no_match" "No error message for no matching files"
        return
    fi
    pass_test "collect_db_files_no_match"
}

test_output_file_success() {
    setup_lustre
    register_cl
    if [ -z "$CONSUMER_ID" ]; then
        fail_test "output_file_success" "Could not register consumer"
        return
    fi
    if [ ! -x "$CLIENT_BIN" ]; then
        fail_test "output_file_success" "Client binary not found at $CLIENT_BIN"
        return
    fi
    rm -f "$DB_PATH"  # Clear database
    touch "$LUSTRE_TEST_DIR_FP/output_test_file"
    sleep 2
    $DAEMON_BIN "$MDT_NAME" -D "$DB_PATH" -C "$CONSUMER_ID"  # Process to create records
    local fid=$($DUCKDB_CLI -line "$DB_PATH" -c "SELECT fid FROM files WHERE current_path LIKE '%output_test_file'" | grep fid | sed -e 's/\s*fid = //g' | head -n1 )
    if [ -z "$fid" ] || [[ ! "$fid" =~ ^\[0x ]]; then
        fail_test "output_file_success" "Could not resolve FID for test file"
        return
    fi
    local output_file="${TEST_DIR}/output_success.txt"
    rm -f "$output_file"  # Ensure clean
    local stdout_check="${TEST_DIR}/stdout_check.txt"
    local expected_output="File Created.*output_test_file"  # Pattern for expected record
    log "Executing: $CLIENT_BIN --db $DB_PATH -f csv $fid -o $output_file"
    "$CLIENT_BIN" --db "$DB_PATH" -f csv "$fid" -o "$output_file" > "$stdout_check" 2>&1
    local exit_status=$?
    if [ $exit_status -ne 0 ]; then
        fail_test "output_file_success" "Command failed with exit status $exit_status"
        return
    fi
    if [ -s "$stdout_check" ]; then
        fail_test "output_file_success" "Unexpected output on stdout: $(cat $stdout_check)"
        return
    fi
    if [ ! -f "$output_file" ] || [ ! -s "$output_file" ]; then
        fail_test "output_file_success" "Output file not created or empty"
        return
    fi
    if ! grep -q "$expected_output" "$output_file"; then
        fail_test "output_file_success" "Expected content not in output file: $(head -5 $output_file)"
        return
    fi
    log "Verified output file content: $(grep "$expected_output" "$output_file")"
    pass_test "output_file_success"
    rm -f "$output_file" "$stdout_check"
}

test_output_file_failure() {
    setup_lustre
    register_cl
    if [ -z "$CONSUMER_ID" ]; then
        fail_test "output_file_failure" "Could not register consumer"
        return
    fi
    if [ ! -x "$CLIENT_BIN" ]; then
        fail_test "output_file_failure" "Client binary not found at $CLIENT_BIN"
        return
    fi
    rm -f "$DB_PATH"  # Clear database
    touch "$LUSTRE_TEST_DIR_FP/output_failure_file"
    sleep 2
    $DAEMON_BIN "$MDT_NAME" -D "$DB_PATH" -C "$CONSUMER_ID"
    local fid=$($DUCKDB_CLI -line "$DB_PATH" -c "SELECT fid FROM files WHERE current_path LIKE '%output_failure_file'" | grep fid | sed -e 's/\s*fid = //g' | head -n1)
    if [ -z "$fid" ] || [[ ! "$fid" =~ ^\[0x ]]; then
        fail_test "output_file_failure" "Could not resolve FID for test file"
        return
    fi
    local invalid_output_file="/root/.lsentinel_testing_please_remove_unwritable_output.$(date +%s).txt"
    local stderr_check="${TEST_DIR}/stderr_check.txt"
    local expected_error="Failed to open output file"
    local immutable_set=0

    # If user is root, create immutable file to force write failure
    if [ "$(id -u)" -eq 0 ]; then
        touch "$invalid_output_file"
        chattr +i "$invalid_output_file" || {
            fail_test "output_file_failure" "Failed to set immutable attribute on $invalid_output_file"
            return
        }
        immutable_set=1
    fi

    # Verify file size is zero (or file doesn't exist for non-root)
    local file_size
    if [ -f "$invalid_output_file" ]; then
        file_size=$(stat -c %s "$invalid_output_file" 2>/dev/null || echo -1)
        if [ "$file_size" -ne 0 ]; then
            if [ $immutable_set -eq 1 ]; then
                chattr -i "$invalid_output_file"  # Clean up immutable attribute
            fi
            fail_test "output_file_failure" "Initial file size of $invalid_output_file is $file_size, expected 0"
            rm -f "$invalid_output_file" "$stderr_check"
            return
        fi
    fi

    log "Executing with invalid output: $CLIENT_BIN --db $DB_PATH -f text $fid -o $invalid_output_file"
    "$CLIENT_BIN" --db "$DB_PATH" -f text "$fid" -o "$invalid_output_file" > /dev/null 2>"$stderr_check"
    local exit_status=$?

    # Verify file size remains zero (or file doesn't exist for non-root)
    if [ -f "$invalid_output_file" ]; then
        file_size=$(stat -c %s "$invalid_output_file" 2>/dev/null || echo -1)
        if [ "$file_size" -ne 0 ]; then
            if [ $immutable_set -eq 1 ]; then
                chattr -i "$invalid_output_file"  # Clean up immutable attribute
            fi
            fail_test "output_file_failure" "File size of $invalid_output_file is $file_size after write attempt, expected 0"
            rm -f "$invalid_output_file" "$stderr_check"
            return
        fi
    fi

    if [ $exit_status -eq 0 ]; then
        if [ $immutable_set -eq 1 ]; then
            chattr -i "$invalid_output_file"  # Clean up immutable attribute
        fi
        fail_test "output_file_failure" "Command succeeded unexpectedly (exit status 0)"
        rm -f "$invalid_output_file" "$stderr_check"
        return
    fi
    if ! grep -q "$expected_error" "$stderr_check"; then
        if [ $immutable_set -eq 1 ]; then
            chattr -i "$invalid_output_file"  # Clean up immutable attribute
        fi
        fail_test "output_file_failure" "Expected error not reported: $(cat $stderr_check)"
        rm -f "$invalid_output_file" "$stderr_check"
        return
    fi

    # For root, verify file still exists; for non-root, it's okay if it doesn't
    if [ $immutable_set -eq 1 ] && [ ! -f "$invalid_output_file" ]; then
        chattr -i "$invalid_output_file"  # Clean up immutable attribute
        fail_test "output_file_failure" "Invalid output file was unexpectedly removed"
        rm -f "$stderr_check"
        return
    fi

    # Clean up: Remove immutable attribute (if set) and test files
    if [ $immutable_set -eq 1 ]; then
        chattr -i "$invalid_output_file" || {
            fail_test "output_file_failure" "Failed to remove immutable attribute from $invalid_output_file"
            rm -f "$stderr_check"
            return
        }
    fi
    cat "$invalid_output_file"
    pass_test "output_file_failure"
}

test_default_db_collection() {
    setup_lustre
    register_cl
    if [ -z "$CONSUMER_ID" ]; then
        fail_test "default_db_collection" "Could not register consumer"
        return
    fi
    if [ ! -x "$CLIENT_BIN" ]; then
        fail_test "default_db_collection" "Client binary not found at $CLIENT_BIN"
        return
    fi
    local test_db_dir="${TEST_DIR}/default_dbs"
    mkdir -p "$test_db_dir"
    rm -f "${test_db_dir}/*.db"  # Clean up
    # Create multiple test DBs in directory (simulates default collection)
    create_test_db "${test_db_dir}/sentinel1.db" || { fail_test "default_db_collection" "Failed to create sentinel1.db"; return; }
    create_test_db "${test_db_dir}/sentinel2.db" || { fail_test "default_db_collection" "Failed to create sentinel2.db"; return; }
    touch "${test_db_dir}/non_db.txt"  # Non-DB file
    local temp_output="${TEST_DIR}/default_db_collection.out"
    local fid="[0x200000401:0x1:0x0]"  # From test DB
    log "Executing without --db (defaults to current dir, but simulate with dir): $CLIENT_BIN --db $test_db_dir -f csv $fid"
    # Note: To test pure default, cd to $test_db_dir or adjust; here we pass dir to simulate db_path="."
    "$CLIENT_BIN" --db "$test_db_dir" -f csv "$fid" -v 10 > "$temp_output" 2>&1
    local exit_status=$?
    local output=$(cat "$temp_output")
    rm -f "$temp_output"
    if [ $exit_status -ne 0 ]; then
        fail_test "default_db_collection" "Command failed with exit status $exit_status"
        return
    fi
    if ! echo "$output" | grep -q "Collected [1-9] database files"; then  # At least some DBs collected
        fail_test "default_db_collection" "Did not collect database files from directory $test_db_dir"
        return
    fi
    if ! echo "$output" | grep -q "Added file: ${test_db_dir}/sentinel1.db" || ! echo "$output" | grep -q "Added file: ${test_db_dir}/sentinel2.db"; then
        fail_test "default_db_collection" "Did not collect expected DB files from directory"
        return
    fi
    if echo "$output" | grep -q "Added file: ${test_db_dir}/non_db.txt"; then
        fail_test "default_db_collection" "Collected non-DB file"
        return
    fi
    if ! echo "$output" | grep -q "File Created.*testfile"; then
        fail_test "default_db_collection" "Query failed to return expected records from collected DBs"
        return
    fi
    pass_test "default_db_collection"
    rm -rf "$test_db_dir"
}

test_query_method_loop() {
    setup_lustre
    register_cl
    if [ -z "$CONSUMER_ID" ]; then
        fail_test "query_method_loop" "Could not register consumer"
        return
    fi
    if [ ! -x "$CLIENT_BIN" ]; then
        fail_test "query_method_loop" "Client binary not found at $CLIENT_BIN"
        return
    fi
    rm -f "$DB_PATH"
    touch "$LUSTRE_TEST_DIR_FP/method_loop_test"
    sleep 2
    $DAEMON_BIN "$MDT_NAME" -D "$DB_PATH" -C "$CONSUMER_ID"
    local fid=$($DUCKDB_CLI -line "$DB_PATH" -c "SELECT fid FROM files WHERE current_path LIKE '%method_loop_test'" | grep fid | sed -e 's/\s*fid = //g' | head -n1)
    if [ -z "$fid" ] || [[ ! "$fid" =~ ^\[0x ]]; then
        fail_test "query_method_loop" "Could not resolve FID for test file"
        return
    fi
    local temp_output="${TEST_DIR}/method_loop.out"
    log "Executing with --method loop: $CLIENT_BIN --db $DB_PATH --method loop -f csv $fid -v 10"
    "$CLIENT_BIN" --db "$DB_PATH" --method loop -f csv "$fid" -v 10 &> "$temp_output"
    local exit_status=$?
    local output=$(cat "$temp_output")
    rm -f "$temp_output"
    if [ $exit_status -ne 0 ]; then
        fail_test "query_method_loop" "Loop method failed with exit status $exit_status"
        return
    fi
    if ! echo "$output" | grep "Querying with method: loop"; then  # Assume verbose logs mention method
        fail_test "query_method_loop" "Loop method not used (check verbose logs)"
        return
    fi
    if ! echo "$output" | grep -q "File Created.*method_loop_test"; then
        fail_test "query_method_loop" "Expected record not found with loop method"
        return
    fi
    pass_test "query_method_loop"
}

test_query_method_attach() {
    setup_lustre
    register_cl
    if [ -z "$CONSUMER_ID" ]; then
        fail_test "query_method_attach" "Could not register consumer"
        return
    fi
    if [ ! -x "$CLIENT_BIN" ]; then
        fail_test "query_method_attach" "Client binary not found at $CLIENT_BIN"
        return
    fi
    rm -f "$DB_PATH"
    touch "$LUSTRE_TEST_DIR_FP/method_attach_test"
    sleep 2
    $DAEMON_BIN "$MDT_NAME" -D "$DB_PATH" -C "$CONSUMER_ID"
    local fid=$($DUCKDB_CLI -line "$DB_PATH" -c "SELECT fid FROM files WHERE current_path LIKE '%method_attach_test'" | grep fid | sed -e 's/\s*fid = //g' | head -n1)
    if [ -z "$fid" ] || [[ ! "$fid" =~ ^\[0x ]]; then
        fail_test "query_method_attach" "Could not resolve FID for test file"
        return
    fi
    local temp_output="${TEST_DIR}/method_attach.out"
    log "Executing with --method attach: $CLIENT_BIN --db $DB_PATH --method attach -f csv $fid -v 10"
    "$CLIENT_BIN" --db "$DB_PATH" --method attach -f csv "$fid" -v 8 > "$temp_output" 2>&1
    local exit_status=$?
    local output=$(cat "$temp_output")
    rm -f "$temp_output"
    if [ $exit_status -ne 0 ]; then
        fail_test "query_method_attach" "Attach method failed with exit status $exit_status"
        return
    fi
    if ! echo "$output" | grep "Querying with method: attach"; then  # Assume verbose logs mention method
        fail_test "query_method_attach" "Attach method not used (check verbose logs)"
        return
    fi
    if ! echo "$output" | grep -q "File Created.*method_attach_test"; then
        fail_test "query_method_attach" "Expected record not found with attach method"
        return
    fi
    pass_test "query_method_attach"
}

test_query_method_invalid() {
    setup_lustre
    register_cl
    if [ -z "$CONSUMER_ID" ]; then
        fail_test "query_method_invalid" "Could not register consumer"
        return
    fi
    if [ ! -x "$CLIENT_BIN" ]; then
        fail_test "query_method_invalid" "Client binary not found at $CLIENT_BIN"
        return
    fi
    rm -f "$DB_PATH"
    touch "$LUSTRE_TEST_DIR_FP/method_invalid_test"
    sleep 2
    $DAEMON_BIN "$MDT_NAME" -D "$DB_PATH" -C "$CONSUMER_ID"
    local fid=$($DUCKDB_CLI -line "$DB_PATH" -c "SELECT fid FROM files WHERE current_path LIKE '%method_invalid_test'" | grep fid | sed -e 's/\s*fid = //g')
    if [ -z "$fid" ] || [[ ! "$fid" =~ ^\[0x ]]; then
        fail_test "query_method_invalid" "Could not resolve FID for test file"
        return
    fi
    local temp_output="${TEST_DIR}/method_invalid.out"
    local expected_error="Invalid method: invalid"
    log "Executing with invalid method: $CLIENT_BIN --db $DB_PATH --method invalid -f text $fid"
    "$CLIENT_BIN" --db "$DB_PATH" --method invalid -f text "$fid" > /dev/null 2>"$temp_output"
    local exit_status=$?
    local output=$(cat "$temp_output")
    rm -f "$temp_output"
    if [ $exit_status -eq 0 ]; then
        fail_test "query_method_invalid" "Invalid method succeeded unexpectedly"
        return
    fi
    if ! echo "$output" | grep -q "$expected_error"; then
        fail_test "query_method_invalid" "Expected error not reported for invalid method"
        return
    fi
    pass_test "query_method_invalid"
}

test_verbose_repeated() {
    if [ ! -x "$CLIENT_BIN" ]; then
        fail_test "verbose_repeated" "Client binary not found at $CLIENT_BIN"
        return
    fi
    # Test repeated -v flags; last value (5) should win
    local temp_output="${TEST_DIR}/verbose_repeated_v5.out"
    log "Executing with repeated -v: $CLIENT_BIN -v 1 -v 5 -h"
    "$CLIENT_BIN" -v 1 -v 5 -h > "$temp_output" 2>&1
    local output_v5=$(cat "$temp_output")
    local line_count_v5=$(echo "$output_v5" | wc -l)
    rm -f "$temp_output"

    # Compare with -v 1 to ensure higher verbosity produces more output
    temp_output="${TEST_DIR}/verbose_repeated_v1.out"
    log "Executing with -v 1: $CLIENT_BIN -v 1 -h"
    "$CLIENT_BIN" -v 1 -h > "$temp_output" 2>&1
    local output_v1=$(cat "$temp_output")
    local line_count_v1=$(echo "$output_v1" | wc -l)
    rm -f "$temp_output"

    # Check if verbosity 5 produces more output than verbosity 1
    if [ "$line_count_v5" -le "$line_count_v1" ]; then
        fail_test "verbose_repeated" "Verbosity 5 did not produce more output than verbosity 1 (v5: $line_count_v5 lines, v1: $line_count_v1 lines)"
        return
    fi

    # Test invalid verbosity value (e.g., 15) is clamped to valid range (assume 10)
    temp_output="${TEST_DIR}/verbose_repeated_v15.out"
    log "Executing with invalid -v: $CLIENT_BIN -v 15 -h"
    "$CLIENT_BIN" -v 15 -h > "$temp_output" 2>&1
    local exit_status=$?
    local output_v15=$(cat "$temp_output")
    rm -f "$temp_output"

    # Check if command succeeded (exit status 0) and output indicates clamping
    if [ $exit_status -ne 0 ]; then
        fail_test "verbose_repeated" "Command failed with invalid verbosity 15 (exit status: $exit_status)"
        return
    fi
    # Assume implementation logs verbosity level or produces same output as max verbosity (e.g., 10)
    local line_count_v10=$("$CLIENT_BIN" -v 10 -h 2>&1 | wc -l)
    local line_count_v15=$(echo "$output_v15" | wc -l)
    if [ "$line_count_v15" -gt "$line_count_v10" ]; then
        fail_test "verbose_repeated" "Verbosity 15 produced more output than 10, not clamped (v15: $line_count_v15, v10: $line_count_v10)"
        return
    fi
    # Optionally, check for explicit clamping message if implementation logs it
    if echo "$output_v15" | grep -q "Set verbose level:.*1[0-5]"; then
        fail_test "verbose_repeated" "Verbosity 15 not clamped to valid range (1-10)"
        return
    fi

    pass_test "verbose_repeated"
}


# All tests to run
declare -A ALL_TESTS=(
    ["daemon_compile"]="Verifies that lsentineld.c compiles successfully"
    ["client_compile"]="Verifies that sentinel.c compiles successfully"
    ["help"]="Checks if the help message is displayed correctly"
    ["register_full_mask"]="Tests changelog consumer registration with full mask"
    ["register_partial_mask"]="Tests changelog consumer registration with partial mask"
    ["dry_run_register"]="Verifies dry-run mode for consumer registration"
    ["parse_config"]="Ensures configuration file is parsed and database is created"
    ["process_create"]="Tests changelog processing for file creation"
    ["process_delete"]="Tests changelog processing for file deletion"
    ["process_rename"]="Tests changelog processing for file renaming"
    ["daemon_mode"]="Verifies daemon mode with file creation and signal handling"
    ["cache"]="Checks if FID-to-path cache is used during processing"
    ["verbose_levels"]="Ensures higher verbosity levels produce more output"
    ["database_rotation"]="Tests database file rotation and record consistency"
    ["database_rotation_as_daemon"]="Tests database file rotation and record consistency while running in daemon mode"
    ["invalid_config"]="Verifies error handling for invalid configuration files"
#    ["cache_timeout"]="Tests cache entry expiration after timeout"
    ["process_symlink"]="Tests changelog processing for symlink creation"
    ["mount_point_failure"]="Verifies error handling when mount point detection fails"
    ["sql_escape"]="Tests SQL escaping of special characters in filenames"
    ["client_fid"]="Tests clienting history by FID from a single generated DB, verifying record extraction and sorting"
    ["client_path"]="Tests resolving a path to FID and clienting history, checking path normalization"
    ["output_formats"]="Verifies output in JSON, YAML, CSV formats parses correctly and matches text output"
    ["mount_integration"]="Tests live path resolution with -m mount point (requires root), checking current path and summary"
    ["config_client"]="Tests loading client options from config file, overriding CLI args for DB and format"
    ["multi_db_client"]="Tests clienting across multiple rotated DBs, ensuring complete history aggregation"
    ["edge_utf8_path"]="Tests handling UTF-8 paths in queries, verifying encoding and SQL escaping"
    ["invalid_search"]="Tests error handling for invalid FID/path, checking 'unknown' fallback and exit status"
    ["verbose_client"]="Tests verbosity levels during client, ensuring progressive log output without errors"
    ["summary_accuracy"]="Tests summary generation for file existence, name changes, and parent FID matching"
    ["collect_db_files_glob"]="Tests collect_db_files with a glob pattern, verifying correct file collection"
    ["collect_db_files_template"]="Tests collect_db_files with a template pattern, verifying correct file collection"
    ["collect_db_files_explicit"]="Tests collect_db_files with an explicit file list, verifying correct file collection"
    ["collect_db_files_invalid_dir"]="Tests collect_db_files error handling for invalid directory"
    ["collect_db_files_no_match"]="Tests collect_db_files error handling when no files match the glob"
    ["output_file_success"]="Tests successful output redirection to file, verifying no stdout leakage and content"
    ["output_file_failure"]="Tests error handling when output file cannot be opened (e.g., permissions)"
    ["default_db_collection"]="Tests default database collection from directory (implicit glob search)"
    ["query_method_loop"]="Tests --method loop for querying multiple/single DBs"
    ["query_method_attach"]="Tests --method attach for querying multiple/single DBs"
    ["query_method_invalid"]="Tests error handling for invalid --method value"
    ["verbose_repeated"]="Tests handling of repeated -v/--verbose (last value wins, clamping 1-10)"
)

# Helper functions
log() {
    echo "$1"
}

error() {
    echo "ERROR: $1" >&2
}

fail_test() {
    local test_name="$1"
    local reason="$2"
    FAILED_TESTS+=("$test_name: $reason")
    error "Test $test_name FAILED: $reason"
}

pass_test() {
    local test_name="$1"
    PASSED_TESTS+=("$test_name")
    log "Test $test_name PASSED"
}

setup_lustre() {
    if [ ! -d ${LUSTRE_TEST_DIR_FP} ]; then
	    mkdir -p ${LUSTRE_TEST_DIR_FP} || {
		    error "Unable to create tests directory: ${LUSTRE_TEST_DIR_FP}"
	    	    exit 1;
	    }
    fi

    if [ $SETUP_DONE -eq 1 ]; then return; fi
    log "Setting up Lustre filesystem"
    if ! "${LUSTRE_TESTS_DIR}/llmount.sh"; then
        error "Failed to set up Lustre with llmount.sh"
        exit 1
    fi
    # Wait for mount
    sleep 2
    if ! mount | grep -q "$MOUNT_POINT"; then
        error "Lustre not mounted at $MOUNT_POINT"
        exit 1
    fi
    log "Letting the FS settle..."
    sleep 5 

    log "Creating test directory at $LUSTRE_TEST_DIR_FP"
    mkdir -p $LUSTRE_TEST_DIR_FP || {
       error "Unable to create test directory $LUSTRE_TEST_DIR_FP"
       exit 1
    }

    SETUP_DONE=1
}

compile_program() {
	compile_sentinel_daemon
	compile_sentinel_client
}


compile_sentinel_daemon() {
    log "Compiling lsentineld.c"
    if ! gcc $DAEMON_COMPILE_FLAGS; then
        error "Daemon compilation failed"
        exit 1
    fi
    if [ ! -x "$DAEMON_BIN" ]; then
        error "Daemon binary not created"
        exit 1
    fi
}

compile_sentinel_client() {
    log "Compiling sentinel.c"
    if ! gcc $CLIENT_COMPILE_FLAGS; then
	error "Client compilation failed"
        exit 1
    fi
    if [ ! -x "$CLIENT_BIN" ]; then
	error "Client binary not created"
        exit 1
    fi
    return 0
}

cleanup() {
    if [ $CLEANUP_DONE -eq 1 ]; then return; fi
    log "Cleaning up"
    pkill -9 -f "$DAEMON_BIN" 2>/dev/null || true
    sleep 1
    rm -rf "$TEST_DIR" "$DAEMON_BIN" "$CLIENT_BIN" "$LUSTRE_TEST_DIR_FP"
    if [ "$SETUP_DONE" -eq 1 ] && [ -z "$NO_TEAR_DOWN" ]; then
	log "Tearing down Lustre"
        "${LUSTRE_TESTS_DIR}/llmountcleanup.sh" || error "Lustre cleanup failed"
    fi
    CLEANUP_DONE=1
}

# Initialize CONSUMER_ID with a default
CONSUMER_ID="${CONSUMER_ID:-test_sentinel}"

# Modified register_cl function
register_cl() {
    setup_lustre
    log "Checking for existing consumer: $CONSUMER_ID"
    local mask=$(get_mask "$CONSUMER_ID")
    local expected_types_partial=(CREAT MKDIR UNLNK RMDIR RENME RNMTO CLOSE TRUNC SATTR XATTR MTIME CTIME)
    local expected_types_full=(CREAT MKDIR HLINK SLINK MKNOD UNLNK RMDIR RENME RNMTO TRUNC SATTR XATTR MTIME CTIME ATIME MIGRT GXATR)
    local desired_mask="partial"
    local expected_types=("${expected_types_partial[@]}")

    # Check if consumer exists with desired mask
    if [ -n "$mask" ]; then
        local mask_valid=1
        for typ in "${expected_types[@]}"; do
            if [[ ! "$mask" == *"$typ"* ]]; then
                mask_valid=0
                break
            fi
        done
        if [ $mask_valid -eq 1 ]; then
            log "Found existing consumer $CONSUMER_ID with valid partial mask"
            return
        else
            log "Existing consumer $CONSUMER_ID has incompatible mask: $mask"
            CONSUMER_ID="${CONSUMER_ID}_$(date +%s)"  # Generate new ID
            log "Generated new consumer ID: $CONSUMER_ID"
        fi
    fi

    log "No suitable consumer found, running partial register test"
    test_register_partial_mask
}

get_mask() {
    local user="$1"
    if [ -z "$user" ]; then
        echo "Error: No user provided" >&2
        return 1
    fi

    local raw_mask
    raw_mask=$(lctl get_param mdd.$MDT_NAME.changelog_users | grep -F "$user" | cut -d= -f2 | sed -e 's/,/ /g');
    echo "$raw_mask";
}

verify_db_table() {
    local table="$1"
    local expected_count="$2"
    local client="SELECT COUNT(*) FROM $table"
    local count=$($DUCKDB_CLI "$DB_PATH" -csv -c "$client" | tail -n 1)
    if [ "$count" -ne "$expected_count" ]; then
        return 1
    fi
    return 0
}

verify_db_record() {
    local client="$1"
    local expected="$2"
    local result=$($DUCKDB_CLI "$DB_PATH" -csv -c "$client")
    if ! echo "$result" | grep -i -F "$expected" > /dev/null; then
        return 1
    fi
    return 0
}

generate_config() {
    cat <<EOF > "$CONFIG_FILE"
fsname=$MDT_NAME
daemon=0
consumer=$CONSUMER_ID
mask=partial
db=$DB_PATH
register=0
EOF
}

generate_invalid_config() {
    cat <<EOF > "${TEST_DIR}/invalid.conf"
fsname=$MDT_NAME
daemon=invalid_value
consumer=$CONSUMER_ID
mask=invalid_mask
db=$DB_PATH
register=invalid
rotate=invalid
EOF
}

create_test_db() {
    local db_file="$1"
    if [ -f "$db_file" ]; then
    	rm -vf "$db_file"
    fi
    log "Creating test database: $db_file"
    $DUCKDB_CLI "$db_file" -c "
        CREATE TABLE changelog (
            id BIGINT,
            operation_type VARCHAR,
            operation_time VARCHAR,
            flags INTEGER,
            tfid VARCHAR,
            pfid VARCHAR,
            name VARCHAR,
            uid VARCHAR,
            gid VARCHAR,
            client_nid VARCHAR,
            job_id VARCHAR,
            full_path VARCHAR
        );
        INSERT INTO changelog VALUES (
            1, 'CREAT', '2025-09-08 09:00:00', 0,
            '[0x200000401:0x1:0x0]', '[0x200000400:0x1:0x0]', 'testfile',
            '0', '0', 'unknown', 'none', '/lsentinel_test/testfile'
        );
        CREATE TABLE files (
            fid VARCHAR,
            current_path VARCHAR
        );
        INSERT INTO files VALUES ('[0x200000401:0x1:0x0]', '/lsentinel_test/testfile');
    " > /dev/null 2>&1
    if [ $? -ne 0 ]; then
        error "Failed to create test database $db_file"
        return 1
    fi
    if [ ! -f "$db_file" ]; then
        error "Test database $db_file not created"
        return 1
    fi
    return 0
}

run_tests() {
    local mode="$1"
    shift
    local tests=("$@")

    log "Test run mode: $mode"
    if [ "$mode" == "sequential" ]; then
        for test in "${tests[@]}"; do
            log "Running test: $test"
            "test_$test"
        done
    elif [ "$mode" == "random" ]; then
        local shuffled=($(shuf -e "${tests[@]}"))
        for test in "${shuffled[@]}"; do
            log "Running test: $test"
            "test_$test"
        done
    fi
}

print_help() {
    echo "Usage: $0 [options]"
    echo " Options:"
    echo "  --all               Run all tests"
    echo "  --sequential        Run all tests in sequential order"
    echo "  --random            Run all tests in random order"
    echo "  --test <test name>  Only run 1 specific test"
    echo "  --help              This page"
    echo "  --no-lustre-setup   Don't perform lustre setup steps"
    echo "  --no-cleanup        Don't clean up"
    echo "  --list-tests        List all available tests"
    echo "  --consumer-id       The ID of the consumer to use"
    exit 0;
}

list_tests() {
    echo "Available tests:"
    for test_name in "${!ALL_TESTS[@]}"; do
        echo "   $test_name: ${ALL_TESTS[$test_name]}"
    done
    exit 0;
}

##############
# Main logic #
##############
if [ ! -x "$DUCKDB_CLI" ]; then
    error "DuckDB CLI not found at $DUCKDB_CLI"
    exit 1
fi


while [[ $# -gt 0 ]]; do
    case "$1" in
        --all) RUN_ALL=1 ;;
        --sequential) MODE="sequential" ;;
        --random) MODE="random" ;;
        --test) shift; SINGLE_TEST="$1" ;;
        --help) print_help; exit 0 ;;
        --no-lustre-setup) SETUP_DONE="1"; NO_TEAR_DOWN="1" ;;
	--no-cleanup) CLEANUP_DONE="1" ;;
        --list-tests) list_tests; exit 0 ;;
	--consumer-id) shift; CONSUMER_ID="$1" ;;
        *) error "Unknown option: $1  see: $0 --help for valid options"; exit 1 ;;
    esac
    shift
done

mkdir -p "$TEST_DIR" || {
	error "Unable to make test directory: $TEST_DIR"
	exit 1;
}

if [ ! -x "$BIN" ]; then
   compile_program
fi 

if [ -n "$SINGLE_TEST" ]; then
    SELECTED_TESTS=("$SINGLE_TEST")
elif [ $RUN_ALL -eq 1 ]; then
    SELECTED_TESTS=("${!ALL_TESTS[@]}")
else
    SELECTED_TESTS=("${!ALL_TESTS[@]}")
fi

run_tests "$MODE" "${SELECTED_TESTS[@]}"

# Report
log ""
log "******************"
log "* TESTING REPORT *"
log "******************"
log "Passed tests: ${#PASSED_TESTS[@]}"
for t in "${PASSED_TESTS[@]}"; do log "$t PASS"; done
log ""
log "Failed tests: ${#FAILED_TESTS[@]}"
for f in "${FAILED_TESTS[@]}"; do log "$f FAIL"; done

if [ ${#FAILED_TESTS[@]} -gt 0 ]; then
    log ""
    log "Skipping cleanup due to failed tests"
    log "Test data available here: ${TEST_DIR}"    
    exit 1
else
    trap cleanup EXIT
    exit 0
fi
