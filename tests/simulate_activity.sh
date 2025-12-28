#!/bin/bash

# Shell script to simulate metadata operations on a Lustre filesystem for testing
# the Lustre Sentinel daemon (lsentineld.c). It performs operations like create,
# mkdir, link, rename, setattr, xattr, truncate, time changes, open/close, and
# delete in varying activity levels: high, medium, low, bursty, or random.
#
# Usage: ./simulate_ops.sh <directory> [mode]
#   - <directory>: Path to a directory on a Lustre-mounted filesystem where operations will be performed.
#                  A subdirectory 'sentinel_sim_<PID>' will be created to contain the operations.
#   - [mode]: Optional, one of 'high', 'medium', 'low', 'bursty', 'random' (default: bursty).
#
# Activity levels:
#   - high: Many operations (100 per cycle) with no inter-op sleep, short cycle sleep (1s).
#   - medium: Moderate operations (50 per cycle) with slight inter-op sleep (0.05s), medium cycle sleep (5s).
#   - low: Few operations (10 per cycle) with inter-op sleep (0.1s), long cycle sleep (10s).
#   - bursty: Alternates between intense bursts (200 ops, no inter-sleep, long post-sleep 10s)
#             and light activity (5 ops, 0.2s inter-sleep, short post-sleep 2s).
#   - random: Randomly selects one of the above four modes for each cycle.
#
# The script runs continuously until interrupted (Ctrl+C). It cleans up files/directories
# at the end of each cycle to simulate deletes and prevent unbounded growth.
#
# Requirements:
#   - The directory must be on a Lustre filesystem to generate changelog records.
#   - Commands like setfattr, truncate must be available (install attr, coreutils if needed).
#   - Run as a user with write permissions; root not required for most ops.
#
# Note: This simulates metadata-heavy workloads. Adjust numbers for your environment.

if [ $# -lt 1 ]; then
  echo "Usage: $0 <directory> [mode]"
  echo "Mode options: high, medium, low, bursty, random (default: bursty)"
  exit 1
fi

DIR="$1"
MODE="${2:-bursty}"
WORK_DIR="$DIR/sentinel_sim_$$"

# Check if directory is writable
if [ ! -w "$DIR" ]; then
  echo "Error: Directory $DIR is not writable"
  exit 1
fi

mkdir -p "$WORK_DIR" || { echo "Failed to create $WORK_DIR"; exit 1; }
cd "$WORK_DIR" || { echo "Failed to cd into $WORK_DIR"; exit 1; }

# Function to perform a batch of metadata operations
do_ops() {
  local num_ops=$1
  local inter_sleep=$2

  for ((i=1; i<=num_ops; i++)); do
    rand=$RANDOM
    file="file_${rand}.txt"
    dir="dir_${rand}"
    renamed_file="renamed_file_${rand}.txt"
    renamed_dir="renamed_dir_${rand}"
    hard_link="hard_${rand}.txt"
    soft_link="soft_${rand}.txt"

    # Create file (CL_CREATE)
    touch "$file" 2>/dev/null || echo "Failed to create file $file"

    # Create directory (CL_MKDIR)
    mkdir "$dir" 2>/dev/null || echo "Failed to create directory $dir"

    # Hardlink (CL_HARDLINK)
    [ -f "$file" ] && ln "$file" "$hard_link" 2>/dev/null || echo "Failed to create hardlink $hard_link"

    # Softlink (CL_SOFTLINK)
    [ -f "$file" ] && ln -s "$file" "$soft_link" 2>/dev/null || echo "Failed to create softlink $soft_link"

    # Rename file (CL_RENAME)
    [ -f "$file" ] && mv "$file" "$renamed_file" 2>/dev/null || echo "Failed to rename file to $renamed_file"

    # Rename directory (CL_RENAME)
    [ -d "$dir" ] && mv "$dir" "$renamed_dir" 2>/dev/null || echo "Failed to rename directory to $renamed_dir"

    # Setattr (CL_SETATTR) - random chmod with valid octal (000-777)
    if [ -f "$renamed_file" ]; then
      # Generate random octal number between 000 and 777
      perm=$(printf "%03o" $((RANDOM % 512)))  # 512 in octal is 777
      chmod "$perm" "$renamed_file" 2>/dev/null || echo "Failed to chmod $renamed_file to $perm"
    fi

    # Xattr (CL_XATTR)
    [ -f "$renamed_file" ] && setfattr -n "user.sim" -v "value_${rand}" "$renamed_file" 2>/dev/null || echo "Failed to set xattr on $renamed_file"

    # Truncate (CL_TRUNC)
    [ -f "$renamed_file" ] && truncate -s $((RANDOM % 1024)) "$renamed_file" 2>/dev/null || echo "Failed to truncate $renamed_file"

    # Time changes (CL_MTIME, CL_ATIME, CL_CTIME indirectly via chmod)
    [ -f "$renamed_file" ] && touch -m "$renamed_file" 2>/dev/null || echo "Failed to update mtime on $renamed_file"
    [ -f "$renamed_file" ] && touch -a "$renamed_file" 2>/dev/null || echo "Failed to update atime on $renamed_file"

    # Open/Close (CL_OPEN, CL_CLOSE) - simple read
    [ -f "$renamed_file" ] && cat "$renamed_file" >/dev/null 2>&1 || echo "Failed to read $renamed_file"

    # Sleep between operations if specified
    sleep "$inter_sleep"
  done
}

# Function to simulate deletes (CL_UNLINK, CL_RMDIR)
clean() {
  rm -rf ./* 2>/dev/null || echo "Failed to clean up $WORK_DIR"
}

# Trap for cleanup on exit
trap 'clean; echo "Cleaned up $WORK_DIR"; exit 0' INT TERM

echo "Starting simulation in $WORK_DIR with mode: $MODE"

while true; do
  submode="$MODE"
  if [ "$MODE" == "random" ]; then
    rand_mode=$((RANDOM % 4))
    case $rand_mode in
      0) submode="high" ;;
      1) submode="medium" ;;
      2) submode="low" ;;
      3) submode="bursty" ;;
    esac
    echo "Randomly selected submode: $submode"
  fi

  case "$submode" in
    high)
      num_ops=100
      inter_sleep=0
      cycle_sleep=1
      ;;
    medium)
      num_ops=50
      inter_sleep=0.05
      cycle_sleep=5
      ;;
    low)
      num_ops=10
      inter_sleep=0.1
      cycle_sleep=10
      ;;
    bursty)
      if (( RANDOM % 2 == 0 )); then
        num_ops=200
        inter_sleep=0
        cycle_sleep=10  # Long sleep after burst
      else
        num_ops=5
        inter_sleep=0.2
        cycle_sleep=2   # Short sleep after light activity
      fi
      ;;
    *)
      echo "Invalid mode: $submode"
      exit 1
      ;;
  esac

  echo "Cycle: Performing $num_ops operations (mode: $submode)"
  do_ops "$num_ops" "$inter_sleep"

  echo "Cycle: Cleaning up (simulating deletes)"
  clean

  echo "Cycle: Sleeping for $cycle_sleep seconds"
  sleep "$cycle_sleep"
done
