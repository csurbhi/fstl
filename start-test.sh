#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Configuration
DEVICE="/dev/dm-0"
MOUNT_POINT="/mnt"
SOURCE_DIR="/home/surbhi/github/linux"
FIO_CONFIG="./fio-fill-cache.fio"

echo "--- Starting LSDM Test Suite ---"

# 1. Format and Mount
echo "Formatting $DEVICE..."
mkfs.ext4 -F "$DEVICE"
mount -t ext4 "$DEVICE" "$MOUNT_POINT"

# 2. Prepare Data
echo "Copying Linux source to $MOUNT_POINT..."
cp -r "$SOURCE_DIR" "$MOUNT_POINT"

# 3. Fill Cache and Sync
echo "Running FIO and syncing..."
fio "$FIO_CONFIG"
sync

# 4. Calculate Watermark
# Note: Using $(( )) for math is cleaner than eval
NR_FREEZONES=$(cat /sys/kernel/lsdm_stats/nr_freezones)
MIDDLE_VAL=$((NR_FREEZONES - 4))
echo "Setting middle_watermark to: $MIDDLE_VAL"
echo "$MIDDLE_VAL" > /sys/kernel/lsdm_stats/middle_watermark

# 5. Build - Initial Run (Before)
echo "Starting iostat (Before)..."
iostat -d 1 > ./iostat-compile.before &
IOSTAT_PID=$!

cd "$MOUNT_POINT/linux"
echo "Running make -j 12..."
make -j 12 || { kill $IOSTAT_PID; exit 1; }

kill $IOSTAT_PID
echo "First build finished."

# 6. Touch C Files
echo "Touching all .c files..."
find . -iname "*.c" -print0 | xargs -0 touch

# 7. Build - Incremental Run (After)
echo "Starting iostat (After)..."
iostat -d 1 > ./iostat-compile.after &
IOSTAT_PID=$!

echo "Running make -j 12 (Incremental)..."
make -j 12 || { kill $IOSTAT_PID; exit 1; }

kill $IOSTAT_PID

echo "--- Test Suite Complete ---"
cd ~
umount "$MOUNT_POINT"
