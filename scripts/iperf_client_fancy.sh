#!/bin/bash

# Destination IP address
DEST_IP=""

# Time for the test to run
TIME="180"

# Enhanced reports
ENHANCED_REPORTS="-e"

# Number of parallel client threads to run
PARALLEL_THREADS="8"

# Time between periodic bandwidth reports
INTERVAL="20"

# TCP window size
WINDOW_SIZE="512M"

# Server port to connect to
PORT="5558"

# Path to the iperf binary
IPERF_PATH="/global/cfs/cdirs/nstaff/iperf/iperf2/install_dir/bin/iperf"

# Output directory for the test results
OUTPUT_DIR="changeme"

# Ensure the output directory exists
mkdir -p $OUTPUT_DIR

# Loop through NUMA nodes 0 to 7 and run the command with the corresponding cpunodebind
for NODE in {0..7}
do
  # Define the output file for this test
  OUTPUT_FILE="$OUTPUT_DIR/test_NUMA_$NODE.txt"

  echo "Running on NUMA node $NODE, output to $OUTPUT_FILE"
  numactl --cpunodebind=$NODE $IPERF_PATH -c $DEST_IP -t $TIME $ENHANCED_REPORTS -P $PARALLEL_THREADS -i $INTERVAL -w $WINDOW_SIZE -p $PORT > $OUTPUT_FILE
done

echo "All tests completed."