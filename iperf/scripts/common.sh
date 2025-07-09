#!/bin/bash

# Common configuration and functions for iperf benchmarks

# Time for the test to run
TIME="60"
TIME_SERVER="70"

# Enhanced reports
ENHANCED_REPORTS="-e"

# Number of parallel client threads to run
PARALLEL_THREADS="8"

# TCP window size
WINDOW_SIZE="512M"

# Server port to connect to
PORT="5558"

# Path to the iperf binary
IPERF_PATH="/global/cfs/cdirs/nstaff/iperf/iperf2/install_dir/bin/iperf"

# Function to start iperf server
start_server() {
    local use_numa=$1
    local numa_node=$2
    local benchmark_number=$3
    
    echo "Starting server on $server_node..."
    
    if [ "$use_numa" = true ]; then
        echo "Binding server to NUMA node $numa_node"
        SERVER_OUTPUT_FILE="$RESULTS_DIR/${benchmark_number}_server_NUMA_${numa_node}.out"
        server_cmd="numactl --cpunodebind=$numa_node $IPERF_PATH -s -f g -w $WINDOW_SIZE -p $PORT -t $TIME_SERVER"
    else
        echo "Starting server without NUMA binding"
        SERVER_OUTPUT_FILE="$RESULTS_DIR/${benchmark_number}_server_NUMA_none.out"
        server_cmd="$IPERF_PATH -s -f g -w $WINDOW_SIZE -p $PORT -t $TIME_SERVER"
    fi
    
    if [ "$SERVER_METHOD" = "srun" ]; then
        srun --nodes=1 --ntasks=1 -w "$server_node" $server_cmd > $SERVER_OUTPUT_FILE &
        SERVER_PID=$!
    else
        ssh "$server_node" "$server_cmd" > $SERVER_OUTPUT_FILE &
        SERVER_PID=$!
    fi
    
    sleep 5  # Give server time to start
}

# Function to start iperf clients
start_client() {
    local use_numa=$1
    local numa_node=$2
    local benchmark_number=$3
    
    echo "Starting client on $client_node..."
    
    if [ "$use_numa" = true ]; then
        echo "Using NUMA binding for client on node $numa_node"
        CLIENT_OUTPUT_FILE="$RESULTS_DIR/${benchmark_number}_client_NUMA_${numa_node}.out"
        client_cmd="numactl --cpunodebind=$numa_node $IPERF_PATH -c $DEST_IP -t $TIME $ENHANCED_REPORTS -P $PARALLEL_THREADS -w $WINDOW_SIZE -p $PORT"
    else
        echo "Running client without NUMA binding"
        CLIENT_OUTPUT_FILE="$RESULTS_DIR/${benchmark_number}_client_NUMA_none.out"
        client_cmd="$IPERF_PATH -c $DEST_IP -t $TIME $ENHANCED_REPORTS -P $PARALLEL_THREADS -w $WINDOW_SIZE -p $PORT"
    fi
    
    if [ "$CLIENT_METHOD" = "srun" ]; then
        srun --nodes=1 --ntasks=1 -w "$client_node" $client_cmd > $CLIENT_OUTPUT_FILE &
        CLIENT_PID=$!
    else
        ssh "$client_node" "$client_cmd" > $CLIENT_OUTPUT_FILE &
        CLIENT_PID=$!
    fi
}

# Function to wait for benchmark completion and cleanup
wait_and_cleanup() {
    echo "Waiting for client to complete..."
    wait $CLIENT_PID
    echo "Client finished. Stopping server..."
    
    # Kill server process - method depends on how it was started
    if [ "$SERVER_METHOD" = "srun" ]; then
        kill $SERVER_PID 2>/dev/null
    else
        # For SSH, we need to kill the remote iperf process
        ssh "$server_node" "pkill -f 'iperf.*-s.*-p $PORT'" 2>/dev/null
        kill $SERVER_PID 2>/dev/null  # Kill the SSH process itself
    fi
    
    wait $SERVER_PID 2>/dev/null
    echo "Server stopped. Waiting 1 second before next benchmark..."
    sleep 1
}