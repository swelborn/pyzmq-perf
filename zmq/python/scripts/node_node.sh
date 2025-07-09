#!/bin/bash
#SBATCH --job-name=pyzmq-bench-numa
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=1
#SBATCH --qos=debug
#SBATCH --time=00:30:00
#SBATCH --constraint=cpu
#SBATCH --account=nstaff
#SBATCH --output=%j.out

CURRENT_DIR=$(pwd)
today_datetime=$(date +%Y-%m-%d_%H-%M-%S)
echo "NUMA benchmark started at: $today_datetime"
BASE_RESULTS_DIR="$HOME/pyzmq-bench-output/numa_$today_datetime"
mkdir -p "$BASE_RESULTS_DIR"

# Get the hostnames of the allocated nodes
nodes=$(scontrol show hostnames "$SLURM_NODELIST")
nodes_array=($nodes)

# Assign roles
coordinator_node=${nodes_array[0]}
sender_node=${nodes_array[1]}
echo "Coordinator/Receiver node: $coordinator_node"
echo "Sender node: $sender_node"

coordinator_node_hostname=${coordinator_node}.chn.perlmutter.nersc.gov
echo "Coordinator node hostname: $coordinator_node_hostname"

# Function to run a single NUMA benchmark
run_numa_benchmark() {
    local coord_numa=$1
    local sender_numa=$2
    local benchmark_name=$3
    
    echo "=== Running benchmark: $benchmark_name ==="
    echo "Coordinator NUMA: $coord_numa, Sender NUMA: $sender_numa"
    
    RESULTS_DIR="$BASE_RESULTS_DIR/${benchmark_name}"
    mkdir -p "$RESULTS_DIR"
    cd "$RESULTS_DIR"
    
    # Create .env file for this benchmark
    echo "PYZMQ_BENCH_SHORT_TEST=false" > ".env"
    echo "PYZMQ_BENCH_NUM_PAIRS=8" >> ".env"
    echo "PYZMQ_BENCH_NETWORK__COORDINATOR_IP=$coordinator_node_hostname" >> ".env"
    echo "PYZMQ_BENCH_NETWORK__COORDINATOR_ROUTER_PORT=5599" >> ".env"
    echo "PYZMQ_BENCH_NETWORK__COORDINATOR_PUB_PORT=5600" >> ".env"
    echo "PYZMQ_BENCH_NETWORK__DATA_PORT_START=43000" >> ".env"
    echo "PYZMQ_BENCH_OUTPUT__ADD_DATE_TIME=false" >> ".env"
    echo "PYZMQ_BENCH_TEST_MATRIX__MESSAGE_COUNTS='[10001]'" >> ".env"
    echo "PYZMQ_BENCH_TEST_MATRIX__ZERO_COPY_OPTIONS='[true]'" >> ".env"
    echo "PYZMQ_BENCH_TEST_MATRIX__SEND_HWM_VALUES='[10000]'" >> ".env"
    echo "PYZMQ_BENCH_TEST_MATRIX__RECV_HWM_VALUES='[10000]'" >> ".env"
    
    # Build commands with or without NUMA binding
    if [ "$coord_numa" = "none" ]; then
        coord_cmd="pyzmq-bench receiver --coordinator"
    else
        coord_cmd="numactl --cpunodebind=$coord_numa pyzmq-bench receiver --coordinator"
    fi
    
    if [ "$sender_numa" = "none" ]; then
        sender_cmd="pyzmq-bench sender"
    else
        sender_cmd="numactl --cpunodebind=$sender_numa pyzmq-bench sender"
    fi
    
    # Launch the receiver and coordinator on the first node
    echo "Starting receiver and coordinator on $coordinator_node with NUMA $coord_numa..."
    srun --nodes=1 --ntasks=1 -w "$coordinator_node" $coord_cmd &
    COORD_PID=$!
    
    # Give coordinator time to start
    sleep 5
    
    # Launch the sender on the second node
    echo "Starting sender on $sender_node with NUMA $sender_numa..."
    srun --nodes=1 --ntasks=1 -w "$sender_node" $sender_cmd &
    SENDER_PID=$!
    
    # Wait for both processes to complete
    wait $COORD_PID
    wait $SENDER_PID
    
    echo "Benchmark $benchmark_name finished."
    
    # Create plot for this benchmark
    if [ -f "$RESULTS_DIR/out/results.csv" ]; then
        echo "Creating plot for $benchmark_name..."
        pyzmq-bench plot "$RESULTS_DIR/out/results.csv"
    fi
    
    # Small delay between benchmarks
    sleep 2
}

# Run different NUMA configurations
# Format: run_numa_benchmark <coordinator_numa> <sender_numa> <benchmark_name>

# Benchmark 1: No NUMA binding (baseline)
run_numa_benchmark "none" "none" "baseline_no_numa"

# Benchmark 2: Both on NUMA node 1 (12 distance)
run_numa_benchmark "1" "1" "both_numa_1"

# Benchmark 3: Both on NUMA node 2 (NIC NUMA)
run_numa_benchmark "2" "2" "both_numa_2_nic"

# Benchmark 4: Both on NUMA node 7 (32 distance)
run_numa_benchmark "7" "7" "both_numa_7"

# Benchmark 5: Coordinator on NIC NUMA, Sender on 12 distance
run_numa_benchmark "2" "1" "coord_nic_sender_12"

# Benchmark 6: Coordinator on 12 distance, Sender on NIC NUMA
run_numa_benchmark "1" "2" "coord_12_sender_nic"

# Benchmark 7: Coordinator on NIC NUMA, Sender on 32 distance
run_numa_benchmark "2" "7" "coord_nic_sender_32"

# Benchmark 8: Coordinator on 32 distance, Sender on NIC NUMA
run_numa_benchmark "7" "2" "coord_32_sender_nic"

# Generate summary report
echo "=== NUMA Benchmark Summary ==="
echo "Results saved in: $BASE_RESULTS_DIR"
echo "Benchmark configurations:"
echo "  - baseline_no_numa: No NUMA binding"
echo "  - both_numa_1: Both on NUMA 1 (12 distance)"
echo "  - both_numa_2_nic: Both on NUMA 2 (NIC NUMA)"
echo "  - both_numa_7: Both on NUMA 7 (32 distance)"
echo "  - coord_nic_sender_12: Coordinator NIC, Sender 12 distance"
echo "  - coord_12_sender_nic: Coordinator 12 distance, Sender NIC"
echo "  - coord_nic_sender_32: Coordinator NIC, Sender 32 distance"
echo "  - coord_32_sender_nic: Coordinator 32 distance, Sender NIC"

# Move SLURM output to results directory
mv "$CURRENT_DIR/$SLURM_JOB_ID.out" "$BASE_RESULTS_DIR/"

echo "All NUMA benchmarks completed!"