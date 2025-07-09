#!/bin/bash
#SBATCH --job-name=iperf-node-login
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --qos=debug
#SBATCH --time=00:30:00
#SBATCH --constraint=cpu
#SBATCH --account=nstaff
#SBATCH --output=%j.out
#SBATCH --exclusive

CURRENT_DIR=$(pwd)
today_datetime=$(date +%Y-%m-%d_%H-%M-%S)
echo "Node-to-Login benchmark started at: $today_datetime"
RESULTS_DIR="$CURRENT_DIR/out/node_login_$today_datetime"
mkdir -p "$RESULTS_DIR"
cd "$RESULTS_DIR"

# Configuration for server and client execution methods
SERVER_METHOD="srun"
CLIENT_METHOD="ssh"

# Get the hostnames of the allocated nodes
nodes=$(scontrol show hostnames "$SLURM_NODELIST")
nodes_array=($nodes)

# Assign roles
server_node=${nodes_array[0]}
server_node_hostname=${server_node}.chn.perlmutter.nersc.gov
client_node="login01"  # Login node

echo "Server method: $SERVER_METHOD"
echo "Server node: $server_node"
echo "Server node hostname: $server_node_hostname"
echo "Client method: $CLIENT_METHOD"
echo "Client node: $client_node"

# Destination IP address
DEST_IP="$server_node_hostname"

# Source common functions
source "$CURRENT_DIR/common.sh"

# Node-to-Login specific benchmarks (login nodes have different NUMA layout)
# Benchmark 1: Non-bound
echo "=== Benchmark 1: Non-bound ==="
start_server false 0 1
start_client false 0 1
wait_and_cleanup

# Benchmark 2: Server NUMA 1, Client NUMA 0
echo "=== Benchmark 2: Server NUMA 1, Client NUMA 0 ==="
start_server true 1 2
start_client true 0 2
wait_and_cleanup

# Benchmark 3: Server NUMA 2, Client NUMA 1
echo "=== Benchmark 3: Server NUMA 2, Client NUMA 1 ==="
start_server true 2 3
start_client true 1 3
wait_and_cleanup

# Benchmark 4: Server NUMA 7, Client NUMA 0
echo "=== Benchmark 4: Server NUMA 7, Client NUMA 0 ==="
start_server true 7 4
start_client true 0 4
wait_and_cleanup

# Benchmark 5: Server NUMA 2, Client NUMA 0
echo "=== Benchmark 5: Server NUMA 2, Client NUMA 0 ==="
start_server true 2 5
start_client true 0 5
wait_and_cleanup

echo "Node-to-Login benchmark finished."
mv "$CURRENT_DIR/$SLURM_JOB_ID.out" "$RESULTS_DIR/"