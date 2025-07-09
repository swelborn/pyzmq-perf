#!/bin/bash
#SBATCH --job-name=iperf-node-node
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=1
#SBATCH --qos=debug
#SBATCH --time=00:30:00
#SBATCH --constraint=cpu
#SBATCH --account=nstaff
#SBATCH --output=%j.out
#SBATCH --exclusive

CURRENT_DIR=$(pwd)
today_datetime=$(date +%Y-%m-%d_%H-%M-%S)
echo "Node-to-Node benchmark started at: $today_datetime"
RESULTS_DIR="$CURRENT_DIR/out/node_node_$today_datetime"
mkdir -p "$RESULTS_DIR"
cd "$RESULTS_DIR"

# Configuration for server and client execution methods
SERVER_METHOD="srun"
CLIENT_METHOD="srun"

# Get the hostnames of the allocated nodes
nodes=$(scontrol show hostnames "$SLURM_NODELIST")
nodes_array=($nodes)

# Assign roles
server_node=${nodes_array[0]}
server_node_hostname=${server_node}.chn.perlmutter.nersc.gov
client_node=${nodes_array[1]}

echo "Server method: $SERVER_METHOD"
echo "Server node: $server_node"
echo "Server node hostname: $server_node_hostname"
echo "Client method: $CLIENT_METHOD"
echo "Client node: $client_node"

# Destination IP address
DEST_IP="$server_node_hostname"

# Source common functions
source "$CURRENT_DIR/common.sh"

# Node-to-Node specific benchmarks
# Benchmark 1: Non-bound
echo "=== Benchmark 1: Non-bound ==="
start_server false 0 1
start_client false 0 1
wait_and_cleanup

# Benchmark 2: Distance 12 on both
echo "=== Benchmark 2: Distance 12 on both ==="
start_server true 1 2
start_client true 1 2
wait_and_cleanup

# Benchmark 3: 32 distance numa on server, nic numa on client 
echo "=== Benchmark 3: 32 distance numa on server, nic numa on client ==="
start_server true 7 3
start_client true 2 3
wait_and_cleanup

# Benchmark 4: 32 distance numa on client, nic numa on server
echo "=== Benchmark 4: 32 distance numa on client, nic numa on server ==="
start_server true 2 4
start_client true 7 4
wait_and_cleanup

# Benchmark 5: nic numa on both
echo "=== Benchmark 5: nic numa on both ==="
start_server true 2 5
start_client true 2 5
wait_and_cleanup

# Benchmark 6: 12 distance on server, nic numa on client
echo "=== Benchmark 6: 12 distance on server, nic numa on client ==="
start_server true 1 6
start_client true 2 6
wait_and_cleanup

# Benchmark 7: nic numa on server, 12 distance on client
echo "=== Benchmark 7: nic numa on server, 12 distance on client ==="
start_server true 2 7
start_client true 1 7
wait_and_cleanup

# Benchmark 8: nic numa on server, 12 distance on client
echo "=== Benchmark 8: 32 distance on both ==="
start_server true 7 8
start_client true 7 8
wait_and_cleanup

echo "Node-to-Node benchmark finished."
mv "$CURRENT_DIR/$SLURM_JOB_ID.out" "$RESULTS_DIR/"