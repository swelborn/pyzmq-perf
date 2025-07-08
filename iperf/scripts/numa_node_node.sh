#!/bin/bash
#SBATCH --job-name=iperf-numa-bench
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
echo "Benchmark started at: $today_datetime"
RESULTS_DIR="$CURRENT_DIR/out/$today_datetime"
mkdir -p "$RESULTS_DIR"
cd "$RESULTS_DIR"

# Get the hostnames of the allocated nodes
nodes=$(scontrol show hostnames "$SLURM_NODELIST")
nodes_array=($nodes)

# Assign roles
server_node=${nodes_array[0]}
client_node=${nodes_array[1]}
echo "Server node: $server_node"
echo "Client node: $client_node"

server_node_hostname=${server_node}.chn.perlmutter.nersc.gov
echo "Server node hostname: $server_node_hostname"

# Destination IP address
DEST_IP="$server_node_hostname"

# Time for the test to run
TIME="180"
TIME_SERVER="1600" # Needs to be long enough for all client tests

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

# Launch the server on the first node
echo "Starting server on $server_node..."
SERVER_OUTPUT_FILE="$RESULTS_DIR/server_out.txt"
srun --nodes=1 --ntasks=1 -w "$server_node" \
    $IPERF_PATH -s -i $INTERVAL -f g -w $WINDOW_SIZE -p $PORT -t $TIME_SERVER > $SERVER_OUTPUT_FILE &

sleep 10  # Give the server plenty of time to start

# Launch the clients on the second node, iterating through NUMA domains
echo "Starting clients on $client_node, testing each NUMA domain..."
srun --nodes=1 --ntasks=1 -w "$client_node" bash -c "
for NODE in {0..7}
do
  CLIENT_OUTPUT_FILE=\"$RESULTS_DIR/client_out_NUMA_\${NODE}.txt\"
  echo \"Client starting on NUMA node \${NODE}...\"
  numactl --cpunodebind=\${NODE} $IPERF_PATH -c $DEST_IP -t $TIME $ENHANCED_REPORTS -P $PARALLEL_THREADS -i $INTERVAL -w $WINDOW_SIZE -p $PORT > \$CLIENT_OUTPUT_FILE
  echo \"Client on NUMA node \${NODE} finished.\"
  sleep 5 # Brief pause between tests
done
" &

wait

echo "Benchmark finished."

mv "$CURRENT_DIR/$SLURM_JOB_ID.out"