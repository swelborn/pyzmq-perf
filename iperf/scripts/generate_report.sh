#!/bin/bash

# Usage: ./generate_report.sh <results_directory>

if [ $# -ne 1 ]; then
    echo "Usage: $0 <results_directory>"
    echo "Example: $0 ./out/2025-07-08_08-25-45"
    exit 1
fi

RESULTS_DIR="$1"

if [ ! -d "$RESULTS_DIR" ]; then
    echo "Error: Directory $RESULTS_DIR does not exist"
    exit 1
fi

# Function to extract bandwidth from iperf output
extract_bandwidth() {
    local file="$1"
    if [ -f "$file" ]; then
        # Look for SUM line and extract bandwidth (last field before "Gbits/sec")
        grep "^\[SUM\]" "$file" | awk '{
            for(i=1; i<=NF; i++) {
                if($i == "Gbits/sec") {
                    print $(i-1)
                    break
                }
            }
        }'
    else
        echo "N/A"
    fi
}

# Function to extract NUMA node from filename
extract_numa() {
    local filename="$1"
    if [[ "$filename" =~ _NUMA_([^.]+)\.out$ ]]; then
        echo "${BASH_REMATCH[1]}"
    else
        echo "N/A"
    fi
}

echo "========================================================================"
echo "                    IPERF BENCHMARK RESULTS REPORT"
echo "========================================================================"
printf "%-10s %-15s %-15s %-20s %-20s\n" "Benchmark" "Server NUMA" "Client NUMA" "Server BW (Gbps)" "Client BW (Gbps)"
echo "------------------------------------------------------------------------"

# Find all benchmark numbers
benchmark_numbers=$(ls "$RESULTS_DIR" | grep -o '^[0-9]\+' | grep -v '^[0-9]\{7,\}' | sort -n | uniq)

for bench_num in $benchmark_numbers; do
    # Find server and client files for this benchmark
    server_file=$(ls "$RESULTS_DIR"/${bench_num}_server_*.out 2>/dev/null | head -1)
    client_file=$(ls "$RESULTS_DIR"/${bench_num}_client_*.out 2>/dev/null | head -1)
    
    if [ -n "$server_file" ] && [ -n "$client_file" ]; then
        # Extract NUMA nodes from filenames
        server_numa=$(extract_numa "$(basename "$server_file")")
        client_numa=$(extract_numa "$(basename "$client_file")")
        
        # Extract bandwidth values
        server_bw=$(extract_bandwidth "$server_file")
        client_bw=$(extract_bandwidth "$client_file")
        
        # Format and display the row
        printf "%-10s %-15s %-15s %-20s %-20s\n" \
            "$bench_num" \
            "$server_numa" \
            "$client_numa" \
            "$server_bw" \
            "$client_bw"
    else
        printf "%-10s %-15s %-15s %-20s %-20s\n" \
            "$bench_num" \
            "MISSING" \
            "MISSING" \
            "N/A" \
            "N/A"
    fi
done

echo "------------------------------------------------------------------------"
echo "Report generated for: $RESULTS_DIR"
echo "Date: $(date)"
echo "========================================================================"