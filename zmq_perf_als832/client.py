#!/usr/bin/env python
import zmq
import argparse
import time
import csv
from .config_model import TestConfig  # Adjust import path as needed
import json
import math
from itertools import product
import os


def generate_test_matrix():
    counts = [1000, 10000, 100000]
    sizes = [64 * 2 ** i for i in range(int(math.log2(10 * 2**20 / 64)) + 1)]
    zero_copies = [True, False]
    pubs = [True, False]
    sndhwms = [0]
    rcvhwms = [0]
    sndtimeos = [1000]
    rcvtimeos = [1000]

    # Compute the cartesian product of the parameter lists
    test_combinations = product(counts, sizes, zero_copies, pubs, sndhwms, rcvhwms, sndtimeos, rcvtimeos)

    # Create a dictionary for each combination and append to test_matrix
    test_matrix = [
        {'count': count, 'size': size, 'zero_copy': zero_copy, 'pub': pub,
         'sndhwm': sndhwm, 'rcvhwm': rcvhwm, 'sndtimeo': sndtimeo, 'rcvtimeo': rcvtimeo}
        for count, size, zero_copy, pub, sndhwm, rcvhwm, sndtimeo, rcvtimeo in test_combinations
    ]

    return test_matrix


def run_test(server_ip: str, rep_port: int, data_port: int, config: TestConfig):
    ctx = zmq.Context()

    # Setup configuration socket
    config_socket = ctx.socket(zmq.REQ)
    config_url = f"tcp://{server_ip}:{rep_port}"
    config_socket.connect(config_url)

    # Setup data socket based on config
    if config.pub:
        data_socket = ctx.socket(zmq.SUB)
        data_socket.setsockopt_string(zmq.SUBSCRIBE, '')
    else:
        data_socket = ctx.socket(zmq.PULL)
    data_socket.connect(f"tcp://{server_ip}:{data_port}")
    data_socket.setsockopt(zmq.RCVTIMEO, config.rcvtimeo)
    data_socket.setsockopt(zmq.RCVHWM, config.rcvhwm)

    # Send configuration to server
    print("Sending test configuration to server...")
    config_socket.send_json(config.model_dump())
    message = config_socket.recv_string()
    assert message == "ACK", "Didn't receive ACK from server"
    print("Test configuration acknowledged.")

    # Receive messages
    messages_received = 0
    start_time = time.time()
    try:
        while messages_received < config.count:
            msg = data_socket.recv(copy=not config.zero_copy)
            messages_received += 1
    except zmq.Again:
        print("Timeout reached. No more messages received.")
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    throughput = (messages_received * config.size * 8) / (elapsed_time * 1024 * 1024)

    # Clean up
    config_socket.close()
    data_socket.close()
    ctx.term()

    # Return results
    return {
        'config': config.model_dump(),
        'messages_received': messages_received,
        'elapsed_time': elapsed_time,
        'throughput': throughput,
        'start_time': start_time,
        'end_time': end_time
    }
    
def load_existing_results(filename="test_results.json"):
    if os.path.exists(filename):
        with open(filename, 'r') as f:
            return json.load(f)
    else:
        return []

def check_if_config_tested(config, existing_results):
    for result in existing_results:
        if result['config'] == config.model_dump():
            return True
    return False

def save_results(results, filename="test_results.csv"):
    with open(filename, 'w', newline='') as csvfile:
        fieldnames = ['start_time', 'end_time', 'elapsed_time', 'throughput_mbps',
                      'messages_received', 'count', 'size', 'zero_copy', 'pub']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for result in results:
            config = result['config']
            row = {**result, **config}
            del row['config']  # Remove the nested dictionary
            writer.writerow(row)
            
def parse_args():
    parser = argparse.ArgumentParser(description="Run the client part of a zmq performance test")
    parser.add_argument("--server-ip", type=str, default="localhost", help="Server IP address")
    parser.add_argument("--rep-port", type=int, default=5001, help="Port for setup communication")
    parser.add_argument("--data-port", type=int, default=5002, help="Port for data communication")
    return parser.parse_args()

def main():
    args = parse_args()
    existing_results = load_existing_results()

    test_matrix = generate_test_matrix()
    results = []
    for config_dict in test_matrix:
        config = TestConfig(**config_dict)
        
        # Check if this configuration has already been tested
        if check_if_config_tested(config, existing_results):
            print(f"Skipping already tested configuration: {config.model_dump()}")
            continue
        
        # Run the test
        result = run_test(args.server_ip, args.rep_port, args.data_port, config)
        
        # Add the result to the existing results
        existing_results.append(result)
        
        # Save updated results after each test
        with open('test_results.json', 'w') as f:
            json.dump(existing_results, f, indent=2)

        print(json.dumps(result, indent=2))

if __name__ == '__main__':
    main()
