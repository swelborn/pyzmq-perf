#!/usr/bin/env python
import time
import zmq
import argparse
from .config_model import TestConfig


def thr(rep_url: str, src_url: str):
    """Setup test based on client request."""
    ctx = zmq.Context()
    
    start_socket = ctx.socket(zmq.REP)
    start_socket.bind(rep_url)
    
    print("Server ready. Waiting for test configuration from client...")
    config_json = start_socket.recv_json()
    
    if not isinstance(config_json, dict):
        raise ValueError("Received configuration is not a dictionary.")

    config = TestConfig(**config_json)

    if config.pub:
        data_socket = ctx.socket(zmq.PUB)
    else:
        data_socket = ctx.socket(zmq.PUSH)
    
    
    data_socket.setsockopt(zmq.SNDHWM, config.sndhwm)
    data_socket.setsockopt(zmq.SNDTIMEO, config.sndtimeo)
    data_socket.bind(src_url)
    time.sleep(1)
    
    # send ack after set up
    start_socket.send_string("ACK")
    
    print(f"Starting to send messages: count={config.count}, size={config.size}, zero-copy={config.zero_copy}, pub={config.pub}")
    data = b' ' * config.size
    copy = not config.zero_copy
    for i in range(config.count):
        data_socket.send(data, copy=copy)
        
    data_socket.close()
    start_socket.close()
    ctx.term()

def parse_args():
    parser = argparse.ArgumentParser(description='ZMQ Performance Test Server')
    parser.add_argument('--rep-port', type=int, default=5001, help='Port for setup communication')
    parser.add_argument('--data-port', type=int, default=5002, help='Port for data communication')
    parser.add_argument('--ip', type=str, default='*', help='Bind IP address')
    return parser.parse_args()

def main():
    args = parse_args()
    rep_url = f"tcp://{args.ip}:{args.rep_port}"
    src_url = f"tcp://{args.ip}:{args.data_port}"
    while(True):
        thr(rep_url, src_url)

if __name__ == '__main__':
    main()
