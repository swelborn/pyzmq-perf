# Python ZeroMQ Streaming Skeleton for Throughput Benchmarking

## Description

This project provides a framework for benchmarking the throughput of ZeroMQ messaging patterns in Python. It uses a coordinator-worker architecture to systematically test various configurations, including:

* **Messaging Patterns**: PUSH/PULL and PUB/SUB.
* **Message Size**: A configurable range of message sizes.
* **Message Count**: The number of messages to transmit for each test.
* **Zero-Copy**: Option to enable or disable PyZMQ's zero-copy feature.
* **Socket Options**: High-water mark settings for send and receive sockets.

The coordinator manages the lifecycle of the benchmark, distributing test configurations to worker pairs (one sender and one receiver) and collecting the results. The results, including throughput measurements in Mbps, are saved to a CSV file for analysis.

## Installation

To install the necessary dependencies, you can use `pip` with the provided `pyproject.toml` file. It is recommended to use a virtual environment.

```bash
# It is recommended to use a virtual environment
uv venv .venv
source .venv/bin/activate

# Install the project and its dependencies
pip install .
```

## Usage

The primary command-line interface is `pyzmq-bench`.

### Running a Benchmark

To run a benchmark, you need to start a coordinator process and one or more pairs of sender and receiver workers.

1. **Start the Coordinator and a Sender Worker:**

    On one terminal, start the coordinator. The coordinator will also start the first worker process. The following command starts a coordinator and a sender worker.

    ```bash
    pyzmq-bench sender --coordinator
    ```

2. **Start a Receiver Worker:**

    On another terminal, start a receiver worker that connects to the coordinator.

    ```bash
    pyzmq-bench receiver
    ```

    The benchmark will begin automatically once the required number of worker pairs have registered with the coordinator.

### Command-Line Options

You can customize the benchmark run with various command-line options:

* `--num-pairs`: Specify the number of sender/receiver pairs (default: 1).
* `--coordinator-ip`: The IP address of the node running the coordinator.
* `--short`: Run a reduced test matrix for a quick test.
* `--log-level`: Set the logging level (e.g., `DEBUG`, `INFO`).

**Example: Running a benchmark with 2 pairs on the same machine:**

```bash
# Terminal 1: Start coordinator and 2 sender workers
pyzmq-bench sender --coordinator --num-pairs 2

# Terminal 2: Start 2 receiver workers
pyzmq-bench receiver --num-pairs 2
```

### Plotting Results

After a benchmark run is complete, a `test_results.csv` file will be generated. You can use the `pyzmq-plot` command to visualize the results:

```bash
pyzmq-plot test_results.csv
```

This will generate a plot showing throughput vs. message size, which can be saved to a file.

## Acknowledgments

This project was created to facilitate high-performance messaging benchmarks.
