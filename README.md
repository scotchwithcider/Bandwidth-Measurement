# TCP Bandwidth Measurement Tool

## Problem Statement

This project implements a minimal but technically correct client-server bandwidth measurement tool in Python. The goal is to measure TCP upload throughput from a client to a server with enough rigor to avoid the most common mistakes seen in simplistic socket benchmarks.

The tool supports:

- TCP client/server architecture using Python sockets
- Configurable transfer sizes from small tests to large sustained transfers
- Warm-up traffic before measurement
- High-resolution timing using `time.perf_counter()`
- Large application and socket buffers
- Single-connection and parallel-connection modes
- Structured logging to JSON Lines or CSV

## Project Structure

- `server.py`: TCP receiver and measurement endpoint
- `client.py`: TCP sender and benchmark driver
- `utils.py`: protocol helpers, timing helpers, size parsing, and logging
- `README.md`: design notes, analysis, observations, and limitations

## Design Decisions

### 1. Explicit warm-up phase

The client sends a configurable amount of warm-up data before the measured interval begins. This reduces startup distortion from:

- TCP slow start
- socket buffer ramp-up
- early scheduler noise
- one-time page faults or allocation effects

Without a warm-up phase, very short measurements can accidentally report a rate that is not representative of steady-state throughput.

### 2. High-resolution timing

Measured transfer time uses `time.perf_counter()` on both client and server. This is preferable to wall-clock timestamps because it offers:

- monotonic behavior
- higher resolution
- less sensitivity to system clock adjustments

### 3. Large buffers

The implementation uses:

- a reusable large payload buffer in user space
- large application chunk sizes by default (`1 MiB`)
- configurable OS socket buffers (`4 MiB` requested by default)

This reduces Python overhead per byte and helps the sender keep the TCP stack busy on higher-bandwidth paths.

### 4. Per-connection control message

Before data transfer starts, the client sends a small JSON control message describing:

- test ID
- connection index
- total bytes
- warm-up bytes
- chunk size
- socket buffer size
- test mode

This keeps the data phase simple and lets the server know exactly how many warm-up and measured bytes to expect.

### 5. Parallel mode using threads

Parallel mode opens multiple independent TCP flows. Each worker thread performs its own socket setup, warm-up, transfer, and result logging. Aggregate throughput is computed using total measured bytes divided by the overall wall-clock duration of the parallel test.

## How To Run

### Start the server

```bash
python server.py --host 0.0.0.0 --port 5001 --log server_results.jsonl --verbose
```

### Run a single-connection test

```bash
python client.py --host 127.0.0.1 --port 5001 --size 100MiB --warmup 4MiB --parallel 1 --log client_results.jsonl --verbose
```

### Run a parallel-connection test

```bash
python client.py --host 127.0.0.1 --port 5001 --size 256MiB --warmup 4MiB --parallel 4 --log client_results.csv --verbose
```

## Command-Line Options

### `server.py`

- `--host`: bind address, default `0.0.0.0`
- `--port`: listen port, default `5001`
- `--socket-buffer`: requested socket buffer size in bytes
- `--log`: output file path, `.jsonl` or `.csv`
- `--verbose`: print per-transfer summaries

### `client.py`

- `--host`: server hostname or IP
- `--port`: server port, default `5001`
- `--size`: total bytes to send, examples: `100MiB`, `500MB`
- `--warmup`: warm-up bytes before timing begins
- `--chunk-size`: application send chunk size
- `--socket-buffer`: requested socket buffer size in bytes
- `--parallel`: number of parallel TCP connections
- `--log`: output file path, `.jsonl` or `.csv`
- `--verbose`: print per-connection summaries

## Output Format

Each logged record includes:

- `test_id`
- `role`
- `mode`
- `connection_index`
- `total_bytes`
- `measured_bytes`
- `warmup_bytes`
- `duration_seconds`
- `throughput_mbps`
- `throughput_MBps`
- `chunk_size`
- `socket_buffer`
- `peer`
- `started_at_utc`
- `finished_at_utc`

JSON logging uses JSON Lines format, which is easy to append and analyze with scripts. CSV is also supported for spreadsheet-style inspection.

## Analysis

### Why small transfers can overestimate bandwidth

Small transfers are often misleading because the measured interval can be dominated by startup effects instead of steady-state transport behavior. Several things can go wrong:

- The timing window may be too short, so timer granularity and scheduler jitter become a large percentage of the result.
- Application code may measure only the time needed to copy data into local buffers, not the time needed for the network stack to fully drain the data to the receiver.
- Short bursts can fit largely inside sender and receiver buffering, making the apparent rate look better than the sustained end-to-end rate.
- TCP may not have reached a stable congestion window yet, so the measured sample reflects an early transient rather than long-run throughput.

In practice, a larger measured transfer after a warm-up phase produces results that are much closer to true sustainable throughput.

### TCP window size, latency, and congestion control

TCP throughput is strongly influenced by the bandwidth-delay product (BDP), which is approximately:

`throughput <= window_size / RTT`

That means:

- Higher latency requires a larger effective congestion window and receive window to keep the link full.
- If the TCP window is too small for the path's BDP, the sender will idle while waiting for acknowledgments.
- Congestion control starts conservatively and increases the sending rate over time.

Key implications:

- On low-latency LANs, a single TCP connection may quickly achieve line rate.
- On higher-latency WAN paths, a single flow may underutilize the link unless the window grows large enough.
- Loss, reordering, or queue pressure can reduce the congestion window and lower throughput.

This is one reason parallel TCP streams sometimes outperform a single stream on real networks: each stream gets its own congestion-control state, and the aggregate can fill the path more effectively.

### Single vs multi-threaded parallel performance

Single-connection mode is useful because:

- it better reflects the performance of one ordinary TCP flow
- it is simpler to reason about
- it avoids masking per-flow limitations

Parallel mode is useful because:

- it can better saturate high-BDP paths
- it reduces the chance that one flow's congestion window is the limiting factor
- it often exposes the maximum aggregate throughput the path can sustain

However, parallel mode also has trade-offs:

- it is less representative of single-flow applications
- it adds CPU scheduling overhead
- it may be less fair to competing traffic
- it can hide issues that matter for real single-connection workloads

## Observations

Expected behavior when running the tool:

- Larger transfers are usually more stable and repeatable than very small ones.
- Warm-up generally lowers the risk of inflated results.
- On localhost or very fast LANs, Python overhead may become a visible part of the measurement.
- Parallel mode may improve aggregate throughput, especially when one flow cannot fill the path alone.

## Limitations

- This tool measures client-to-server TCP upload throughput only.
- It does not implement reverse tests, jitter, packet loss, or latency measurement.
- It depends on OS socket buffer behavior; the requested buffer size is not guaranteed to be the exact effective size chosen by the kernel.
- Python is not a zero-overhead environment, so this is not a replacement for highly optimized native benchmarking tools.
- The client and server do not perform cross-machine clock synchronization; each side measures its own receive/send interval independently.
- Parallel mode uses threads and blocking sockets for simplicity rather than `asyncio` or zero-copy APIs.

## Suggested Result Interpretation

For credible results:

1. Use at least tens or hundreds of megabytes of measured data.
2. Keep warm-up enabled.
3. Run multiple trials and compare variance.
4. Compare single and parallel modes.
5. Test on the actual network path of interest rather than only on localhost.

## Example Result Snippet

```json
{
  "test_id": "bwtest-20260420T120000.000000Z",
  "mode": "single",
  "parallel_connections": 1,
  "configured_total_bytes": 104857600,
  "measured_total_bytes": 100663296,
  "aggregate_duration_seconds": 0.812345,
  "aggregate_throughput_mbps": 991.122,
  "aggregate_throughput_MBps": 118.150
}
```

The exact values will vary by host CPU, NIC speed, kernel tuning, RTT, and background traffic.
