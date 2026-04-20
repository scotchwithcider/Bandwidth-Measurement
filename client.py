import argparse
import json
import socket
import threading
import time
from typing import Dict, List, Optional

from utils import (
    DEFAULT_CHUNK_SIZE,
    DEFAULT_SOCKET_BUFFER,
    DEFAULT_WARMUP_BYTES,
    ResultLogger,
    allocate_payload,
    build_result,
    configure_socket_buffers,
    make_test_id,
    parse_size,
    send_control,
    timed_send,
    utc_now_iso,
)


def connection_worker(
    *,
    host: str,
    port: int,
    test_id: str,
    mode: str,
    connection_index: int,
    total_bytes: int,
    warmup_bytes: int,
    chunk_size: int,
    socket_buffer: int,
    payload: bytes,
    results: List[Dict[str, float]],
    logger: ResultLogger,
    verbose: bool,
) -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        configure_socket_buffers(sock, socket_buffer)
        sock.connect((host, port))
        peer = f"{host}:{port}"

        send_control(
            sock,
            {
                "test_id": test_id,
                "connection_index": connection_index,
                "total_bytes": total_bytes,
                "warmup_bytes": warmup_bytes,
                "chunk_size": chunk_size,
                "socket_buffer": socket_buffer,
                "mode": mode,
            },
        )

        timed_send(sock, warmup_bytes, payload)

        started_at = utc_now_iso()
        start = time.perf_counter()
        timed_send(sock, total_bytes - warmup_bytes, payload)
        end = time.perf_counter()
        finished_at = utc_now_iso()

        ack = sock.recv(256)
        if not ack:
            raise ConnectionError("Server closed without acknowledging transfer")

        result = build_result(
            test_id=test_id,
            role="client",
            mode=mode,
            connection_index=connection_index,
            total_bytes=total_bytes,
            measured_bytes=total_bytes - warmup_bytes,
            warmup_bytes=warmup_bytes,
            duration_seconds=end - start,
            chunk_size=chunk_size,
            socket_buffer=socket_buffer,
            peer=peer,
            started_at_utc=started_at,
            finished_at_utc=finished_at,
        )
        logger.write(result)
        results.append(
            {
                "connection_index": connection_index,
                "duration_seconds": result.duration_seconds,
                "measured_bytes": result.measured_bytes,
                "measured_start_perf": start,
                "measured_end_perf": end,
                "throughput_mbps": result.throughput_mbps,
                "throughput_MBps": result.throughput_MBps,
            }
        )

        if verbose:
            print(
                json.dumps(
                    {
                        "event": "connection_complete",
                        "connection_index": connection_index,
                        "duration_seconds": round(result.duration_seconds, 6),
                        "throughput_mbps": round(result.throughput_mbps, 3),
                        "throughput_MBps": round(result.throughput_MBps, 3),
                    }
                )
            )


def aggregate_parallel_results(results: List[Dict[str, float]], wall_time: float) -> Dict[str, float]:
    if not results:
        return {"duration_seconds": 0.0, "measured_bytes": 0, "throughput_mbps": 0.0, "throughput_MBps": 0.0}
    total_bytes = sum(int(item["measured_bytes"]) for item in results)
    mbps = (total_bytes * 8 / wall_time) / 1_000_000 if wall_time > 0 else 0.0
    MBps = total_bytes / wall_time / (1024 * 1024) if wall_time > 0 else 0.0
    return {
        "duration_seconds": wall_time,
        "measured_bytes": total_bytes,
        "throughput_mbps": mbps,
        "throughput_MBps": MBps,
    }


def split_bytes(total_bytes: int, parts: int) -> List[int]:
    base, remainder = divmod(total_bytes, parts)
    return [base + (1 if index < remainder else 0) for index in range(parts)]


def detect_saturation(connections: List[int], speeds_mbps: List[float], threshold: float) -> int:
    if not connections or not speeds_mbps or len(connections) != len(speeds_mbps):
        raise ValueError("connections and speeds_mbps must be non-empty lists of equal length")
    for index in range(1, len(speeds_mbps)):
        previous_speed = speeds_mbps[index - 1]
        current_speed = speeds_mbps[index]
        if previous_speed <= 0:
            continue
        improvement = (current_speed - previous_speed) / previous_speed
        if improvement < threshold:
            return connections[index]
    return connections[-1]


def perform_test(
    *,
    host: str,
    port: int,
    total_bytes: int,
    warmup_bytes: int,
    chunk_size: int,
    socket_buffer: int,
    parallel: int,
    log_path: str,
    verbose: bool,
    mode_override: Optional[str] = None,
) -> Dict[str, object]:
    if total_bytes <= warmup_bytes:
        raise ValueError("total_bytes must be larger than warmup_bytes so measured data remains")

    logger = ResultLogger(log_path)
    test_id = make_test_id()
    mode = mode_override or ("parallel" if parallel > 1 else "single")
    payload = allocate_payload(chunk_size)
    results: List[Dict[str, float]] = []
    threads: List[threading.Thread] = []
    errors: List[BaseException] = []
    results_lock = threading.Lock()

    if parallel < 1:
        raise ValueError("parallel must be at least 1")
    if total_bytes < parallel:
        raise ValueError("total_bytes must be at least as large as the number of parallel connections")

    total_splits = split_bytes(total_bytes, parallel)
    warmup_splits = split_bytes(warmup_bytes, parallel)

    def wrapped_worker(index: int, conn_bytes: int, conn_warmup: int) -> None:
        try:
            local_results: List[Dict[str, float]] = []
            connection_worker(
                host=host,
                port=port,
                test_id=test_id,
                mode=mode,
                connection_index=index,
                total_bytes=conn_bytes,
                warmup_bytes=conn_warmup,
                chunk_size=chunk_size,
                socket_buffer=socket_buffer,
                payload=payload,
                results=local_results,
                logger=logger,
                verbose=verbose,
            )
            with results_lock:
                results.extend(local_results)
        except BaseException as exc:  # noqa: BLE001
            with results_lock:
                errors.append(exc)

    for idx, (conn_bytes, conn_warmup) in enumerate(zip(total_splits, warmup_splits)):
        thread = threading.Thread(target=wrapped_worker, args=(idx, conn_bytes, conn_warmup))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    if errors:
        raise RuntimeError(f"One or more connections failed: {[str(err) for err in errors]}")

    measured_starts = [float(item["measured_start_perf"]) for item in results if int(item["measured_bytes"]) > 0]
    measured_ends = [float(item["measured_end_perf"]) for item in results if int(item["measured_bytes"]) > 0]
    measured_wall_time = max(measured_ends) - min(measured_starts) if measured_starts and measured_ends else 0.0
    summary = aggregate_parallel_results(results, measured_wall_time)
    return {
        "test_id": test_id,
        "mode": mode,
        "parallel_connections": parallel,
        "configured_total_bytes": total_bytes,
        "measured_total_bytes": summary["measured_bytes"],
        "aggregate_duration_seconds": round(summary["duration_seconds"], 6),
        "aggregate_throughput_mbps": round(summary["throughput_mbps"], 3),
        "aggregate_throughput_MBps": round(summary["throughput_MBps"], 3),
        "per_connection_results": sorted(results, key=lambda item: item["connection_index"]),
    }


def run_client(
    host: str,
    port: int,
    total_bytes: int,
    warmup_bytes: int,
    chunk_size: int,
    socket_buffer: int,
    parallel: int,
    log_path: str,
    verbose: bool,
) -> None:
    summary = perform_test(
        host=host,
        port=port,
        total_bytes=total_bytes,
        warmup_bytes=warmup_bytes,
        chunk_size=chunk_size,
        socket_buffer=socket_buffer,
        parallel=parallel,
        log_path=log_path,
        verbose=verbose,
    )
    print(json.dumps(summary, indent=2))


def run_saturation_scan(
    *,
    host: str,
    port: int,
    total_bytes: int,
    warmup_bytes: int,
    chunk_size: int,
    socket_buffer: int,
    max_parallel: int,
    saturation_threshold: float,
    log_path: str,
    verbose: bool,
) -> None:
    if max_parallel < 1:
        raise ValueError("max_parallel must be at least 1")

    connections: List[int] = []
    candidate = 1
    while candidate <= max_parallel:
        connections.append(candidate)
        candidate *= 2
    if connections[-1] != max_parallel:
        connections.append(max_parallel)

    scan_results: List[Dict[str, object]] = []
    speeds_mbps: List[float] = []

    for parallel in connections:
        summary = perform_test(
            host=host,
            port=port,
            total_bytes=total_bytes,
            warmup_bytes=warmup_bytes,
            chunk_size=chunk_size,
            socket_buffer=socket_buffer,
            parallel=parallel,
            log_path=log_path,
            verbose=verbose,
            mode_override="saturation_scan",
        )
        scan_results.append(summary)
        speeds_mbps.append(float(summary["aggregate_throughput_mbps"]))

    saturation_connections = detect_saturation(connections, speeds_mbps, saturation_threshold)
    max_summary = max(scan_results, key=lambda item: float(item["aggregate_throughput_mbps"]))

    print("Saturation scan results:")
    for summary in scan_results:
        print(
            f"Connections: {summary['parallel_connections']} -> "
            f"{summary['aggregate_throughput_mbps']} Mbps ({summary['aggregate_throughput_MBps']} MB/s)"
        )

    print(f"Network saturation reached at {saturation_connections} connections")
    print(f"Estimated max bandwidth: ~{max_summary['aggregate_throughput_mbps']} Mbps")
    print(
        json.dumps(
            {
                "mode": "saturation_scan",
                "threshold": saturation_threshold,
                "saturation_connections": saturation_connections,
                "estimated_max_bandwidth_mbps": max_summary["aggregate_throughput_mbps"],
                "estimated_max_bandwidth_MBps": max_summary["aggregate_throughput_MBps"],
                "scan_results": scan_results,
            },
            indent=2,
        )
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TCP bandwidth measurement client")
    parser.add_argument("--host", required=True, help="Server hostname or IP")
    parser.add_argument("--port", type=int, default=5001, help="Server port")
    parser.add_argument(
        "--size",
        default="100MiB",
        help="Total bytes to send across all connections (examples: 100MiB, 500MB)",
    )
    parser.add_argument(
        "--warmup",
        default=str(DEFAULT_WARMUP_BYTES),
        help="Warm-up bytes per test before measurement starts",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=DEFAULT_CHUNK_SIZE,
        help="Application send chunk size in bytes",
    )
    parser.add_argument(
        "--socket-buffer",
        type=int,
        default=DEFAULT_SOCKET_BUFFER,
        help="Requested OS socket send/receive buffer size in bytes",
    )
    parser.add_argument(
        "--parallel",
        type=int,
        default=1,
        help="Number of parallel TCP connections",
    )
    parser.add_argument(
        "--scan-saturation",
        action="store_true",
        help="Run stepped connection counts and detect when added parallelism stops helping",
    )
    parser.add_argument(
        "--max-parallel",
        type=int,
        default=8,
        help="Maximum connection count to test during saturation scans",
    )
    parser.add_argument(
        "--saturation-threshold",
        type=float,
        default=0.05,
        help="Minimum relative throughput gain required to treat more parallelism as beneficial",
    )
    parser.add_argument(
        "--log",
        default="client_results.jsonl",
        help="Structured results output path (.jsonl or .csv)",
    )
    parser.add_argument("--verbose", action="store_true", help="Print per-connection summaries")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.scan_saturation:
        run_saturation_scan(
            host=args.host,
            port=args.port,
            total_bytes=parse_size(args.size),
            warmup_bytes=parse_size(args.warmup),
            chunk_size=args.chunk_size,
            socket_buffer=args.socket_buffer,
            max_parallel=args.max_parallel,
            saturation_threshold=args.saturation_threshold,
            log_path=args.log,
            verbose=args.verbose,
        )
    else:
        run_client(
            host=args.host,
            port=args.port,
            total_bytes=parse_size(args.size),
            warmup_bytes=parse_size(args.warmup),
            chunk_size=args.chunk_size,
            socket_buffer=args.socket_buffer,
            parallel=args.parallel,
            log_path=args.log,
            verbose=args.verbose,
        )
