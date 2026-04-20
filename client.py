import argparse
import json
import socket
import threading
import time
from typing import Dict, List

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
    if total_bytes <= warmup_bytes:
        raise ValueError("total_bytes must be larger than warmup_bytes so measured data remains")

    logger = ResultLogger(log_path)
    test_id = make_test_id()
    mode = "parallel" if parallel > 1 else "single"
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
    print(
        json.dumps(
            {
                "test_id": test_id,
                "mode": mode,
                "parallel_connections": parallel,
                "configured_total_bytes": total_bytes,
                "measured_total_bytes": summary["measured_bytes"],
                "aggregate_duration_seconds": round(summary["duration_seconds"], 6),
                "aggregate_throughput_mbps": round(summary["throughput_mbps"], 3),
                "aggregate_throughput_MBps": round(summary["throughput_MBps"], 3),
                "per_connection_results": sorted(results, key=lambda item: item["connection_index"]),
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
        "--log",
        default="client_results.jsonl",
        help="Structured results output path (.jsonl or .csv)",
    )
    parser.add_argument("--verbose", action="store_true", help="Print per-connection summaries")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
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
