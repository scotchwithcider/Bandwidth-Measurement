import argparse
import json
import socket
import threading
import time
from typing import Tuple

from utils import (
    DEFAULT_SOCKET_BUFFER,
    ResultLogger,
    build_result,
    configure_socket_buffers,
    recv_control,
    timed_receive,
    utc_now_iso,
)


def handle_client(conn: socket.socket, addr: Tuple[str, int], logger: ResultLogger, verbose: bool) -> None:
    peer = f"{addr[0]}:{addr[1]}"
    try:
        config = recv_control(conn)
        total_bytes = int(config["total_bytes"])
        warmup_bytes = int(config["warmup_bytes"])
        measured_bytes = max(total_bytes - warmup_bytes, 0)
        chunk_size = int(config["chunk_size"])
        socket_buffer = int(config["socket_buffer"])
        mode = config["mode"]
        test_id = config["test_id"]
        connection_index = int(config["connection_index"])
        recv_size = max(chunk_size, 64 * 1024)

        configure_socket_buffers(conn, socket_buffer)

        warmup_received = timed_receive(conn, warmup_bytes, recv_size)
        if warmup_received != warmup_bytes:
            raise ConnectionError(
                f"Warm-up incomplete from {peer}: expected {warmup_bytes}, received {warmup_received}"
            )

        started_at = utc_now_iso()
        start = time.perf_counter()
        received = timed_receive(conn, measured_bytes, recv_size)
        end = time.perf_counter()
        finished_at = utc_now_iso()
        if received != measured_bytes:
            raise ConnectionError(
                f"Measured transfer incomplete from {peer}: expected {measured_bytes}, received {received}"
            )

        result = build_result(
            test_id=test_id,
            role="server",
            mode=mode,
            connection_index=connection_index,
            total_bytes=total_bytes,
            measured_bytes=received,
            warmup_bytes=warmup_bytes,
            duration_seconds=end - start,
            chunk_size=chunk_size,
            socket_buffer=socket_buffer,
            peer=peer,
            started_at_utc=started_at,
            finished_at_utc=finished_at,
        )
        logger.write(result)
        conn.sendall(json.dumps({"status": "ok"}).encode("utf-8"))

        if verbose:
            print(
                json.dumps(
                    {
                        "event": "transfer_complete",
                        "peer": peer,
                        "test_id": test_id,
                        "mode": mode,
                        "connection_index": connection_index,
                        "measured_bytes": received,
                        "duration_seconds": round(result.duration_seconds, 6),
                        "throughput_mbps": round(result.throughput_mbps, 3),
                        "throughput_MBps": round(result.throughput_MBps, 3),
                    }
                )
            )
    except Exception as exc:  # noqa: BLE001
        if verbose:
            print(json.dumps({"event": "client_error", "peer": peer, "error": str(exc)}))
    finally:
        conn.close()


def run_server(host: str, port: int, socket_buffer: int, log_path: str, verbose: bool) -> None:
    logger = ResultLogger(log_path)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_sock:
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        configure_socket_buffers(server_sock, socket_buffer)
        server_sock.bind((host, port))
        server_sock.listen()
        print(json.dumps({"event": "server_listening", "host": host, "port": port, "log_path": log_path}))

        while True:
            conn, addr = server_sock.accept()
            worker = threading.Thread(
                target=handle_client,
                args=(conn, addr, logger, verbose),
                daemon=True,
            )
            worker.start()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TCP bandwidth measurement server")
    parser.add_argument("--host", default="0.0.0.0", help="Host/IP to bind")
    parser.add_argument("--port", type=int, default=5001, help="Port to listen on")
    parser.add_argument(
        "--socket-buffer",
        type=int,
        default=DEFAULT_SOCKET_BUFFER,
        help="Requested OS socket send/receive buffer size in bytes",
    )
    parser.add_argument(
        "--log",
        default="server_results.jsonl",
        help="Structured results output path (.jsonl or .csv)",
    )
    parser.add_argument("--verbose", action="store_true", help="Print per-transfer summaries")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_server(
        host=args.host,
        port=args.port,
        socket_buffer=args.socket_buffer,
        log_path=args.log,
        verbose=args.verbose,
    )
