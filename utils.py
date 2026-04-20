import csv
import json
import math
import os
import socket
import struct
import threading
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional


HEADER_LEN = 4
CTRL_STRUCT = struct.Struct("!I")
DEFAULT_CHUNK_SIZE = 1024 * 1024
DEFAULT_WARMUP_BYTES = 4 * 1024 * 1024
DEFAULT_SOCKET_BUFFER = 4 * 1024 * 1024
PROTOCOL_VERSION = 1


@dataclass
class TestConfig:
    test_id: str
    connection_index: int
    total_bytes: int
    warmup_bytes: int
    chunk_size: int
    socket_buffer: int
    mode: str
    protocol_version: int = PROTOCOL_VERSION


@dataclass
class ResultRecord:
    test_id: str
    role: str
    mode: str
    connection_index: int
    total_bytes: int
    measured_bytes: int
    warmup_bytes: int
    duration_seconds: float
    throughput_mbps: float
    throughput_MBps: float
    chunk_size: int
    socket_buffer: int
    peer: str
    started_at_utc: str
    finished_at_utc: str


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_test_id(prefix: str = "bwtest") -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    return f"{prefix}-{stamp}"


def compute_throughput(bytes_sent: int, duration_seconds: float) -> Dict[str, float]:
    if duration_seconds <= 0:
        return {"throughput_mbps": 0.0, "throughput_MBps": 0.0}
    bits_per_second = (bytes_sent * 8) / duration_seconds
    mbps = bits_per_second / 1_000_000
    MBps = bytes_sent / duration_seconds / (1024 * 1024)
    return {"throughput_mbps": mbps, "throughput_MBps": MBps}


def parse_size(value: str) -> int:
    units = {
        "b": 1,
        "kb": 1000,
        "mb": 1000**2,
        "gb": 1000**3,
        "kib": 1024,
        "mib": 1024**2,
        "gib": 1024**3,
    }
    raw = value.strip().lower()
    if raw.isdigit():
        return int(raw)

    number = ""
    suffix = ""
    for ch in raw:
        if ch.isdigit() or ch == ".":
            number += ch
        else:
            suffix += ch

    if not number or suffix not in units:
        raise ValueError(f"Invalid size value: {value!r}")
    return int(float(number) * units[suffix])


def human_bytes(num_bytes: int) -> str:
    if num_bytes < 1024:
        return f"{num_bytes} B"
    units = ["KiB", "MiB", "GiB", "TiB"]
    size = float(num_bytes)
    for unit in units:
        size /= 1024.0
        if size < 1024.0:
            return f"{size:.2f} {unit}"
    return f"{size:.2f} PiB"


def send_control(sock: socket.socket, payload: Dict[str, Any]) -> None:
    body = json.dumps(payload).encode("utf-8")
    sock.sendall(CTRL_STRUCT.pack(len(body)))
    sock.sendall(body)


def recv_exact(sock: socket.socket, size: int) -> bytes:
    chunks = bytearray()
    while len(chunks) < size:
        part = sock.recv(size - len(chunks))
        if not part:
            raise ConnectionError("Socket closed while reading control message")
        chunks.extend(part)
    return bytes(chunks)


def recv_control(sock: socket.socket) -> Dict[str, Any]:
    header = recv_exact(sock, HEADER_LEN)
    (body_len,) = CTRL_STRUCT.unpack(header)
    body = recv_exact(sock, body_len)
    return json.loads(body.decode("utf-8"))


def configure_socket_buffers(sock: socket.socket, size: int) -> None:
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, size)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, size)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 0)


def allocate_payload(chunk_size: int) -> bytes:
    # Reuse a single payload buffer to avoid per-send allocation overhead.
    return os.urandom(chunk_size)


def timed_send(sock: socket.socket, total_bytes: int, chunk: bytes) -> None:
    sent = 0
    chunk_len = len(chunk)
    while sent < total_bytes:
        remaining = total_bytes - sent
        sock.sendall(chunk if remaining >= chunk_len else chunk[:remaining])
        sent += min(chunk_len, remaining)


def timed_receive(sock: socket.socket, expected_bytes: int, recv_size: int) -> int:
    received = 0
    while received < expected_bytes:
        remaining = expected_bytes - received
        data = sock.recv(min(recv_size, remaining))
        if not data:
            break
        received += len(data)
    return received


class ResultLogger:
    def __init__(self, path: Optional[str]) -> None:
        self.path = path
        self._lock = threading.Lock()

    def write(self, record: ResultRecord) -> None:
        if not self.path:
            return
        os.makedirs(os.path.dirname(os.path.abspath(self.path)), exist_ok=True)
        row = asdict(record)
        ext = os.path.splitext(self.path)[1].lower()
        with self._lock:
            if ext == ".csv":
                self._write_csv(row)
            else:
                self._write_jsonl(row)

    def _write_jsonl(self, row: Dict[str, Any]) -> None:
        with open(self.path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, sort_keys=True) + "\n")

    def _write_csv(self, row: Dict[str, Any]) -> None:
        file_exists = os.path.exists(self.path)
        with open(self.path, "a", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(row.keys()))
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)


def build_result(
    *,
    test_id: str,
    role: str,
    mode: str,
    connection_index: int,
    total_bytes: int,
    measured_bytes: int,
    warmup_bytes: int,
    duration_seconds: float,
    chunk_size: int,
    socket_buffer: int,
    peer: str,
    started_at_utc: str,
    finished_at_utc: str,
) -> ResultRecord:
    rates = compute_throughput(measured_bytes, duration_seconds)
    return ResultRecord(
        test_id=test_id,
        role=role,
        mode=mode,
        connection_index=connection_index,
        total_bytes=total_bytes,
        measured_bytes=measured_bytes,
        warmup_bytes=warmup_bytes,
        duration_seconds=duration_seconds,
        throughput_mbps=rates["throughput_mbps"],
        throughput_MBps=rates["throughput_MBps"],
        chunk_size=chunk_size,
        socket_buffer=socket_buffer,
        peer=peer,
        started_at_utc=started_at_utc,
        finished_at_utc=finished_at_utc,
    )


def ceil_div(a: int, b: int) -> int:
    return math.ceil(a / b)
