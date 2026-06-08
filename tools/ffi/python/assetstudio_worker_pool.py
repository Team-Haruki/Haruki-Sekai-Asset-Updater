#!/usr/bin/env python3
"""Python sample client for the Rust assetstudio_ffi_worker pool bridge."""

from __future__ import annotations

import argparse
import json
import queue
import struct
import subprocess
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class WorkerResponse:
    response: dict[str, Any]
    payload: bytes


class AssetStudioWorker:
    def __init__(self, worker_path: str | Path, ffi_library: str | Path):
        self._next_id = 1
        self._lock = threading.Lock()
        worker_path = Path(worker_path).resolve()
        ffi_library = Path(ffi_library).resolve()
        self.proc = subprocess.Popen(
            [
                str(worker_path),
                "--server",
                "--ffi-library",
                str(ffi_library),
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=Path(ffi_library).resolve().parent,
        )

    def call(self, operation: str, request: dict[str, Any]) -> WorkerResponse:
        with self._lock:
            request_id = self._next_id
            self._next_id += 1
            frame = json.dumps(
                {
                    "id": request_id,
                    "request": {
                        "operation": operation,
                        "request": request,
                    },
                },
                separators=(",", ":"),
            ).encode("utf-8")
            self._write_frame(frame)
            response = json.loads(self._read_frame())
            if response.get("id") != request_id:
                raise RuntimeError(
                    f"worker response id mismatch: expected {request_id}, got {response.get('id')}"
                )
            if response.get("error"):
                raise RuntimeError(response["error"])
            payload_len = int(response.get("payload_len") or 0)
            payload = b""
            if response.get("payload_file"):
                payload_path = Path(response["payload_file"])
                payload = payload_path.read_bytes()
                payload_path.unlink(missing_ok=True)
            elif payload_len:
                payload = self._read_frame()
            if len(payload) != payload_len:
                raise RuntimeError(
                    f"worker payload length mismatch: expected {payload_len}, got {len(payload)}"
                )
            return WorkerResponse(response=response, payload=payload)

    def close(self) -> None:
        if self.proc.stdin:
            try:
                self.proc.stdin.close()
            except BrokenPipeError:
                pass
        try:
            self.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()

    def _write_frame(self, payload: bytes) -> None:
        if self.proc.stdin is None:
            raise RuntimeError("worker stdin is closed")
        self.proc.stdin.write(struct.pack("<Q", len(payload)))
        self.proc.stdin.write(payload)
        self.proc.stdin.flush()

    def _read_frame(self) -> bytes:
        if self.proc.stdout is None:
            raise RuntimeError("worker stdout is closed")
        header = self.proc.stdout.read(8)
        if len(header) != 8:
            stderr = (
                self.proc.stderr.read().decode("utf-8", errors="replace")
                if self.proc.stderr
                else ""
            )
            raise RuntimeError(f"worker closed stdout: {stderr.strip()}")
        size = struct.unpack("<Q", header)[0]
        payload = self.proc.stdout.read(size)
        if len(payload) != size:
            raise RuntimeError(f"truncated worker frame: expected {size}, got {len(payload)}")
        return payload


class WorkerLease:
    def __init__(self, pool: "AssetStudioWorkerPool", worker: AssetStudioWorker):
        self._pool = pool
        self.worker = worker
        self._released = False

    def __enter__(self) -> AssetStudioWorker:
        return self.worker

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()

    def release(self) -> None:
        if not self._released:
            self._released = True
            self._pool.release(self.worker)


class AssetStudioWorkerPool:
    def __init__(self, worker_path: str | Path, ffi_library: str | Path, size: int):
        self._workers: queue.LifoQueue[AssetStudioWorker] = queue.LifoQueue()
        self._all_workers = [
            AssetStudioWorker(worker_path, ffi_library) for _ in range(max(1, size))
        ]
        for worker in self._all_workers:
            self._workers.put(worker)

    def acquire(self) -> WorkerLease:
        return WorkerLease(self, self._workers.get())

    def release(self, worker: AssetStudioWorker) -> None:
        self._workers.put(worker)

    def close(self) -> None:
        for worker in self._all_workers:
            worker.close()


def response_body(output: WorkerResponse, expected: str) -> dict[str, Any]:
    outer = output.response.get("response") or {}
    if outer.get("operation") != expected:
        raise RuntimeError(f"unexpected worker response: {outer.get('operation')}, wanted {expected}")
    body = outer.get("response") or {}
    if not body.get("success", False):
        raise RuntimeError(body.get("error") or f"{expected} failed")
    return body


def open_context(worker: AssetStudioWorker, bundle: str, unity_version: str) -> int:
    output = worker.call(
        "context_open",
        {
            "input_path": bundle,
            "asset_types": [],
            "unity_version": unity_version,
            "filter_exclude_mode": False,
            "filter_with_regex": False,
            "filter_by_name": None,
            "filter_by_container": None,
            "filter_by_path_ids": [],
            "load_all_assets": True,
            "include_assets": False,
        },
    )
    return int(response_body(output, "context_open")["context_id"])


def list_all_objects(worker: AssetStudioWorker, context_id: int) -> list[dict[str, Any]]:
    assets: list[dict[str, Any]] = []
    offset = 0
    while True:
        output = worker.call(
            "context_list_objects",
            {"context_id": context_id, "offset": offset, "limit": 2048},
        )
        body = response_body(output, "context_list_objects")
        assets.extend(body.get("assets") or [])
        next_offset = body.get("next_offset")
        if next_offset is None:
            return assets
        offset = int(next_offset)


def read_texture2d(worker: AssetStudioWorker, context_id: int, assets: list[dict[str, Any]]) -> dict[str, Any]:
    textures = [asset for asset in assets if asset.get("type") == "Texture2D"]
    output = worker.call(
        "context_read_objects",
        {
            "context_id": context_id,
            "objects": [
                {"path_id": asset["path_id"], "kind": "image", "image_format": "raw_rgba"}
                for asset in textures
            ],
        },
    )
    body = response_body(output, "context_read_objects")
    return {
        "requested": len(textures),
        "payload_len": len(output.payload),
        "reads": [
            {
                "path_id": read.get("asset", {}).get("path_id"),
                "success": read.get("success"),
                "payload_kind": read.get("payload_kind"),
                "payload_len": read.get("payload_len"),
                "error": read.get("error"),
            }
            for read in body.get("reads", [])
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ffi-library", required=True)
    parser.add_argument("--ffi-worker", default="target/release/assetstudio_ffi_worker")
    parser.add_argument("--bundle", required=True)
    parser.add_argument("--unity-version", default="2022.3.21f1")
    parser.add_argument("--pool-size", type=int, default=2)
    parser.add_argument("--read-images", action="store_true")
    args = parser.parse_args()

    pool = AssetStudioWorkerPool(args.ffi_worker, args.ffi_library, args.pool_size)
    try:
        with pool.acquire() as worker:
            context_id = open_context(worker, str(Path(args.bundle).resolve()), args.unity_version)
            try:
                assets = list_all_objects(worker, context_id)
                types: dict[str, int] = {}
                for asset in assets:
                    types[asset.get("type") or ""] = types.get(asset.get("type") or "", 0) + 1
                result: dict[str, Any] = {"asset_count": len(assets), "types": types}
                if args.read_images:
                    result["image_reads"] = read_texture2d(worker, context_id, assets)
                print(json.dumps(result, indent=2, ensure_ascii=False))
            finally:
                worker.call("context_close", {"context_id": context_id})
    finally:
        pool.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
