#!/usr/bin/env node
// Node.js sample client for the Rust assetstudio_ffi_worker pool bridge.
//
// Zero dependencies: speaks the worker's length-prefixed (u64 LE) JSON frame
// protocol over stdin/stdout, so no native FFI module is needed. Build the
// worker first: cargo build --release --bin assetstudio_ffi_worker

import { spawn } from "node:child_process";
import { once } from "node:events";
import { readFile, unlink } from "node:fs/promises";
import { resolve, dirname } from "node:path";
import { parseArgs } from "node:util";

const MAX_FRAME_SIZE = 256 * 1024 * 1024;

class FrameReader {
  constructor(stream) {
    this.stream = stream;
    this.chunks = [];
    this.length = 0;
  }

  async read() {
    const header = await this.take(8);
    const size = header.readBigUInt64LE(0);
    if (size > BigInt(MAX_FRAME_SIZE)) {
      throw new Error(`worker frame too large: ${size}`);
    }
    return this.take(Number(size));
  }

  async take(size) {
    for (;;) {
      if (this.length >= size) {
        const buffer = Buffer.concat(this.chunks, this.length);
        this.chunks = size < buffer.length ? [buffer.subarray(size)] : [];
        this.length = buffer.length - size;
        return buffer.subarray(0, size);
      }
      const [chunk] = await Promise.race([
        once(this.stream, "data"),
        once(this.stream, "end").then(() => {
          throw new Error("worker closed stdout");
        }),
      ]);
      this.chunks.push(chunk);
      this.length += chunk.length;
    }
  }
}

export class AssetStudioWorker {
  constructor(workerPath, ffiLibrary) {
    workerPath = resolve(workerPath);
    ffiLibrary = resolve(ffiLibrary);
    this.nextId = 1;
    this.queue = Promise.resolve();
    this.proc = spawn(workerPath, ["--server", "--ffi-library", ffiLibrary], {
      cwd: dirname(ffiLibrary),
      stdio: ["pipe", "pipe", "inherit"],
    });
    this.reader = new FrameReader(this.proc.stdout);
  }

  call(operation, request) {
    // Serialize calls per worker: the protocol is strictly request/response.
    const result = this.queue.then(() => this.dispatch(operation, request));
    this.queue = result.catch(() => {});
    return result;
  }

  async dispatch(operation, request) {
    const id = this.nextId;
    this.nextId += 1;
    const frame = Buffer.from(JSON.stringify({ id, request: { operation, request } }), "utf-8");
    const header = Buffer.alloc(8);
    header.writeBigUInt64LE(BigInt(frame.length), 0);
    this.proc.stdin.write(header);
    this.proc.stdin.write(frame);

    const response = JSON.parse((await this.reader.read()).toString("utf-8"));
    if (response.id !== id) {
      throw new Error(`worker response id mismatch: expected ${id}, got ${response.id}`);
    }
    if (response.error) {
      throw new Error(response.error);
    }
    const payloadLen = response.payload_len ?? 0;
    let payload = Buffer.alloc(0);
    if (response.payload_file) {
      // Large payloads are spilled to a temp file; the client owns its cleanup.
      payload = await readFile(response.payload_file);
      await unlink(response.payload_file).catch(() => {});
    } else if (payloadLen > 0) {
      payload = await this.reader.read();
    }
    if (payload.length !== payloadLen) {
      throw new Error(`worker payload length mismatch: expected ${payloadLen}, got ${payload.length}`);
    }
    return { response, payload };
  }

  async close() {
    this.proc.stdin.end();
    if (this.proc.exitCode === null) {
      await once(this.proc, "exit");
    }
  }
}

export class AssetStudioWorkerPool {
  constructor(workerPath, ffiLibrary, size) {
    this.workers = Array.from({ length: Math.max(1, size) }, () => new AssetStudioWorker(workerPath, ffiLibrary));
    this.free = [...this.workers];
    this.waiters = [];
  }

  async acquire() {
    if (this.free.length === 0) {
      await new Promise((resolveWaiter) => this.waiters.push(resolveWaiter));
    }
    return this.free.pop();
  }

  release(worker) {
    this.free.push(worker);
    const waiter = this.waiters.shift();
    if (waiter) {
      waiter();
    }
  }

  async close() {
    await Promise.all(this.workers.map((worker) => worker.close()));
  }
}

function responseBody(output, expected) {
  const outer = output.response.response ?? {};
  if (outer.operation !== expected) {
    throw new Error(`unexpected worker response: ${outer.operation}, wanted ${expected}`);
  }
  const body = outer.response ?? {};
  if (!body.success) {
    throw new Error(body.error ?? `${expected} failed`);
  }
  return body;
}

async function openContext(worker, bundle, unityVersion) {
  const output = await worker.call("context_open", {
    input_path: bundle,
    asset_types: [],
    unity_version: unityVersion,
    filter_exclude_mode: false,
    filter_with_regex: false,
    filter_by_name: null,
    filter_by_container: null,
    filter_by_path_ids: [],
    load_all_assets: true,
    include_assets: false,
  });
  return responseBody(output, "context_open").context_id;
}

async function listAllObjects(worker, contextId) {
  const assets = [];
  let offset = 0;
  for (;;) {
    const output = await worker.call("context_list_objects", { context_id: contextId, offset, limit: 2048 });
    const body = responseBody(output, "context_list_objects");
    assets.push(...(body.assets ?? []));
    if (body.next_offset === null || body.next_offset === undefined) {
      return assets;
    }
    offset = body.next_offset;
  }
}

async function readTexture2D(worker, contextId, assets) {
  const textures = assets.filter((asset) => asset.type === "Texture2D");
  // Upper bound for the packed payload block; compressed GPU formats can expand
  // up to ~16x when decoded to raw RGBA. Above the worker's spill threshold this
  // makes the worker stream payloads through a sparse temp file instead of
  // memory. 0 keeps the in-memory path.
  const payloadCapacityHint = textures.reduce(
    (total, asset) => total + Math.max(0, asset.size ?? 0) * 16 + 1024 * 1024,
    0
  );
  const output = await worker.call("context_read_objects", {
    context_id: contextId,
    objects: textures.map((asset) => ({ path_id: asset.path_id, kind: "image", image_format: "raw_rgba" })),
    payload_capacity_hint: payloadCapacityHint,
  });
  const body = responseBody(output, "context_read_objects");
  return {
    requested: textures.length,
    payload_len: output.payload.length,
    reads: (body.reads ?? []).map((read) => ({
      path_id: read.asset?.path_id,
      success: read.success,
      payload_kind: read.payload_kind,
      payload_len: read.payload_len,
      error: read.error,
    })),
  };
}

async function main() {
  const { values } = parseArgs({
    options: {
      "ffi-library": { type: "string" },
      "ffi-worker": { type: "string", default: "target/release/assetstudio_ffi_worker" },
      bundle: { type: "string" },
      "unity-version": { type: "string", default: "2022.3.21f1" },
      "pool-size": { type: "string", default: "2" },
      "read-images": { type: "boolean", default: false },
    },
  });
  if (!values["ffi-library"] || !values.bundle) {
    console.error(
      "usage: node assetstudio_worker_pool.mjs --ffi-library <path> --bundle <path> [--ffi-worker <path>] [--pool-size <n>] [--read-images]"
    );
    return 2;
  }

  const pool = new AssetStudioWorkerPool(values["ffi-worker"], values["ffi-library"], Number(values["pool-size"]));
  try {
    const worker = await pool.acquire();
    try {
      const contextId = await openContext(worker, resolve(values.bundle), values["unity-version"]);
      try {
        const assets = await listAllObjects(worker, contextId);
        const types = {};
        for (const asset of assets) {
          types[asset.type ?? ""] = (types[asset.type ?? ""] ?? 0) + 1;
        }
        const result = { asset_count: assets.length, types };
        if (values["read-images"]) {
          result.image_reads = await readTexture2D(worker, contextId, assets);
        }
        console.log(JSON.stringify(result, null, 2));
      } finally {
        await worker.call("context_close", { context_id: contextId });
      }
    } finally {
      pool.release(worker);
    }
  } finally {
    await pool.close();
  }
  return 0;
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().then(
    (code) => {
      process.exitCode = code;
    },
    (error) => {
      console.error(error);
      process.exitCode = 1;
    }
  );
}
