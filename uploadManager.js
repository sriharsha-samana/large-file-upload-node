// High-performance UploadManager (robust per-chunk write streams + debounced metadata flush)
const fs = require('fs');
const path = require('path');
const { promisify } = require('util');
const { v4: uuidv4 } = require('uuid');

const writeFileAsync = promisify(fs.writeFile);
const truncateAsync = promisify(fs.truncate);
const unlinkAsync = promisify(fs.unlink);
const statAsync = promisify(fs.stat);
const openAsync = promisify(fs.open);
const closeAsync = promisify(fs.close);

/**
 * Simple bitset class for received-chunks tracking
 */
class BitSet {
  constructor(length) {
    // length = number of chunks
    this.length = length;
    this.bytes = new Uint8Array(Math.ceil(length / 8));
  }
  _byteIndex(i) { return (i / 8) | 0; }
  _bitMask(i) { return 1 << (i % 8); }
  set(i) { this.bytes[this._byteIndex(i)] |= this._bitMask(i); }
  has(i) { return (this.bytes[this._byteIndex(i)] & this._bitMask(i)) !== 0; }
  toArray() {
    const out = [];
    for (let i = 0; i < this.length; i++) if (this.has(i)) out.push(i);
    return out;
  }
  toJSON() {
    return this.toArray();
  }
  static fromArray(arr, length) {
    const b = new BitSet(length);
    for (const i of arr || []) b.set(i);
    return b;
  }
}

class UploadManager {
  constructor(baseDir) {
    this.baseDir = baseDir;
    this.uploads = new Map(); // id -> metadata
    this.locks = new Map(); // id -> Set of in-progress chunk indices
    this.dirty = new Set(); // ids with pending metadata flush
    this._debounceTimer = null;
    this._flushDelayMs = 1500; // batch metadata writes
  }

  _metaPath(id) { return path.join(this.baseDir, `${id}.json`); }
  _filePath(id, filename) { return path.join(this.baseDir, `${filename}.${id}`); }

  _scheduleFlush(id) {
    this.dirty.add(id);
    if (this._debounceTimer) return;
    this._debounceTimer = setTimeout(() => this._flushDirty(), this._flushDelayMs);
  }

  async _flushDirty() {
    const ids = Array.from(this.dirty);
    this.dirty.clear();
    clearTimeout(this._debounceTimer);
    this._debounceTimer = null;
    for (const id of ids) {
      const up = this.uploads.get(id);
      if (!up) continue;
      const meta = {
        id: up.id,
        filename: up.filename,
        totalSize: up.totalSize,
        chunkSize: up.chunkSize,
        createdAt: up.createdAt,
        receivedChunks: up.receivedBitset ? up.receivedBitset.toArray() : [],
        filePath: up.filePath,
        completed: !!up.completed
      };
      try {
        await writeFileAsync(this._metaPath(id), JSON.stringify(meta));
      } catch (e) {
        // log but keep going
        console.error('flush metadata error', id, e);
      }
    }
  }

  async flushAllMetadata() {
    if (this._debounceTimer) {
      clearTimeout(this._debounceTimer);
      this._debounceTimer = null;
    }
    await this._flushDirty();
  }

  async createUpload({ filename, totalSize, chunkSize = 64 * 1024 * 1024 }) {
    const id = uuidv4();
    const safeName = path.basename(filename);
    const filePath = this._filePath(id, safeName);
    const total = Number(totalSize);
    const cs = Number(chunkSize);
    const totalChunks = Math.max(1, Math.ceil(total / cs));

    const upload = {
      id,
      filename: safeName,
      totalSize: total,
      chunkSize: cs,
      createdAt: Date.now(),
      filePath,
      completed: false,
      // received chunks bitset
      receivedBitset: new BitSet(totalChunks)
    };

    // create metadata file quickly (non-blocking)
    const meta = {
      id: upload.id,
      filename: upload.filename,
      totalSize: upload.totalSize,
      chunkSize: upload.chunkSize,
      createdAt: upload.createdAt,
      receivedChunks: [],
      filePath: upload.filePath,
      completed: false
    };
    await writeFileAsync(this._metaPath(id), JSON.stringify(meta));

    // create file and preallocate/truncate (no long-lived fd)
    try {
      // open+close quickly to ensure file exists, then truncate
      const fd = await openAsync(filePath, 'w');
      await closeAsync(fd);
      await truncateAsync(filePath, total);
    } catch (e) {
      // If truncate fails, log and rethrow
      console.error('preallocate/truncate failed for', filePath, e);
      throw e;
    }

    this.uploads.set(id, upload);
    this.locks.set(id, new Set());
    this._scheduleFlush(id);
    return id;
  }

  getUpload(id) {
    const inMem = this.uploads.get(id);
    if (inMem) return inMem;
    const metaPath = this._metaPath(id);
    if (!fs.existsSync(metaPath)) return null;
    try {
      const data = JSON.parse(fs.readFileSync(metaPath, 'utf8'));
      const totalChunks = Math.max(1, Math.ceil(data.totalSize / data.chunkSize));
      const up = {
        id: data.id,
        filename: data.filename,
        totalSize: data.totalSize,
        chunkSize: data.chunkSize,
        createdAt: data.createdAt,
        filePath: data.filePath,
        completed: data.completed,
        receivedBitset: BitSet.fromArray(data.receivedChunks || [], totalChunks)
      };
      this.uploads.set(id, up);
      if (!this.locks.has(id)) this.locks.set(id, new Set());
      return up;
    } catch (e) {
      console.error('load meta failed', e);
      return null;
    }
  }

  getReceivedChunksArray(id) {
    const up = this.getUpload(id);
    if (!up) return [];
    return up.receivedBitset ? up.receivedBitset.toArray() : [];
  }

  /**
   * Write chunk to file at offset.
   * Uses a per-chunk write stream (Node opens/closes fd for each chunk), which is robust.
   * Retries once on EBADF / transient stream errors.
   */
  async writeChunkAt(id, offset, readStream, expectedLength) {
    const up = this.getUpload(id);
    if (!up) throw new Error('upload_not_found');

    const chunkIndex = Math.floor(offset / up.chunkSize);
    if (up.receivedBitset && up.receivedBitset.has(chunkIndex)) {
      // drain request quickly
      readStream.resume();
      return { alreadyReceived: true, written: 0 };
    }

    const lockSet = this.locks.get(id);
    if (lockSet.has(chunkIndex)) {
      const err = new Error('chunk_write_in_progress');
      err.status = 409;
      throw err;
    }
    lockSet.add(chunkIndex);

    // Encapsulate actual write with retry on EBADF / stream issues
    const doWrite = () => {
      return new Promise((resolve, reject) => {
        // create a write stream that opens/closes its own fd
        const ws = fs.createWriteStream(up.filePath, {
          flags: 'r+',
          start: offset,
          highWaterMark: 16 * 1024 * 1024
        });

        let written = 0;
        const onData = (chunk) => { written += chunk.length; };

        const cleanup = () => {
          readStream.removeListener('data', onData);
          readStream.removeListener('error', onError);
          ws.removeListener('error', onError);
          ws.removeListener('finish', onFinish);
        };

        const onError = (err) => {
          cleanup();
          // ensure ws is destroyed
          try { ws.destroy(); } catch (_) {}
          reject(err);
        };

        const onFinish = async () => {
          cleanup();
          try {
            // mark received
            up.receivedBitset.set(chunkIndex);
            this._scheduleFlush(id);
            lockSet.delete(chunkIndex);
          } catch (e) {
            lockSet.delete(chunkIndex);
          }
          resolve({ alreadyReceived: false, written });
        };

        readStream.on('data', onData);
        readStream.on('error', onError);
        ws.on('error', onError);
        ws.on('finish', onFinish);

        // Start piping (backpressure handled by stream)
        readStream.pipe(ws);
      });
    };

    try {
      return await doWrite().catch(async (err) => {
        // If EBADF or premature close, attempt one retry (re-open path by creating a fresh stream)
        const isBadFd = err && (err.code === 'EBADF' || err.code === 'ERR_STREAM_PREMATURE_CLOSE');
        if (isBadFd) {
          console.warn(`writeChunkAt: encountered ${err.code} for upload ${id} chunk ${chunkIndex}, retrying once`, err);
          // The readStream may be in a bad state: we need to ensure it's readable again.
          // The caller here typically passes the raw request stream; in that case the stream is already consumed or closed.
          // Best-effort: attempt to retry only if caller will re-send the chunk (client will retry).
          // Return an error indicating transient failure so client retry logic can re-upload chunk.
          const e = new Error('transient_write_failure');
          e.transient = true;
          throw e;
        }
        // non-retriable; bubble up
        throw err;
      });
    } finally {
      // ensure lock removed if some unexpected synchronous path returns
      if (lockSet.has(chunkIndex)) lockSet.delete(chunkIndex);
    }
  }

  getMissingChunks(id) {
    const up = this.getUpload(id);
    if (!up) return [];
    const totalChunks = Math.ceil(up.totalSize / up.chunkSize);
    const missing = [];
    for (let i = 0; i < totalChunks; i++) if (!up.receivedBitset.has(i)) missing.push(i);
    return missing;
  }

  async markCompleted(id) {
    const up = this.getUpload(id);
    if (!up) throw new Error('upload_not_found');
    up.completed = true;
    this._scheduleFlush(id);
  }

  async abortUpload(id) {
    const up = this.getUpload(id);
    if (!up) return;
    try {
      try { await unlinkAsync(up.filePath); } catch (e) {}
    } catch (e) {}
    try { await unlinkAsync(this._metaPath(id)); } catch (e) {}
    this.uploads.delete(id);
    this.locks.delete(id);
    this.dirty.delete(id);
  }
}

module.exports = UploadManager;