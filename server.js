// server.js (CORS-enabled, returns clear error for disk-full (ENOSPC) and transient write failures)
const http = require('http');
const fs = require('fs');
const path = require('path');
const UploadManager = require('./uploadManager');

const PORT = process.env.PORT ? Number(process.env.PORT) : 4000;
const STORAGE_DIR = process.env.STORAGE_DIR || path.resolve(__dirname, 'uploads');

if (!fs.existsSync(STORAGE_DIR)) fs.mkdirSync(STORAGE_DIR, { recursive: true });

const manager = new UploadManager(STORAGE_DIR);

// small helper to parse JSON bodies for small endpoints (initiate only)
async function collectJson(req, maxBytes = 1024 * 1024) {
  return new Promise((resolve, reject) => {
    let buf = [];
    let size = 0;
    req.on('data', (chunk) => {
      size += chunk.length;
      if (size > maxBytes) {
        reject(new Error('json_body_too_large'));
        req.destroy();
        return;
      }
      buf.push(chunk);
    });
    req.on('end', () => {
      try {
        const s = Buffer.concat(buf).toString('utf8') || '{}';
        resolve(JSON.parse(s));
      } catch (err) {
        reject(err);
      }
    });
    req.on('error', reject);
  });
}

function sendJSON(res, code, obj, origin = '*') {
  const s = JSON.stringify(obj);
  const headers = {
    'Content-Type': 'application/json',
    'Content-Length': Buffer.byteLength(s),
    'Cache-Control': 'no-store'
  };
  if (origin) {
    headers['Access-Control-Allow-Origin'] = origin;
    headers['Access-Control-Allow-Credentials'] = 'true';
  }
  res.writeHead(code, headers);
  res.end(s);
}

function setCorsHeaders(res, origin = '*') {
  res.setHeader('Access-Control-Allow-Origin', origin);
  res.setHeader('Access-Control-Allow-Credentials', 'true');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,PUT,DELETE,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type,Content-Range,x-chunk-offset,x-chunk-size,Authorization');
  res.setHeader('Access-Control-Max-Age', '600');
}

// create server
const server = http.createServer((req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const pathname = url.pathname;

  const origin = req.headers.origin || '*';
  setCorsHeaders(res, origin);

  // OPTIONS preflight
  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  // Health
  if (req.method === 'GET' && pathname === '/ping') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('ok');
    return;
  }

  // POST /upload/initiate
  if (req.method === 'POST' && pathname === '/upload/initiate') {
    collectJson(req).then(async (body) => {
      const { filename, totalSize, chunkSize } = body || {};
      if (!filename || !totalSize) {
        sendJSON(res, 400, { error: 'filename and totalSize required' }, origin);
        return;
      }
      try {
        const id = await manager.createUpload({
          filename,
          totalSize: Number(totalSize),
          chunkSize: chunkSize ? Number(chunkSize) : undefined
        });
        const up = manager.getUpload(id);
        sendJSON(res, 200, { uploadId: id, chunkSize: up.chunkSize }, origin);
      } catch (err) {
        // disk full during createUpload or metadata write
        if (err && (err.code === 'ENOSPC' || err.status === 507)) {
          sendJSON(res, 507, { error: 'disk_full', message: 'No space left on device' }, origin);
          return;
        }
        console.error('initiate error', err);
        sendJSON(res, 500, { error: 'initiate_failed', details: String(err) }, origin);
      }
    }).catch((err) => {
      sendJSON(res, 400, { error: 'invalid_json', details: String(err) }, origin);
    });
    return;
  }

  // GET /upload/:id/status
  if (req.method === 'GET' && pathname.startsWith('/upload/') && pathname.endsWith('/status')) {
    const id = pathname.split('/')[2];
    const up = manager.getUpload(id);
    if (!up) {
      sendJSON(res, 404, { error: 'upload_not_found' }, origin);
      return;
    }
    const totalChunks = Math.ceil(up.totalSize / up.chunkSize);
    const received = manager.getReceivedChunksArray(id);
    sendJSON(res, 200, {
      uploadId: id,
      filename: up.filename,
      totalSize: up.totalSize,
      chunkSize: up.chunkSize,
      totalChunks,
      receivedChunks: received,
      receivedCount: received.length
    }, origin);
    return;
  }

  // POST /upload/:id/complete
  if (req.method === 'POST' && pathname.startsWith('/upload/') && pathname.endsWith('/complete')) {
    const id = pathname.split('/')[2];
    const up = manager.getUpload(id);
    if (!up) {
      sendJSON(res, 404, { error: 'upload_not_found' }, origin);
      return;
    }
    const missing = manager.getMissingChunks(id);
    if (missing.length > 0) {
      sendJSON(res, 400, { error: 'missing_chunks', missing }, origin);
      return;
    }
    try {
      manager.markCompleted(id);
      sendJSON(res, 200, { ok: true, path: up.filePath }, origin);
    } catch (err) {
      console.error('complete error', err);
      sendJSON(res, 500, { error: 'complete_failed', details: String(err) }, origin);
    }
    return;
  }

  // DELETE /upload/:id
  if (req.method === 'DELETE' && pathname.startsWith('/upload/')) {
    const id = pathname.split('/')[2];
    const up = manager.getUpload(id);
    if (!up) {
      sendJSON(res, 404, { error: 'upload_not_found' }, origin);
      return;
    }
    manager.abortUpload(id).then(() => {
      sendJSON(res, 200, { ok: true }, origin);
    }).catch((err) => {
      console.error('abort error', err);
      // disk errors rarely happen on delete; return generic 500
      sendJSON(res, 500, { error: 'abort_failed', details: String(err) }, origin);
    });
    return;
  }

  // PUT /upload/:id (chunk upload)
  if (req.method === 'PUT' && pathname.startsWith('/upload/')) {
    const id = pathname.split('/')[2];
    const up = manager.getUpload(id);
    if (!up) {
      sendJSON(res, 404, { error: 'upload_not_found' }, origin);
      req.resume(); // drain body
      return;
    }

    // parse Content-Range or x-chunk-offset
    const cr = req.headers['content-range'];
    let offset = null;
    let length = null;
    if (cr) {
      const m = cr.match(/bytes\s+(\d+)-(\d+)\/(\d+|\*)/i);
      if (!m) {
        sendJSON(res, 400, { error: 'invalid_content_range' }, origin);
        req.resume();
        return;
      }
      offset = Number(m[1]);
      length = Number(m[2]) - Number(m[1]) + 1;
    } else {
      const xoffset = req.headers['x-chunk-offset'];
      const xlen = req.headers['x-chunk-size'] || req.headers['content-length'];
      if (!xoffset || !xlen) {
        sendJSON(res, 400, { error: 'missing_offset_or_length' }, origin);
        req.resume();
        return;
      }
      offset = Number(xoffset);
      length = Number(xlen);
    }

    if (isNaN(offset) || isNaN(length) || offset < 0 || length <= 0 || offset + length > up.totalSize) {
      sendJSON(res, 400, { error: 'invalid_range' }, origin);
      req.resume();
      return;
    }

    manager.writeChunkAt(id, offset, req, length).then((result) => {
      if (result.alreadyReceived) {
        sendJSON(res, 200, { ok: true, message: 'chunk_already_received' }, origin);
      } else {
        sendJSON(res, 200, { ok: true, written: result.written }, origin);
      }
    }).catch((err) => {
      // disk full
      if (err && (err.code === 'ENOSPC' || err.status === 507)) {
        console.error('Chunk write failed due to disk full:', err);
        sendJSON(res, 507, { error: 'disk_full', message: 'No space left on device' }, origin);
        return;
      }
      // write in progress / conflict
      if (err && err.status === 409) {
        sendJSON(res, 409, { error: 'chunk_write_in_progress' }, origin);
        return;
      }
      // transient write failure: tell client to retry
      if (err && err.transient) {
        console.warn('Transient write failure, advise client to retry chunk', err);
        sendJSON(res, 503, { error: 'transient_write_failure', message: 'Temporary write error; retry chunk' }, origin);
        return;
      }
      console.error('writeChunkAt error', err);
      if (!res.headersSent) sendJSON(res, 500, { error: 'chunk_upload_failed', details: String(err) }, origin);
    });

    return;
  }

  // Not found
  res.writeHead(404, { 'Content-Type': 'text/plain' });
  res.end('not found');
});

// tune sockets (disable Nagle)
server.on('connection', (socket) => {
  try { socket.setNoDelay(true); } catch (e) {}
});

server.listen(PORT, () => {
  console.log(`Fast upload server listening on ${PORT}`);
  console.log(`Storage dir: ${STORAGE_DIR}`);
});

// On process exit, ensure metadata is flushed
async function shutdown() {
  console.log('Shutting down, flushing metadata...');
  try {
    await manager.flushAllMetadata();
  } catch (e) {
    console.error('flush metadata failed', e);
  }
  process.exit(0);
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);