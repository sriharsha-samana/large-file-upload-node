# Fast large-file uploader (local disk) — optimized for pure Node.js environment

This repository contains a high-performance Node.js server and a simple browser client for uploading very large files (100GB+) directly to a folder on the server. The implementation focuses on maximizing throughput using only Node.js-level changes (no OS or hardware tuning required).

Files included:
- server.js            — fast raw-http Node.js server
- uploadManager.js    — preallocation, fd reuse, debounced metadata flush
- index.html           — simple browser UI
- upload-client.js     — lane-based, high-throughput browser client
- package.json

Quick start:
1. npm install
2. node server.js
3. Open index.html in a browser (set server base URL if not same origin) and upload a file.

API summary:
- POST /upload/initiate  { filename, totalSize, chunkSize? } -> { uploadId, chunkSize }
- PUT /upload/:id        with Content-Range: bytes start-end/total (body: raw chunk bytes)
- GET /upload/:id/status -> { receivedChunks }
- POST /upload/:id/complete
- DELETE /upload/:id

Notes:
- Metadata is flushed with a short debounce to reduce disk writes. The server flushes metadata on exit as well.
- This implementation is single-process. For multi-process horizontal scaling, move metadata and locks to Redis.

License: MIT
