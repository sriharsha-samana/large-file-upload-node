// High-performance lane-based browser client
(() => {
  // UI
  const fileInput = document.getElementById('file');
  const startBtn = document.getElementById('startBtn');
  const pauseBtn = document.getElementById('pauseBtn');
  const abortBtn = document.getElementById('abortBtn');
  const chunkSizeMBInput = document.getElementById('chunkSizeMB');
  const concurrencyInput = document.getElementById('concurrency');
  const progressBar = document.querySelector('#progress > i');
  const percentLabel = document.getElementById('percent');
  const statusLabel = document.getElementById('status');
  const infoPre = document.getElementById('info');
  const baseUrlInput = document.getElementById('baseUrl');

  const STORAGE_KEY = 'fast-large-upload-meta-v1';

  let paused = false;
  let abortRequested = false;

  // sensible defaults
  if (!chunkSizeMBInput.value) chunkSizeMBInput.value = 64;
  if (!concurrencyInput.value) {
    const hw = navigator.hardwareConcurrency || 4;
    concurrencyInput.value = Math.min(4, hw);
  }

  function baseUrl() {
    const v = (baseUrlInput.value || '').trim();
    return v || window.location.origin;
  }

  async function apiFetch(path, opts = {}) {
    const url = baseUrl().replace(/\/$/, '') + path;
    return fetch(url, opts);
  }

  function fileKey(file) { return `${file.name}:${file.size}`; }

  function saveMeta(key, meta) {
    try {
      const all = JSON.parse(localStorage.getItem(STORAGE_KEY) || '{}');
      all[key] = meta;
      localStorage.setItem(STORAGE_KEY, JSON.stringify(all));
    } catch (e) { /* ignore */ }
  }
  function loadMeta(key) {
    try {
      const all = JSON.parse(localStorage.getItem(STORAGE_KEY) || '{}');
      return all[key] || null;
    } catch (e) { return null; }
  }
  function removeMeta(key) {
    try {
      const all = JSON.parse(localStorage.getItem(STORAGE_KEY) || '{}');
      delete all[key];
      localStorage.setItem(STORAGE_KEY, JSON.stringify(all));
    } catch (e) {}
  }

  function logInfo(obj) { infoPre.textContent = JSON.stringify(obj, null, 2); }

  async function startOrResume() {
    const file = fileInput.files && fileInput.files[0];
    if (!file) { alert('Select a file'); return; }
    paused = false; abortRequested = false;
    startBtn.disabled = true; pauseBtn.disabled = false; abortBtn.disabled = false;

    const chunkSize = Math.max(1, Number(chunkSizeMBInput.value || 64)) * 1024 * 1024;
    const concurrency = Math.max(1, Math.min(16, Number(concurrencyInput.value || 4)));

    const key = fileKey(file);
    let meta = loadMeta(key);
    if (meta) {
      // confirm server status and update received list
      try {
        const r = await apiFetch(`/upload/${meta.uploadId}/status`);
        if (!r.ok) {
          // server lost it; start fresh
          meta = null; removeMeta(key);
        } else {
          const st = await r.json();
          meta.chunkSize = st.chunkSize || meta.chunkSize;
          meta.received = new Set(st.receivedChunks || []);
        }
      } catch (e) { /* ignore, will try resume locally */ }
    }

    if (!meta) {
      // initiate new upload
      const r = await apiFetch('/upload/initiate', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ filename: file.name, totalSize: file.size, chunkSize })
      });
      if (!r.ok) {
        alert('Initiate failed: ' + await r.text());
        startBtn.disabled = false; pauseBtn.disabled = true; abortBtn.disabled = true;
        return;
      }
      const j = await r.json();
      meta = {
        uploadId: j.uploadId,
        chunkSize: j.chunkSize || chunkSize,
        totalSize: file.size,
        received: new Set()
      };
      saveMeta(key, { uploadId: meta.uploadId, chunkSize: meta.chunkSize, totalSize: meta.totalSize, received: [] });
    }

    logInfo({ file: file.name, size: file.size, uploadId: meta.uploadId, chunkSize: meta.chunkSize });

    // compute total chunks
    const totalChunks = Math.ceil(meta.totalSize / meta.chunkSize);
    // refresh server received
    try {
      const r = await apiFetch(`/upload/${meta.uploadId}/status`);
      if (r.ok) {
        const st = await r.json();
        meta.received = new Set(st.receivedChunks || []);
      }
    } catch (e) {}

    updateProgress(meta.received.size, totalChunks);
    // build lanes: each lane gets contiguous chunk indices
    const lanes = buildLanes(totalChunks, meta.received, concurrency);
    statusLabel.textContent = `Uploading in ${lanes.length} lanes (concurrency ${lanes.length})`;

    try {
      await Promise.all(lanes.map((lane) => laneWorker(lane, file, meta)));
      if (abortRequested) {
        statusLabel.textContent = 'Aborted';
      } else {
        statusLabel.textContent = 'All chunks uploaded, completing...';
        const r = await apiFetch(`/upload/${meta.uploadId}/complete`, { method: 'POST' });
        if (r.ok) {
          statusLabel.textContent = 'Upload complete';
          removeMeta(key);
        } else {
          statusLabel.textContent = 'Complete failed: ' + (await r.text());
        }
      }
    } catch (err) {
      if (err && err.name === 'AbortError') {
        statusLabel.textContent = 'Paused';
      } else {
        console.error('Upload error', err);
        statusLabel.textContent = 'Upload error: ' + (err && err.message ? err.message : String(err));
      }
    } finally {
      startBtn.disabled = false; pauseBtn.disabled = true; abortBtn.disabled = true;
    }
  }

  function buildLanes(totalChunks, receivedSet, concurrency) {
    const lanes = [];
    const chunksPerLane = Math.ceil(totalChunks / concurrency);
    for (let i = 0; i < concurrency; i++) {
      const start = i * chunksPerLane;
      const end = Math.min(totalChunks, (i + 1) * chunksPerLane);
      const lane = [];
      for (let c = start; c < end; c++) {
        if (!receivedSet.has(c)) lane.push(c);
      }
      if (lane.length) lanes.push(lane);
    }
    // if no lanes (all done), return empty
    return lanes;
  }

  async function laneWorker(laneIndices, file, meta) {
    for (const idx of laneIndices) {
      if (abortRequested) throw new Error('abort_requested');
      if (paused) throw new DOMException('paused', 'AbortError');
      await uploadWithRetries(file, meta, idx);
      // mark locally
      meta.received.add(idx);
      const key = fileKey(file);
      const stored = loadMeta(key) || {};
      stored.received = Array.from(meta.received);
      saveMeta(key, stored);
      updateProgress(meta.received.size, Math.ceil(meta.totalSize / meta.chunkSize));
    }
  }

  async function uploadWithRetries(file, meta, chunkIndex) {
    const maxRetries = 6;
    let attempt = 0;
    while (attempt <= maxRetries) {
      try {
        await uploadChunk(file, meta.uploadId, meta.chunkSize, chunkIndex, meta.totalSize);
        return;
      } catch (err) {
        // treat 409 or network errors as transient
        const transient = (err && (err.transient || err.name === 'TypeError' || err.status === 409));
        attempt++;
        if (!transient || attempt > maxRetries) throw err;
        const backoff = Math.min(30000, 300 * 2 ** attempt + Math.random() * 200);
        await new Promise((r) => setTimeout(r, backoff));
      }
    }
  }

  async function uploadChunk(file, uploadId, chunkSize, chunkIndex, totalSize) {
    const start = chunkIndex * chunkSize;
    const endExclusive = Math.min(start + chunkSize, totalSize);
    const endInclusive = endExclusive - 1;
    const slice = file.slice(start, endExclusive);
    const headers = new Headers();
    headers.set('Content-Range', `bytes ${start}-${endInclusive}/${totalSize}`);
    // Use fetch with Blob body (browser will stream)
    const resp = await apiFetch(`/upload/${uploadId}`, {
      method: 'PUT',
      headers,
      body: slice
    });
    if (resp.status === 200) {
      try {
        const j = await resp.json();
        if (j && j.message === 'chunk_already_received') {
          const e = new Error('already_received');
          e.already = true;
          throw e;
        }
      } catch (_) {}
      return;
    } else if (resp.status === 409) {
      const e = new Error('conflict');
      e.status = 409;
      e.transient = true;
      throw e;
    } else {
      const txt = await resp.text().catch(() => '');
      const e = new Error('upload_failed ' + resp.status + ' ' + txt);
      e.status = resp.status;
      e.transient = resp.status >= 500 && resp.status < 600;
      throw e;
    }
  }

  function updateProgress(doneChunks, totalChunks) {
    const pct = totalChunks === 0 ? 0 : Math.round((doneChunks / totalChunks) * 10000) / 100;
    progressBar.style.width = `${pct}%`;
    percentLabel.textContent = `${pct}% (${doneChunks}/${totalChunks})`;
  }

  // UI handlers
  startBtn.addEventListener('click', async () => {
    try {
      await startOrResume();
    } catch (e) {
      console.error('Start failed', e);
      alert('Start failed: ' + (e.message || e));
    }
  });

  pauseBtn.addEventListener('click', () => {
    paused = true;
    pauseBtn.disabled = true;
    startBtn.disabled = false;
    statusLabel.textContent = 'Pausing...';
  });

  abortBtn.addEventListener('click', async () => {
    if (!confirm('Abort and delete remote file+metadata?')) return;
    abortRequested = true;
    paused = false;
    pauseBtn.disabled = true;
    startBtn.disabled = false;
    const file = fileInput.files && fileInput.files[0];
    if (!file) return;
    const key = fileKey(file);
    const meta = loadMeta(key);
    if (!meta) return;
    try {
      await apiFetch(`/upload/${meta.uploadId}`, { method: 'DELETE' });
    } catch (e) { console.warn('abort API error', e); }
    removeMeta(key);
    statusLabel.textContent = 'Aborted';
    progressBar.style.width = '0%';
    infoPre.textContent = 'No upload in progress';
  });
})();
