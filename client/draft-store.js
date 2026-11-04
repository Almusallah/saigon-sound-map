/* One recoverable contribution on this device. Writes stay in user-action order. */
window.DraftStore = (() => {
  let dbPromise, conn = null, busy = 0, tail = Promise.resolve(), seq = 0, known = false;
  function db() {
    return dbPromise ||= new Promise((resolve, reject) => {
      const r = indexedDB.open('saigon-map-drafts', 1);
      r.onupgradeneeded = () => r.result.createObjectStore('drafts');
      r.onblocked = () => reject(new Error('Draft storage is busy in another tab'));
      r.onsuccess = () => { conn = r.result; r.result.onversionchange = () => { r.result.close(); conn = null; dbPromise = null; }; resolve(r.result); }; r.onerror = () => reject(r.error);
    });
  }
  // Throws when the connection has closed, so write() can reopen instead.
  // commit() at once: a closing page never runs the request callbacks
  // that an auto-commit waits for, so its last save would be dropped.
  function run(d, mode, action) {
    const tx = d.transaction('drafts', mode), r = action(tx.objectStore('drafts'));
    tx.commit?.();
    return new Promise((resolve, reject) => {
      tx.oncomplete = () => resolve(r.result); tx.onerror = tx.onabort = () => reject(tx.error);
    });
  }
  function transaction(mode, action) { return db().then(d => run(d, mode, action)); }
  function write(action) {
    // Nothing queued and the database open: start now, in this task. A save
    // made as the page goes away (pagehide) cannot wait on promise hops the
    // closing page may never run.
    let job = null;
    if (!busy && conn) { try { job = run(conn, 'readwrite', action); } catch { conn = null; dbPromise = null; } }
    job ||= tail.catch(() => {}).then(() => transaction('readwrite', action));
    busy++;
    const done = () => { busy--; };
    job.then(done, done);
    tail = job; return job;
  }

  // The closing page's words. IndexedDB may never finish a write that
  // carries a recording or photos — or that waits behind one — before the
  // page is gone; a localStorage write is done when it returns. Every save
  // is stamped when it is made, so whichever is newer wins however the
  // two writes land.
  const TEXT = 'saigon-map-draft-text';
  const stamp = () => (seq = Math.max(seq + 1, Date.now()));
  const overlay = () => { try { return JSON.parse(localStorage.getItem(TEXT)) || null; } catch { return null; } };
  const dropOverlay = upTo => {
    try { const o = overlay(); if (o && (upTo === undefined || !(o.seq > upTo))) localStorage.removeItem(TEXT); } catch {}
  };
  // The words are laid over the saved draft only when newer, and only on
  // the same contribution's media; another contribution's words stand alone.
  // Drafts saved before draft keys existed have none to compare, and words
  // from a page that could not read the store claim no contribution: that
  // page never saw the media, so it cannot have discarded it.
  const withOverlay = full => {
    const text = overlay();
    if (!text || !(text.seq > (full?.seq || 0))) return full;
    return !full?.key || !text.key || full.key === text.key ? { ...(full || {}), ...text } : text;
  };
  // Stamps on this page start above anything stored, even if the clock
  // has since gone back.
  const seed = (...found) => { seq = Math.max(seq, ...found.map(d => d?.seq || 0)); };
  return {
    read: () => transaction('readonly', s => s.get('current')).then(full => { known = true; seed(full, overlay()); return withOverlay(full); },
      err => { const text = overlay(); if (text) { seed(text); return text; } throw err; }),
    save: value => {
      const n = value.seq = stamp();
      return write(s => s.put(value, 'current')).then(r => { dropOverlay(n); return r; });
    },
    saveText: value => { try { localStorage.setItem(TEXT, JSON.stringify({ ...value, key: known ? value.key : undefined, seq: stamp() })); } catch {} },
    clear: () => { dropOverlay(); return write(s => s.delete('current')); },
  };
})();
