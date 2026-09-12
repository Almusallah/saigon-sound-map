/* One recoverable contribution on this device. Writes stay in user-action order. */
window.DraftStore = (() => {
  let dbPromise, tail = Promise.resolve();
  function db() {
    return dbPromise ||= new Promise((resolve, reject) => {
      const r = indexedDB.open('saigon-map-drafts', 1);
      r.onupgradeneeded = () => r.result.createObjectStore('drafts');
      r.onblocked = () => reject(new Error('Draft storage is busy in another tab'));
      r.onsuccess = () => { r.result.onversionchange = () => { r.result.close(); dbPromise = null; }; resolve(r.result); }; r.onerror = () => reject(r.error);
    });
  }
  function transaction(mode, action) {
    return db().then(d => new Promise((resolve, reject) => {
      const tx = d.transaction('drafts', mode), r = action(tx.objectStore('drafts'));
      tx.oncomplete = () => resolve(r.result); tx.onerror = tx.onabort = () => reject(tx.error);
    }));
  }
  function write(action) {
    const job = tail.catch(() => {}).then(() => transaction('readwrite', action));
    tail = job; return job;
  }
  return { read: () => transaction('readonly', s => s.get('current')),
    save: value => write(s => s.put(value, 'current')),
    clear: () => write(s => s.delete('current')) };
})();
