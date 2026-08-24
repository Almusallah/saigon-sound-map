// Service worker for installability + a cached app shell.
//
// Caching is deliberately conservative:
// - HTML and /api/* are NETWORK-FIRST: index.html is served no-cache on
//   purpose (a cached copy pins visitors to an old build) and API data
//   must stay live. The cache is only a fallback when offline.
// - Big versioned CDN assets (mapbox-gl js/css, fonts) are CACHE-FIRST:
//   they never change under the same URL and are the bulk of load time.
// - Backblaze audio/images are NEVER touched: the audio player relies on
//   range requests, which a cache-through service worker breaks.
const SHELL = 'shell-v1';
const CDN = ['api.mapbox.com', 'fonts.googleapis.com', 'fonts.gstatic.com'];

self.addEventListener('install', (e) => self.skipWaiting());
self.addEventListener('activate', (e) => e.waitUntil(clients.claim()));

self.addEventListener('fetch', (e) => {
  const url = new URL(e.request.url);
  if (e.request.method !== 'GET') return;
  if (url.hostname.endsWith('backblazeb2.com')) return;          // audio: hands off

  // static CDN libraries: cache-first
  if (CDN.includes(url.hostname)) {
    e.respondWith(
      caches.open(SHELL).then(async (c) => {
        const hit = await c.match(e.request);
        if (hit) return hit;
        const res = await fetch(e.request);
        if (res.ok) c.put(e.request, res.clone());
        return res;
      })
    );
    return;
  }

  // same-origin navigations + api: network-first, cache as offline fallback
  if (url.origin === location.origin) {
    e.respondWith(
      fetch(e.request)
        .then((res) => {
          if (res.ok && (e.request.mode === 'navigate' || url.pathname.startsWith('/icons/'))) {
            const copy = res.clone();
            caches.open(SHELL).then((c) => c.put(e.request, copy));
          }
          return res;
        })
        .catch(() => caches.match(e.request))
    );
  }
});
