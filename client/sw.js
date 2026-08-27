// Service worker for installability + a cached app shell.
//
// Caching is deliberately conservative:
// - HTML and /api/* are NETWORK-FIRST: index.html is served no-cache on
//   purpose (a cached copy pins visitors to an old build) and API data
//   must stay live. The cache is only a fallback when offline.
// - ONLY truly immutable static assets are CACHE-FIRST: the versioned
//   mapbox-gl-js library (js/css) and the Google Fonts files.
// - Everything else on api.mapbox.com is NOT intercepted. Styles, sprites,
//   glyphs, vector/raster tiles and the map-sessions endpoint all live on
//   the same hostname as the library — an earlier version cache-firsted the
//   whole host and hoarded hundreds of tiles (56 MB in minutes). On iOS
//   Safari, whose CacheStorage is slow and tightly quota'd, the first tap
//   (first camera move = a burst of new tile fetches) then wedged behind
//   cache lookups and the map froze. Tiles must go straight to the network.
// - Backblaze audio/images are NEVER touched: the audio player relies on
//   range requests, which a cache-through service worker breaks.
const SHELL = 'shell-v2';

const isStaticCdn = (url) =>
  (url.hostname === 'api.mapbox.com' && url.pathname.startsWith('/mapbox-gl-js/')) ||
  url.hostname === 'fonts.googleapis.com' ||
  url.hostname === 'fonts.gstatic.com';

self.addEventListener('install', (e) => self.skipWaiting());

// Deleting old caches here also heals devices bloated by the shell-v1 bug.
self.addEventListener('activate', (e) => e.waitUntil(
  caches.keys()
    .then((names) => Promise.all(names.filter((n) => n !== SHELL).map((n) => caches.delete(n))))
    .then(() => clients.claim())
));

self.addEventListener('fetch', (e) => {
  const url = new URL(e.request.url);
  if (e.request.method !== 'GET') return;
  if (url.hostname.endsWith('backblazeb2.com')) return;          // audio: hands off

  // immutable static libraries: cache-first
  if (isStaticCdn(url)) {
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

  if (url.hostname === 'api.mapbox.com') return;                 // tiles/styles/sessions: hands off

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
