/* Two interfaces on one page (8 Nov 2026). The sound map opens by default:
   sounds only, one button to record. The archive, one tap away
   (?view=archive), takes sounds, images, videos and texts. The view lives in the
   address so Back, share and reload all land where the visitor was; which
   side of the archive (list or map) does not. Pure helpers, shared by the
   page and the tests. */
(function (root) {
  'use strict';
  const KINDS = ['sound', 'image', 'video', 'text'];

  // ?view=archive, and only that, opens the archive.
  function pick(search) {
    try { return new URLSearchParams(search || '').get('view') === 'archive' ? 'archive' : 'sound'; }
    catch { return 'sound'; }
  }
  // The view an entry of this kind can be shown in, starting from `mode`:
  // a sound shows in either; images, videos and texts only in the archive.
  const forLink = (mode, kind) => (kind === 'sound' ? mode : 'archive');
  // Whether an entry of `kind` is on the map in `mode`. The archive's kind
  // toggle (activeKind) means nothing on the sound map.
  function shows(mode, kind, activeKind) {
    return mode === 'archive' ? (!activeKind || kind === activeKind) : kind === 'sound';
  }
  // The kinds that occur, in a fixed order.
  function kindsPresent(kinds) {
    const seen = new Set(kinds);
    return KINDS.filter(k => seen.has(k));
  }
  // The same address in the other view; every other parameter is kept.
  function withView(href, mode) {
    const url = new URL(href);
    if (mode === 'archive') url.searchParams.set('view', 'archive');
    else url.searchParams.delete('view');
    return url.href;
  }

  const api = { KINDS, pick, forLink, shows, kindsPresent, withView };
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  else root.Views = api;
})(typeof window === 'undefined' ? globalThis : window);
