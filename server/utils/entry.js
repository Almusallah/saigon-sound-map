/**
 * What a contribution is, decided from what actually arrived.
 *
 * The map began as a sound archive; it now also holds drawings,
 * sketches, photos and writing. Pure functions only, so the upload route
 * and the tests share one definition.
 */

const LIMITS = { title: 140, description: 4000, body: 20000, author: 80, images: 8 };

// Free text from a form field. Strings only (a crafted request can send
// arrays or objects), Unicode NFC so Vietnamese typed on different
// keyboards searches alike, control characters dropped. Single-line
// fields collapse whitespace; multiline ones keep line breaks and
// indentation — the shape of a poem is part of the poem.
function cleanText(value, max, { multiline = false } = {}) {
  if (typeof value !== 'string') return '';
  let s = value.normalize('NFC').replace(/\r\n?/g, '\n')
    .replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F]/g, '');
  // trimEnd, not /\s+$/: that regex backtracks quadratically on a long
  // run of spaces followed by a letter (one crafted field froze the server).
  s = multiline ? s.replace(/^(?:[ \t]*\n)+/, '').trimEnd() : s.replace(/\s+/g, ' ').trim();
  if (s.length <= max) return s;
  // Cap in UTF-16 units — what the model's maxlength counts, so a capped
  // value always saves (an emoji is two) — but cut between characters,
  // never through a surrogate pair.
  let out = '';
  for (const ch of s) {
    if (out.length + ch.length > max) break;
    out += ch;
  }
  return out.trimEnd();
}

// Audio always leads: every contribution with sound stays in the
// listening room, the streets and the Sampler. A text that comes with
// illustrations stays a text when the contributor said so.
function entryKind({ hasAudio, imageCount, body, hint }) {
  if (hasAudio) return 'sound';
  if (hint === 'text' && body) return 'text';
  if (imageCount > 0) return 'image';
  if (body) return 'text';
  return null;
}

// The /api/recordings family serves playable sounds only.
function isSound(r) { return !!(r && r.audioUrl); }

// Every B2 object an entry owns besides its audio (cover, images, thumbs).
function imageUrlsOf(r) {
  const urls = new Set();
  if (r && r.imageUrl) urls.add(r.imageUrl);
  for (const img of (r && r.images) || []) {
    if (img && img.url) urls.add(img.url);
    if (img && img.thumbUrl) urls.add(img.thumbUrl);
  }
  return [...urls];
}

module.exports = { LIMITS, cleanText, entryKind, isSound, imageUrlsOf };
