require('dotenv').config();

const express   = require('express');
const cors      = require('cors');
const fs        = require('fs');
const path      = require('path');
const multer    = require('multer');
const mongoose  = require('mongoose');
const rateLimit = require('express-rate-limit');

const Recording = require('./models/Recording');
const { uploadId } = require('./utils/upload-id');
const pendingUploads = new Map();
const { syncB2ToMongo, uploadRecording, cleanupOrphans, deleteB2Object, deleteImagesOf, streamB2ObjectAsMp3, transcodeLegacyToMp3, backfillDurations } = require('./utils/b2');
const { LIMITS, cleanText, entryKind, isSound, imageUrlsOf } = require('./utils/entry');
const { THUMB_EDGE } = require('./utils/image');

// ── App setup ────────────────────────────────────────────────────────────
const app  = express();
const PORT = process.env.PORT || 3000;

// Render terminates TLS at its edge proxy; trust X-Forwarded-For for accurate
// client IPs (rate limiter relies on this).
app.set('trust proxy', 1);

app.use(cors({ origin: '*', methods: ['GET', 'POST', 'DELETE', 'OPTIONS'] }));
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// ── Rate limiters ────────────────────────────────────────────────────────
// Anonymous uploads need spam protection, but limits are PER IP — and a
// workshop room full of contributors shares one venue WiFi IP. 60/hour
// keeps a group productive while still making scripted abuse uneconomic
// (B2 daily spend caps bound the worst case anyway).
const uploadLimiter = rateLimit({
  windowMs: 60 * 60 * 1000,
  // Per IP — and a whole workshop can share one. Vietnamese mobile
  // carriers put many subscribers behind the same public address (CGNAT),
  // so 25 walkers on Viettel can look like a single uploader. 300 leaves
  // room for a group session; B2 spend caps still bound real abuse.
  max: 300,
  standardHeaders: true,
  legacyHeaders: false,
  message: { success: false, message: 'Too many uploads from this IP. Try again later.' },
});

// Read endpoints get a generous bucket — protects against scrape floods
// without affecting normal browsing (sized for ~50 people on shared WiFi:
// each open tab refreshes the recordings list once a minute).
const readLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: 600, // same CGNAT reasoning; a group all opening the map at once
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: 'Rate limited. Slow down.' },
});

// Admin auth for destructive routes
function requireAdmin(req, res, next) {
    const provided = req.headers['x-admin-token'] ||
                         (req.headers.authorization || '').replace(/^Bearer\s+/i, '');
    if (!process.env.ADMIN_TOKEN || provided !== process.env.ADMIN_TOKEN) {
          return res.status(401).json({ error: 'unauthorized' });
    }
    next();
}

const upload = multer({
  storage: multer.memoryStorage(),
  // 25 MB per file; one audio plus up to LIMITS.images images.
  // Text fields: the longest is a 20,000-character text (≤80 KB as UTF-8).
  limits: { fileSize: 25 * 1024 * 1024, files: 1 + LIMITS.images, fieldSize: 128 * 1024, fields: 20 },
  // A picked "photo" that isn't actually an image (raw HEIC/DNG fallback
  // from the client sends application/octet-stream) is silently dropped —
  // a bad attachment must never reject the audio that came with it.
  fileFilter: (req, file, cb) => {
    if (file.fieldname === 'imageFile' && !/^image\//.test(file.mimetype)) {
      req.imagesRefused = (req.imagesRefused || 0) + 1;
      return cb(null, false);
    }
    cb(null, true);
  },
});
// multer's limits are per file; nine files at 25 MB would hold ~225 MB of
// one request in memory on a 512 MB instance. Bound the whole body before
// multer reads it: a long recording plus the images the client prepares
// (each a few MB) fits easily. Browsers always send Content-Length for a
// FormData upload, and Node never reads past it.
const MAX_UPLOAD_BYTES = 60 * 1024 * 1024;
function boundUpload(req, res, next) {
  const len = Number(req.headers['content-length']);
  if (!Number.isFinite(len)) return res.status(411).json({ success: false, message: 'Upload rejected: length required' });
  if (len > MAX_UPLOAD_BYTES) {
    return res.status(413).json({ success: false, message: 'This upload is too large (over 60 MB). Try fewer or smaller images.' });
  }
  next();
}

// ── Cache ────────────────────────────────────────────────────────────────
let cache = [];
let cacheTime = 0;
const CACHE_TTL = 3 * 60 * 1000;

async function refreshCache() {
  cache = await Recording.find().sort({ createdAt: -1 }).lean();
  cacheTime = Date.now();
  return cache;
}

function cacheIsFresh() { return cacheTime && (Date.now() - cacheTime < CACHE_TTL); }

// ── API routes ───────────────────────────────────────────────────────────

app.get('/api/health', (req, res) => {
  res.json({
    status: 'ok',
    db: mongoose.connection.readyState === 1,
    recordings: cache.filter(isSound).length,
    entries: cache.length,
    uptime: process.uptime(),
  });
});

// Sounds only. Phòng nghe, the streets, the classic room, the Sampler app
// and outside listeners read this to get playable audio — images and
// texts live at /api/entries.
app.get('/api/recordings', readLimiter, async (req, res) => {
  try {
    const list = cacheIsFresh() ? cache : await refreshCache();
    res.json({ recordings: list.filter(isSound) });
  } catch (err) {
    console.error('[GET /recordings]', err.message);
    res.status(500).json({ error: 'Failed to fetch recordings' });
  }
});

// The whole living archive: sounds, images (drawings, sketches, photos)
// and texts (stories, poems). The map reads this.
app.get('/api/entries', readLimiter, async (req, res) => {
  try {
    const entries = cacheIsFresh() ? cache : await refreshCache();
    res.json({ entries });
  } catch (err) {
    console.error('[GET /entries]', err.message);
    res.status(500).json({ error: 'Failed to fetch entries' });
  }
});

app.get('/api/search', readLimiter, async (req, res) => {
  try {
    // Escape regex specials — a bare "(" would otherwise throw a 500.
    const q = String(req.query.q || '').normalize('NFC').slice(0, 100) // stored text is NFC
      .replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const results = await Recording.find({
      audioUrl: { $type: 'string', $ne: '' }, // a "recordings" endpoint: sounds only
      $or: [
        { title:       { $regex: q, $options: 'i' } },
        { description: { $regex: q, $options: 'i' } },
      ]
    }).sort({ createdAt: -1 }).lean();
    res.json({ recordings: results });
  } catch (err) {
    console.error('[GET /search]', err.message);
    res.status(500).json({ error: 'Search failed' });
  }
});

// Full-archive GeoJSON export — for GIS tools, research use, and backups.
// Example: curl -o saigon-sound-map.geojson .../api/export.geojson
app.get('/api/export.geojson', readLimiter, async (req, res) => {
  try {
    const list = cacheIsFresh() ? cache : await refreshCache();
    res.set('Content-Disposition', 'inline; filename="saigon-sound-map.geojson"');
    res.type('application/geo+json').json({
      type: 'FeatureCollection',
      features: list.map(r => ({
        type: 'Feature',
        geometry: { type: 'Point', coordinates: [r.longitude, r.latitude] },
        // Everything, all kinds: this export is also the weekly backup,
        // and the only copy of a text's words outside the database.
        properties: {
          id:          r.id,
          kind:        r.kind || 'sound',
          title:       r.title,
          description: r.description || '',
          category:    r.category,
          audioUrl:    r.audioUrl || '',
          imageUrl:    r.imageUrl || '',
          images:      r.images || [],
          body:        r.body || '',
          author:      r.author || '',
          duration:    r.duration || 0,
          createdAt:   r.createdAt,
        },
      })),
    });
  } catch (err) {
    console.error('[GET /export.geojson]', err.message);
    res.status(500).json({ error: 'Export failed' });
  }
});

// Proximity query — finds recordings near a point using the 2dsphere index.
// Example: GET /api/recordings/near?lat=10.776&lng=106.701&radius=500
app.get('/api/recordings/near', readLimiter, async (req, res) => {
  try {
    const lat    = parseFloat(req.query.lat);
    const lng    = parseFloat(req.query.lng);
    const radius = Math.min(parseFloat(req.query.radius) || 500, 50000); // cap 50 km
    if (isNaN(lat) || isNaN(lng)) {
      return res.status(400).json({ error: 'lat and lng are required' });
    }
    const recordings = await Recording.find({
      audioUrl: { $type: 'string', $ne: '' }, // sounds only
      location: {
        $near: {
          $geometry:    { type: 'Point', coordinates: [lng, lat] },
          $maxDistance: radius,
        },
      },
    }).limit(50).lean();
    res.json({ recordings, radius });
  } catch (err) {
    console.error('[GET /recordings/near]', err.message);
    res.status(500).json({ error: 'Proximity query failed' });
  }
});

// Download proxy. The `<a download>` attribute is ignored when the file
// lives on a different domain (Backblaze) from the page (Render). This
// endpoint fetches the audio from B2 server-side, transcodes anything
// that isn't already MP3 (webm, m4a, mp4, etc.) to MP3 via ffmpeg, and
// streams the result back with Content-Disposition: attachment so the
// browser actually saves it under a friendly filename built from the
// recording's title. Downloaded files are always .mp3 — universally
// playable on iOS, Android, Windows, macOS, VLC, etc.
app.get('/api/download/:idPrefix', readLimiter, async (req, res) => {
  try {
    const prefix = req.params.idPrefix;
    if (!/^[a-f0-9]{4,}$/i.test(prefix)) {
      return res.status(400).json({ error: 'Invalid id prefix' });
    }
    // Sounds only: a text sharing the id prefix must not shadow the sound.
    const r = await Recording.findOne({ id: { $regex: '^' + prefix }, audioUrl: { $type: 'string', $ne: '' } });
    if (!r) return res.status(404).json({ error: 'Recording not found' });
    if (!r.audioUrl) return res.status(404).json({ error: 'This entry has no sound' });
    // Keep Vietnamese diacritics so saved filenames like "Bánh Bao Đây.mp3"
    // round-trip cleanly through RFC 5987 encoding.
    const safe = (r.title || 'recording')
      .replace(/^\[[^\]]*\]\s*/, '') // legacy "[Category] " title prefix
      .replace(/[^\w\s\-À-ɏḀ-ỿ]/g, '')
      .replace(/\s+/g, '_')
      .slice(0, 60)
      .replace(/_+$/, '');
    const filename = `${safe || 'recording'}.mp3`;
    await streamB2ObjectAsMp3(r.audioUrl, filename, res);
  } catch (err) {
    console.error('[GET /download]', err.message);
    if (!res.headersSent) res.status(500).json({ error: 'Download failed' });
  }
});

// One contribution: a sound (audio + optional photos), an image entry
// (drawings, sketches, photos) or a text (a story, a poem; optional
// images). The kind follows what actually arrived — see utils/entry.js.
app.post('/api/upload', uploadLimiter, boundUpload, upload.fields([
  { name: 'audioFile', maxCount: 1 },
  { name: 'imageFile', maxCount: LIMITS.images }, // non-images already dropped by fileFilter
]), async (req, res) => {
  try {
    const audio = req.files?.audioFile?.[0];
    const images = req.files?.imageFile || [];
    const body = cleanText(req.body.body, LIMITS.body, { multiline: true });
    const kind = entryKind({ hasAudio: !!audio, imageCount: images.length, body, hint: req.body.kind });
    if (!kind) return res.status(400).json({ success: false, message: req.imagesRefused
      ? 'None of the images could be read. Try a JPEG or PNG.' : 'Add a sound, an image or a text' });

    const lat = parseFloat(req.body.latitude);
    const lng = parseFloat(req.body.longitude);
    if (!Number.isFinite(lat) || !Number.isFinite(lng) || Math.abs(lat) > 90 || Math.abs(lng) > 180) {
      return res.status(400).json({ success: false, message: 'Invalid coordinates' });
    }

    const key = req.body.uploadKey;
    if (key && (typeof key !== 'string' || !/^[a-zA-Z0-9-]{16,80}$/.test(key))) {
      return res.status(400).json({ success: false, message: 'Invalid upload key' });
    }
    const metadata = {
      kind,
      title:            cleanText(req.body.title, LIMITS.title),
      description:      cleanText(req.body.description, LIMITS.description, { multiline: true }),
      category:         typeof req.body.category === 'string' ? req.body.category : '',
      body,
      author:           cleanText(req.body.author, LIMITS.author),
      latitude:         lat,
      longitude:        lng,
      originalFilename: audio ? audio.originalname : '',
      imageBuffers:     images.map(i => i.buffer),
    };
    if (!metadata.title) return res.status(400).json({ success: false, message: 'A title is needed' });
    // A sound the previous form could also have sent keeps that form's
    // identity (its raw fields), so a draft retried across the deploy is
    // recognised instead of saved twice.
    const legacy = kind === 'sound' && !body && !metadata.author && images.length <= 1;
    const id = uploadId(key, audio ? audio.buffer : Buffer.alloc(0),
      legacy ? [req.body.title, req.body.description, req.body.category, lat, lng]
             : [metadata.title, metadata.description, metadata.category, lat, lng, kind, body, metadata.author],
      metadata.imageBuffers);
    let job = id && pendingUploads.get(id);
    if (!job) {
      job = uploadRecording(audio ? audio.buffer : null, audio ? audio.mimetype : '', { ...metadata, id });
      if (id) pendingUploads.set(id, job);
    }
    let recording;
    try { recording = await job; }
    finally { if (id && pendingUploads.get(id) === job) pendingUploads.delete(id); }

    cache = [recording.toObject(), ...cache.filter(r => r.id !== recording.id)];
    cacheTime = Date.now();
    res.json({ success: true, recording });
  } catch (err) {
    console.error('[POST /upload]', err.message);
    // Bad client data is a 400, not a 500 — the client retries 5xx with
    // the full payload, which is how one failed upload once became six
    // orphaned B2 objects.
    const status = err.name === 'ValidationError' || err.status === 400 ? 400 : 500;
    res.status(status).json({ success: false, message: err.status === 400 ? err.message : 'Upload failed', error: err.message });
  }
});

app.delete('/api/recordings/:id', requireAdmin, async (req, res) => {
  try {
    // This route intentionally leaves the AUDIO in B2 (the next sync
    // re-imports it), but images can never be re-linked by the sync —
    // delete them so they don't strand in the bucket.
    const doc = await Recording.findOne({ id: req.params.id }, 'imageUrl images').lean();
    const imgs = doc ? await deleteImagesOf(doc) : { deleted: 0, errors: [] };
    await Recording.deleteOne({ id: req.params.id });
    cache = cache.filter(r => r.id !== req.params.id);
    res.json({ success: true, ...(imgs.errors.length ? { b2ImageErrors: imgs.errors } : {}) });
  } catch (err) {
    console.error('[DELETE /recordings]', err.message);
    res.status(500).json({ success: false, message: 'Delete failed' });
  }
});

app.get('/api/resync', requireAdmin, async (req, res) => {
  try {
    const result = await syncB2ToMongo();
    await refreshCache();
    res.json({ success: true, ...result });
  } catch (err) {
    console.error('[GET /resync]', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get('/api/cleanup', requireAdmin, async (req, res) => {
  try {
    const removed = await cleanupOrphans();
    await refreshCache();
    res.json({ success: true, removed });
  } catch (err) {
    console.error('[GET /cleanup]', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// Restore from the weekly backup (backups/latest.geojson — the export
// above). /api/resync can only bring sounds back from B2: a drawing's
// entry and a poem's words exist nowhere but this file. Adds every
// feature whose id is not in the database and never changes one that is.
// Media is checked first: an image deleted since the backup is dropped
// (listed in droppedMedia), an entry whose audio, or every image, is gone
// is skipped, and one whose storage can't be checked right now is left
// for a rerun. A text deleted since the backup has no media to miss and
// comes back — read the dry run before running it for real.
//   curl -H "X-Admin-Token: …" -F geojson=@backups/latest.geojson \
//        -F dryRun=false https://…/api/admin/import-geojson
// dryRun defaults to true. Run it before /api/resync, which would bring
// the same audio back as new "Auto-discovered" entries.
const backupFile = multer({ storage: multer.memoryStorage(), limits: { fileSize: 30 * 1024 * 1024, files: 1, fields: 4 } });
app.post('/api/admin/import-geojson', requireAdmin, backupFile.single('geojson'), async (req, res) => {
  const dryRun = String(req.body?.dryRun) !== 'false';
  let features;
  try { features = JSON.parse(req.file?.buffer?.toString('utf8') || '').features; } catch {}
  if (!Array.isArray(features)) {
    return res.status(400).json({ success: false, error: 'Send the backup GeoJSON as the multipart file field "geojson"' });
  }
  const result = { dryRun, alreadyPresent: 0, restored: [], skipped: [], errors: [] };
  try {
    const present = await Recording.find({}, { id: 1, audioUrl: 1 }).lean();
    const ids = new Set(present.map(r => r.id));
    const audio = new Set(present.map(r => r.audioUrl).filter(Boolean));
    const url = v => typeof v === 'string' && /^https?:\/\//.test(v) ? v : '';
    // What the backup would add, before touching the network.
    const todo = [], seen = new Set();
    for (const f of features) {
      const p = f?.properties || {};
      const id = typeof p.id === 'string' ? p.id : '';
      const [lng, lat] = Array.isArray(f?.geometry?.coordinates) ? f.geometry.coordinates : [];
      if (!/^[0-9a-f][0-9a-f-]{7,35}$/i.test(id)) { result.errors.push({ id, error: 'No valid id' }); continue; }
      if (seen.has(id)) continue; // listed twice in the file
      seen.add(id);
      if (ids.has(id)) { result.alreadyPresent++; continue; }
      if (!Number.isFinite(lat) || !Number.isFinite(lng) || Math.abs(lat) > 90 || Math.abs(lng) > 180) {
        result.errors.push({ id, error: 'Invalid coordinates' }); continue;
      }
      if (url(p.audioUrl) && audio.has(p.audioUrl)) {
        result.skipped.push({ id, reason: 'its audio already belongs to another entry (resync?)' }); continue;
      }
      if (url(p.audioUrl)) audio.add(p.audioUrl);
      todo.push({ id, p, lat, lng });
    }
    // HEAD each media URL, eight entries at a time. Only a 404/410 means
    // gone; a 503, a timeout or a dropped connection is retried, and if it
    // still can't tell, the entry is left out unsaved — a rerun picks it up
    // (it is still missing) instead of an image being dropped for good.
    const probe = async u => {
      for (let t = 0; t < 3; t++) {
        try {
          const r = await fetch(u, { method: 'HEAD', signal: AbortSignal.timeout(10000) });
          if (r.ok) return 'ok';
          if (r.status === 404 || r.status === 410) return 'gone';
        } catch {}
        if (t < 2) await new Promise(r => setTimeout(r, 500 * 2 ** t));
      }
      return 'unknown';
    };
    const docs = [];
    for (let i = 0; i < todo.length; i += 8) {
      docs.push(...await Promise.all(todo.slice(i, i + 8).map(async ({ id, p, lat, lng }) => {
        const kind = Recording.KINDS.includes(p.kind) ? p.kind : 'sound';
        const unsure = u => ({ id, error: `could not check ${u} — run the restore again` });
        const audioUrl = url(p.audioUrl);
        if (kind === 'sound') {
          const state = audioUrl ? await probe(audioUrl) : 'gone';
          if (state === 'unknown') return unsure(audioUrl);
          if (state === 'gone') return { id, skip: 'its audio is gone' };
        }
        const listed = (Array.isArray(p.images) ? p.images : []).filter(img => url(img?.url)).slice(0, LIMITS.images);
        const images = [], dropped = [];
        for (const img of listed) {
          const state = await probe(img.url);
          if (state === 'unknown') return unsure(img.url);
          if (state === 'gone') { dropped.push(img.url); continue; }
          // A thumbnail that is gone leaves '' — popups and og:image fall back to the image.
          let thumbUrl = url(img.thumbUrl);
          if (thumbUrl) {
            const t = await probe(thumbUrl);
            if (t === 'unknown') return unsure(thumbUrl);
            if (t === 'gone') { dropped.push(thumbUrl); thumbUrl = ''; }
          }
          images.push({ url: img.url, thumbUrl, width: Math.max(0, +img.width || 0), height: Math.max(0, +img.height || 0) });
        }
        // Older sounds have one photo and no images list.
        let imageUrl = images[0]?.url || '';
        if (!listed.length && url(p.imageUrl)) {
          const state = await probe(p.imageUrl);
          if (state === 'unknown') return unsure(p.imageUrl);
          if (state === 'ok') imageUrl = p.imageUrl; else dropped.push(p.imageUrl);
        }
        if (kind === 'image' && !images.length) return { id, skip: 'every image is gone' };
        const body = cleanText(p.body, LIMITS.body, { multiline: true });
        if (kind === 'text' && !body) return { id, skip: 'the text is empty' };
        const createdAt = new Date(p.createdAt);
        return { id, dropped, doc: new Recording({
          id, kind, audioUrl: kind === 'sound' ? audioUrl : '', imageUrl, images: images.length ? images : undefined, body,
          title: cleanText(p.title, LIMITS.title) || 'Untitled',
          description: cleanText(p.description, LIMITS.description, { multiline: true }),
          category: Recording.CATEGORIES.includes(p.category) ? p.category : 'Background',
          author: cleanText(p.author, LIMITS.author),
          latitude: lat, longitude: lng,
          duration: Number.isFinite(+p.duration) && +p.duration > 0 ? +p.duration : 0,
          ...(Number.isNaN(createdAt.getTime()) ? {} : { createdAt }),
        }) };
      })));
    }
    for (const { id, skip, error, doc, dropped } of docs) {
      if (error) { result.errors.push({ id, error }); continue; }
      if (skip) { result.skipped.push({ id, reason: skip }); continue; }
      const invalid = doc.validateSync();
      if (invalid) { result.errors.push({ id, error: invalid.message }); continue; }
      if (!dryRun) {
        try { await doc.save(); }
        catch (e) { result.errors.push({ id, error: e.message }); continue; }
      }
      result.restored.push({ id, kind: doc.kind, title: doc.title, images: doc.images?.length || (doc.imageUrl ? 1 : 0),
        ...(dropped.length ? { droppedMedia: dropped } : {}) });
    }
    if (!dryRun && result.restored.length) await refreshCache();
    res.json({ success: true, ...result });
  } catch (err) {
    console.error('[POST /admin/import-geojson]', err.message);
    res.status(500).json({ success: false, error: err.message, partial: result });
  }
});

// One-shot migration: backfills GeoJSON `location` for records that pre-date
// the schema change. Safe to run more than once (no-op for already-migrated
// docs). Admin-only because it touches every record.
app.get('/api/migrate-geojson', requireAdmin, async (req, res) => {
  try {
    const result = await Recording.updateMany(
      { 'location.coordinates': { $exists: false } },
      [{
        $set: {
          location: {
            type:        'Point',
            coordinates: ['$longitude', '$latitude'],
          },
        },
      }]
    );
    await refreshCache();
    res.json({
      success:  true,
      matched:  result.matchedCount,
      modified: result.modifiedCount,
    });
  } catch (err) {
    console.error('[GET /migrate-geojson]', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// Bulk-update endpoint for curating recording titles/categories at scale.
// Body shape:
//   {
//     "dryRun": false,
//     "updates": [
//       { "idPrefix": "f6fd31a9", "title": "Kids playing", "category": "Play & Leisure" },
//       { "idPrefix": "ee30aaa0", "author": "", "removeImages": [3] }
//     ],
//     "deleteByPrefix": ["3f6cfa4f"],
//     "deleteByMatch":  ["FUJIRO"]   // matches title or audioUrl, case-insensitive;
//                                    // ≥3 characters; over 5 entries in all needs
//                                    // "confirmCount": n (the dry run's matchCount)
//   }
// idPrefix is matched against the START of the recording's UUID.
// Every field is kept unless the update sends it: title, category,
// description ("" clears it), body (a text's words), author ("" removes
// the credit). removeImages lists images by their number as the viewer
// shows them (1 = cover); they are deleted from B2 too — run a dryRun
// first, it lists the exact URLs. The placeholder that /api/resync gives
// imported files is always cleared.
app.post('/api/admin/bulk-update', requireAdmin, async (req, res) => {
  const { dryRun = false, updates = [], deleteByPrefix = [], deleteByMatch = [], alsoDeleteB2 = false } = req.body;
  const result = { dryRun, updated: [], deleted: [], notFound: [], ambiguous: [], errors: [] };

  // Helper: find a unique recording by id-prefix, or report ambiguity.
  async function findOneByPrefix(prefix) {
    // Escape regex specials in prefix; idPrefix is meant to be hex-like
    const escaped = prefix.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const matches = await Recording.find({ id: { $regex: '^' + escaped } }).limit(2).lean();
    return matches;
  }

  try {
    // Malformed input is refused before anything is written or deleted.
    if (![updates, deleteByPrefix, deleteByMatch].every(Array.isArray) || updates.some(u => !u || typeof u !== 'object')
        || ![...deleteByPrefix, ...deleteByMatch].every(t => typeof t === 'string')) {
      return res.status(400).json({ success: false, error: 'updates must be objects; deleteByPrefix and deleteByMatch strings' });
    }
    // Every delete target is worked out first, from the collection as it is
    // before this request changes anything — an update could otherwise
    // rename an entry into a match — so a dry run lists exactly what the
    // real run deletes, each entry once.
    const targets = new Map(); // id -> { doc, entry, byPrefix }
    for (const prefix of deleteByPrefix) {
      const matches = await findOneByPrefix(prefix);
      if (matches.length === 0) { result.notFound.push(prefix); continue; }
      if (matches.length > 1) { result.ambiguous.push({ prefix, count: matches.length }); continue; }
      const r = matches[0];
      if (!targets.has(r.id)) targets.set(r.id, { doc: r, byPrefix: true, entry: { idPrefix: prefix, fullId: r.id, title: r.title, b2Deleted: false } });
    }
    let matched = 0;
    for (const term of deleteByMatch) {
      // An empty or tiny term matches everything — and a delete now takes
      // every image with it, for good.
      if (term.trim().length < 3) { result.errors.push({ term, error: 'deleteByMatch needs a term of at least 3 characters' }); continue; }
      const escaped = term.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      const re = { $regex: escaped, $options: 'i' };
      const matches = await Recording.find({ $or: [{ title: re }, { audioUrl: re }] }).lean();
      if (matches.length === 0) result.notFound.push(`(match) ${term}`);
      for (const m of matches) {
        if (targets.has(m.id)) continue;
        targets.set(m.id, { doc: m, entry: { matchedTerm: term, fullId: m.id, title: m.title } });
        matched++;
      }
    }
    // A broad match is confirmed with the number the dry run reported;
    // without it nothing in the request is applied.
    if (matched) result.matchCount = matched;
    if (matched > 5) {
      if (!dryRun && req.body.confirmCount !== matched) {
        return res.status(400).json({ success: false, matchCount: matched,
          error: `deleteByMatch matches ${matched} entries: dry-run it, then resend with confirmCount: ${matched}` });
      }
    }

    // Updates. One per entry per request: image numbers refer to the list
    // as the viewer showed it, so a second update would count from a list
    // the first one already changed — and differ from what the dry run said.
    const handled = new Set();
    for (const u of updates) {
      const has = k => Object.prototype.hasOwnProperty.call(u, k);
      if (typeof u.idPrefix !== 'string' || !u.idPrefix || (has('title') && !u.title) || (has('category') && !u.category)) {
        result.errors.push({ idPrefix: u.idPrefix, error: 'Missing idPrefix, or an empty title/category' });
        continue;
      }
      const matches = await findOneByPrefix(u.idPrefix);
      if (matches.length === 0) { result.notFound.push(u.idPrefix); continue; }
      if (matches.length > 1) { result.ambiguous.push({ idPrefix: u.idPrefix, count: matches.length }); continue; }
      const existing = matches[0];
      if (handled.has(existing.id)) {
        result.errors.push({ idPrefix: u.idPrefix, error: 'This entry already has an update in this request; put all its changes in one' });
        continue;
      }
      handled.add(existing.id);
      if (targets.has(existing.id)) {
        result.errors.push({ idPrefix: u.idPrefix, error: 'This entry is deleted in this request' });
        continue;
      }
      const kind = existing.kind || 'sound';
      const before = { title: existing.title, category: existing.category, description: existing.description };
      const after = { title: has('title') ? u.title : existing.title, category: has('category') ? u.category : existing.category,
        description: has('description') ? u.description
          : existing.description === 'Auto-discovered recording' ? '' : existing.description };
      if (has('body')) {
        before.body = existing.body || '';
        after.body = cleanText(u.body, LIMITS.body, { multiline: true });
        if (kind === 'text' && !after.body) {
          result.errors.push({ idPrefix: u.idPrefix, error: 'A text cannot lose its words — delete the entry instead' });
          continue;
        }
      }
      if (has('author')) { before.author = existing.author || ''; after.author = cleanText(u.author, LIMITS.author); }
      let removed = [];
      if (has('removeImages')) {
        const imgs = existing.images?.length ? existing.images : existing.imageUrl ? [{ url: existing.imageUrl }] : [];
        const drop = new Set(Array.isArray(u.removeImages) ? u.removeImages : [u.removeImages]);
        if (![...drop].every(n => Number.isInteger(n) && n >= 1 && n <= imgs.length)) {
          result.errors.push({ idPrefix: u.idPrefix, error: `removeImages takes image numbers 1–${imgs.length}` });
          continue;
        }
        const keep = imgs.filter((_, i) => !drop.has(i + 1));
        removed = imgs.filter((_, i) => drop.has(i + 1));
        if (kind === 'image' && !keep.length) {
          result.errors.push({ idPrefix: u.idPrefix, error: 'An image entry keeps at least one image — delete the entry instead' });
          continue;
        }
        before.images = imgs.length;
        after.images = keep;
        after.imageUrl = keep[0]?.url || '';
      }
      const report = () => ({ idPrefix: u.idPrefix, fullId: existing.id, before,
        after: after.images ? { ...after, images: after.images.length } : after,
        ...(removed.length ? { removedImages: removed.map(i => i.url) } : {}) });
      if (dryRun) {
        result.updated.push(report());
      } else {
        try {
          // Use updateOne so the validate hook re-runs and we don't have to re-set location
          await Recording.updateOne(
            { id: existing.id },
            { $set: after },
            { runValidators: true }
          );
          const entry = report();
          // Only once the entry no longer points at them.
          if (removed.length) {
            const imgs = await deleteImagesOf({ images: removed });
            entry.b2ImagesDeleted = imgs.deleted;
            if (imgs.errors.length) entry.b2ImageErrors = imgs.errors;
          }
          result.updated.push(entry);
        } catch (e) {
          result.errors.push({ idPrefix: u.idPrefix, error: e.message });
        }
      }
    }

    // Deletes. alsoDeleteB2 (deleteByPrefix only) also removes the B2 audio
    // so a resync won't re-import the recording. Images go with the entry
    // either way: unlike audio, nothing can re-link them.
    for (const { doc: r, entry, byPrefix } of targets.values()) {
      if (dryRun) {
        if (byPrefix) entry.b2WouldDelete = alsoDeleteB2 && !!r.audioUrl;
        entry.b2ImagesWouldDelete = imageUrlsOf(r).length;
        result.deleted.push(entry);
        continue;
      }
      // Another request may have deleted it while the updates ran; that one
      // removed the images. The audio still goes when asked, so a plain
      // DELETE (which keeps it) can't be revived by a resync.
      const { deletedCount } = await Recording.deleteOne({ id: r.id });
      if (!deletedCount) entry.alreadyDeleted = true;
      if (byPrefix && alsoDeleteB2 && r.audioUrl) {
        try { await deleteB2Object(r.audioUrl); entry.b2Deleted = true; }
        catch (e) { entry.b2Error = e.message; }
      }
      if (deletedCount) {
        const imgs = await deleteImagesOf(r);
        if (imgs.deleted) entry.b2ImagesDeleted = imgs.deleted;
        if (imgs.errors.length) entry.b2ImageErrors = imgs.errors;
      }
      result.deleted.push(entry);
    }

    if (!dryRun) await refreshCache();
    res.json({ success: true, ...result });
  } catch (err) {
    console.error('[POST /admin/bulk-update]', err.message);
    if (!dryRun) await refreshCache().catch(() => {}); // some changes may have landed
    res.status(500).json({ success: false, error: err.message, partial: result });
  }
});

// One-time migration: re-encode legacy webm/m4a/mp4 recordings to MP3 so
// they play on Safari. POST {dryRun:true} to list, {dryRun:false, limit:6}
// to convert a batch; repeat until `remaining` is 0.
app.post('/api/admin/transcode-legacy', requireAdmin, async (req, res) => {
  try {
    const result = await transcodeLegacyToMp3({
      dryRun: req.body?.dryRun !== false,
      limit:  Math.min(parseInt(req.body?.limit, 10) || 6, 12),
    });
    if (!result.dryRun) await refreshCache();
    res.json({ success: true, ...result });
  } catch (err) {
    console.error('[POST /admin/transcode-legacy]', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// Backfill `duration` for recordings uploaded before durations were
// captured at upload. POST {limit:10}; repeat until `remaining` is 0.
app.post('/api/admin/backfill-durations', requireAdmin, async (req, res) => {
  try {
    const result = await backfillDurations({
      limit: Math.min(parseInt(req.body?.limit, 10) || 10, 20),
    });
    if (result.updated.length) await refreshCache();
    res.json({ success: true, ...result });
  } catch (err) {
    console.error('[POST /admin/backfill-durations]', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ── Static files ─────────────────────────────────────────────────────────
// index: false so '/' falls through to the OG-injecting handler below —
// social scrapers don't run JS, so ?rec= share links need their og: tags
// rewritten server-side to unfurl with the recording's title on WhatsApp/
// Zalo/Facebook.
app.use(express.static(path.join(__dirname, '../client'), { index: false }));

const CLIENT_INDEX = fs.readFileSync(path.join(__dirname, '../client/index.html'), 'utf8');
const SITE_URL = 'https://saigon-soundscape.onrender.com';

function escAttr(s) {
  return String(s).replace(/&/g, '&amp;').replace(/"/g, '&quot;')
                  .replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

// Replace an og: meta's content via callback (a "$&" in a recording title
// would corrupt a string-replacement pattern).
function setOg(html, prop, value) {
  return html.replace(
    new RegExp(`(property="og:${prop}" content=")[^"]*`),
    (_, p1) => p1 + escAttr(value)
  );
}

// App download page (Android APK + iOS install steps)
app.get('/app', (req, res) => {
  res.set('Cache-Control', 'no-cache');
  res.sendFile(path.join(__dirname, '../client/app.html'));
});

// Phòng nghe — the virtual listening room (generative Web Audio composition
// over the live archive; audio streams direct from B2, which allows this origin)
app.get('/listen', (req, res) => {
  res.set('Cache-Control', 'no-cache');
  res.sendFile(path.join(__dirname, '../client/listen.html'));
});

// Digital Asset Links: lets the Android TWA app open full-screen without
// a browser bar. Fingerprint = SHA-256 of the APK signing key.
app.get('/.well-known/assetlinks.json', (req, res) => {
  res.json([{
    relation: ['delegate_permission/common.handle_all_urls'],
    target: {
      namespace: 'android_app',
      package_name: 'com.officinegap.soundmap',
      sha256_cert_fingerprints: [process.env.APK_CERT_SHA256 || 'PLACEHOLDER'],
    },
  }]);
});

// An unknown API path is a JSON 404, not the map's HTML: a newer client
// talking to an older server tells "no such endpoint" from a reply.
app.all('/api/*', (req, res) => res.status(404).json({ error: 'Not found' }));

app.get('*', async (req, res) => {
  let html = CLIENT_INDEX;
  const ref = typeof req.query.rec === 'string' ? req.query.rec : null;
  if (ref && /^[a-f0-9]{4,36}$/i.test(ref)) {
    try {
      const list = cacheIsFresh() ? cache : await refreshCache();
      const r = list.find(x => x.id && x.id.startsWith(ref));
      if (r) {
        const title = (r.title || '').replace(/^\[[^\]]*\]\s*/, '').trim() || 'Untitled';
        // A shared poem unfurls with its opening lines.
        const opening = (r.body || '').replace(/\s+/g, ' ').trim();
        const what = { image: 'An image', text: 'A text' }[r.kind] || `A ${r.category || 'field'} recording`;
        const desc  = r.description ||
          (opening ? (opening.length > 200 ? cleanText(opening, 199) + '…' : opening) : '') ||
          `${what} from the Saigon_Miền Tây Sound Map.`;
        html = setOg(html, 'title', `${title} — Saigon_Miền Tây Sound Map`);
        html = setOg(html, 'description', desc);
        html = setOg(html, 'url', `${SITE_URL}/?rec=${ref}`);
        // Recording photos make share links unfurl with a picture. The
        // 720 px thumbnail, not the 2400 px drawing: WhatsApp drops
        // previews whose image is over about 300 KB.
        const first = r.images?.[0];
        const ogImage = first?.thumbUrl || r.imageUrl;
        if (ogImage) {
          let meta = `<meta property="og:image" content="${escAttr(ogImage)}">`;
          if (first?.thumbUrl && first.width > 0 && first.height > 0) {
            const k = Math.min(1, THUMB_EDGE / Math.max(first.width, first.height));
            meta += `<meta property="og:image:width" content="${Math.round(first.width * k)}">` +
              `<meta property="og:image:height" content="${Math.round(first.height * k)}">`;
          }
          html = html.replace('<!--og:image-->', meta);
          html = html.replace('name="twitter:card" content="summary"',
            'name="twitter:card" content="summary_large_image"');
        }
      }
    } catch (err) {
      console.error('[og-inject]', err.message); // serve defaults on failure
    }
  }
  // The whole app is this one HTML file, so a cached copy pins a visitor
  // to an old build until they clear it by hand. Revalidate every load —
  // it is a few KB, and fixes must actually reach people.
  res.set('Cache-Control', 'no-cache');
  res.type('html').send(html);
});

// Multer errors (file too large etc.) must come back as JSON 400, not
// Express's default HTML 500 — the client's retry loop treats non-4xx as
// retryable and would pointlessly re-send the full payload twice more.
app.use((err, req, res, next) => {
  if (err instanceof multer.MulterError) {
    return res.status(400).json({ success: false, message: `Upload rejected: ${err.message}` });
  }
  next(err);
});

// ── Start ────────────────────────────────────────────────────────────────
// Atlas occasionally refuses the first connection after a cold start (the
// cause of Render's "Exited with status 1" alerts). Retry with backoff
// instead of dying — the container is useless without the DB anyway.
async function connectWithRetry() {
  for (let attempt = 1; ; attempt++) {
    try {
      await mongoose.connect(process.env.MONGO_URI);
      console.log('MongoDB connected');
      return;
    } catch (err) {
      const delay = Math.min(30000, 1000 * 2 ** attempt);
      console.error(`MongoDB connect failed (attempt ${attempt}): ${err.message} — retrying in ${delay}ms`);
      await new Promise(r => setTimeout(r, delay));
    }
  }
}

async function start() {
  await connectWithRetry();

  app.listen(PORT, () => console.log(`Server on port ${PORT}`));

  // The B2->Mongo sync used to run on every boot. On the free tier the
  // service cold-starts many times a day, so that meant re-listing the
  // whole bucket and re-scanning the collection for nothing — and it is
  // what resurrected orphaned objects as "Auto-discovered recording"
  // junk after the auto-translate incident. Recovery is still one call
  // to the admin route /api/resync away; set BOOT_SYNC=true to restore
  // the old behaviour.
  try {
    if (process.env.BOOT_SYNC === 'true') await syncB2ToMongo();
    await refreshCache();
    console.log(`Cache: ${cache.length} recordings`);
  } catch (err) {
    console.error('Initial cache load failed (non-fatal):', err.message);
    await refreshCache().catch(() => {});
  }
}

start();
module.exports = app;
