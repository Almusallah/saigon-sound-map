require('dotenv').config();

const express   = require('express');
const cors      = require('cors');
const path      = require('path');
const multer    = require('multer');
const mongoose  = require('mongoose');
const rateLimit = require('express-rate-limit');

const Recording = require('./models/Recording');
const { syncB2ToMongo, uploadRecording, cleanupOrphans, deleteB2Object, streamB2ObjectAsAttachment } = require('./utils/b2');

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
// Anonymous uploads need spam protection. 10/hour/IP is generous for a
// genuine field-recordist (a typical session yields 1-3 uploads), tight
// enough to make scripted abuse uneconomic.
const uploadLimiter = rateLimit({
  windowMs: 60 * 60 * 1000,
  max: 10,
  standardHeaders: true,
  legacyHeaders: false,
  message: { success: false, message: 'Too many uploads from this IP. Try again later.' },
});

// Read endpoints get a generous bucket — protects against scrape floods
// without affecting normal browsing.
const readLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: 120,
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
  limits: { fileSize: 25 * 1024 * 1024 }, // 25 MB
});

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
    recordings: cache.length,
    uptime: process.uptime(),
  });
});

app.get('/api/recordings', readLimiter, async (req, res) => {
  try {
    const recordings = cacheIsFresh() ? cache : await refreshCache();
    res.json({ recordings });
  } catch (err) {
    console.error('[GET /recordings]', err.message);
    res.status(500).json({ error: 'Failed to fetch recordings' });
  }
});

app.get('/api/search', readLimiter, async (req, res) => {
  try {
    const q = req.query.q || '';
    const results = await Recording.find({
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
// endpoint fetches from B2 server-side and re-streams with a
// Content-Disposition: attachment header so the browser actually saves it
// with a friendly filename derived from the recording's title.
app.get('/api/download/:idPrefix', readLimiter, async (req, res) => {
  try {
    const prefix = req.params.idPrefix;
    if (!/^[a-f0-9]{4,}$/i.test(prefix)) {
      return res.status(400).json({ error: 'Invalid id prefix' });
    }
    const r = await Recording.findOne({ id: { $regex: '^' + prefix } });
    if (!r) return res.status(404).json({ error: 'Recording not found' });
    const ext = (r.audioUrl.split('.').pop() || 'audio').split('?')[0];
    const safe = (r.title || 'recording')
      .replace(/[^\w\s\-À-ɏḀ-ỿ]/g, '')
      .replace(/\s+/g, '_')
      .slice(0, 60)
      .replace(/_+$/, '');
    const filename = `${safe || 'recording'}.${ext}`;
    await streamB2ObjectAsAttachment(r.audioUrl, filename, res);
  } catch (err) {
    console.error('[GET /download]', err.message);
    if (!res.headersSent) res.status(500).json({ error: 'Download failed' });
  }
});

app.post('/api/upload', uploadLimiter, upload.single('audioFile'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ success: false, message: 'No audio file' });

    const lat = parseFloat(req.body.latitude);
    const lng = parseFloat(req.body.longitude);
    if (isNaN(lat) || isNaN(lng)) return res.status(400).json({ success: false, message: 'Invalid coordinates' });

    const recording = await uploadRecording(req.file.buffer, req.file.mimetype, {
      title:            req.body.title,
      description:      req.body.description,
      category:         req.body.category,
      latitude:         lat,
      longitude:        lng,
      originalFilename: req.file.originalname,
    });

    cache = [recording.toObject(), ...cache];
    cacheTime = Date.now();
    res.json({ success: true, recording });
  } catch (err) {
    console.error('[POST /upload]', err.message);
    res.status(500).json({ success: false, message: 'Upload failed', error: err.message });
  }
});

app.delete('/api/recordings/:id', requireAdmin, async (req, res) => {
  try {
    await Recording.deleteOne({ id: req.params.id });
    cache = cache.filter(r => r.id !== req.params.id);
    res.json({ success: true });
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
//       { "idPrefix": "f6fd31a9", "title": "Kids playing", "category": "Play & Leisure" }
//     ],
//     "deleteByPrefix": ["3f6cfa4f"],
//     "deleteByMatch":  ["FUJIRO"]   // matches title or audioUrl, case-insensitive
//   }
// idPrefix is matched against the START of the recording's UUID.
// description is set to '' on every update (clears the auto-discovered text).
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
    // Updates
    for (const u of updates) {
      if (!u.idPrefix || !u.title || !u.category) {
        result.errors.push({ idPrefix: u.idPrefix, error: 'Missing idPrefix/title/category' });
        continue;
      }
      const matches = await findOneByPrefix(u.idPrefix);
      if (matches.length === 0) { result.notFound.push(u.idPrefix); continue; }
      if (matches.length > 1) { result.ambiguous.push({ idPrefix: u.idPrefix, count: matches.length }); continue; }
      const existing = matches[0];
      const before = { title: existing.title, category: existing.category, description: existing.description };
      if (dryRun) {
        result.updated.push({ idPrefix: u.idPrefix, fullId: existing.id, before, after: { title: u.title, category: u.category, description: '' } });
      } else {
        try {
          // Use updateOne so the validate hook re-runs and we don't have to re-set location
          await Recording.updateOne(
            { id: existing.id },
            { $set: { title: u.title, category: u.category, description: '' } },
            { runValidators: true }
          );
          result.updated.push({ idPrefix: u.idPrefix, fullId: existing.id, before, after: { title: u.title, category: u.category, description: '' } });
        } catch (e) {
          result.errors.push({ idPrefix: u.idPrefix, error: e.message });
        }
      }
    }

    // Deletes by id-prefix. If alsoDeleteB2=true, the underlying B2 audio
    // file is also removed so the auto-sync won't re-import the recording.
    for (const prefix of deleteByPrefix) {
      const matches = await findOneByPrefix(prefix);
      if (matches.length === 0) { result.notFound.push(prefix); continue; }
      if (matches.length > 1) { result.ambiguous.push({ prefix, count: matches.length }); continue; }
      const r = matches[0];
      const entry = { idPrefix: prefix, fullId: r.id, title: r.title, b2Deleted: false };
      if (dryRun) {
        entry.b2WouldDelete = alsoDeleteB2;
        result.deleted.push(entry);
      } else {
        await Recording.deleteOne({ id: r.id });
        if (alsoDeleteB2) {
          try { await deleteB2Object(r.audioUrl); entry.b2Deleted = true; }
          catch (e) { entry.b2Error = e.message; }
        }
        result.deleted.push(entry);
      }
    }

    // Deletes by string match (title or audioUrl, case-insensitive)
    for (const term of deleteByMatch) {
      const escaped = term.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      const re = { $regex: escaped, $options: 'i' };
      const matches = await Recording.find({ $or: [{ title: re }, { audioUrl: re }] }).lean();
      for (const m of matches) {
        if (dryRun) {
          result.deleted.push({ matchedTerm: term, fullId: m.id, title: m.title });
        } else {
          await Recording.deleteOne({ id: m.id });
          result.deleted.push({ matchedTerm: term, fullId: m.id, title: m.title });
        }
      }
      if (matches.length === 0) result.notFound.push(`(match) ${term}`);
    }

    if (!dryRun) await refreshCache();
    res.json({ success: true, ...result });
  } catch (err) {
    console.error('[POST /admin/bulk-update]', err.message);
    res.status(500).json({ success: false, error: err.message, partial: result });
  }
});

// ── Static files ─────────────────────────────────────────────────────────
app.use(express.static(path.join(__dirname, '../client')));
app.get('*', (req, res) => res.sendFile(path.join(__dirname, '../client/index.html')));

// ── Start ────────────────────────────────────────────────────────────────
async function start() {
  try {
    await mongoose.connect(process.env.MONGO_URI);
    console.log('MongoDB connected');
  } catch (err) {
    console.error('MongoDB failed:', err.message);
    process.exit(1);
  }

  app.listen(PORT, () => console.log(`Server on port ${PORT}`));

  try {
    await syncB2ToMongo();
    await refreshCache();
    console.log(`Cache: ${cache.length} recordings`);
  } catch (err) {
    console.error('Initial sync failed (non-fatal):', err.message);
    await refreshCache();
  }
}

start();
module.exports = app;
