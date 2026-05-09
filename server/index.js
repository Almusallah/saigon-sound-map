require('dotenv').config();

const express   = require('express');
const cors      = require('cors');
const path      = require('path');
const multer    = require('multer');
const mongoose  = require('mongoose');
const rateLimit = require('express-rate-limit');

const Recording = require('./models/Recording');
const { syncB2ToMongo, uploadRecording, cleanupOrphans } = require('./utils/b2');

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
