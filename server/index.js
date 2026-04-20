require('dotenv').config();

const express  = require('express');
const cors     = require('cors');
const path     = require('path');
const multer   = require('multer');
const mongoose = require('mongoose');

const Recording = require('./models/Recording');
const { syncB2ToMongo, uploadRecording, cleanupOrphans } = require('./utils/b2');

// ── App setup ────────────────────────────────────────────────────────────
const app  = express();
const PORT = process.env.PORT || 3000;

app.use(cors({ origin: '*', methods: ['GET', 'POST', 'DELETE', 'OPTIONS'] }));
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

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

app.get('/api/recordings', async (req, res) => {
  try {
    const recordings = cacheIsFresh() ? cache : await refreshCache();
    res.json({ recordings });
  } catch (err) {
    console.error('[GET /recordings]', err.message);
    res.status(500).json({ error: 'Failed to fetch recordings' });
  }
});

app.get('/api/search', async (req, res) => {
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

app.post('/api/upload', upload.single('audioFile'), async (req, res) => {
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

app.delete('/api/recordings/:id', async (req, res) => {
  try {
    await Recording.deleteOne({ id: req.params.id });
    cache = cache.filter(r => r.id !== req.params.id);
    res.json({ success: true });
  } catch (err) {
    console.error('[DELETE /recordings]', err.message);
    res.status(500).json({ success: false, message: 'Delete failed' });
  }
});

app.get('/api/resync', async (req, res) => {
  try {
    const result = await syncB2ToMongo();
    await refreshCache();
    res.json({ success: true, ...result });
  } catch (err) {
    console.error('[GET /resync]', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get('/api/cleanup', async (req, res) => {
  try {
    const removed = await cleanupOrphans();
    await refreshCache();
    res.json({ success: true, removed });
  } catch (err) {
    console.error('[GET /cleanup]', err.message);
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
