/**
 * Backblaze B2 utilities — listing, uploading, syncing with MongoDB.
 * Uses AWS SDK v3 (S3-compatible).
 */

const { S3Client, ListObjectsV2Command, HeadObjectCommand } = require('@aws-sdk/client-s3');
const { Upload } = require('@aws-sdk/lib-storage');
const { v4: uuidv4 } = require('uuid');
const Recording = require('../models/Recording');
const { transcodeToMp3, isMp3Buffer, probeDuration } = require('./transcode');

const AUDIO_EXTENSIONS = new Set(['webm', 'mp3', 'mp4', 'm4a', 'ogg', 'wav', 'aac', 'flac']);

// ── S3 client ────────────────────────────────────────────────────────────
let _s3 = null;

function getS3() {
  if (!_s3) {
    _s3 = new S3Client({
      endpoint:     `https://${process.env.B2_ENDPOINT}`,
      region:       process.env.B2_REGION || 'us-west-004',
      credentials: {
        accessKeyId:     process.env.B2_APPLICATION_KEY_ID,
        secretAccessKey: process.env.B2_APPLICATION_KEY,
      },
      forcePathStyle: true,
    });
  }
  return _s3;
}

function getBucket() { return process.env.B2_BUCKET_NAME; }

// ── Filename helpers ─────────────────────────────────────────────────────

function buildKey(id, lat, lng, ext) {
  return `recordings/${id}_${lat}_${lng}.${ext}`;
}

function keyToUrl(key) {
  return `https://${process.env.B2_ENDPOINT}/${getBucket()}/${key}`;
}

function parseFilename(filename) {
  try {
    const withoutExt = filename.replace(/\.[^_.]+$/, '');
    const parts = withoutExt.split('_');
    if (parts.length < 3) return null;
    const lng = parseFloat(parts[parts.length - 1]);
    const lat = parseFloat(parts[parts.length - 2]);
    const id  = parts.slice(0, parts.length - 2).join('_');
    if (isNaN(lat) || isNaN(lng)) return null;
    if (lat < -90 || lat > 90 || lng < -180 || lng > 180) return null;
    return { id, latitude: lat, longitude: lng };
  } catch { return null; }
}

function fileExtension(key) {
  const dot = key.lastIndexOf('.');
  return dot === -1 ? null : key.slice(dot + 1).toLowerCase();
}

// ── List all audio files (paginated) ─────────────────────────────────────

async function listAllAudioFiles() {
  const s3 = getS3();
  const files = [];
  let token;
  do {
    const res = await s3.send(new ListObjectsV2Command({
      Bucket: getBucket(),
      ContinuationToken: token,
    }));
    for (const obj of (res.Contents || [])) {
      if (obj.Key.endsWith('/')) continue;
      const ext = fileExtension(obj.Key);
      if (!ext || !AUDIO_EXTENSIONS.has(ext)) continue;
      files.push({ key: obj.Key, size: obj.Size, lastModified: obj.LastModified });
    }
    token = res.IsTruncated ? res.NextContinuationToken : undefined;
  } while (token);
  return files;
}

// ── Sync B2 -> MongoDB ───────────────────────────────────────────────────

async function syncB2ToMongo() {
  console.log('[sync] Scanning B2 bucket...');
  const files = await listAllAudioFiles();
  console.log(`[sync] Found ${files.length} audio files`);

  const existing = await Recording.find({}, 'audioUrl').lean();
  const existingUrls = new Set(existing.map(r => r.audioUrl));
  let added = 0;

  for (const file of files) {
    const url = keyToUrl(file.key);
    if (existingUrls.has(url)) continue;

    const filename = file.key.split('/').pop();
    const meta = parseFilename(filename);
    if (!meta) { console.log(`[sync] Skipping: ${file.key}`); continue; }

    try {
      await new Recording({
        id: meta.id,
        title: `Recording ${meta.id.substring(0, 8)}`,
        description: 'Auto-discovered recording',
        category: 'Background',
        audioUrl: url,
        latitude: meta.latitude,
        longitude: meta.longitude,
        source: 'b2-sync',
        fileSize: file.size,
      }).save();
      added++;
    } catch (err) {
      if (err.code === 11000) {
        await Recording.updateOne({ id: meta.id }, { audioUrl: url });
      } else {
        console.error(`[sync] Error: ${meta.id}:`, err.message);
      }
    }
  }

  const total = await Recording.countDocuments();
  console.log(`[sync] Done. +${added} new, ${total} total`);
  return { added, total };
}

// ── Upload ───────────────────────────────────────────────────────────────

function buildImageKey(id, lat, lng, ext) {
  return `images/${id}_${lat}_${lng}.${ext}`;
}

async function uploadRecording(fileBuffer, mimeType, { title, description, category, latitude, longitude, originalFilename, imageBuffer }) {
  const id = uuidv4();

  // Normalise everything to MP3 before it reaches B2 — WebM/Opus uploads
  // (Chrome/Android recordings) are silent for every Safari listener.
  if (!isMp3Buffer(fileBuffer)) {
    const before = fileBuffer.length;
    fileBuffer = await transcodeToMp3(fileBuffer);
    console.log(`[upload] transcoded to mp3: ${before} -> ${fileBuffer.length} bytes`);
  }
  const key = buildKey(id, latitude, longitude, 'mp3');
  const duration = await probeDuration(fileBuffer);

  console.log(`[upload] ${key} (${fileBuffer.length} bytes, ${duration}s)`);

  await new Upload({
    client: getS3(),
    params: { Bucket: getBucket(), Key: key, Body: fileBuffer, ContentType: 'audio/mpeg' },
  }).done();

  // Optional photo. Best-effort: a photo that fails to decode must never
  // sink the sound itself — the image is simply skipped.
  let imageUrl = '';
  if (imageBuffer && imageBuffer.length) {
    try {
      const { processImage } = require('./image');
      const img = await processImage(imageBuffer);
      const imgKey = buildImageKey(id, latitude, longitude, img.ext);
      await new Upload({
        client: getS3(),
        params: { Bucket: getBucket(), Key: imgKey, Body: img.buffer, ContentType: img.mime },
      }).done();
      imageUrl = keyToUrl(imgKey);
      console.log(`[upload] photo ${imgKey} (${imageBuffer.length} -> ${img.buffer.length} bytes)`);
    } catch (err) {
      console.error('[upload] photo skipped:', err.message);
    }
  }

  // Unknown category values must never sink an upload. Browser
  // auto-translate once submitted "Casa" for "Home" — coerce anything
  // off-list to Background instead of failing enum validation.
  const safeCategory = Recording.CATEGORIES.includes(category) ? category : 'Background';
  if (safeCategory !== category && category) {
    console.log(`[upload] unknown category ${JSON.stringify(category)} -> Background`);
  }

  const doc = new Recording({
    id, title: title || 'New Recording', description: description || '',
    category: safeCategory, audioUrl: keyToUrl(key), imageUrl,
    latitude: parseFloat(latitude), longitude: parseFloat(longitude),
    source: 'upload', fileSize: fileBuffer.length, duration,
  });
  try {
    await doc.save();
  } catch (err) {
    // An unsaved recording must leave nothing behind in B2: a leftover
    // audio object gets re-imported by the next sync as an anonymous
    // "Auto-discovered recording" (junk — exactly what the failed "Casa"
    // uploads produced), and nothing could ever re-link the photo.
    try { await deleteB2Object(keyToUrl(key)); } catch {}
    if (imageUrl) {
      try { await deleteB2Object(imageUrl); } catch {}
    }
    throw err;
  }
  console.log(`[upload] Saved ${id}`);
  return doc;
}

// ── Cleanup orphans ──────────────────────────────────────────────────────

async function cleanupOrphans() {
  const s3 = getS3();
  const all = await Recording.find({}, 'audioUrl imageUrl').lean();
  const toRemove = [];

  for (const rec of all) {
    const key = rec.audioUrl.split(`${getBucket()}/`)[1];
    if (!key) { toRemove.push(rec); continue; }
    try {
      await s3.send(new HeadObjectCommand({ Bucket: getBucket(), Key: key }));
    } catch (err) {
      if (err.name === 'NotFound' || err.$metadata?.httpStatusCode === 404) {
        toRemove.push(rec);
      }
    }
  }

  if (toRemove.length > 0) {
    // A removed doc's photo would otherwise strand in B2 with nothing
    // referencing it — delete images along with the docs.
    for (const rec of toRemove) {
      if (rec.imageUrl) {
        try { await deleteB2Object(rec.imageUrl); } catch {}
      }
    }
    await Recording.deleteMany({ _id: { $in: toRemove.map(r => r._id) } });
    console.log(`[cleanup] Removed ${toRemove.length} orphans`);
  }
  return toRemove.length;
}

// Delete a single object from the B2 bucket. Used when permanently
// removing a recording so the B2 sync doesn't re-import it on next run.
async function deleteB2Object(audioUrl) {
  const { DeleteObjectCommand } = require('@aws-sdk/client-s3');
  const prefix = `https://${process.env.B2_ENDPOINT}/${getBucket()}/`;
  if (!audioUrl.startsWith(prefix)) {
    throw new Error(`audioUrl doesn't match expected prefix: ${audioUrl}`);
  }
  const key = audioUrl.slice(prefix.length);
  await getS3().send(new DeleteObjectCommand({ Bucket: getBucket(), Key: key }));
  return { key };
}

// Fetch a B2 object and stream its bytes through `res` as an MP3
// attachment. If the source is already MP3, the bytes are piped straight
// through. Otherwise (webm/opus, m4a, mp4, ogg, etc.) the bytes go
// through ffmpeg, which re-encodes to MP3 at 128 kbps so downloads are
// always universally playable. Used by /api/download/:idPrefix.
async function streamB2ObjectAsMp3(audioUrl, filename, res) {
  const { GetObjectCommand } = require('@aws-sdk/client-s3');
  const { spawn } = require('child_process');
  const prefix = `https://${process.env.B2_ENDPOINT}/${getBucket()}/`;
  if (!audioUrl.startsWith(prefix)) {
    throw new Error(`audioUrl doesn't match expected prefix: ${audioUrl}`);
  }
  const key = audioUrl.slice(prefix.length);
  const isMp3 = /\.mp3$/i.test(key);

  const obj = await getS3().send(new GetObjectCommand({ Bucket: getBucket(), Key: key }));

  // ASCII-safe filename fallback for old browsers + RFC 5987 form for new ones
  const asciiName = filename.replace(/[^\w.\-]/g, '_');
  res.set('Content-Type',         'audio/mpeg');
  res.set('Content-Disposition',
    `attachment; filename="${asciiName}"; filename*=UTF-8''${encodeURIComponent(filename)}`);
  res.set('Cache-Control',        'public, max-age=3600');

  if (isMp3) {
    // Pass-through: no transcode needed. Send Content-Length so the
    // browser shows accurate progress.
    if (obj.ContentLength) res.set('Content-Length', obj.ContentLength);
    obj.Body.pipe(res);
    return;
  }

  // Transcode via ffmpeg. We can't predict the output length, so no
  // Content-Length header — browser shows indeterminate progress.
  const ff = spawn('ffmpeg', [
    '-hide_banner', '-loglevel', 'error',
    '-i', 'pipe:0',
    '-vn',                       // strip video tracks (mp4/m4a often have one)
    '-acodec', 'libmp3lame',
    '-b:a', '128k',
    '-ar', '44100',
    '-f', 'mp3',
    'pipe:1',
  ]);
  obj.Body.pipe(ff.stdin);
  ff.stdout.pipe(res);
  ff.stderr.on('data', d => console.log('[ffmpeg]', d.toString().trim().slice(0, 200)));
  ff.on('error', err => {
    console.error('[ffmpeg spawn]', err.message);
    if (!res.headersSent) res.status(500).end();
  });
  // Clean up ffmpeg if the client disconnects mid-download
  res.on('close', () => { try { ff.kill('SIGKILL'); } catch {} });
}

// One-time migration: re-encode every recording that isn't genuine MP3.
// Extensions lie — six legacy ".mp3" files in the bucket are really MP4
// containers — so each object's first bytes are probed instead. Sequential
// on purpose (free-tier RAM); `limit` keeps each HTTP call well under
// Render's request timeout — the caller loops until `remaining` is 0.
async function transcodeLegacyToMp3({ dryRun = true, limit = 6 } = {}) {
  const { GetObjectCommand } = require('@aws-sdk/client-s3');
  const s3 = getS3();
  const prefix = `https://${process.env.B2_ENDPOINT}/${getBucket()}/`;

  const all = await Recording.find({}, 'id title audioUrl latitude longitude').lean();
  const pending = [];
  for (const rec of all) {
    const key = rec.audioUrl.slice(prefix.length);
    try {
      const probe = await s3.send(new GetObjectCommand({
        Bucket: getBucket(), Key: key, Range: 'bytes=0-15',
      }));
      const chunks = [];
      for await (const c of probe.Body) chunks.push(c);
      if (!isMp3Buffer(Buffer.concat(chunks))) pending.push(rec);
    } catch (err) {
      console.error(`[transcode-legacy] probe ${rec.id.slice(0, 8)}:`, err.message);
    }
  }

  if (dryRun) {
    return {
      dryRun: true,
      remaining: pending.length,
      files: pending.map(r => ({ id: r.id.slice(0, 8), title: r.title, url: r.audioUrl })),
    };
  }

  const done = [], errors = [];
  for (const rec of pending.slice(0, limit)) {
    try {
      const oldKey = rec.audioUrl.slice(prefix.length);
      const obj = await s3.send(new GetObjectCommand({ Bucket: getBucket(), Key: oldKey }));
      const chunks = [];
      for await (const c of obj.Body) chunks.push(c);
      const original = Buffer.concat(chunks);

      const mp3 = await transcodeToMp3(original);
      const newKey = buildKey(rec.id, rec.latitude, rec.longitude, 'mp3');

      await new Upload({
        client: s3,
        params: { Bucket: getBucket(), Key: newKey, Body: mp3, ContentType: 'audio/mpeg' },
      }).done();

      await Recording.updateOne(
        { id: rec.id },
        { audioUrl: keyToUrl(newKey), fileSize: mp3.length }
      );
      // The fake-".mp3" files keep the same key — the upload above already
      // overwrote them, so deleting would destroy the fresh copy.
      if (oldKey !== newKey) await deleteB2Object(rec.audioUrl);

      done.push({ id: rec.id.slice(0, 8), title: rec.title, oldSize: original.length, newSize: mp3.length });
      console.log(`[transcode-legacy] ${rec.id.slice(0, 8)} ${oldKey} -> ${newKey}`);
    } catch (err) {
      errors.push({ id: rec.id.slice(0, 8), error: err.message });
      console.error(`[transcode-legacy] ${rec.id.slice(0, 8)}:`, err.message);
    }
  }

  return { dryRun: false, converted: done, errors, remaining: pending.length - done.length };
}

// Backfill `duration` for recordings that predate duration capture.
// Downloads each file (all genuine MP3 now, ~1-2 MB each) and ffprobes it.
// Batched like transcodeLegacyToMp3 — call until `remaining` is 0.
async function backfillDurations({ limit = 10 } = {}) {
  const { GetObjectCommand } = require('@aws-sdk/client-s3');
  const s3 = getS3();
  const prefix = `https://${process.env.B2_ENDPOINT}/${getBucket()}/`;

  const pending = await Recording.find(
    { $or: [{ duration: { $exists: false } }, { duration: 0 }] },
    'id title audioUrl'
  ).lean();

  const done = [], errors = [];
  for (const rec of pending.slice(0, limit)) {
    try {
      const key = rec.audioUrl.slice(prefix.length);
      const obj = await s3.send(new GetObjectCommand({ Bucket: getBucket(), Key: key }));
      const chunks = [];
      for await (const c of obj.Body) chunks.push(c);
      const duration = await probeDuration(Buffer.concat(chunks));
      if (!duration) throw new Error('ffprobe returned no duration');
      await Recording.updateOne({ id: rec.id }, { duration });
      done.push({ id: rec.id.slice(0, 8), title: rec.title, duration });
    } catch (err) {
      errors.push({ id: rec.id.slice(0, 8), error: err.message });
      console.error(`[backfill-durations] ${rec.id.slice(0, 8)}:`, err.message);
    }
  }

  return { updated: done, errors, remaining: pending.length - done.length };
}

module.exports = {
  syncB2ToMongo, uploadRecording, cleanupOrphans,
  deleteB2Object, streamB2ObjectAsMp3, transcodeLegacyToMp3, backfillDurations,
};
