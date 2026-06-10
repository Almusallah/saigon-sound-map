/**
 * Backblaze B2 utilities — listing, uploading, syncing with MongoDB.
 * Uses AWS SDK v3 (S3-compatible).
 */

const { S3Client, ListObjectsV2Command, HeadObjectCommand } = require('@aws-sdk/client-s3');
const { Upload } = require('@aws-sdk/lib-storage');
const { v4: uuidv4 } = require('uuid');
const Recording = require('../models/Recording');
const { transcodeToMp3, isMp3Buffer } = require('./transcode');

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

async function uploadRecording(fileBuffer, mimeType, { title, description, category, latitude, longitude, originalFilename }) {
  const id = uuidv4();

  // Normalise everything to MP3 before it reaches B2 — WebM/Opus uploads
  // (Chrome/Android recordings) are silent for every Safari listener.
  if (!isMp3Buffer(fileBuffer)) {
    const before = fileBuffer.length;
    fileBuffer = await transcodeToMp3(fileBuffer);
    console.log(`[upload] transcoded to mp3: ${before} -> ${fileBuffer.length} bytes`);
  }
  const key = buildKey(id, latitude, longitude, 'mp3');

  console.log(`[upload] ${key} (${fileBuffer.length} bytes)`);

  await new Upload({
    client: getS3(),
    params: { Bucket: getBucket(), Key: key, Body: fileBuffer, ContentType: 'audio/mpeg' },
  }).done();

  const doc = new Recording({
    id, title: title || 'New Recording', description: description || '',
    category: category || 'Background', audioUrl: keyToUrl(key),
    latitude: parseFloat(latitude), longitude: parseFloat(longitude),
    source: 'upload', fileSize: fileBuffer.length,
  });
  await doc.save();
  console.log(`[upload] Saved ${id}`);
  return doc;
}

// ── Cleanup orphans ──────────────────────────────────────────────────────

async function cleanupOrphans() {
  const s3 = getS3();
  const all = await Recording.find({}, 'audioUrl').lean();
  const toRemove = [];

  for (const rec of all) {
    const key = rec.audioUrl.split(`${getBucket()}/`)[1];
    if (!key) { toRemove.push(rec._id); continue; }
    try {
      await s3.send(new HeadObjectCommand({ Bucket: getBucket(), Key: key }));
    } catch (err) {
      if (err.name === 'NotFound' || err.$metadata?.httpStatusCode === 404) {
        toRemove.push(rec._id);
      }
    }
  }

  if (toRemove.length > 0) {
    await Recording.deleteMany({ _id: { $in: toRemove } });
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

module.exports = {
  syncB2ToMongo, uploadRecording, cleanupOrphans,
  deleteB2Object, streamB2ObjectAsMp3, transcodeLegacyToMp3,
};
