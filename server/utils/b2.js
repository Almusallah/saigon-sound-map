/**
 * Backblaze B2 utilities — listing, uploading, syncing with MongoDB.
 * Uses AWS SDK v3 (S3-compatible).
 */

const { S3Client, ListObjectsV2Command, HeadObjectCommand } = require('@aws-sdk/client-s3');
const { Upload } = require('@aws-sdk/lib-storage');
const { v4: uuidv4 } = require('uuid');
const Recording = require('../models/Recording');

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
        category: 'Others',
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
  const id  = uuidv4();
  const ext = (originalFilename || '').split('.').pop() || 'webm';
  const key = buildKey(id, latitude, longitude, ext);

  console.log(`[upload] ${key} (${fileBuffer.length} bytes)`);

  await new Upload({
    client: getS3(),
    params: { Bucket: getBucket(), Key: key, Body: fileBuffer, ContentType: mimeType },
  }).done();

  const doc = new Recording({
    id, title: title || 'New Recording', description: description || '',
    category: category || 'Others', audioUrl: keyToUrl(key),
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

module.exports = { syncB2ToMongo, uploadRecording, cleanupOrphans };
