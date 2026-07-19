/**
 * Image normalisation for optional recording photos.
 *
 * Why normalise: phone photos arrive as 3–12 MB JPEG/HEIC with EXIF
 * (including GPS). Stored as-is they would bloat B2 and leak contributor
 * location metadata. Everything is re-encoded to WebP ≤1600px long edge,
 * quality 80 (~100–250 KB typical) — universally decodable since 2020 and
 * roughly half the bytes of an equivalent JPEG, which keeps the archive
 * cheap to store and fast to load for the long haul.
 *
 * rotate() first: applies the EXIF orientation flag BEFORE metadata is
 * dropped, so portrait shots don't end up sideways.
 */

const sharp = require('sharp');

const MAX_EDGE = 1600;
const WEBP_QUALITY = 80;

async function processImage(inputBuffer) {
  // limitInputPixels: refuse decompression bombs (a 268 MP PNG would
  // balloon to ~1 GB of pixels on a 512 MB instance). 50 MP is far above
  // any real phone photo; over-limit images throw and are skipped.
  const out = await sharp(inputBuffer, { failOn: 'error', limitInputPixels: 50_000_000 })
    .rotate()
    .resize(MAX_EDGE, MAX_EDGE, { fit: 'inside', withoutEnlargement: true })
    .webp({ quality: WEBP_QUALITY })
    .toBuffer();
  return { buffer: out, ext: 'webp', mime: 'image/webp' };
}

module.exports = { processImage };
