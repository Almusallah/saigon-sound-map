/**
 * Image normalisation for optional recording photos.
 *
 * Why normalise: phone photos arrive as 3–12 MB JPEG/HEIC with EXIF
 * (including GPS). Stored as-is they would bloat B2 and leak contributor
 * location metadata. Everything is re-encoded to mozjpeg ≤1600px long
 * edge (~150–350 KB typical). JPEG over WebP deliberately: the same URL
 * doubles as og:image for share links, and WhatsApp/Zalo/Facebook link
 * previews — the channels this map is shared on — don't reliably render
 * WebP. mozjpeg closes most of the size gap, and JPEG decodes anywhere,
 * forever.
 *
 * rotate() first: applies the EXIF orientation flag BEFORE metadata is
 * dropped, so portrait shots don't end up sideways.
 */

const sharp = require('sharp');

const MAX_EDGE = 1600;
const JPEG_QUALITY = 78;

async function processImage(inputBuffer) {
  // limitInputPixels: refuse decompression bombs (a 268 MP PNG would
  // balloon to ~1 GB of pixels on a 512 MB instance). 50 MP is far above
  // any real phone photo; over-limit images throw and are skipped.
  const out = await sharp(inputBuffer, { failOn: 'error', limitInputPixels: 50_000_000 })
    .rotate()
    .resize(MAX_EDGE, MAX_EDGE, { fit: 'inside', withoutEnlargement: true })
    .jpeg({ quality: JPEG_QUALITY, mozjpeg: true })
    .toBuffer();
  return { buffer: out, ext: 'jpg', mime: 'image/jpeg' };
}

module.exports = { processImage };
