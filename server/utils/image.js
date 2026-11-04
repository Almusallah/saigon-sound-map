/**
 * Image normalisation for recording photos and image contributions
 * (drawings, sketches, photos).
 *
 * Why normalise: phone photos arrive as 3–12 MB JPEG/HEIC with EXIF
 * (including GPS). Stored as-is they would bloat B2 and leak contributor
 * location metadata. Everything is re-encoded to mozjpeg (~150–350 KB
 * typical at 1600px). JPEG over WebP deliberately: the same URL
 * doubles as og:image for share links, and WhatsApp/Zalo/Facebook link
 * previews — the channels this map is shared on — don't reliably render
 * WebP. mozjpeg closes most of the size gap, and JPEG decodes anywhere,
 * forever.
 *
 * rotate() first: applies the EXIF orientation flag BEFORE metadata is
 * dropped, so portrait shots don't end up sideways.
 *
 * Drawings get a larger long edge (fine pencil lines and handwriting
 * need it) and often arrive as transparent PNGs — JPEG has no alpha, so
 * they are flattened onto paper white instead of turning black.
 * A small thumbnail serves popups and archive cards, so a list of 24
 * drawings does not pull 24 full-size images over 4G.
 */

const sharp = require('sharp');

const PHOTO_EDGE = 1600;
const DRAWING_EDGE = 2400;
const THUMB_EDGE = 720;
const JPEG_QUALITY = 78;

async function processImage(inputBuffer, { maxEdge = PHOTO_EDGE } = {}) {
  // limitInputPixels: refuse decompression bombs (a 268 MP PNG would
  // balloon to ~1 GB of pixels on a 512 MB instance). 50 MP is far above
  // any real phone photo or scan; over-limit images throw and are skipped.
  const { data, info } = await sharp(inputBuffer, { failOn: 'error', limitInputPixels: 50_000_000 })
    .rotate()
    .flatten({ background: '#ffffff' })
    .resize(maxEdge, maxEdge, { fit: 'inside', withoutEnlargement: true })
    .jpeg({ quality: JPEG_QUALITY, mozjpeg: true })
    .toBuffer({ resolveWithObject: true });
  const thumb = await sharp(data)
    .resize(THUMB_EDGE, THUMB_EDGE, { fit: 'inside', withoutEnlargement: true })
    .jpeg({ quality: 72, mozjpeg: true })
    .toBuffer();
  return { buffer: data, thumb, width: info.width, height: info.height, ext: 'jpg', mime: 'image/jpeg' };
}

module.exports = { processImage, PHOTO_EDGE, DRAWING_EDGE, THUMB_EDGE };
