const { createHash } = require('node:crypto');
// The private per-draft key plus payload identity survives response loss and restarts.
// `images` is one buffer or a list, hashed one at a time: the same digest
// as hashing them joined, without a second in-memory copy of every image.
function uploadId(key, audio, metadata, images) {
  if (!key) return undefined;
  const hash = createHash('sha256').update(key).update(audio).update(JSON.stringify(metadata));
  for (const b of Array.isArray(images) ? images : images ? [images] : []) hash.update(b);
  const hex = hash.digest('hex');
  return `${hex.slice(0,8)}-${hex.slice(8,12)}-4${hex.slice(13,16)}-a${hex.slice(17,20)}-${hex.slice(20,32)}`;
}
module.exports = { uploadId };
