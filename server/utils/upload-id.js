const { createHash } = require('node:crypto');
// The private per-draft key plus payload identity survives response loss and restarts.
function uploadId(key, audio, metadata, image) {
  if (!key) return undefined;
  const hash = createHash('sha256').update(key).update(audio)
    .update(JSON.stringify(metadata)).update(image || Buffer.alloc(0)).digest('hex');
  return `${hash.slice(0,8)}-${hash.slice(8,12)}-4${hash.slice(13,16)}-a${hash.slice(17,20)}-${hash.slice(20,32)}`;
}
module.exports = { uploadId };
