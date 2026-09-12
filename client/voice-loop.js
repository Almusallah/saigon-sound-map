/* PCM loop preparation shared by the room and the listening experiments. */
(function (root) {
  function prepare(samples, sampleRate, seconds = 14, crossfade = 1.8, offset = 0) {
    const start = Math.max(0, Math.min(samples.length - 2, Math.floor(offset * sampleRate)));
    const length = Math.min(samples.length - start, Math.floor(seconds * sampleRate));
    if (length < 2) return new Float32Array(samples);
    const fade = Math.max(1, Math.min(Math.floor(crossfade * sampleRate), Math.floor(length / 3)));
    const out = samples.slice(start, start + length - fade);
    for (let i = 0; i < fade; i++) {
      const mix = .5 - .5 * Math.cos(Math.PI * i / fade);
      out[i] = samples[start + length - fade + i] * (1 - mix) + samples[start + i] * mix;
    }
    return out;
  }
  function rms(samples) { let sum = 0; for (const v of samples) sum += v*v; return Math.sqrt(sum / Math.max(1,samples.length)); }
  const api = { prepare, rms };
  if (typeof module !== 'undefined') module.exports = api;
  else root.VoiceLoop = api;
})(typeof window === 'undefined' ? {} : window);
