/* Saigon_Miền Tây Sound Map — Phòng nghe / listening room engine (v9)
   A generative Web Audio conductor over the live archive.
   - corpus = /api/recordings (live, grows with every upload) + listen-features.json (offline analysis)
   - aesthetic tuned in the studio 2026-08-25: field recordings foreground ("the city speaks"),
     dub delay + long reverb dream layer, and a day-evolution: ambient documentary by day,
     rolling swung ro-minimal groove after dark. Synths stay minimal.                      */
'use strict';

const BPM = 122, BEAT = 60 / BPM;
const swingT = () => state.swing * 0.12 * BEAT;
const HCMC = { latMin: 10.62, latMax: 10.98, lngMin: 106.52, lngMax: 106.98 };
const IS_DEV = ['localhost', '127.0.0.1', '[::1]'].includes(location.hostname) && location.port === '8342';
const DEFAULTS = Object.freeze({
  dream: .35, percDensity: null, bright: .8, echo: .35, space: .4, reso: .12,
  sub: .5, swing: .5, pitch: 0, synthOn: false, synthLevel: .5,
  synthWave: .35, synthTone: .45, synthShape: .3, synthDetune: .25,
});
const pendingBuffers = new Map(), failedBuffers = new Map();
const timers = [];
let startPromise = null, rotation = null, percussionLoad = null;
const CACHE_BYTES = 64 * 1024 * 1024;
function announce(message) {
  state.message = message;
  window.dispatchEvent(new Event('room-status'));
}
async function request(url, json = true) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 20000);
  try {
    const response = await fetch(url, { signal: controller.signal });
    if (!response.ok) throw new Error('Request failed (' + response.status + ')');
    return await (json ? response.json() : response.arrayBuffer());
  } finally { clearTimeout(timeout); }
}

const state = {
  ctx: null, started: false, paused: false, volume: .7, muted: false, touring: true, message: '', bufferBytes: 0,
  recs: [], buffers: new Map(),
  listener: { lat: 10.79, lng: 106.70 },
  hour: null,                 // null = live Saigon time
  dream: 0.35, percDensity: null, bright: 0.8,   // percDensity null = follow the day
  echo: 0.35, space: 0.4, reso: 0.12, sub: 0.5, swing: 0.5, pitch: 0,
  synthOn: false, synthLevel: 0.5, synthWave: 0.35, synthTone: 0.45, synthShape: 0.3, synthDetune: 0.25,
  lastTouch: 0,
  slots: {}, nowPlaying: new Map(), onNowPlaying: () => {},
};

/* ---------- corpus ---------- */
const ROLE_BY_CAT = {
  'Conversations': 'voice', 'Nature': 'bed', 'Background': 'bed', 'Waterways': 'bed',
  'Vehicles': 'texture', 'Music': 'tonal', 'Ritual & Ceremony': 'voice',
  'Announcements & Signals': 'voice', 'Street Vendors': 'voice',
};
async function loadCorpus() {
  const [featuresResult, apiResult] = await Promise.allSettled([
    request('listen-features.json'), IS_DEV ? Promise.resolve(null) : request('/api/recordings'),
  ]);
  const feats = featuresResult.status === 'fulfilled' && Array.isArray(featuresResult.value) ? featuresResult.value : [];
  const fmap = new Map(feats.map(f => [f.id, f]));
  const data = apiResult.status === 'fulfilled' ? apiResult.value : null;
  const source = IS_DEV ? feats : (Array.isArray(data) ? data : data?.recordings);
  if (!Array.isArray(source)) throw new Error('The archive is unavailable. Please try again.');
  const recs = source.map(r => {
    const shortId = (r.id || '').slice(0, 8);
    const f = IS_DEV ? r : (fmap.get(shortId) || {});
    return {
      id: r.id, title: r.title || f.title || 'Untitled recording', category: r.category || f.category || 'Background',
      lat: Number(r.latitude ?? f.lat), lng: Number(r.longitude ?? f.lng),
      duration: r.duration || f.duration || 30,
      // Upload time is not evidence of the recording's time of day.
      hour: Number.isFinite(f.hour) ? f.hour : null,
      createdDay: r.createdAt ? r.createdAt.slice(0, 10) : null,
      lufs: Number.isFinite(f.lufs) ? f.lufs : -32,
      role: f.role || ROLE_BY_CAT[r.category || f.category] || ((r.duration || 30) >= 40 ? 'bed' : 'texture'),
      onset: f.onset ?? 1,
      audioUrl: r.audioUrl || null, file: f.file || null,
    };
  }).filter(r => r.id && Number.isFinite(r.lat) && Number.isFinite(r.lng) && (IS_DEV ? r.file : r.audioUrl));
  const previous = state.recs, previousFilter = state.roomFilter;
  state.recs = recs;
  applyRoomFilter();
  if (!state.recs.length) {
    state.recs = previous; state.roomFilter = previousFilter;
    throw new Error('No recordings match this room. Try the whole-city room.');
  }
  window.dispatchEvent(new Event('room-corpus'));
}

/* ---------- site-specific rooms (?near= ?walk= ?date= ?cat= ?hours=) ---------- */
const WALKS = {
  'thanh-da': { near: [10.837, 106.727, 2500], label: 'Thanh Đa' },
};
function applyRoomFilter() {
  const q = new URLSearchParams(location.search);
  let pool = state.recs, labels = [];
  const walk = q.get('walk') && WALKS[q.get('walk').toLowerCase()];
  const near = walk ? walk.near.join(',') : q.get('near');
  if (near) {
    const [la, ln, m] = near.split(',').map(Number);
    if (isFinite(la) && isFinite(ln)) {
      const km = (isFinite(m) ? m : 1000) / 1000;
      pool = pool.filter(r => kmDist({ lat: la, lng: ln }, r) <= km);
      labels.push(walk ? walk.label : 'within ' + Math.round(km * 1000) + ' m');
    }
  }
  if (q.get('date')) {
    const days = q.get('date').split(',');
    pool = pool.filter(r => r.createdDay && days.includes(r.createdDay));
    labels.push(q.get('date'));
  }
  if (q.get('cat')) {
    const cats = q.get('cat').toLowerCase().split(',');
    pool = pool.filter(r => cats.includes((r.category || '').toLowerCase()));
    labels.push(q.get('cat'));
  }
  if (q.get('hours')) {
    const [h0, h1] = q.get('hours').split('-').map(Number);
    if (isFinite(h0) && isFinite(h1)) {
      pool = pool.filter(r => r.hour !== null && (h0 <= h1 ? (r.hour >= h0 && r.hour <= h1) : (r.hour >= h0 || r.hour <= h1)));
      labels.push(q.get('hours') + 'h');
    }
  }
  if (!labels.length) { state.roomFilter = null; return; }
  if (!pool.length) { state.recs = []; return; }
  state.recs = pool;
  const lats = pool.map(r => r.lat), lngs = pool.map(r => r.lng);
  const padLat = Math.max(0.004, (Math.max(...lats) - Math.min(...lats)) * 0.2);
  const padLng = Math.max(0.004, (Math.max(...lngs) - Math.min(...lngs)) * 0.2);
  const bounds = { latMin: Math.min(...lats) - padLat, latMax: Math.max(...lats) + padLat,
                   lngMin: Math.min(...lngs) - padLng, lngMax: Math.max(...lngs) + padLng };
  state.roomFilter = { label: labels.join(' · '), count: pool.length, bounds };
  if (!state._filterInit) {   // center the listener once; re-polls must not move them
    state._filterInit = true;
    state.listener = { lat: (bounds.latMin + bounds.latMax) / 2, lng: (bounds.lngMin + bounds.lngMax) / 2 };
  }
}
function audioSrc(rec) {
  if (IS_DEV && rec.file) return 'audio/' + encodeURIComponent(rec.file);
  return rec.audioUrl || ('audio/' + encodeURIComponent(rec.file || ''));
}

/* ---------- utils ---------- */
const saigonHour = () => { const now = new Date(); return (now.getUTCHours() + 7) % 24 + now.getUTCMinutes() / 60; };
const curHour = () => state.hour === null ? saigonHour() : state.hour;
function section(h) {
  if (h >= 5 && h < 11) return 'morning';
  if (h >= 11 && h < 17) return 'midday';
  if (h >= 17 && h < 22) return 'evening';
  return 'night';
}
// how much the dance groove has emerged (the evolution macro)
function grooveAmt() {
  // untouched: follow the day; touched: the visitor's slider is the truth
  if (state.percDensity !== null) return state.percDensity;
  return { morning: 0.12, midday: 0.28, evening: 0.55, night: 0.85 }[section(curHour())];
}
const rnd = (a, b) => a + Math.random() * (b - a);
const choice = arr => arr[Math.floor(Math.random() * arr.length)];
const kmDist = (a, b) => Math.hypot((a.lng - b.lng) * 102, (a.lat - b.lat) * 111);
const gainForLufs = (lufs, t = -28) => Math.min(3.2, Math.max(0.05, Math.pow(10, (t - lufs) / 20)));

/* ---------- graph ---------- */
let output, master, lowpass, comp, delaySend, delayNode, delayFb, delayFilter, reverbSend, convolver, synthBus, percBus, fieldBus;
function buildGraph() {
  const c = state.ctx;
  master = c.createGain(); master.gain.value = 0.9;
  lowpass = c.createBiquadFilter(); lowpass.type = 'lowpass'; lowpass.frequency.value = 16000; lowpass.Q.value = 0.4;
  comp = c.createDynamicsCompressor();
  comp.threshold.value = -18; comp.ratio.value = 3; comp.attack.value = 0.02; comp.release.value = 0.3;
  master.connect(lowpass); lowpass.connect(comp); output = c.createGain(); output.gain.value = 0; comp.connect(output); output.connect(c.destination);
  fieldBus = c.createGain(); fieldBus.connect(master);
  synthBus = c.createGain(); synthBus.gain.value = 0.25; synthBus.connect(master);
  percBus = c.createGain(); percBus.gain.value = 0.5; percBus.connect(master);
  delaySend = c.createGain(); delaySend.gain.value = 0.25;
  delayNode = c.createDelay(2); delayNode.delayTime.value = BEAT * 0.75;
  delayFb = c.createGain(); delayFb.gain.value = 0.45;
  delayFilter = c.createBiquadFilter(); delayFilter.type = 'bandpass'; delayFilter.frequency.value = 1400; delayFilter.Q.value = 0.5;
  delaySend.connect(delayNode); delayNode.connect(delayFilter); delayFilter.connect(delayFb);
  delayFb.connect(delayNode); delayFilter.connect(master);
  reverbSend = c.createGain(); reverbSend.gain.value = 0.3;
  convolver = c.createConvolver(); convolver.buffer = makeImpulse(5.0, 2.4);
  reverbSend.connect(convolver); convolver.connect(master);
}
function makeImpulse(seconds, decay) {
  const c = state.ctx, len = c.sampleRate * seconds, buf = c.createBuffer(2, len, c.sampleRate);
  for (let ch = 0; ch < 2; ch++) {
    const d = buf.getChannelData(ch);
    for (let i = 0; i < len; i++) d[i] = (Math.random() * 2 - 1) * Math.pow(1 - i / len, decay);
  }
  return buf;
}

/* ---------- field-recording layers ---------- */
async function getBuffer(rec) {
  if (state.buffers.has(rec.id)) {
    const buffer = state.buffers.get(rec.id);
    state.buffers.delete(rec.id); state.buffers.set(rec.id, buffer);
    return buffer;
  }
  if (pendingBuffers.has(rec.id)) return pendingBuffers.get(rec.id);
  if ((failedBuffers.get(rec.id) || 0) > Date.now()) throw new Error('Recording temporarily unavailable');
  const context = state.ctx;
  const job = (async () => {
    try {
      const bytes = await request(audioSrc(rec), false);
      const buf = await context.decodeAudioData(bytes);
      const size = buf.length * buf.numberOfChannels * 4;
      while (state.buffers.size && (state.bufferBytes + size > CACHE_BYTES || state.buffers.size >= 16)) {
        const oldest = state.buffers.keys().next().value, old = state.buffers.get(oldest);
        state.bufferBytes -= old.length * old.numberOfChannels * 4;
        state.buffers.delete(oldest);
      }
      if (size <= CACHE_BYTES) { state.buffers.set(rec.id, buf); state.bufferBytes += size; }
      failedBuffers.delete(rec.id);
      return buf;
    } catch (error) {
      failedBuffers.set(rec.id, Date.now() + 30000);
      announce('A recording could not load. Trying another sound…');
      throw error;
    } finally { pendingBuffers.delete(rec.id); }
  })();
  pendingBuffers.set(rec.id, job);
  return job;
}

function weight(rec) {
  const inCity = state.roomFilter ? true :
    (rec.lat >= HCMC.latMin && rec.lat <= HCMC.latMax && rec.lng >= HCMC.lngMin && rec.lng <= HCMC.lngMax);
  const wDist = inCity ? Math.exp(-kmDist(state.listener, rec) / (state.roomFilter ? 0.6 : 2.5)) : 0.05;
  let wHour = 1;
  if (rec.hour !== null && rec.hour !== undefined) {
    const dh = Math.min(Math.abs(rec.hour - curHour()), 24 - Math.abs(rec.hour - curHour()));
    wHour = 0.35 + 0.65 * Math.exp(-dh / 4);
  }
  return wDist * wHour * (0.3 + Math.random());
}
function pickRec(roles, excludeIds) {
  const pool = state.recs.filter(r => roles.includes(r.role) && !excludeIds.has(r.id) && r.lufs > -55 && (failedBuffers.get(r.id) || 0) <= Date.now());
  if (!pool.length) return null;
  return pool.map(r => [weight(r), r]).sort((a, b) => b[0] - a[0])[0][1];
}
async function startLayer(slotName, rec, { fadeIn = 6, level = 1 } = {}) {
  const c = state.ctx;
  let buf;
  try { buf = await getBuffer(rec); } catch (e) { return false; }
  if (!state.started || c !== state.ctx) return false;
  const src = c.createBufferSource(); src.buffer = buf;
  src.loop = buf.duration > 8 && rec.role !== 'voice';   // announcements & voices never loop
  src.playbackRate.value = Math.pow(2, state.pitch / 12);
  const g = c.createGain(); g.gain.value = 0;
  const dSend = c.createGain(), rSend = c.createGain();
  const spatial = c.createGain();
  const panner = c.createStereoPanner ? c.createStereoPanner() : null;
  src.connect(g); g.connect(spatial);
  const positioned = panner || spatial;
  if (panner) spatial.connect(panner);
  positioned.connect(fieldBus); positioned.connect(dSend); positioned.connect(rSend);
  dSend.connect(delaySend); rSend.connect(reverbSend);
  g.gain.linearRampToValueAtTime(gainForLufs(rec.lufs) * level, c.currentTime + fadeIn);
  updateSendMix(dSend, rSend, rec.role);
  src.start(0, Math.random() * Math.max(0, buf.duration - 20));
  const old = state.slots[slotName];
  if (old) stopLayer(old, 8);
  const layer = { src, g, spatial, panner, dSend, rSend, rec };
  updatePosition(layer);
  state.slots[slotName] = layer;
  state.nowPlaying.set(slotName, rec);
  state.onNowPlaying();
  // Audio-clock lifetimes freeze with pause; short voices clean up on their natural end.
  layer.endsAt = c.currentTime + (rec.role === 'voice' ? Math.min(buf.duration + fadeIn, 70) : rnd(50, 90));
  src.onended = () => {
    src.disconnect(); g.disconnect(); spatial.disconnect(); panner?.disconnect(); dSend.disconnect(); rSend.disconnect();
    if (state.slots[slotName] === layer) {
      delete state.slots[slotName]; state.nowPlaying.delete(slotName); state.onNowPlaying();
    }
  };
  announce('');
  return true;
}
function updatePosition(layer) {
  const distance = kmDist(state.listener, layer.rec), c = state.ctx;
  // Separate spatial gain from the envelope so movement cannot cancel crossfades.
  layer.spatial.gain.setTargetAtTime(.3 + .7 * Math.exp(-distance / (state.roomFilter ? .6 : 2.5)), c.currentTime, .5);
  if (layer.panner) {
    const spread = state.roomFilter ? .008 : .035;
    layer.panner.pan.setTargetAtTime(Math.max(-.85, Math.min(.85, (layer.rec.lng - state.listener.lng) / spread)), c.currentTime, .5);
  }
}
function updatePositions() { for (const layer of Object.values(state.slots)) updatePosition(layer); }
function stopLayer(layer, fade = 8) {
  const c = state.ctx;
  try {
    layer.g.gain.cancelScheduledValues(c.currentTime);
    layer.g.gain.setValueAtTime(layer.g.gain.value, c.currentTime);
    layer.g.gain.linearRampToValueAtTime(0, c.currentTime + fade);
    layer.src.stop(c.currentTime + fade + 0.1);
  } catch (e) {}
}
function updateSendMix(dSend, rSend, role) {
  const wet = 0.06 + state.dream * 0.55;
  dSend.gain.value = role === 'bed' ? wet * 0.3 : wet * 0.8;
  rSend.gain.value = role === 'bed' ? wet : wet * 0.6;
}

/* ---------- found percussion: two voices, swung ---------- */
let percGainA = 1, percGainB = 1;
let percBufA = null, percBufB = null, percOffA = [], percOffB = [];
function stableOffsets(buf, n) {
  // fixed hit-points per buffer: the SAME transients return every bar — that
  // repetition is what makes found sound read as groove instead of collage
  const out = [];
  for (let i = 0; i < n; i++) out.push(rnd(0, Math.max(0, buf.duration - 0.4)));
  return out;
}
async function loadPerc() {
  if (percussionLoad) return percussionLoad;
  percussionLoad = loadPercBuffers();
  try { await percussionLoad; } finally { percussionLoad = null; }
}
async function loadPercBuffers() {
  const cand = state.recs.filter(r => r.role === 'rhythm' && r.onset > 2.5 && r.lufs > -55);
  const pool = cand.length >= 2 ? cand : state.recs.filter(r => r.role === 'rhythm' && r.lufs > -55);
  if (!pool.length) return;
  const a = choice(pool), b = choice(pool.filter(r => r.id !== a.id)) || a;
  // Publish buffers and their offsets together: a slow load must not pair a new
  // short recording with the previous recording's out-of-range hit positions.
  const [bufferA, bufferB] = await Promise.all([getBuffer(a).catch(() => null), getBuffer(b).catch(() => null)]);
  percBufA = bufferA; percBufB = bufferB;
  percOffA = bufferA ? stableOffsets(bufferA, 4) : [];
  percOffB = bufferB ? stableOffsets(bufferB, 3) : [];
  percGainA = gainForLufs(a.lufs, -28); percGainB = gainForLufs(b.lufs, -28);
}
function grain(buf, t, vel, rate, offset, dur) {
  vel *= buf === percBufA ? percGainA : percGainB;
  const c = state.ctx, src = c.createBufferSource();
  src.buffer = buf; src.playbackRate.value = rate;
  const g = c.createGain();
  g.gain.setValueAtTime(0, t); g.gain.linearRampToValueAtTime(vel, t + 0.004);
  g.gain.exponentialRampToValueAtTime(0.001, t + dur);
  src.connect(g); g.connect(percBus); g.connect(delaySend);
  src.onended = () => { src.disconnect(); g.disconnect(); };
  src.start(t, offset, dur + 0.05);
}
function schedulePercStep(t, step) {
  const g = grooveAmt();
  if (g < 0.05) return;
  const inBar = step % 16, isDown = inBar % 4 === 0, isAnd = inBar % 4 === 2;
  const ts = t + (inBar % 2 === 1 ? swingT() : 0);   // swing the off-16ths
  // voice A: rolling body — solid repeating downbeat, ghosted 16ths between
  if (percBufA && percOffA.length) {
    if (isDown) {
      if (g > 0.45) grain(percBufA, ts, 0.45 + g * 0.4, 1, percOffA[0], 0.12);   // anchor only after dark
      else if (Math.random() < g * 0.3) grain(percBufA, ts, rnd(0.08, 0.18), 0.75, percOffA[0], 0.1);
    } else {
      const p = (inBar % 2 === 0 ? 0.55 : 0.32) * g;
      if (Math.random() < p)
        grain(percBufA, ts, rnd(0.12, 0.3) * (0.5 + g), choice([0.75, 1, 1]),
              percOffA[1 + (inBar % (percOffA.length - 1))], rnd(0.07, 0.12));
    }
  }
  // voice B: offbeat ticks (the ro-minimal "ands")
  if (percBufB && percOffB.length && isAnd && Math.random() < 0.25 + 0.65 * g) {
    grain(percBufB, ts, rnd(0.12, 0.28) * (0.5 + g), choice([1, 1.5]),
          percOffB[(step >> 2) % percOffB.length], 0.08);
  }
}

/* ---------- minimal synth layer (recedes behind the city) ---------- */
const CHORD_SETS = {
  morning: [64, 68, 71, 75], midday: [64, 69, 71, 76],
  evening: [55, 59, 64], night: [52, 59, 62],
};
const midiHz = m => 440 * Math.pow(2, (m - 69) / 12);
function scheduleSynthStep(t, step) {
  if (!state.synthOn) return;
  const g = grooveAmt(), sec = section(curHour());
  const bar = Math.floor(step / 16), inBar = step % 16;
  const ts = t + (inBar % 2 === 1 ? swingT() : 0);
  // sub: silent by day; when the groove emerges it breathes — rests whole
  // bars, skips hits, varies weight — never a metronome
  if (g > 0.35) {
    if (inBar === 0) state._subRest = Math.random() < 0.3;   // ~1 bar in 3 is silent
    if (!state._subRest) {
      if (inBar === 2 && Math.random() < 0.75) subNote(ts, 28, rnd(0.25, 0.45), (0.35 + g * 0.25) * rnd(0.75, 1.1));
      else if (inBar === 11 && bar % 2 === 0 && Math.random() < 0.55) subNote(ts, 28, rnd(0.2, 0.3), (0.28 + g * 0.18) * rnd(0.7, 1));
      else if (inBar === 13 && bar % 4 === 3 && Math.random() < 0.5) subNote(ts, choice([31, 26]), 0.3, 0.25 * rnd(0.7, 1));
    }
  }
  // chords: soft dub stabs; the Level knob also makes them come more often
  const every = Math.max(2, Math.round((g > 0.5 ? 12 : 6) * (1.2 - state.synthLevel * 0.9)));
  if (inBar === 6 && bar % every === every - 2) {
    for (const m of CHORD_SETS[sec]) chordNote(ts + rnd(0, 0.02), m, 0.4, 0.1);
  }
}
function subNote(t, midi, dur, vel0) {
  const vel = vel0 * (0.25 + state.sub * 1.5);
  const c = state.ctx, o = c.createOscillator(), g = c.createGain();
  o.type = 'sine'; o.frequency.value = midiHz(midi);
  g.gain.setValueAtTime(0, t); g.gain.linearRampToValueAtTime(vel * 0.5, t + 0.02);
  g.gain.setTargetAtTime(0, t + dur, 0.08);
  o.connect(g); g.connect(synthBus); o.onended = () => { o.disconnect(); g.disconnect(); }; o.start(t); o.stop(t + dur + 0.6);
}
function synthWaveType() {
  return state.synthWave < 0.33 ? 'triangle' : state.synthWave < 0.66 ? 'sawtooth' : 'square';
}
function chordNote(t, midi, dur, vel) {
  const c = state.ctx, f = c.createBiquadFilter(), g = c.createGain();
  const attack = 0.01 + state.synthShape * 1.0;          // stab ... pad
  const relTau = 0.12 + state.synthShape * 1.2;
  const hold = dur + state.synthShape * 2.0;
  f.type = 'lowpass'; f.frequency.value = 400 * Math.pow(2, state.synthTone * 3.9); f.Q.value = 1 + state.reso * 6;
  g.gain.setValueAtTime(0, t); g.gain.linearRampToValueAtTime(vel, t + attack);
  g.gain.setTargetAtTime(0, t + attack + hold, relTau);
  const cents = state.synthDetune * 25;
  let voices = 2;
  for (const det of [-cents, cents]) {
    const o = c.createOscillator();
    o.type = synthWaveType(); o.frequency.value = midiHz(midi); o.detune.value = det;
    o.connect(f); o.onended = () => { o.disconnect(); if (--voices === 0) { f.disconnect(); g.disconnect(); } }; o.start(t); o.stop(t + attack + hold + relTau * 5);
  }
  f.connect(g); g.connect(synthBus); g.connect(delaySend); g.connect(reverbSend);
}
let previewAt = 0;
function previewStab() {
  // instant audition when a synth knob moves
  if (!state.started || state.ctx.state !== 'running' || !state.synthOn) return;
  const now = state.ctx.currentTime;
  if (now - previewAt < 0.25) return;
  previewAt = now;
  const sec = section(curHour());
  for (const m of CHORD_SETS[sec].slice(0, 3)) chordNote(now + 0.01, m, 0.3, 0.14);
}

/* ---------- the journey: a slow continuous ride between the city's places ---------- */
let journey = null;
function pickDestination() {
  const b = state.roomFilter ? state.roomFilter.bounds : HCMC;
  const pool = state.recs.filter(r =>
    r.lat >= b.latMin && r.lat <= b.latMax && r.lng >= b.lngMin && r.lng <= b.lngMax &&
    kmDist(r, state.listener) > (state.roomFilter ? 0.25 : 1.2));
  const dest = pool.length ? choice(pool) : { lat: (b.latMin + b.latMax) / 2, lng: (b.lngMin + b.lngMax) / 2 };
  const km = kmDist(dest, state.listener);
  journey = {
    from: { ...state.listener }, to: { lat: dest.lat, lng: dest.lng },
    t0: Date.now(), dur: Math.max(45, Math.min(150, km * 35)) * 1000,   // slow ride, ~2 km/min
  };
}
function journeyTick() {
  if (!state.started || !state.touring || state.ctx.state !== 'running') return;
  if (Date.now() - state.lastTouch < 30000) { journey = null; return; }  // hands on = you drive
  if (!journey) pickDestination();
  const p = Math.min(1, (Date.now() - journey.t0) / journey.dur);
  const e = p < 0.5 ? 2 * p * p : 1 - Math.pow(-2 * p + 2, 2) / 2;       // ease in-out
  state.listener = {
    lat: journey.from.lat + (journey.to.lat - journey.from.lat) * e,
    lng: journey.from.lng + (journey.to.lng - journey.from.lng) * e,
  };
  updatePositions();
  window.dispatchEvent(new Event('room-drift'));
  if (p >= 1) journey = null;                                            // arrive, then wander on
}

/* ---------- scheduler & rotation ---------- */
let nextStepTime = 0, stepCount = 0;
function tick() {
  const c = state.ctx;
  if (!state.started || c.state !== 'running') return;
  for (const [slot, layer] of Object.entries(state.slots)) {
    if (c.currentTime >= layer.endsAt) {
      stopLayer(layer, 6); delete state.slots[slot]; state.nowPlaying.delete(slot); state.onNowPlaying();
    }
  }
  // A throttled background tab must not replay a backlog of missed beats.
  if (nextStepTime < c.currentTime - .2) nextStepTime = c.currentTime + .02;
  while (nextStepTime < c.currentTime + 0.15) {
    schedulePercStep(nextStepTime, stepCount);
    scheduleSynthStep(nextStepTime, stepCount);
    nextStepTime += BEAT / 4; stepCount++;
  }
}
async function rotate(force) {
  if (!state.started || state.ctx.state !== 'running') return;
  if (rotation) return rotation;
  rotation = rotateLayers(force);
  try { await rotation; } finally { rotation = null; }
}
async function rotateLayers(force) {
  const active = new Set(Object.values(state.slots).filter(Boolean).map(l => l.rec.id));
  const plan = [
    ['bedA', ['bed', 'tonal'], 1.0], ['bedB', ['bed', 'texture'], 0.8],
    ['texA', ['texture'], 0.85], ['texB', ['texture', 'rhythm'], 0.7],
    ['voice', ['voice'], 0.8],
  ];
  const empty = plan.filter(([s]) => !state.slots[s]);
  const target = empty.length ? empty : (force ? [choice(plan)] : []);
  for (const [slot, roles, level] of target.slice(0, empty.length ? 2 : 1)) {
    if (state.ctx.state !== 'running') break;
    const rec = pickRec(roles, active) || (active.size === 0 ? pickRec(['bed', 'texture', 'rhythm', 'voice', 'tonal'], active) : null);
    if (rec && await startLayer(slot, rec, { level, fadeIn: empty.length ? 4 : 8 })) active.add(rec.id);
  }
}

/* ---------- public API ---------- */
window.Room = {
  state, defaults: DEFAULTS, grooveAmt, curHour, section: () => section(curHour()),
  async start() {
    if (state.started) return this.resume();
    if (startPromise) return startPromise;
    startPromise = (async () => {
      try {
        state.ctx = new (window.AudioContext || window.webkitAudioContext)();
        // Resume inside the user's click, before any network work.
        await state.ctx.resume();
        buildGraph();
        announce('Opening the archive…');
        await loadCorpus();
        state.started = true; state.paused = false;
        this.applyControls();
        nextStepTime = state.ctx.currentTime + .2;
        state.ctx.onstatechange = () => {
          state.paused = state.ctx.state !== 'running';
          if (state.paused) journey = null;
          window.dispatchEvent(new Event('room-status'));
        };
        timers.push(setInterval(tick, 60));
        announce('Loading the first recordings…');
        await rotate(true);
        if (!state.nowPlaying.size) announce('No audio loaded yet. Choose New sounds to retry.');
        void loadPerc();
        timers.push(setInterval(() => { void rotate(false); this.applyControls(); }, 9000));
        timers.push(setInterval(() => { void rotate(true); }, 35000));
        timers.push(setInterval(() => {
          loadCorpus().catch(() => announce('Archive refresh unavailable. Your current room is still playing.'));
        }, 5 * 60 * 1000));
        timers.push(setInterval(journeyTick, 100));
      } catch (error) {
        timers.splice(0).forEach(clearInterval);
        state.started = false;
        if (state.ctx && state.ctx.state !== 'closed') await state.ctx.close();
        announce(error.message || 'Could not open the room. Please try again.');
        throw error;
      }
    })();
    try { await startPromise; } finally { startPromise = null; }
  },
  async pause() {
    if (!state.started) return;
    await state.ctx.suspend(); state.paused = true; journey = null;
    window.dispatchEvent(new Event('room-status'));
  },
  async resume() {
    if (!state.started) return;
    await state.ctx.resume(); state.paused = false; journey = null;
    window.dispatchEvent(new Event('room-status'));
  },
  setVolume(value) {
    state.volume = Math.max(0, Math.min(1, Number(value) || 0));
    this.applyControls(); window.dispatchEvent(new Event('room-status'));
  },
  toggleMute() { state.muted = !state.muted; this.applyControls(); window.dispatchEvent(new Event('room-status')); },
  setTour(on) { state.touring = !!on; state.lastTouch = 0; journey = null; window.dispatchEvent(new Event('room-status')); },
  reset() { Object.assign(state, DEFAULTS); state.hour = null; this.applyControls(); window.dispatchEvent(new Event('room-status')); },
  applyControls() {
    if (!state.started) return;
    output.gain.setTargetAtTime(state.muted ? 0 : state.volume, state.ctx.currentTime, .025);
    lowpass.frequency.setTargetAtTime(800 * Math.pow(22.5, state.bright), state.ctx.currentTime, 0.3);
    lowpass.Q.setTargetAtTime(0.3 + state.reso * 9, state.ctx.currentTime, 0.3);
    const synthG = state.synthOn ? (0.06 + state.synthLevel * 0.65) * (0.55 + state.dream * 0.7) : 0;
    synthBus.gain.setTargetAtTime(synthG, state.ctx.currentTime, 0.4);
    delaySend.gain.setTargetAtTime(0.05 + state.echo * 0.55, state.ctx.currentTime, 0.3);
    delayFb.gain.setTargetAtTime(Math.min(0.85, 0.25 + state.echo * 0.5 + state.dream * 0.1), state.ctx.currentTime, .3);
    reverbSend.gain.setTargetAtTime(0.05 + state.space * 0.65, state.ctx.currentTime, 0.3);
    const rate = Math.pow(2, state.pitch / 12);
    for (const l of Object.values(state.slots)) if (l) { try { l.src.playbackRate.setTargetAtTime(rate, state.ctx.currentTime, 0.5); } catch (e) {} }
    for (const l of Object.values(state.slots)) if (l) updateSendMix(l.dSend, l.rSend, l.rec.role);
    percBus.gain.setTargetAtTime(0.25 + grooveAmt() * 0.75, state.ctx.currentTime, 0.3);
  },
  moveListener(lat, lng) {
    const bounds = state.roomFilter?.bounds || HCMC;
    state.listener = { lat: Math.max(bounds.latMin, Math.min(bounds.latMax, lat)), lng: Math.max(bounds.lngMin, Math.min(bounds.lngMax, lng)) };
    state.lastTouch = Date.now(); journey = null; updatePositions();
    window.dispatchEvent(new Event('room-status'));
  },
  refresh() { return rotate(true); },
  setHour(h) { state.hour = ((Number(h) || 0) % 24 + 24) % 24; this.applyControls(); rotate(true); },
  setLive() { state.hour = null; this.applyControls(); },
  set(key, v) {
    if (!Object.prototype.hasOwnProperty.call(DEFAULTS, key)) return;
    state[key] = v; this.applyControls();
    if (key.startsWith('synth') && key !== 'synthOn') previewStab();
  },
  async reroll() { announce('Finding new sounds…'); await Promise.all([rotate(true), loadPerc()]); if (state.nowPlaying.size) announce(''); },
};

window.addEventListener('pagehide', () => { if (state.started) void Room.pause(); });
