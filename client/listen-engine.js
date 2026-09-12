/* Saigon_Miền Tây Sound Map — Phòng nghe / listening room engine (v10)
   A generative Web Audio conductor over the live archive.
   - corpus = /api/recordings (live, grows with every upload) + listen-features.json (offline analysis)
   - aesthetic tuned in the studio 2026-08-25: field recordings foreground ("the city speaks"),
     dub delay + long reverb dream layer, and a day-evolution: ambient documentary by day,
     rolling swung ro-minimal groove after dark. Synths stay minimal.                      */
'use strict';

const BPM = 72, BEAT = 60 / BPM;
const swingT = () => state.swing * 0.12 * BEAT;
const HCMC = { latMin: 10.62, latMax: 10.98, lngMin: 106.52, lngMax: 106.98 };
const IS_DEV = ['localhost', '127.0.0.1', '[::1]'].includes(location.hostname) && location.port === '8342';
const DEFAULTS = Object.freeze({
  dream: .28, percDensity: .28, bright: .8, echo: .12, space: .18, reso: .08, effectsOn: false, bassLevel: .18, ambienceLevel: 0, voiceSolo: false,
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
  dream: .28, percDensity: .28, bright: .8,   // percDensity null = follow the day
  echo: .12, space: .18, reso: .08, effectsOn: false, bassLevel: .18, ambienceLevel: 0, voiceSolo: false, sub: 0.5, swing: 0.5, pitch: 0,
  synthOn: false, synthLevel: 0.5, synthWave: 0.35, synthTone: 0.45, synthShape: 0.3, synthDetune: 0.25,
  lastTouch: 0,
  anchorHeld: false, anchorChangedAt: 0, anchorPeriod: 260,
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
let output, master, lowpass, comp, delaySend, delayNode, delayFb, delayFilter, reverbSend, reverbTone, convolver, synthBus, synthWet, percBus, bassBus, fieldBus, voiceBus, cityDelayGate, cityReverbGate;
function buildGraph() {
  const c = state.ctx;
  master = c.createGain(); master.gain.value = 0.9;
  lowpass = c.createBiquadFilter(); lowpass.type = 'lowpass'; lowpass.frequency.value = 16000; lowpass.Q.value = 0.4;
  comp = c.createDynamicsCompressor();
  comp.threshold.value = -18; comp.ratio.value = 3; comp.attack.value = 0.02; comp.release.value = 0.3;
  master.connect(lowpass); lowpass.connect(comp); output = c.createGain(); output.gain.value = 0; comp.connect(output); output.connect(c.destination);
  fieldBus = c.createGain(); fieldBus.gain.value = 0; fieldBus.connect(master);
  voiceBus = c.createGain(); voiceBus.connect(master);
  synthBus = c.createGain(); synthBus.gain.value = 0; synthBus.connect(master);
  synthWet = c.createGain(); synthWet.gain.value = 0;
  percBus = c.createGain(); percBus.gain.value = 0; percBus.connect(master);
  bassBus = c.createGain(); bassBus.gain.value = 0; bassBus.connect(master);
  delaySend = c.createGain(); delaySend.gain.value = 0.25;
  delayNode = c.createDelay(2); delayNode.delayTime.value = BEAT * 0.75;
  delayFb = c.createGain(); delayFb.gain.value = 0.45;
  delayFilter = c.createBiquadFilter(); delayFilter.type = 'bandpass'; delayFilter.frequency.value = 1400; delayFilter.Q.value = 0.5;
  delaySend.connect(delayNode); delayNode.connect(delayFilter); delayFilter.connect(delayFb);
  delayFb.connect(delayNode); delayFilter.connect(master);
  reverbSend = c.createGain(); reverbSend.gain.value = 0.3;
  convolver = c.createConvolver(); convolver.buffer = makeImpulse(2.8, 3.2);
  reverbTone=c.createBiquadFilter(); reverbTone.type='lowpass'; reverbTone.frequency.value=2100; reverbTone.Q.value=.4;
  reverbSend.connect(convolver); convolver.connect(reverbTone); reverbTone.connect(master);
  synthWet.connect(delaySend); synthWet.connect(reverbSend);
  cityDelayGate=c.createGain(); cityDelayGate.gain.value=0; cityDelayGate.connect(delaySend);
  cityReverbGate=c.createGain(); cityReverbGate.gain.value=0; cityReverbGate.connect(reverbSend);
  state.meters = {};
  for (const [name,bus] of Object.entries({voice:voiceBus,city:fieldBus,percussion:percBus,bass:bassBus})) {
    const analyser=c.createAnalyser();analyser.fftSize=1024;analyser.smoothingTimeConstant=.8;
    bus.connect(analyser);state.meters[name]=analyser;
  }
}
function makeImpulse(seconds, decay) {
  const c = state.ctx, len = c.sampleRate * seconds, buf = c.createBuffer(2, len, c.sampleRate);
  for (let ch = 0; ch < 2; ch++) {
    const d = buf.getChannelData(ch);
    for (let i = 0; i < len; i++) d[i] = (Math.random() * 2 - 1) * Math.pow(1 - i / len, decay);
  }
  return buf;
}

/* ---------- a voice, returning: audio-clock loops and very slow modulation ---------- */
function loopBuffer(buffer, seconds = 14, seam = 1.8, offset = 3) {
  const rate = buffer.sampleRate || state.ctx.sampleRate;
  const channels = [];
  for (let ch=0; ch<buffer.numberOfChannels; ch++) channels.push(VoiceLoop.prepare(buffer.getChannelData(ch), rate, seconds, seam, Math.min(offset,buffer.duration/5)));
  const out = state.ctx.createBuffer(buffer.numberOfChannels, channels[0].length, rate);
  channels.forEach((data,ch) => out.getChannelData(ch).set(data));
  return out;
}
function measuredGain(buffer, target = .045) {
  let sum=0, peak=0, count=0;
  for (let ch=0;ch<buffer.numberOfChannels;ch++) {
    const data=buffer.getChannelData(ch);
    for (let i=0;i<data.length;i++) { sum+=data[i]*data[i]; peak=Math.max(peak,Math.abs(data[i])); count++; }
  }
  return Math.min(3.2, target/Math.max(.001,Math.sqrt(sum/Math.max(1,count))), .38/Math.max(.001,peak));
}
let anchorLoad = null;
async function renewAnchor(force = false) {
  if (anchorLoad) return anchorLoad;
  const old=state.slots.anchor;
  if (!force && old && (state.anchorHeld || state.ctx.currentTime < old.endsAt)) return;
  anchorLoad = (async () => {
    const active = new Set(Object.values(state.slots).map(l=>l.rec.id));
    // Stay inside the selected room. A room without a voice gets an explicitly labelled texture.
    const voicePool = state.recs.filter(r=>r.role==='voice' && r.duration>=8 && !active.has(r.id) && (failedBuffers.get(r.id)||0)<=Date.now());
    const pool = voicePool.length ? voicePool : state.recs.filter(r=>['bed','texture','tonal'].includes(r.role) && r.duration>=8 && !active.has(r.id) && (failedBuffers.get(r.id)||0)<=Date.now());
    if (!pool.length) { if(old) old.endsAt=state.ctx.currentTime+60; return; }
    const preferred = !old && new URLSearchParams(location.search).get('voice');
    const rec=pool.find(r=>r.id===preferred) || pool.map(r=>[weight(r),r]).sort((a,b)=>b[0]-a[0])[0][1];
    let full;
    try { full=await getBuffer(rec); } catch { return; }
    if (!state.started) return;
    const c=state.ctx, src=c.createBufferSource(), g=c.createGain(), warmth=c.createBiquadFilter(), breath=c.createGain();
    src.buffer=loopBuffer(full); src.loop=true;
    warmth.type='lowpass'; warmth.frequency.value=2600; warmth.Q.value=.45;
    breath.gain.value=.94; g.gain.value=0;
    const panner=c.createStereoPanner ? c.createStereoPanner() : c.createGain();
    const dSend=c.createGain(), rSend=c.createGain(); dSend.gain.value=.10; rSend.gain.value=.45;
    src.connect(warmth); warmth.connect(breath); breath.connect(g); g.connect(panner);
    panner.connect(voiceBus); panner.connect(dSend); panner.connect(rSend); dSend.connect(delaySend); rSend.connect(reverbSend);
    const modulators=[];
    function sway(param, period, amount) {
      if (!param) return;
      const osc=c.createOscillator(), depth=c.createGain(); osc.type='sine'; osc.frequency.value=1/period; depth.gain.value=amount;
      osc.connect(depth); depth.connect(param); osc.start(); modulators.push([osc,depth]);
    }
    sway(breath.gain,43,.04); sway(warmth.frequency,67,220); sway(panner.pan,79,.12);
    const fade=old ? 24 : 8;
    g.gain.linearRampToValueAtTime(measuredGain(src.buffer),c.currentTime+fade); src.start();
    if(old) stopLayer(old,fade);
    const layer={src,g,rec,anchor:true,endsAt:c.currentTime+state.anchorPeriod,loopSeconds:src.buffer.duration};
    state.slots.anchor=layer; state.anchorChangedAt=c.currentTime;
    state.nowPlaying.set('anchor',rec); state.onNowPlaying();
    src.onended=()=>{
      [src,g,warmth,breath,panner,dSend,rSend].forEach(n=>n.disconnect());
      modulators.forEach(([osc,depth])=>{osc.stop();osc.disconnect();depth.disconnect();});
      if(state.slots.anchor===layer) { delete state.slots.anchor;state.nowPlaying.delete('anchor');state.onNowPlaying(); }
    };
    announce(''); window.dispatchEvent(new Event('room-status'));
  })();
  try { await anchorLoad; } finally { anchorLoad=null; }
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
async function startLayer(slotName, rec, { fadeIn = 22, level = 1 } = {}) {
  const c = state.ctx;
  let buf;
  try { buf = await getBuffer(rec); } catch (e) { return false; }
  if (!state.started || c !== state.ctx) return false;
  const src = c.createBufferSource();
  // Long ambience also needs a soft seam; only the dedicated foundation repeats speech.
  const looping = buf.duration > 8 && rec.role !== 'voice';
  src.buffer = looping ? loopBuffer(buf, Math.min(65, buf.duration), 2, 0) : buf;
  src.loop = looping;
  src.playbackRate.value = Math.pow(2, state.pitch / 12);
  const g = c.createGain(); g.gain.value = 0;
  const dSend = c.createGain(), rSend = c.createGain();
  const spatial = c.createGain();
  const panner = c.createStereoPanner ? c.createStereoPanner() : null;
  src.connect(g); g.connect(spatial);
  const positioned = panner || spatial;
  if (panner) spatial.connect(panner);
  positioned.connect(fieldBus); positioned.connect(dSend); positioned.connect(rSend);
  dSend.connect(cityDelayGate); rSend.connect(cityReverbGate);
  g.gain.linearRampToValueAtTime(measuredGain(src.buffer, .055) * level, c.currentTime + fadeIn);
  updateSendMix(dSend, rSend, rec.role);
  src.start(0, 0);
  const old = state.slots[slotName];
  if (old) stopLayer(old, fadeIn);
  const layer = { src, g, spatial, panner, dSend, rSend, rec };
  updatePosition(layer);
  state.slots[slotName] = layer;
  state.nowPlaying.set(slotName, rec);
  state.onNowPlaying();
  // Audio-clock lifetimes freeze with pause; short voices clean up on their natural end.
  layer.endsAt = c.currentTime + (rec.role === 'voice' ? Math.min(buf.duration + fadeIn, 70) : rnd(110, 170));
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
  if (layer.anchor) return;
  const distance = kmDist(state.listener, layer.rec), c = state.ctx;
  // Separate spatial gain from the envelope so movement cannot cancel crossfades.
  layer.spatial.gain.setTargetAtTime(.3 + .7 * Math.exp(-distance / (state.roomFilter ? .6 : 2.5)), c.currentTime, .5);
  if (layer.panner) {
    const spread = state.roomFilter ? .008 : .035;
    layer.panner.pan.setTargetAtTime(Math.max(-.85, Math.min(.85, (layer.rec.lng - state.listener.lng) / spread)), c.currentTime, .5);
  }
}
function updatePositions() { for (const layer of Object.values(state.slots)) updatePosition(layer); }
function stopLayer(layer, fade = 22) {
  const c = state.ctx;
  try {
    if (layer.g.gain.cancelAndHoldAtTime) layer.g.gain.cancelAndHoldAtTime(c.currentTime);
    else { const value = layer.g.gain.value; layer.g.gain.cancelScheduledValues(c.currentTime); layer.g.gain.setValueAtTime(value, c.currentTime); }
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
  if (state.started) startPercussionLoop();
}
/* Looped accompaniment runs on the audio clock, including when JS timers sleep. */
let rhythmEpoch = 0, percussionLoop = null, bassLoop = null;
function loopSource(buffer, bus, previous) {
  const c=state.ctx, src=c.createBufferSource(), envelope=c.createGain();
  src.buffer=buffer;src.loop=true;envelope.gain.value=0;src.connect(envelope);envelope.connect(bus);
  const now=c.currentTime+.04;
  src.start(now,Math.max(0,(now-rhythmEpoch)%buffer.duration));
  envelope.gain.linearRampToValueAtTime(1,now+8);
  if(previous) {
    const param=previous.envelope.gain;
    if(param.cancelAndHoldAtTime)param.cancelAndHoldAtTime(now);
    else {const value=param.value;param.cancelScheduledValues(now);param.setValueAtTime(value,now);}
    param.linearRampToValueAtTime(0,now+8);previous.src.stop(now+8.1);
  }
  src.onended=()=>{src.disconnect();envelope.disconnect();};
  return {src,envelope};
}
function startBassLoop() {
  if(bassLoop)return;
  const c=state.ctx, sr=c.sampleRate, length=Math.round(sr*BEAT*32), buffer=c.createBuffer(1,length,sr), out=buffer.getChannelData(0);
  // A soft two-bar figure; the last note changes slightly only at the end of eight bars.
  for(let bar=0;bar<8;bar++) {
    const notes=bar%2===0 ? [[0,41.203,1.5,.15],[2.75,41.203,.55,.065]] : [[1,41.203,1.2,.105]];
    if(bar===7)notes.push([3,61.735,.65,.045]);
    for(const [beat,hz,seconds,velocity] of notes) {
      const start=Math.round((bar*4+beat)*BEAT*sr), count=Math.min(Math.round(seconds*sr),length-start);
      for(let i=0;i<count;i++) {
        const t=i/sr, attack=Math.min(1,t/.09), release=Math.min(1,(count-i)/(sr*.28));
        const tone=Math.sin(2*Math.PI*hz*t)+.10*Math.sin(4*Math.PI*hz*t);
        out[start+i]+=tone*velocity*attack*release*Math.exp(-t*1.4);
      }
    }
  }
  rhythmEpoch=c.currentTime; bassLoop=loopSource(buffer,bassBus,null);
}
function startPercussionLoop() {
  if(!percBufA && !percBufB)return;
  const c=state.ctx,sr=c.sampleRate,length=Math.round(sr*BEAT*32),buffer=c.createBuffer(1,length,sr),out=buffer.getChannelData(0);
  function hit(source,beat,seconds,level,point) {
    if(!source)return;
    const input=source.getChannelData(0),rate=source.sampleRate||sr;
    const start=Math.round(beat*BEAT*sr), count=Math.min(Math.round(seconds*sr),length-start);
    const offset=Math.min(Math.max(0,point),Math.max(0,source.duration-seconds));
    let peak=0;
    for(let i=0;i<count;i++){const j=Math.floor((offset+i/sr)*rate);peak=Math.max(peak,Math.abs(input[j]||0));}
    const scale=level/Math.max(.02,peak);
    for(let i=0;i<count;i++) {
      const j=Math.floor((offset+i/sr)*rate),envelope=Math.min(1,i/(sr*.008))*Math.pow(1-i/count,2);
      out[start+i]+=(input[j]||0)*scale*envelope;
    }
  }
  for(let phrase=0;phrase<4;phrase++) {
    const base=phrase*8;
    for(const beat of [0,3,4,7])hit(percBufA,base+beat,.16,beat%4===0?.18:.10,percOffA[0]||0);
    for(const beat of [1.5,5.5])hit(percBufB,base+beat,.10,.08,percOffB[0]||0);
    if(phrase===3)hit(percBufB,base+6.75,.08,.035,percOffB[1]||0);
  }
  percussionLoop=loopSource(buffer,percBus,percussionLoop);
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
  const density=grooveAmt(); if(density<.15) return;
  const position=step%32, cycle=Math.floor(step/32);
  const ts=t+(position%2?swingT():0);
  // The same two-bar pattern returns. Only one quiet accent changes every eight cycles.
  if(percBufA && percOffA.length && [0,12,16,28].includes(position))
    grain(percBufA,ts,.09*density,1,percOffA[position===0?0:1],.18);
  if(percBufB && percOffB.length && (position===6 || (position===23 && Math.floor(cycle/8)%2)))
    grain(percBufB,ts,.045*density,1,percOffB[0],.12);
}

/* ---------- minimal synth layer (recedes behind the city) ---------- */
const CHORD_SETS = {
  morning: [64, 68, 71, 75], midday: [64, 69, 71, 76],
  evening: [55, 59, 64], night: [52, 59, 62],
};
const midiHz = m => 440 * Math.pow(2, (m - 69) / 12);
function scheduleSynthStep(t, step) {
  if(!state.synthOn) return;
  const position=step%64, phrase=Math.floor(step/64);
  if(position===0) subNote(t,40,2.5,.08);
  if(position===8 || position===40) {
    const notes=position===8 ? [52,59] : [52,Math.floor(phrase/8)%2 ? 62 : 64];
    notes.forEach(m=>chordNote(t,m,2.5,.045));
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
  const attack = .6 + state.synthShape * 1.5;          // stab ... pad
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
  f.connect(g); g.connect(synthBus); g.connect(synthWet);
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
  // A throttled background tab must not replay a backlog of missed beats.
  if (nextStepTime < c.currentTime - .2) nextStepTime = c.currentTime + .02;
  while (nextStepTime < c.currentTime + 0.15) {
    // Found percussion repeats in an audio-clock buffer; no JS timing gaps.
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
  const plan = [['bedA', ['bed','texture'], .7], ['texA', ['texture','tonal','rhythm'], .45]];
  const empty=plan.filter(([slot])=>!state.slots[slot]);
  const expired=plan.filter(([slot])=>state.slots[slot] && state.ctx.currentTime>=state.slots[slot].endsAt);
  const target=empty.length ? empty.slice(0,1) : expired.length ? expired.slice(0,1) : force ? [choice(plan)] : [];
  for(const [slot,roles,level] of target) {
    if(state.ctx.state!=='running') break;
    const rec=pickRec(roles,active);
    if(rec && await startLayer(slot,rec,{level,fadeIn:22})) active.add(rec.id);
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
        await renewAnchor();
        startBassLoop();
        await rotate(true);
        if (!state.nowPlaying.size) announce('No audio loaded yet. Choose New sounds to retry.');
        void loadPerc();
        timers.push(setInterval(() => { void renewAnchor(); void rotate(false); this.applyControls(); }, 9000));
        timers.push(setInterval(() => { void rotate(true); }, 140000));
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
    const accompaniment = state.voiceSolo ? 0 : 1;
    const ambience = accompaniment * state.ambienceLevel;
    for (const bus of [fieldBus, cityDelayGate, cityReverbGate]) bus.gain.setTargetAtTime(ambience,state.ctx.currentTime,.08);
    const synthG = state.synthOn && !state.voiceSolo ? (0.06 + state.synthLevel * 0.65) * (0.55 + state.dream * 0.7) : 0;
    synthBus.gain.setTargetAtTime(synthG, state.ctx.currentTime, 0.4);
    synthWet.gain.setTargetAtTime(synthG * .35, state.ctx.currentTime, .4);
    delaySend.gain.setTargetAtTime(state.effectsOn && !state.voiceSolo ? state.echo * .35 : 0, state.ctx.currentTime, 0.3);
    delayFb.gain.setTargetAtTime(state.effectsOn && !state.voiceSolo && state.echo > 0 ? Math.min(.55, state.echo * .5 + state.dream * .08) : 0, state.ctx.currentTime, .3);
    reverbSend.gain.setTargetAtTime(state.effectsOn && !state.voiceSolo ? state.space * .3 : 0, state.ctx.currentTime, 0.3);
    const rate = Math.pow(2, state.pitch / 12);
    for (const l of Object.values(state.slots)) if (l) { try { l.src.playbackRate.setTargetAtTime(rate, state.ctx.currentTime, 0.5); } catch (e) {} }
    for (const l of Object.values(state.slots)) if (l && !l.anchor) updateSendMix(l.dSend, l.rSend, l.rec.role);
    percBus.gain.setTargetAtTime(accompaniment * grooveAmt() * .55, state.ctx.currentTime, .8);
    bassBus.gain.setTargetAtTime(accompaniment * state.bassLevel * .65, state.ctx.currentTime, .8);
  },
  moveListener(lat, lng) {
    const bounds = state.roomFilter?.bounds || HCMC;
    state.listener = { lat: Math.max(bounds.latMin, Math.min(bounds.latMax, lat)), lng: Math.max(bounds.lngMin, Math.min(bounds.lngMax, lng)) };
    state.lastTouch = Date.now(); journey = null; updatePositions();
    window.dispatchEvent(new Event('room-status'));
  },
  refresh() { return rotate(true); },
  toggleEffects() { state.effectsOn=!state.effectsOn; this.applyControls(); window.dispatchEvent(new Event('room-status')); },
  holdAnchor(on) { state.anchorHeld=!!on; window.dispatchEvent(new Event('room-status')); },
  nextAnchor() { return renewAnchor(true); },
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
