/* Saigon_Miền Tây Sound Map — Phòng nghe / listening room engine (v2)
   A generative Web Audio conductor over the live archive.
   - corpus = /api/recordings (live, grows with every upload) + listen-features.json (offline analysis)
   - aesthetic tuned in the studio 2026-08-25: field recordings foreground ("the city speaks"),
     dub delay + long reverb dream layer, and a day-evolution: ambient documentary by day,
     rolling swung ro-minimal groove after dark. Synths stay minimal.                      */
'use strict';

const BPM = 122, BEAT = 60 / BPM;
const swingT = () => state.swing * 0.12 * BEAT;
const HCMC = { latMin: 10.62, latMax: 10.98, lngMin: 106.52, lngMax: 106.98 };
const IS_DEV = location.port === '8342';

const state = {
  ctx: null, started: false,
  recs: [], buffers: new Map(),
  listener: { lat: 10.79, lng: 106.70 },
  hour: null,                 // null = live Saigon time
  dream: 0.35, percDensity: null, bright: 0.8,   // percDensity null = follow the day
  echo: 0.35, space: 0.4, reso: 0.12, sub: 0.5, swing: 0.5, pitch: 0,
  synthOn: true, synthLevel: 0.5, synthWave: 0.35, synthTone: 0.45, synthShape: 0.3, synthDetune: 0.25,
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
  let feats = [];
  try { feats = await (await fetch('listen-features.json')).json(); } catch (e) {}
  const fmap = new Map(feats.map(f => [f.id, f]));
  let api = [];
  try {
    const r = await (await fetch('/api/recordings')).json();
    api = r.recordings || r;
  } catch (e) { /* dev without API: fall back to features only */ }
  const source = api.length ? api : feats;
  state.recs = source.map(r => {
    const id8 = (r.id || '').slice(0, 8);
    const f = fmap.get(id8) || {};
    const created = r.createdAt || '';
    return {
      id: id8, title: r.title || f.title, category: r.category || f.category || 'Background',
      lat: r.latitude ?? f.lat, lng: r.longitude ?? f.lng,
      duration: r.duration || f.duration || 30,
      hour: f.hour ?? (created ? (parseInt(created.slice(11, 13), 10) + 7) % 24 : null),
      lufs: f.lufs ?? -32,
      role: f.role || ROLE_BY_CAT[r.category || f.category] || ((r.duration || 30) >= 40 ? 'bed' : 'texture'),
      onset: f.onset ?? 1,
      audioUrl: r.audioUrl || null, file: f.file || null,
    };
  }).filter(r => r.lat && r.lng);
}
function audioSrc(rec) {
  if (IS_DEV && rec.file) return 'audio/' + encodeURIComponent(rec.file);
  return rec.audioUrl || ('audio/' + encodeURIComponent(rec.file || ''));
}

/* ---------- utils ---------- */
const saigonHour = () => (new Date().getUTCHours() + 7) % 24 + new Date().getUTCMinutes() / 60;
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
const gainForLufs = (lufs, t = -28) => Math.min(8, Math.max(0.05, Math.pow(10, (t - lufs) / 20)));

/* ---------- graph ---------- */
let master, lowpass, comp, delaySend, delayNode, delayFb, delayFilter, reverbSend, convolver, synthBus, percBus, fieldBus;
function buildGraph() {
  const c = state.ctx;
  master = c.createGain(); master.gain.value = 0.9;
  lowpass = c.createBiquadFilter(); lowpass.type = 'lowpass'; lowpass.frequency.value = 16000; lowpass.Q.value = 0.4;
  comp = c.createDynamicsCompressor();
  comp.threshold.value = -18; comp.ratio.value = 3; comp.attack.value = 0.02; comp.release.value = 0.3;
  master.connect(lowpass); lowpass.connect(comp); comp.connect(c.destination);
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
  if (state.buffers.has(rec.id)) return state.buffers.get(rec.id);
  const res = await fetch(audioSrc(rec));
  const buf = await state.ctx.decodeAudioData(await res.arrayBuffer());
  if (state.buffers.size > 24) state.buffers.delete(state.buffers.keys().next().value);
  state.buffers.set(rec.id, buf);
  return buf;
}
function weight(rec) {
  const inCity = rec.lat >= HCMC.latMin && rec.lat <= HCMC.latMax && rec.lng >= HCMC.lngMin && rec.lng <= HCMC.lngMax;
  const wDist = inCity ? Math.exp(-kmDist(state.listener, rec) / 2.5) : 0.05;
  let wHour = 1;
  if (rec.hour !== null && rec.hour !== undefined) {
    const dh = Math.min(Math.abs(rec.hour - curHour()), 24 - Math.abs(rec.hour - curHour()));
    wHour = 0.35 + 0.65 * Math.exp(-dh / 4);
  }
  return wDist * wHour * (0.3 + Math.random());
}
function pickRec(roles, excludeIds) {
  const pool = state.recs.filter(r => roles.includes(r.role) && !excludeIds.has(r.id));
  if (!pool.length) return null;
  return pool.map(r => [weight(r), r]).sort((a, b) => b[0] - a[0])[0][1];
}
async function startLayer(slotName, rec, { fadeIn = 6, level = 1 } = {}) {
  const c = state.ctx;
  let buf;
  try { buf = await getBuffer(rec); } catch (e) { console.warn('audio fail', rec.title, e); return; }
  const src = c.createBufferSource(); src.buffer = buf; src.loop = buf.duration > 8;
  src.playbackRate.value = Math.pow(2, state.pitch / 12);
  const g = c.createGain(); g.gain.value = 0;
  const dSend = c.createGain(), rSend = c.createGain();
  src.connect(g); g.connect(fieldBus); g.connect(dSend); g.connect(rSend);
  dSend.connect(delaySend); rSend.connect(reverbSend);
  g.gain.linearRampToValueAtTime(gainForLufs(rec.lufs) * level, c.currentTime + fadeIn);
  updateSendMix(dSend, rSend, rec.role);
  src.start(0, Math.random() * Math.max(0, buf.duration - 20));
  const old = state.slots[slotName];
  if (old) stopLayer(old, 8);
  state.slots[slotName] = { src, g, dSend, rSend, rec };
  state.nowPlaying.set(slotName, rec);
  state.onNowPlaying();
}
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
let percBufA = null, percBufB = null, percOffA = [], percOffB = [];
function stableOffsets(buf, n) {
  // fixed hit-points per buffer: the SAME transients return every bar — that
  // repetition is what makes found sound read as groove instead of collage
  const out = [];
  for (let i = 0; i < n; i++) out.push(rnd(0, Math.max(0.1, buf.duration - 0.4)));
  return out;
}
async function loadPerc() {
  const cand = state.recs.filter(r => r.role === 'rhythm' && r.onset > 2.5);
  const pool = cand.length >= 2 ? cand : state.recs.filter(r => r.role === 'rhythm');
  if (!pool.length) return;
  const a = choice(pool), b = choice(pool.filter(r => r.id !== a.id)) || a;
  percBufA = await getBuffer(a).catch(() => null);
  percBufB = await getBuffer(b).catch(() => null);
  if (percBufA) percOffA = stableOffsets(percBufA, 4);
  if (percBufB) percOffB = stableOffsets(percBufB, 3);
}
function grain(buf, t, vel, rate, offset, dur) {
  const c = state.ctx, src = c.createBufferSource();
  src.buffer = buf; src.playbackRate.value = rate;
  const g = c.createGain();
  g.gain.setValueAtTime(0, t); g.gain.linearRampToValueAtTime(vel, t + 0.004);
  g.gain.exponentialRampToValueAtTime(0.001, t + dur);
  src.connect(g); g.connect(percBus); g.connect(delaySend);
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
      grain(percBufA, ts, 0.45 + g * 0.4, 1, percOffA[0], 0.12);           // the anchor hit
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
  // sub: silent by day, sparse deep syncopation as the groove emerges
  if (g > 0.35) {
    if (inBar === 2) subNote(ts, 28, 0.35, 0.4 + g * 0.25);
    else if (inBar === 11 && bar % 2 === 0) subNote(ts, 28, 0.25, 0.3 + g * 0.2);
    else if (inBar === 13 && bar % 4 === 3) subNote(ts, 31, 0.3, 0.28);
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
  o.connect(g); g.connect(synthBus); o.start(t); o.stop(t + dur + 0.6);
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
  for (const det of [-cents, cents]) {
    const o = c.createOscillator();
    o.type = synthWaveType(); o.frequency.value = midiHz(midi); o.detune.value = det;
    o.connect(f); o.start(t); o.stop(t + attack + hold + relTau * 5);
  }
  f.connect(g); g.connect(synthBus); g.connect(delaySend); g.connect(reverbSend);
}
let previewAt = 0;
function previewStab() {
  // instant audition when a synth knob moves
  if (!state.started || !state.synthOn) return;
  const now = state.ctx.currentTime;
  if (now - previewAt < 0.25) return;
  previewAt = now;
  const sec = section(curHour());
  for (const m of CHORD_SETS[sec].slice(0, 3)) chordNote(now + 0.01, m, 0.3, 0.14);
}

/* ---------- scheduler & rotation ---------- */
let nextStepTime = 0, stepCount = 0;
function tick() {
  const c = state.ctx;
  while (nextStepTime < c.currentTime + 0.15) {
    schedulePercStep(nextStepTime, stepCount);
    scheduleSynthStep(nextStepTime, stepCount);
    nextStepTime += BEAT / 4; stepCount++;
  }
}
async function rotate(force) {
  const active = new Set(Object.values(state.slots).filter(Boolean).map(l => l.rec.id));
  const plan = [
    ['bedA', ['bed'], 1.0], ['bedB', ['bed', 'texture'], 0.8],
    ['texA', ['texture'], 0.85], ['texB', ['texture', 'rhythm'], 0.7],
    ['voice', ['voice'], 0.8],
  ];
  const empty = plan.filter(([s]) => !state.slots[s]);
  const target = empty.length ? empty : (force ? [choice(plan)] : []);
  for (const [slot, roles, level] of target.slice(0, empty.length ? 2 : 1)) {
    const rec = pickRec(roles, active);
    if (rec) { await startLayer(slot, rec, { level, fadeIn: empty.length ? 4 : 8 }); active.add(rec.id); }
  }
}

/* ---------- public API ---------- */
window.Room = {
  state, grooveAmt, section: () => section(curHour()),
  async start() {
    if (state.started) return;
    state.ctx = new (window.AudioContext || window.webkitAudioContext)();
    buildGraph();
    await loadCorpus();
    state.started = true;
    nextStepTime = state.ctx.currentTime + 0.2;
    setInterval(tick, 60);
    await rotate(true);
    loadPerc();
    setInterval(() => rotate(false), 9000);
    setInterval(() => rotate(true), 50000);
    setInterval(loadCorpus, 5 * 60 * 1000);          // the archive keeps growing under the piece
    setInterval(() => {
      if (Date.now() - state.lastTouch > 90000) {
        state.listener.lat += rnd(-0.004, 0.004);
        state.listener.lng += rnd(-0.004, 0.004);
        window.dispatchEvent(new Event('room-drift'));
      }
    }, 15000);
    this.applyControls();
  },
  applyControls() {
    if (!state.started) return;
    lowpass.frequency.setTargetAtTime(800 * Math.pow(22.5, state.bright), state.ctx.currentTime, 0.3);
    lowpass.Q.setTargetAtTime(0.3 + state.reso * 9, state.ctx.currentTime, 0.3);
    const synthG = state.synthOn ? (0.06 + state.synthLevel * 0.65) * (0.55 + state.dream * 0.7) : 0;
    synthBus.gain.setTargetAtTime(synthG, state.ctx.currentTime, 0.4);
    delaySend.gain.setTargetAtTime(0.05 + state.echo * 0.55, state.ctx.currentTime, 0.3);
    delayFb.gain.value = Math.min(0.85, 0.25 + state.echo * 0.5 + state.dream * 0.1);
    reverbSend.gain.setTargetAtTime(0.05 + state.space * 0.65, state.ctx.currentTime, 0.3);
    const rate = Math.pow(2, state.pitch / 12);
    for (const l of Object.values(state.slots)) if (l) { try { l.src.playbackRate.setTargetAtTime(rate, state.ctx.currentTime, 0.5); } catch (e) {} }
    for (const l of Object.values(state.slots)) if (l) updateSendMix(l.dSend, l.rSend, l.rec.role);
    percBus.gain.setTargetAtTime(0.25 + grooveAmt() * 0.75, state.ctx.currentTime, 0.3);
  },
  moveListener(lat, lng) { state.listener = { lat, lng }; state.lastTouch = Date.now(); rotate(true); },
  setHour(h) { state.hour = h; state.lastTouch = Date.now(); this.applyControls(); rotate(true); },
  setLive() { state.hour = null; state.lastTouch = Date.now(); this.applyControls(); },
  set(key, v) {
    state[key] = v; state.lastTouch = Date.now(); this.applyControls();
    if (key.startsWith('synth') && key !== 'synthOn') previewStab();
  },
  reroll() { state.lastTouch = Date.now(); rotate(true); loadPerc(); },
};
