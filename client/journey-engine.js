/* Phòng nghe — journey engine.
   The machine composes a journey from the archive, then performs it in a
   room you can walk through.

   compose(corpus, opts) -> score   pure and seeded: same corpus + seed, same piece
   createPerformer(ctx, score, io)  performs a score on any (Offline)AudioContext

   The arc is a day condensed: dawn, gathering, pulse, drift, night,
   dissolve. Recordings are chosen by the hour they were made, so the piece
   moves through the archive's own day. Each recording sounds from its place
   in the room (its map position), heard binaurally from where the listener
   stands. Synth, bass and percussion stay light and come and go; nothing is
   a continuous drone. */
(function (root) {
  'use strict';

  /* ---------- small utilities ---------- */
  function rng(seed) { // mulberry32
    let a = seed >>> 0;
    return () => {
      a = (a + 0x6D2B79F5) >>> 0;
      let t = a;
      t = Math.imul(t ^ (t >>> 15), t | 1);
      t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
      return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    };
  }
  const clamp = (v, a, b) => Math.max(a, Math.min(b, v));
  const db = d => Math.pow(10, d / 20);
  const mtof = m => 440 * Math.pow(2, (m - 69) / 12);
  function hash(str) { let h = 2166136261; for (const c of str) h = Math.imul(h ^ c.charCodeAt(0), 16777619); return h >>> 0; }
  const pick = (r, list) => list[Math.floor(r() * list.length)];
  const between = (r, a, b) => a + r() * (b - a);

  /* ---------- the arc: a day condensed ---------- */
  const ARC = [
    { key: 'dawn',      label: 'DAWN',      from: 0,    to: .14, hours: [4, 9] },
    { key: 'gathering', label: 'GATHERING', from: .12,  to: .40, hours: [7, 12] },
    { key: 'pulse',     label: 'PULSE',     from: .38,  to: .62, hours: [10, 16] },
    { key: 'drift',     label: 'DRIFT',     from: .60,  to: .78, hours: [15, 20] },
    { key: 'night',     label: 'NIGHT',     from: .76,  to: .93, hours: [19, 27] },
    { key: 'dissolve',  label: 'DISSOLVE',  from: .92,  to: 1,   hours: [0, 6] },
  ];

  // Hour fit: 0 inside the window, growing with circular distance outside it.
  function hourMiss(h, [a, b]) {
    if (!Number.isFinite(h)) return 6;
    for (const x of [h, h + 24]) if (x >= a && x <= b) return 0;
    const d = Math.min(...[a, b].flatMap(e => [Math.abs(h - e), Math.abs(h + 24 - e), Math.abs(h - 24 - e)]));
    return d;
  }

  // What each part of the day leans toward. Hour of recording decides first;
  // this weighs about as much as one hour of difference.
  const PREFER = {
    dawn: ['Nature', 'Waterways', 'Ritual & Ceremony', 'Home', 'Background'],
    gathering: ['Street Vendors', 'Markets', 'Sidewalks', 'Food & Drink', 'Home', 'Work'],
    pulse: ['Vehicles', 'Markets', 'Sidewalks', 'Work', 'Infrastructure & Utilities', 'Play & Leisure', 'Street Vendors'],
    drift: ['Waterways', 'Conversations', 'Food & Drink', 'Shops', 'Play & Leisure', 'Background'],
    night: ['Nature', 'Ritual & Ceremony', 'Waterways', 'Background', 'Home', 'Conversations'],
    dissolve: ['Nature', 'Waterways', 'Background'],
  };

  /* ---------- the room: the map becomes the floor ---------- */
  const BOUNDS = 40;
  const SAIGON = { lat: 10.80, lng: 106.72, spanLat: .19, spanLng: .25 };
  // Saigon fills the room; everything farther away sits near the walls in its
  // true direction, so Miền Tây is southwest and Venice far to the northwest.
  function place(rec) {
    const dx = (rec.lng - SAIGON.lng) / SAIGON.spanLng, dz = -(rec.lat - SAIGON.lat) / SAIGON.spanLat;
    const r = Math.hypot(dx, dz);
    const j = hash(rec.id), jx = ((j & 255) / 255 - .5) * 6, jz = (((j >> 8) & 255) / 255 - .5) * 6;
    let x, z;
    if (r <= 1) { x = dx * BOUNDS * .82; z = dz * BOUNDS * .82; }
    else { const k = (.86 + .08 * Math.min(1, Math.log10(r) / 3)) * BOUNDS / r; x = dx * k; z = dz * k; }
    return { x: clamp(x + jx, -BOUNDS + 2, BOUNDS - 2), z: clamp(z + jz, -BOUNDS + 2, BOUNDS - 2) };
  }

  /* ---------- harmony ---------- */
  const MODES = { aeolian: [0, 2, 3, 5, 7, 8, 10], dorian: [0, 2, 3, 5, 7, 9, 10] };
  const PROGRESSIONS = [[0, 5, 2, 6], [0, 3, 5, 4], [0, 6, 5, 6], [5, 3, 0, 4], [0, 2, 3, 5]];
  function chord(root, mode, degree, size = 4) {
    const s = MODES[mode], notes = [];
    for (let i = 0; i < size; i++) {
      const d = degree + i * 2, oct = Math.floor(d / 7);
      notes.push(root + s[d % 7] + 12 * oct);
    }
    return notes;
  }

  /* ---------- corpus ---------- */
  // Music-category recordings are left out: they often carry someone else's
  // copyrighted music, which a composition should not resample.
  function usable(r) {
    return r && r.id && Number.isFinite(r.lat) && Number.isFinite(r.lng) && r.duration >= 2.5 &&
      (r.lufs === undefined || r.lufs > -52) && r.category !== 'Music';
  }
  function poolOf(corpus, test) { return corpus.filter(r => usable(r) && test(r)); }
  const isBed = r => (r.role === 'bed' || r.role === 'texture') && (r.onset ?? 1) < 3 && r.duration >= 20;
  const isDetail = r => ['voice', 'tonal', 'rhythm'].includes(r.role) || (r.role === 'texture' && (r.onset ?? 0) > 2);
  const isVoice = r => r.role === 'voice' && r.duration >= 5;
  const isKit = r => r.role === 'rhythm' && (r.onset ?? 0) > 1.8 && r.duration >= 4;
  const isTonal = r => (r.harm ?? 0) > .55 && r.duration >= 8;

  // Choose a recording for a section: prefer the section's hours, avoid reuse.
  function chooser(r, used) {
    return (pool, hours, opts = {}) => {
      const cands = pool.filter(x => !used.has(x.id) && (!opts.minDur || x.duration >= opts.minDur));
      const list = cands.length ? cands : pool;
      if (!list.length) return null;
      // The archive's own day is lopsided (most recordings come from walks at
      // 11h and 16h), so pick among the few best hour fits, not a broad top
      // slice: dawn then really uses the rare early-morning recordings.
      const scored = list.map(x => ({ x, s: hourMiss(x.hour, hours) + r() * .8 - (opts.prefer && opts.prefer.includes(x.category) ? 1.2 : 0) }));
      scored.sort((a, b) => a.s - b.s);
      const top = scored.slice(0, Math.max(4, Math.ceil(scored.length * .1)));
      const pickd = pick(r, top).x;
      used.add(pickd.id);
      return pickd;
    };
  }

  /* ---------- compose ---------- */
  function compose(corpus, opts = {}) {   // corpus is filtered locally by opts.exclude
    const seed = (opts.seed ?? Math.floor(Math.random() * 2 ** 31)) >>> 0;
    const r = rng(hash('journey:' + seed));   // hashed so nearby seeds give unrelated pieces
    const D = opts.duration || 660;
    const used = new Set();
    const choose = chooser(r, used);
    // Curation: id prefixes Yuri never wants in a journey.
    if (opts.exclude && opts.exclude.length) corpus = corpus.filter(x => !opts.exclude.some(p => String(x.id).startsWith(p)));
    const beds = poolOf(corpus, isBed), details = poolOf(corpus, isDetail), voices = poolOf(corpus, isVoice);
    const kits = poolOf(corpus, isKit), tonal = poolOf(corpus, isTonal);
    if (beds.length < 3) throw new Error('Not enough ambience recordings to compose');

    const bpm = pick(r, [84, 88, 90, 92, 96]), beat = 60 / bpm, bar = beat * 4;
    const root = pick(r, [45, 47, 48, 50, 52]), mode = pick(r, ['aeolian', 'dorian']);
    const prog = pick(r, PROGRESSIONS);
    const S = Object.fromEntries(ARC.map(a => [a.key, { ...a, t0: a.from * D, t1: a.to * D }]));
    const pickFor = (pool, sec, o = {}) => choose(pool, S[sec].hours, { ...o, prefer: PREFER[sec] });
    const events = [], sources = new Map();
    const src = rec => {
      if (!rec) return null;
      if (!sources.has(rec.id)) sources.set(rec.id, { id: rec.id, title: rec.title || '', file: rec.file, audioUrl: rec.audioUrl, duration: rec.duration, lufs: rec.lufs ?? -30, hour: rec.hour, category: rec.category, ...place(rec) });
      return rec.id;
    };
    const ev = e => { if (e.dur > 0.05 && e.t < D) events.push({ ...e, dur: Math.min(e.dur, D - e.t) }); };

    // Beds: long ambience, crossfaded, filter opening at dawn, closing at the end.
    const bed = (sec, t, dur, extra = {}) => {
      const rec = pickFor(beds, sec, { minDur: 20 });
      if (!rec) return null;
      ev({ type: 'bed', id: src(rec), t, dur, gain: -6, fadeIn: 18, fadeOut: 20, section: sec, ...extra });
      return rec;
    };
    const bedA = bed('dawn', 0, .23 * D, { gain: -11, fadeIn: 26, open: [220, 9000, 70] });
    bed('gathering', .13 * D, .19 * D);
    bed('gathering', .27 * D, .17 * D);
    bed('pulse', .40 * D, .23 * D, { gain: -10, lowpass: 2400 });
    bed('drift', .60 * D, .2 * D, { gain: -8 });
    bed('night', .76 * D, D * .24 - 7, { gain: -12, fadeOut: 26, close: [9000, 260, 40] });

    // Textures: granular clouds pulled out of a recording — the city slowed,
    // stretched and tuned into a halo.
    const texture = (rec, t, dur, extra = {}) => rec && ev({ type: 'texture', id: src(rec), t, dur, gain: -2, fadeIn: 20, fadeOut: 24, rates: [.5, .75], grain: [.35, .9], density: 6, seed: Math.floor(r() * 1e9), ...extra });
    texture(bedA, 9, .19 * D, { gain: -6 });
    texture(pickFor(tonal.length ? tonal : beds, 'drift'), .58 * D, .2 * D, { rates: [.5, 1, 1.5], density: 7, gain: -7 });
    texture(pickFor(tonal.length ? tonal : beds, 'night'), .8 * D, .12 * D, { rates: [.25, .5], grain: [.6, 1.4], density: 4, gain: -8 });

    // The memory: one detail at dawn, returning unchanged at night.
    const memRec = pickFor(details, 'dawn', { minDur: 4 });
    let memory = null;
    if (memRec) {
      const len = between(r, 2.6, 4.5), offset = between(r, 0, Math.max(0, memRec.duration - len - .2));
      memory = { id: src(memRec), offset, len };
      ev({ type: 'detail', id: memory.id, t: .10 * D, dur: len, offset, gain: -1, echo: .35, section: 'dawn', memory: true });
      ev({ type: 'detail', id: memory.id, t: .865 * D, dur: len, offset, gain: -1, echo: .45, section: 'night', memory: true });
    }

    // Details: short events that surface out of the ambience.
    const detail = (sec, t, trim = 0) => {
      const rec = pickFor(details, sec, { minDur: 3 });
      if (!rec) return;
      const len = between(r, 2, Math.min(7, rec.duration - .3));
      const roll = r();
      const rate = roll < .14 ? .5 : roll < .22 ? .75 : roll > .93 ? 1.5 : 1;
      ev({ type: 'detail', id: src(rec), t, dur: len / rate, offset: between(r, 0, Math.max(0, rec.duration - len - .2)), rate, gain: -3 + trim + between(r, -2, 2), echo: r() < .4 ? .3 : 0, section: sec });
    };
    for (let t = .15 * D; t < .37 * D; t += between(r, 13, 24)) detail('gathering', t);
    for (let t = .62 * D; t < .76 * D; t += between(r, 16, 26)) detail('drift', t);
    for (let t = .79 * D; t < .9 * D; t += between(r, 20, 32)) detail('night', t, -5);

    // The voice motif: one phrase repeated with micro-variation, then recalled.
    const vRec = pickFor(voices.length ? voices : details, 'gathering', { minDur: 5 });
    if (vRec) {
      const len = between(r, 3, 5.5), offset = between(r, 0, Math.max(0, vRec.duration - len - .2)), id = src(vRec);
      const n = 4 + Math.floor(r() * 3);
      let t = .21 * D;
      for (let i = 0; i < n; i++, t += between(r, 7, 11))
        ev({ type: 'detail', id, t, dur: len, offset: offset + between(r, -.05, .05), rate: 1 + between(r, -.015, .015), gain: 0 + between(r, -1.5, 1), cutoff: between(r, 2500, 12000), echo: i === n - 1 ? .4 : 0, section: 'gathering', motif: true });
      for (let i = 0, t2 = .66 * D; i < 3; i++, t2 += between(r, 9, 14))
        ev({ type: 'detail', id, t: t2, dur: len, offset, rate: 1, gain: -3, echo: .5, section: 'drift', motif: true });
    }

    // Harmony: slow chords that breathe; never a held drone.
    const chordAt = i => chord(root + 12, mode, prog[i % prog.length]);
    const pads = (t0, t1, every, attack, gain, gap = 0) => {
      let i = 0;
      for (let t = t0; t < t1 - 4; t += every, i++)
        ev({ type: 'pad', t, dur: Math.min(every - gap, t1 - t), notes: chordAt(i), attack, release: Math.min(9, every * .6), gain });
    };
    pads(.07 * D, .38 * D, bar * 8, 7, -19);
    pads(.40 * D, .60 * D, bar * 8, 2.5, -21);
    pads(.62 * D, .76 * D, bar * 12, 8, -18);
    pads(.78 * D, .95 * D, bar * 10, 9, -19, bar * 2.5);     // night swells with silences between

    // Pulse: found percussion, bass and a soft arpeggio, built up and broken down.
    const p0 = Math.round(S.pulse.t0 / bar) * bar, bars = Math.floor((.615 * D - p0) / bar);
    const kitRecs = [pickFor(kits, 'pulse'), pickFor(kits, 'pulse')].filter(Boolean);
    const kit = kitRecs.map(src);
    const hits = [], bass = [], arp = [];
    const swing = .08 * beat;
    for (let b = 0; b < bars; b++) {
      const phase = b / bars;                 // 0 → 1 across the pulse
      const energy = phase < .15 ? .3 : phase < .3 ? .6 : phase < .7 ? 1 : phase < .85 ? .55 : .25;
      const kick = phase >= .15 && phase < .82;
      const tb = p0 + b * bar;
      for (let s = 0; s < 16; s++) {
        const t = tb + s * beat / 4 + (s % 2 ? swing : 0);
        if (kick && (s === 0 || s === 8 || (s === 10 && r() < .35 * energy) || (s === 14 && r() < .2 * energy))) hits.push({ t, k: 'k', v: .9 });
        if (energy > .5 && (s === 4 || s === 12)) hits.push({ t, k: 's', v: .7 + between(r, -.1, .1) });
        if (s % 2 === 0 ? r() < .55 + .4 * energy : r() < .18 * energy) hits.push({ t, k: 'h', v: s % 4 === 2 ? .7 : .45 });
      }
      const ch = chordAt(Math.floor(b / 2));
      if (phase > .12 && phase < .9) {
        bass.push({ t: tb, dur: beat * 1.6, m: ch[0] - 24, v: .8 });
        if (r() < .7) bass.push({ t: tb + beat * 2.5, dur: beat * .9, m: ch[r() < .5 ? 0 : 2] - 24, v: .6 });
      }
      if (phase > .25 && phase < .8)
        for (let s = 0; s < 8; s++) if (r() < .75) arp.push({ t: tb + s * beat / 2 + (s % 2 ? swing : 0), dur: beat * .45, m: ch[(s + b) % 4] + 12, v: .55 + .3 * r() });
    }
    // anticipation: a few found hits at the end of gathering
    for (let i = 0; i < 6; i++) hits.push({ t: p0 - bar * 2 + Math.floor(r() * 8) * beat / 2, k: r() < .5 ? 's' : 'h', v: .35 });
    // the night sometimes keeps a slow pulse
    if (r() < .4) {
      const n0 = Math.round(.8 * D / bar) * bar;
      for (let b = 0; b < 8; b++) {
        hits.push({ t: n0 + b * bar, k: 'k', v: .45 });
        for (let s = 0; s < 8; s++) if (r() < .5) hits.push({ t: n0 + b * bar + s * beat / 2, k: 'h', v: .25 });
      }
    }
    hits.sort((a, b) => a.t - b.t);
    if (kit.length && hits.length) ev({ type: 'perc', ids: kit, id: kit[0], t: p0 - bar * 2, dur: hits[hits.length - 1].t + 1 - (p0 - bar * 2), hits, gain: 2 });   // found slices lose most energy to the kit filters
    ev({ type: 'bass', t: p0, dur: bars * bar, notes: bass, gain: -29 });
    ev({ type: 'arp', t: p0, dur: bars * bar, notes: arp, gain: -25 });

    // Stutter: a phrase chopped onto the beat — the machine composing.
    for (let k = 0; k < 2; k++) {
      const rec = pickFor(voices.length ? voices : details, 'pulse', { minDur: 2 });
      if (!rec) continue;
      const tb = p0 + Math.floor(bars * (.35 + .3 * k)) * bar, off = between(r, 0, Math.max(0, rec.duration - 1.2));
      for (let i = 0; i < 6; i++) ev({ type: 'detail', id: src(rec), t: tb + i * beat / 2, dur: beat * (i === 5 ? 1.2 : .42), offset: off, gain: -4 + i * .5, echo: i === 5 ? .4 : 0, section: 'pulse' });
    }

    events.sort((a, b) => a.t - b.t);
    return {
      version: 1, seed, duration: D, bpm, root, mode, bounds: BOUNDS,
      sections: ARC.map(a => ({ key: a.key, label: a.label, start: a.from * D, end: a.to * D })),
      sources: [...sources.values()], events,
      memory,
    };
  }

  /* ---------- perform ---------- */
  // io.getBuffer(source) -> Promise<AudioBuffer>; io.offline -> schedule everything at once.
  function createPerformer(ctx, score, io) {
    const S = score, t0 = io.startAt ?? ctx.currentTime + .25;
    const bufs = new Map(), loading = new Map(), failed = new Set();
    const srcById = new Map(S.sources.map(s => [s.id, s]));
    let nextEvent = 0, stopped = false;
    const live = [];          // scheduled nodes to stop on teardown

    // master: summing → gentle glue → safety limiter → volume
    const out = ctx.createGain(); out.gain.value = io.volume ?? .8;
    const lim = ctx.createDynamicsCompressor();
    lim.threshold.value = -3; lim.knee.value = 0; lim.ratio.value = 20; lim.attack.value = .002; lim.release.value = .12;
    const glue = ctx.createDynamicsCompressor();
    glue.threshold.value = -20; glue.knee.value = 10; glue.ratio.value = 2.2; glue.attack.value = .03; glue.release.value = .35;
    const hp = ctx.createBiquadFilter(); hp.type = 'highpass'; hp.frequency.value = 28;
    const mix = ctx.createGain(); mix.gain.value = db(3.5);   // make-up: the layers are balanced to sit near -19 LUFS together
    mix.connect(hp); hp.connect(glue); glue.connect(lim); const safety = ctx.createWaveShaper(), curve = new Float32Array(2049);
    for (let i = 0; i < curve.length; i++) {   // unity below 0.8 (-2 dBFS), a soft knee to 1.0 above
      const x = i / 1024 - 1, a = Math.abs(x);
      curve[i] = a < .8 ? x : Math.sign(x) * (.8 + .13 * Math.tanh((a - .8) / .13));   // ceiling 0.93: true peak stays under 0 dBTP
    }
    safety.curve = curve; safety.oversample = 'none';   // oversampling's reconstruction filter rang past the ceiling
    lim.connect(out); out.connect(safety); safety.connect(io.destination || ctx.destination);

    // the room's acoustics: one long, soft reverb everything shares
    const verb = ctx.createConvolver(), verbIn = ctx.createGain(), verbOut = ctx.createGain();
    verb.buffer = impulse(ctx, 4.6, 7); verbOut.gain.value = .9;
    verbIn.connect(verb); verb.connect(verbOut); verbOut.connect(mix);
    // a tempo echo (ping-pong) for details and the arp
    const echoIn = ctx.createGain(), dl = ctx.createDelay(4), dr = ctx.createDelay(4), fb = ctx.createGain(), echoTone = ctx.createBiquadFilter(), merger = ctx.createChannelMerger(2), echoOut = ctx.createGain();
    const eighth = 60 / S.bpm / 2;
    dl.delayTime.value = eighth * 3; dr.delayTime.value = eighth * 3; fb.gain.value = .42;
    echoTone.type = 'lowpass'; echoTone.frequency.value = 3200; echoOut.gain.value = .8;
    echoIn.connect(dl); dl.connect(echoTone); echoTone.connect(dr); dr.connect(fb); fb.connect(dl);
    dl.connect(merger, 0, 0); dr.connect(merger, 0, 1); merger.connect(echoOut); echoOut.connect(mix); echoOut.connect(verbIn);
    // non-positional room tone (synth/bass) and the listener
    const tone = ctx.createGain(); tone.connect(mix);
    const toneVerb = ctx.createGain(); toneVerb.gain.value = .55; tone.connect(toneVerb); toneVerb.connect(verbIn);

    const L = ctx.listener;
    const listener = { x: io.listener?.x ?? 0, z: io.listener?.z ?? 18, yaw: io.listener?.yaw ?? 0 };
    function setListener(x, z, yaw, at = ctx.currentTime, ramp = .08) {
      listener.x = x; listener.z = z; listener.yaw = yaw;
      const fx = Math.sin(yaw), fz = -Math.cos(yaw);
      if (L.positionX) {
        const set = (p, v) => { p.cancelScheduledValues(at); p.setTargetAtTime(v, at, ramp); };
        set(L.positionX, x); L.positionY.value = 1.6; set(L.positionZ, z);
        set(L.forwardX, fx); L.forwardY.value = 0; set(L.forwardZ, fz);
        L.upX.value = 0; L.upY.value = 1; L.upZ.value = 0;
      } else { L.setPosition(x, 1.6, z); L.setOrientation(fx, 0, fz, 0, 1, 0); }
    }
    setListener(listener.x, listener.z, listener.yaw, ctx.currentTime, .001);

    // one HRTF panner per recording: its place in the room
    const places = new Map();
    function placeNode(id, ref) {
      if (places.has(id)) return places.get(id);
      const s = srcById.get(id), p = ctx.createPanner();
      p.panningModel = 'HRTF'; p.distanceModel = 'inverse'; p.refDistance = ref; p.rolloffFactor = 1.1; p.maxDistance = 200;
      if (p.positionX) { p.positionX.value = s.x; p.positionY.value = 1.4; p.positionZ.value = s.z; } else p.setPosition(s.x, 1.4, s.z);
      const g = ctx.createGain(); g.connect(p); p.connect(mix);
      const send = ctx.createGain(); send.gain.value = .32; g.connect(send); send.connect(verbIn);   // the room hears it too
      const node = { input: g, echo: ctx.createGain() };
      node.echo.connect(echoIn);
      places.set(id, node);
      return node;
    }
    const norm = id => db(clamp(-24 - (srcById.get(id)?.lufs ?? -30), -12, 14));   // loudness-match recordings
    // ...but never past the recording's own loudest moment: a quiet file with
    // a few hammer blows is matched up by its average and the blows spike.
    const peaks = new WeakMap();
    function bufferPeak(buf) {
      if (peaks.has(buf)) return peaks.get(buf);
      let pk = 1e-4;
      for (let c = 0; c < buf.numberOfChannels; c++) { const d = buf.getChannelData(c); for (let i = 0; i < d.length; i += 8) { const v = Math.abs(d[i]); if (v > pk) pk = v; } }
      peaks.set(buf, pk); return pk;
    }
    const longGain = (id, buf, trim) => Math.min(norm(id) * db(trim), .35 / bufferPeak(buf));   // peaks ≤ -9 dBFS

    function getBuf(id) {
      if (bufs.has(id)) return Promise.resolve(bufs.get(id));
      if (failed.has(id)) return Promise.resolve(null);
      if (!loading.has(id)) loading.set(id, io.getBuffer(srcById.get(id)).then(b => { bufs.set(id, b); return b; }, () => { failed.add(id); return null; }));
      return loading.get(id);
    }

    function envelope(param, t, dur, peak, fadeIn, fadeOut) {
      fadeIn = Math.min(fadeIn, dur / 2); fadeOut = Math.min(fadeOut, dur / 2);
      param.setValueAtTime(0, t);
      param.linearRampToValueAtTime(peak, t + fadeIn);
      param.setValueAtTime(peak, t + dur - fadeOut);
      param.linearRampToValueAtTime(0, t + dur);
    }
    function track(node, end) { live.push({ node, end }); }
    // Finished chains must leave the graph: a dangling gain or filter is
    // still visited every render quantum, so cost would grow all night.
    const unplug = (...nodes) => nodes.forEach(n => { if (n) try { n.disconnect(); } catch {} });

    // A bed longer than its file is laid as overlapping segments, crossfaded,
    // so it never loops audibly or clicks at a seam.
    function playBed(e, buf, at) {
      const place = placeNode(e.id, 10), k = longGain(e.id, buf, e.gain);
      const out = ctx.createGain(); envelope(out.gain, at, e.dur, k, e.fadeIn, e.fadeOut);
      let chain = out;
      if (e.open || e.close || e.lowpass) {
        const f = ctx.createBiquadFilter(); f.type = 'lowpass'; f.Q.value = .5;
        if (e.lowpass) f.frequency.value = e.lowpass;
        if (e.open) { f.frequency.setValueAtTime(e.open[0], at); f.frequency.exponentialRampToValueAtTime(e.open[1], at + e.open[2]); }
        if (e.close) { f.frequency.setValueAtTime(e.close[0], at + e.dur - e.close[2]); f.frequency.exponentialRampToValueAtTime(e.close[1], at + e.dur); }
        out.connect(f); chain = f;
      }
      chain.connect(place.input);
      let pending = 0;
      const usable = Math.max(4, buf.duration - .4), xf = Math.min(4, usable / 4);
      let t = at, off = buf.duration > e.dur + 1 ? (hash(e.id + e.t) % 1000) / 1000 * (buf.duration - e.dur - .5) : 0;
      while (t < at + e.dur) {
        const len = Math.min(usable - off, at + e.dur - t + xf);
        const s = ctx.createBufferSource(), g = ctx.createGain(); s.buffer = buf;
        envelope(g.gain, t, len, 1, t === at ? .05 : xf, xf);
        s.connect(g); g.connect(out); s.start(t, off, len); track(s, t + len); pending++;
        s.onended = () => { unplug(s, g); if (--pending === 0) unplug(out, chain); };
        t += len - xf; off = .2;
      }
    }

    // Granular cloud: overlapping Hann-shaped grains at a few related rates.
    function playTexture(e, buf, at, until) {
      const place = placeNode(e.id, 8);
      if (!e._bus) {
        e._bus = ctx.createGain(); envelope(e._bus.gain, at, e.dur, longGain(e.id, buf, e.gain), e.fadeIn, e.fadeOut);
        e._bus.connect(place.input); e._r = rng(e.seed); e._t = at; e._at = at;
      }
      at = e._at;   // fixed at the first chunk: later calls pass "now", which would push the end out forever
      const r = e._r, end = Math.min(at + e.dur, until);
      while (e._t < end) {
        const rate = pick(r, e.rates), g = between(r, e.grain[0], e.grain[1]);
        const off = between(r, 0, Math.max(0, buf.duration - g * rate - .05));
        const s = ctx.createBufferSource(), env = ctx.createGain(), pan = ctx.createStereoPanner ? ctx.createStereoPanner() : null;
        s.buffer = buf; s.playbackRate.value = rate * (1 + between(r, -.004, .004));
        const amp = .55 * between(r, .5, 1);
        env.gain.setValueAtTime(0, e._t); env.gain.linearRampToValueAtTime(amp, e._t + g * .5); env.gain.linearRampToValueAtTime(0, e._t + g);
        s.connect(env);
        if (pan) { pan.pan.value = between(r, -.6, .6); env.connect(pan); pan.connect(e._bus); } else env.connect(e._bus);
        s.start(e._t, off, g * rate + .02); track(s, e._t + g);
        e._live = (e._live || 0) + 1;
        s.onended = () => { unplug(s, env, pan); if (--e._live === 0 && e._t >= at + e.dur) unplug(e._bus); };
        e._t += 1 / e.density * between(r, .5, 1.5);
      }
      return e._t >= at + e.dur;
    }

    // A short excerpt is levelled on itself: whole-file loudness turns one
    // bang in a quiet recording into a spike (+14 dB of matching on a transient).
    function excerptGain(buf, off, len) {
      const d = buf.getChannelData(0), sr = buf.sampleRate, a = Math.floor(off * sr), b = Math.min(d.length, a + Math.floor(len * sr));
      let sum = 0, pk = 1e-6, n = 0;
      for (let i = a; i < b; i += 4) { const v = d[i]; sum += v * v; n++; if (Math.abs(v) > pk) pk = Math.abs(v); }
      const rms = Math.sqrt(sum / Math.max(1, n)) + 1e-6;
      return clamp(Math.min(.063 / rms, .5 / pk), .05, 16);   // RMS -24 dBFS, peak never above -6 dBFS
    }
    function playDetail(e, buf, at) {
      const place = placeNode(e.id, 3.5), rate = e.rate || 1;
      const s = ctx.createBufferSource(), g = ctx.createGain(); s.buffer = buf; s.playbackRate.value = rate;
      const fade = Math.min(.25, e.dur / 4), off = clamp(e.offset || 0, 0, Math.max(0, buf.duration - .1));
      envelope(g.gain, at, e.dur, excerptGain(buf, off, e.dur * rate) * db(e.gain), fade, Math.min(.6, e.dur / 3));
      let chain = s;
      if (e.cutoff) { const f = ctx.createBiquadFilter(); f.type = 'lowpass'; f.frequency.value = e.cutoff; s.connect(f); chain = f; }
      chain.connect(g); g.connect(place.input);
      let send = null;
      if (e.echo) { send = ctx.createGain(); send.gain.value = e.echo; g.connect(send); send.connect(place.echo); }
      s.start(at, off, e.dur * rate + .05); track(s, at + e.dur);
      s.onended = () => unplug(s, chain === s ? null : chain, g, send);
    }

    // Found percussion: transients sliced out of the recordings themselves.
    function playPerc(e, kitBufs, at, until) {
      if (!e._slices) {
        e._slices = slicesFrom(kitBufs.filter(Boolean));
        e._bus = ctx.createGain(); e._bus.gain.value = db(e.gain);   // slices are peak-matched individually in slicesFrom
        e._bus.connect(placeNode(e.id, 6).input);
        const send = ctx.createGain(); send.gain.value = .25; e._bus.connect(send); send.connect(placeNode(e.id, 6).echo);
        e._i = 0;
      }
      const sl = e._slices;
      while (e._i < e.hits.length && t0 + e.hits[e._i].t < until) {
        const h = e.hits[e._i++], t = t0 + h.t;
        if (t < ctx.currentTime) continue;
        if (h.k === 'k') thump(t, h.v);
        const set = sl[h.k];
        if (!set || !set.length) continue;
        const [buf, off, k] = set[(e._i * 7) % set.length];
        const s = ctx.createBufferSource(), f = ctx.createBiquadFilter(), g = ctx.createGain();
        s.buffer = buf;
        const len = h.k === 'k' ? .16 : h.k === 's' ? .13 : .045;
        if (h.k === 'k') { f.type = 'lowpass'; f.frequency.value = 220; s.playbackRate.value = .72; }
        else if (h.k === 's') { f.type = 'bandpass'; f.frequency.value = 1900; f.Q.value = .8; s.playbackRate.value = 1.05; }
        else { f.type = 'highpass'; f.frequency.value = 6500; s.playbackRate.value = 1.6; }
        g.gain.setValueAtTime(h.v * k * (h.k === 'h' ? .8 : 1.1), t); g.gain.exponentialRampToValueAtTime(.001, t + len);
        s.connect(f); f.connect(g); g.connect(e._bus);
        s.start(t, off, len * 2); track(s, t + len);
        s.onended = () => unplug(s, f, g);
      }
      return e._i >= e.hits.length;
    }
    function thump(t, v) {   // a soft synthetic body under the found kick
      const o = ctx.createOscillator(), g = ctx.createGain();
      o.frequency.setValueAtTime(95, t); o.frequency.exponentialRampToValueAtTime(48, t + .12);
      g.gain.setValueAtTime(.0001, t); // weight under the found hit, never the lead
      g.gain.exponentialRampToValueAtTime(.09 * v, t + .004); g.gain.exponentialRampToValueAtTime(.0001, t + .32);
      o.connect(g); g.connect(tone); o.start(t); o.stop(t + .35); track(o, t + .35);
      o.onended = () => unplug(o, g);
    }

    function playPad(e, at) {
      const out = ctx.createGain(), f = ctx.createBiquadFilter();
      f.type = 'lowpass'; f.Q.value = .4;
      f.frequency.setValueAtTime(500, at); f.frequency.linearRampToValueAtTime(1700, at + e.attack); f.frequency.linearRampToValueAtTime(700, at + e.dur);
      envelope(out.gain, at, e.dur, db(e.gain), e.attack, e.release);
      f.connect(out); out.connect(tone);
      let voices = 0;
      e.notes.forEach((m, i) => {
        for (const [type, det, lvl] of [['sawtooth', -6, .22], ['sawtooth', 6, .22], ['triangle', 0, .35]]) {
          const o = ctx.createOscillator(), g = ctx.createGain(), p = ctx.createStereoPanner ? ctx.createStereoPanner() : null;
          o.type = type; o.frequency.value = mtof(m); o.detune.value = det + (i - 1.5) * 2;
          g.gain.value = lvl / e.notes.length;
          o.connect(g);
          if (p) { p.pan.value = ((i % 2) * 2 - 1) * .45 * (det ? Math.sign(det) : 1); g.connect(p); p.connect(f); } else g.connect(f);
          o.start(at); o.stop(at + e.dur + .1); track(o, at + e.dur); voices++;
          o.onended = () => { unplug(o, g, p); if (--voices === 0) unplug(f, out); };
        }
      });
    }
    function playNotes(e, at, until, kind) {
      if (!e._bus) {
        e._bus = ctx.createGain(); e._bus.gain.value = db(e.gain);
        const f = ctx.createBiquadFilter(); f.type = 'lowpass'; f.frequency.value = kind === 'bass' ? 380 : 2300;
        e._bus.connect(f); f.connect(tone);
        if (kind === 'arp') { const s = ctx.createGain(); s.gain.value = .35; f.connect(s); s.connect(echoIn); }
        e._i = 0;
      }
      while (e._i < e.notes.length && t0 + e.notes[e._i].t < until) {
        const n = e.notes[e._i++], t = t0 + n.t;
        if (t < ctx.currentTime) continue;
        const o = ctx.createOscillator(), g = ctx.createGain();
        o.type = kind === 'bass' ? 'sine' : 'triangle'; o.frequency.value = mtof(n.m);
        const a = kind === 'bass' ? .012 : .004, rel = kind === 'bass' ? .25 : .35;
        g.gain.setValueAtTime(.0001, t); g.gain.exponentialRampToValueAtTime(n.v, t + a);
        if (kind === 'bass') { g.gain.setValueAtTime(n.v, t + n.dur); g.gain.exponentialRampToValueAtTime(.0001, t + n.dur + rel); }
        else g.gain.exponentialRampToValueAtTime(.0001, t + rel);
        o.connect(g); g.connect(e._bus); o.start(t); o.stop(t + n.dur + rel + .05); track(o, t + n.dur + rel);
        o.onended = () => unplug(o, g);
        if (kind === 'bass') {   // a quiet octave so small speakers still hear the line
          const o2 = ctx.createOscillator(), g2 = ctx.createGain(); o2.type = 'triangle'; o2.frequency.value = mtof(n.m + 12);
          g2.gain.value = .18; o2.connect(g2); g2.connect(g); o2.start(t); o2.stop(t + n.dur + rel + .05); track(o2, t + n.dur + rel);
          o2.onended = () => unplug(o2, g2);
        }
      }
      return e._i >= e.notes.length;
    }

    // The scheduler: offline schedules everything at once; live works ahead
    // of the clock and fetches each recording well before it is needed.
    const active = [];
    async function schedule(until) {
      while (nextEvent < S.events.length && t0 + S.events[nextEvent].t < until + 90) {
        const e = S.events[nextEvent++];
        (e.ids || (e.id ? [e.id] : [])).forEach(getBuf);
        active.push(e);
      }
      for (let i = 0; i < active.length; i++) {
        const e = active[i];
        if (e._done || t0 + e.t >= until) continue;
        const at = Math.max(t0 + e.t, ctx.currentTime + .05);
        if (!io.offline && at > t0 + e.t + 8 && !['texture', 'perc', 'bass', 'arp', 'bed'].includes(e.type)) { e._done = true; continue; }
        if (e.type === 'pad') { playPad(e, at); e._done = true; continue; }
        if (e.type === 'bass' || e.type === 'arp') { e._done = playNotes(e, at, until, e.type); continue; }
        if (e.type === 'perc') {
          const kb = await Promise.all(e.ids.map(getBuf));
          if (kb.every(b => !b)) { e._done = true; continue; }
          e._done = playPerc(e, kb, at, until); continue;
        }
        const buf = bufs.get(e.id);
        if (!buf) { if (failed.has(e.id)) e._done = true; continue; }
        if (e.type === 'bed') { playBed(e, buf, at); e._done = true; }
        else if (e.type === 'texture') e._done = playTexture(e, buf, at, until);
        else { playDetail(e, buf, at); e._done = true; }
      }
      for (let i = active.length - 1; i >= 0; i--) if (active[i]._done) active.splice(i, 1);
      for (let i = live.length - 1; i >= 0; i--) if (live[i].end < ctx.currentTime - 1) live.splice(i, 1);
    }

    let timer = null;
    return {
      score: S, startAt: t0, listener, setListener,
      // Offline: preload every recording, then build the graph in chunks at
      // pre-registered suspend points, as live does — laying out the whole
      // piece at once means ~11k nodes visited on every render quantum.
      async prepareOffline(chunk = 15) {
        await Promise.all(S.sources.map(s => getBuf(s.id)));
        await schedule(t0 + chunk * 2);
        for (let t = chunk; t < S.duration; t += chunk)
          ctx.suspend(t0 + t).then(async () => { await schedule(t0 + t + chunk * 2); ctx.resume(); });
      },
      // Live: keep 25 s scheduled ahead; recordings load up to 90 s ahead.
      start() {
        const tick = () => { if (!stopped) schedule(ctx.currentTime + 25).catch(() => {}); };
        tick(); timer = setInterval(tick, 500);
      },
      stop(fade = 2) {
        stopped = true; clearInterval(timer);
        const t = ctx.currentTime;
        out.gain.cancelScheduledValues(t); out.gain.setValueAtTime(out.gain.value, t); out.gain.linearRampToValueAtTime(0, t + fade);
        setTimeout(() => { live.forEach(({ node }) => { try { node.stop(); } catch {} }); try { out.disconnect(); } catch {} }, fade * 1000 + 100);
      },
      setVolume(v) { out.gain.setTargetAtTime(v, ctx.currentTime, .05); },
      position() { return clamp(ctx.currentTime - t0, 0, S.duration); },
      section(time = this.position()) {
        let cur = S.sections[0];
        for (const s of S.sections) if (time >= s.start) cur = s;
        return cur;
      },
      // What is sounding where, 0..1, from the score's own envelopes.
      levels(time = this.position()) {
        const lv = {};
        for (const e of S.events) {
          if (!e.id || e.t > time || e.t + e.dur < time || failed.has(e.id)) continue;
          const fi = e.fadeIn ?? .3, fo = e.fadeOut ?? .5, u = time - e.t;
          const env = Math.min(1, u / Math.max(.01, fi), (e.dur - u) / Math.max(.01, fo));
          const base = e.type === 'bed' ? .55 : e.type === 'texture' ? .45 : e.type === 'perc' ? .7 : .9;
          lv[e.id] = Math.max(lv[e.id] || 0, clamp(env * base, 0, 1));
        }
        return lv;
      },
      // Recordings present now or within the next few seconds, with places.
      presentSources(time = this.position(), ahead = 6) {
        const ids = new Set(S.events.filter(e => e.id && e.t <= time + ahead && e.t + e.dur >= time - 1.5 && !failed.has(e.id)).flatMap(e => e.ids || [e.id]));
        return S.sources.filter(s => ids.has(s.id));
      },
      loaded: id => bufs.has(id),
    };
  }

  // Transient slices sorted into low (kick), mid (snare) and bright (hat).
  function slicesFrom(buffers) {
    const out = { k: [], s: [], h: [] };
    for (const buf of buffers) {
      const sr = buf.sampleRate, d = buf.getChannelData(0), hop = Math.round(sr * .01), n = Math.floor(d.length / hop) - 1;
      const e = new Float32Array(n), hf = new Float32Array(n);
      for (let i = 0; i < n; i++) {
        let a = 0, b = 0;
        for (let j = i * hop + 1; j < (i + 1) * hop; j++) { a += d[j] * d[j]; const df = d[j] - d[j - 1]; b += df * df; }
        e[i] = a; hf[i] = b;
      }
      let mean = 0; for (const v of e) mean += v; mean /= n || 1;
      const found = [];
      for (let i = 2; i < n - 8; i++) {
        const flux = e[i] - e[i - 1];
        if (flux > mean * 2.2 && e[i] >= e[i + 1] * .8 && (!found.length || i - found[found.length - 1].i > 12)) {
          let A = 0, B = 0; for (let j = i; j < i + 6; j++) { A += e[j]; B += hf[j]; }
          found.push({ i, bright: B / (A + 1e-9), peak: e[i] });
        }
      }
      found.sort((a, b) => b.peak - a.peak);
      const top = found.slice(0, 40).sort((a, b) => a.bright - b.bright);
      const third = Math.max(1, Math.floor(top.length / 3));
      top.forEach((f, idx) => {
        const kind = idx < third ? 'k' : idx < 2 * third ? 's' : 'h';
        if (out[kind].length >= 6) return;
        // Peak-match each hit: whole-file loudness says little about one transient.
        const start = Math.max(0, f.i * hop - Math.round(sr * .004)); let pk = 1e-6;
        for (let j = start; j < Math.min(d.length, start + Math.round(sr * .12)); j++) pk = Math.max(pk, Math.abs(d[j]));
        out[kind].push([buf, start / sr, clamp(.5 / pk, .1, 12)]);
      });
    }
    return out;
  }

  // A soft, dark, slightly diffuse room: decorrelated noise with a falling
  // brightness, generated rather than downloaded.
  function impulse(ctx, seconds, seed) {
    const sr = ctx.sampleRate, len = Math.floor(sr * seconds), ir = ctx.createBuffer(2, len, sr), r = rng(seed);
    for (let c = 0; c < 2; c++) {
      const d = ir.getChannelData(c); let lp = 0;
      for (let i = 0; i < len; i++) {
        const t = i / sr, decay = Math.exp(-t * 1.55), k = clamp(.55 - t * .1, .06, .55);
        lp += k * ((r() * 2 - 1) - lp);
        d[i] = (t < .025 ? 0 : 1) * lp * decay;
      }
    }
    return ir;
  }

  const api = { compose, createPerformer, place, ARC, BOUNDS };
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  else root.Journey = api;
})(typeof window === 'undefined' ? globalThis : window);
