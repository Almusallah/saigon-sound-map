/* Phòng nghe — street mode.
   Walk the real streets of Saigon in 3D and find the archive where it was
   recorded. Every recording waits at its true coordinates as a pillar of
   light; come close and it sounds — binaurally, from its real direction and
   distance — and counts as found.

   StreetMode.create({ container, ctx, corpus, getBuffer, theme, mapboxgl, token, onStatus })
     -> { enter(), exit(), setTheme(theme), nextSound(), state }
   The page loads Mapbox GL lazily and passes it in. */
(function (root) {
  'use strict';

  const ORIGIN = { lat: 10.7765, lng: 106.6953 };              // metres are measured from here
  const M_LAT = 110574, M_LNG = 111320 * Math.cos(ORIGIN.lat * Math.PI / 180);
  const toXZ = (lng, lat) => ({ x: (lng - ORIGIN.lng) * M_LNG, z: -(lat - ORIGIN.lat) * M_LAT });
  const fromXZ = (x, z) => ({ lng: ORIGIN.lng + x / M_LNG, lat: ORIGIN.lat - z / M_LAT });
  const clamp = (v, a, b) => Math.max(a, Math.min(b, v));
  const db = d => Math.pow(10, d / 20);

  const AUDIBLE = 160;      // a recording falls silent this far away (m)
  const LOAD = 260;         // ...and starts loading this far away
  const RELEASE = 340;      // ...and lets go of its audio beyond this
  const FOUND = 20;         // walk this close to find it
  const MAX_VOICES = 10;
  const MAX_LOADS = 3;      // downloads at once — nearest first; a dense block must not starve itself
  const PRELOAD = 14;       // only the closest recordings are fetched at all
  const RETRY = 30000;      // a failed recording is tried again after this (ms)
  const SPEED = { street: [4.2, 15], above: [7, 26] };   // m/s: walking pace and scooter, per view
  const EYE = 1.7, LOOK = 82;                            // eye height (m); gaze a little below the horizon (Mapbox allows 85° at most)
  const STORE_KEY = 'phongnghe-found';

  function create(opts) {
    const { container, ctx, getBuffer, mapboxgl } = opts;
    const onStatus = opts.onStatus || (() => {});
    let theme = opts.theme === 'white' ? 'white' : 'black';
    let view = opts.view === 'above' ? 'above' : 'street';         // walking at eye height, or the overview
    let palette = opts.palette === 'mono' ? 'mono' : 'colour';     // the city in colour, or the room's monochrome
    const reduced = typeof matchMedia === 'function' && matchMedia('(prefers-reduced-motion: reduce)').matches;
    const sounds = opts.corpus.filter(r => Number.isFinite(r.lat) && Number.isFinite(r.lng) && r.duration >= 2)
      .map((r, i) => ({ ...r, n: i + 1, ...toXZ(r.lng, r.lat) }));
    // Recordings made at the same spot would stand inside one pillar; fan them
    // a few metres apart, as the map fans its markers in pixels.
    {
      const piles = new Map();
      for (const s of sounds) { const k = s.lat.toFixed(5) + ',' + s.lng.toFixed(5); if (!piles.has(k)) piles.set(k, []); piles.get(k).push(s); }
      for (const pile of piles.values()) if (pile.length > 1) pile.forEach((s, i) => {
        const a = i / pile.length * 2 * Math.PI, r = 3 + .6 * pile.length;
        s.x += r * Math.sin(a); s.z -= r * Math.cos(a);
        const ll = fromXZ(s.x, s.z); s.lng = ll.lng; s.lat = ll.lat;
      });
    }
    const byN = new Map(sounds.map(s => [s.n, s]));
    let found = new Set();
    try { found = new Set(JSON.parse(localStorage.getItem(STORE_KEY) || '[]')); } catch {}

    /* ---------- the player ---------- */
    // Start where the archive is dense and the city is legible: the cluster
    // in central District 1, a few steps south of its middle, facing north.
    const start = (() => {
      const centre = sounds.filter(s => Math.hypot(s.x, s.z) < 3000);
      let best = centre[0] || sounds[0], bestN = -1;
      for (const s of centre) { const n = centre.filter(o => Math.hypot(o.x - s.x, o.z - s.z) < 250).length; if (n > bestN) { best = s; bestN = n; } }
      return { x: best.x + 10, z: best.z + 45 };
    })();
    const player = { x: start.x, z: start.z, bearing: 0, vx: 0, vz: 0, turn: 0 };
    const keys = new Set(), tapped = new Set();   // tapped: pressed since the last frame, even if already released
    let glide = null, map = null, raf = 0, tickTimer = 0, levelTimer = 0, lightTimer = 0, alive = false, entered = false;

    /* ---------- audio graph ---------- */
    const out = ctx.createGain(); out.gain.value = 0;
    const glue = ctx.createDynamicsCompressor();
    glue.threshold.value = -20; glue.knee.value = 10; glue.ratio.value = 2.2; glue.attack.value = .03; glue.release.value = .35;
    const safety = ctx.createWaveShaper(), curve = new Float32Array(2049);
    for (let i = 0; i < curve.length; i++) { const x = i / 1024 - 1, a = Math.abs(x); curve[i] = a < .8 ? x : Math.sign(x) * (.8 + .13 * Math.tanh((a - .8) / .13)); }
    safety.curve = curve;
    const mix = ctx.createGain(); mix.gain.value = db(2);
    const air = ctx.createConvolver(), airSend = ctx.createGain(); airSend.gain.value = .14;
    air.buffer = (() => {   // a short open-air reflection, generated
      const len = Math.floor(ctx.sampleRate * 1.6), b = ctx.createBuffer(2, len, ctx.sampleRate);
      for (let c = 0; c < 2; c++) { const d = b.getChannelData(c); let lp = 0, seed = 7 + c * 31;
        for (let i = 0; i < len; i++) { seed = (seed * 1103515245 + 12345) >>> 0; lp += .4 * (((seed >>> 8) / 8388608 - 1) - lp); d[i] = (i < ctx.sampleRate * .018 ? 0 : 1) * lp * Math.exp(-i / ctx.sampleRate * 3.2); } }
      return b;
    })();
    mix.connect(glue); glue.connect(out); out.connect(safety); safety.connect(ctx.destination);
    airSend.connect(air); air.connect(mix);

    const L = ctx.listener;
    function setListener() {
      const t = ctx.currentTime, b = player.bearing * Math.PI / 180, fx = Math.sin(b), fz = -Math.cos(b);
      if (L.positionX) {
        L.positionX.setTargetAtTime(player.x, t, .05); L.positionY.value = 1.6; L.positionZ.setTargetAtTime(player.z, t, .05);
        L.forwardX.setTargetAtTime(fx, t, .05); L.forwardY.value = 0; L.forwardZ.setTargetAtTime(fz, t, .05);
        L.upX.value = 0; L.upY.value = 1; L.upZ.value = 0;
      } else { L.setPosition(player.x, 1.6, player.z); L.setOrientation(fx, 0, fz, 0, 1, 0); }
    }

    const voices = new Map();   // n -> voice
    const buffers = new Map(), loading = new Map(), failed = new Map();   // n -> time of failure
    function load(s) {
      if (buffers.has(s.n) || loading.has(s.n) || loading.size >= MAX_LOADS) return;
      if (failed.has(s.n) && performance.now() - failed.get(s.n) < RETRY) return;
      loading.set(s.n, getBuffer(s).then(b => { buffers.set(s.n, b); failed.delete(s.n); loading.delete(s.n); }, () => { failed.set(s.n, performance.now()); loading.delete(s.n); }));
    }
    function peakOf(buf) { let pk = 1e-4; const d = buf.getChannelData(0); for (let i = 0; i < d.length; i += 8) pk = Math.max(pk, Math.abs(d[i])); return pk; }

    // A recording loops for as long as you stay near it: overlapping
    // segments crossfaded, so the seam never clicks or repeats audibly.
    function startVoice(s) {
      const buf = buffers.get(s.n); if (!buf) return;
      const p = ctx.createPanner();
      p.panningModel = 'HRTF'; p.distanceModel = 'linear'; p.refDistance = 6; p.maxDistance = AUDIBLE; p.rolloffFactor = 1;
      if (p.positionX) { p.positionX.value = s.x; p.positionY.value = 1.5; p.positionZ.value = s.z; } else p.setPosition(s.x, 1.5, s.z);
      const g = ctx.createGain(), send = ctx.createGain(); send.gain.value = 1;
      const level = Math.min(db(clamp(-22 - (s.lufs ?? -30), -12, 12)), .35 / peakOf(buf));
      g.gain.setValueAtTime(0, ctx.currentTime); g.gain.linearRampToValueAtTime(level, ctx.currentTime + 2.5);
      g.connect(p); p.connect(mix); g.connect(send); send.connect(airSend);
      const v = { s, buf, p, g, send, level, next: ctx.currentTime + .05, off: Math.random() * Math.max(0, buf.duration - 8), nodes: [] };
      voices.set(s.n, v);
      scheduleVoice(v);
    }
    function scheduleVoice(v) {
      const xf = Math.min(3, v.buf.duration / 4), usable = Math.max(2, v.buf.duration - .3);
      while (v.next < ctx.currentTime + 8) {
        const len = Math.min(usable - v.off, 60);
        const src = ctx.createBufferSource(), env = ctx.createGain(); src.buffer = v.buf;
        env.gain.setValueAtTime(0, v.next); env.gain.linearRampToValueAtTime(1, v.next + xf);
        env.gain.setValueAtTime(1, v.next + len - xf); env.gain.linearRampToValueAtTime(0, v.next + len);
        src.connect(env); env.connect(v.g); src.start(v.next, v.off, len);
        src.onended = () => { try { src.disconnect(); env.disconnect(); } catch {} };
        v.nodes.push(src);
        v.next += len - xf; v.off = v.off + len >= usable - 1 ? 0 : v.off + len - xf;
      }
      if (v.nodes.length > 6) v.nodes.splice(0, v.nodes.length - 6);
    }
    function stopVoice(n, fade = 2) {
      const v = voices.get(n); if (!v) return;
      voices.delete(n);
      const t = ctx.currentTime;
      v.g.gain.cancelScheduledValues(t); v.g.gain.setValueAtTime(v.g.gain.value, t); v.g.gain.linearRampToValueAtTime(0, t + fade);
      setTimeout(() => { v.nodes.forEach(s => { try { s.stop(); } catch {} }); try { v.g.disconnect(); v.p.disconnect(); v.send.disconnect(); } catch {} }, fade * 1000 + 100);
    }

    // Every half second: who is near, load ahead, play the closest, release the far.
    function audioTick() {
      const near = sounds.map(s => ({ s, d: Math.hypot(s.x - player.x, s.z - player.z) })).filter(o => o.d < RELEASE).sort((a, b) => a.d - b.d);
      for (const { s, d } of near.slice(0, PRELOAD)) if (d < LOAD) load(s);   // sorted: the nearest claim the free slots
      const want = new Set(near.filter(o => o.d < AUDIBLE && buffers.has(o.s.n)).slice(0, MAX_VOICES).map(o => o.s.n));
      for (const n of [...voices.keys()]) if (!want.has(n)) stopVoice(n);
      for (const n of want) if (!voices.has(n)) startVoice(byN.get(n));
      for (const v of voices.values()) scheduleVoice(v);
      for (const n of [...buffers.keys()]) if (!near.some(o => o.s.n === n)) buffers.delete(n);
      for (const { s, d } of near) {
        if (d < FOUND && voices.has(s.n) && !found.has(s.id)) {
          found.add(s.id);
          try { localStorage.setItem(STORE_KEY, JSON.stringify([...found])); } catch {}
          if (map) { map.setFeatureState({ source: 'sounds', id: s.n }, { found: true }); map.setFeatureState({ source: 'pillars', id: s.n }, { found: true }); }
          onStatus({ type: 'found', title: s.title, count: found.size, total: sounds.length });
        }
      }
      onStatus({ type: 'hud', count: found.size, total: sounds.length, nearest: nearestUnfound(), voices: voices.size });
    }
    function nearestUnfound() {
      let best = null, bd = Infinity;
      for (const s of sounds) { if (found.has(s.id)) continue; const d = Math.hypot(s.x - player.x, s.z - player.z); if (d < bd) { bd = d; best = s; } }
      if (!best) return null;
      const abs = Math.atan2(best.x - player.x, -(best.z - player.z)) * 180 / Math.PI;
      return { title: best.title, distance: bd, relBearing: ((abs - player.bearing) % 360 + 540) % 360 - 180, s: best };
    }

    /* ---------- the world ---------- */
    const hexagon = (s, r = 1.4) => {
      const ring = [];
      for (let i = 0; i <= 6; i++) { const a = i / 6 * 2 * Math.PI, p = fromXZ(s.x + r * Math.cos(a), s.z + r * Math.sin(a)); ring.push([p.lng, p.lat]); }
      return ring;
    };
    function soundData() {
      return {
        points: { type: 'FeatureCollection', features: sounds.map(s => ({ type: 'Feature', id: s.n, properties: { title: (s.title || '').toUpperCase() }, geometry: { type: 'Point', coordinates: [s.lng, s.lat] } })) },
        pillars: { type: 'FeatureCollection', features: sounds.map(s => ({ type: 'Feature', id: s.n, properties: {}, geometry: { type: 'Polygon', coordinates: [hexagon(s)] } })) },
      };
    }
    // In colour the streets are lit by the real hour in Saigon; in monochrome
    // they follow the room's black (night) or white (day).
    function saigonLight() {
      const h = Number(new Date().toLocaleString('en-GB', { hour: '2-digit', hour12: false, timeZone: 'Asia/Ho_Chi_Minh' }));
      return h >= 5 && h < 7 ? 'dawn' : h >= 7 && h < 17 ? 'day' : h >= 17 && h < 19 ? 'dusk' : 'night';
    }
    const light = () => palette === 'mono' ? (theme === 'white' ? 'day' : 'night') : saigonLight();
    const dark = () => ['night', 'dusk'].includes(light());
    const ink = () => dark() ? '#ffffff' : '#141414';
    const foundColour = () => dark() ? '#B8FF00' : '#4F7A00';   // the map's lime; deepened to stay legible in daylight
    const halo = () => dark() ? '#000000' : '#f3f3f1';
    function addLayers() {
      const data = soundData();
      map.addSource('sounds', { type: 'geojson', data: data.points });
      map.addSource('pillars', { type: 'geojson', data: data.pillars });
      const lvl = ['coalesce', ['feature-state', 'level'], 0];
      const colour = ['case', ['boolean', ['feature-state', 'found'], false], foundColour(), ink()];
      map.addLayer({ id: 'sound-ground', type: 'circle', source: 'sounds', slot: 'top', paint: {
        'circle-pitch-alignment': 'map', 'circle-color': colour, 'circle-blur': .9, 'circle-emissive-strength': 1,
        'circle-radius': ['interpolate', ['exponential', 2], ['zoom'], 15, ['+', 2, ['*', 6, lvl]], 20, ['+', 12, ['*', 60, lvl]]],
        'circle-opacity': ['+', .12, ['*', .5, lvl]],
      } });
      map.addLayer({ id: 'sound-pillar', type: 'fill-extrusion', source: 'pillars', slot: 'top', paint: {
        'fill-extrusion-color': colour, 'fill-extrusion-emissive-strength': 1, 'fill-extrusion-opacity': .9,
        'fill-extrusion-height': ['+', 5, ['*', 30, lvl]], 'fill-extrusion-base': 0,
      } });
      map.addLayer({ id: 'sound-label', type: 'symbol', source: 'sounds', slot: 'top', minzoom: 17, layout: {
        'text-field': ['get', 'title'], 'text-size': 10, 'text-letter-spacing': .12, 'text-offset': [0, -2.2],
        'text-font': ['DIN Pro Medium', 'Arial Unicode MS Regular'], 'text-max-width': 14, 'text-optional': true,
      }, paint: { 'text-color': colour, 'text-emissive-strength': 1, 'text-occlusion-opacity': 0, 'text-opacity': ['+', .35, ['*', .65, lvl]], 'text-halo-color': halo(), 'text-halo-width': 1 } });
      for (const id of found) { const s = sounds.find(x => x.id === id); if (s) map.setFeatureState({ source: 'sounds', id: s.n }, { found: true }); }
      for (const id of found) { const s = sounds.find(x => x.id === id); if (s) map.setFeatureState({ source: 'pillars', id: s.n }, { found: true }); }
    }
    function styleConfig() {
      const quiet = { showPointOfInterestLabels: false, showTransitLabels: false, showPlaceLabels: false, showRoadLabels: true };
      if (palette === 'mono') return { theme: 'monochrome', lightPreset: light(), show3dObjects: true, ...quiet };
      return { theme: 'default', lightPreset: light(), show3dObjects: true, show3dBuildings: true, show3dFacades: true,
        show3dTrees: true, show3dLandmarks: true, ...quiet };
    }
    let shownLight = null;
    function recolour() {
      if (!map) return;
      shownLight = light();
      map.setConfig('basemap', styleConfig());
      const colour = ['case', ['boolean', ['feature-state', 'found'], false], foundColour(), ink()];
      if (map.getLayer('sound-ground')) map.setPaintProperty('sound-ground', 'circle-color', colour);
      if (map.getLayer('sound-pillar')) map.setPaintProperty('sound-pillar', 'fill-extrusion-color', colour);
      if (map.getLayer('sound-label')) { map.setPaintProperty('sound-label', 'text-color', colour); map.setPaintProperty('sound-label', 'text-halo-color', halo()); }
      onStatus({ type: 'light', light: shownLight, dark: dark() });
    }

    // Walking: a free camera at eye height on your own position, so what you
    // see and what you hear share one point. Above: the third-person overview.
    let bobPhase = 0, bob = 0;
    function camera() {
      const p = fromXZ(player.x, player.z);
      if (view === 'street') {
        const cam = map.getFreeCameraOptions();
        cam.position = mapboxgl.MercatorCoordinate.fromLngLat([p.lng, p.lat], EYE + bob);
        cam.setPitchBearing(LOOK, player.bearing);
        map.setFreeCameraOptions(cam);
      } else map.jumpTo({ center: [p.lng, p.lat], bearing: player.bearing, pitch: 74, zoom: 19.1 });
    }
    function applyView() { if (map) map.setPadding({ top: view === 'above' ? Math.round(container.clientHeight * .28) : 0, bottom: 0, left: 0, right: 0 }); }
    // Levels on the map follow what you actually hear: distance-weighted.
    function levelTick() {
      if (!map) return;
      for (const s of sounds) {
        const d = Math.hypot(s.x - player.x, s.z - player.z), v = voices.get(s.n);
        const lv = v ? clamp(1 - (d - 6) / (AUDIBLE - 6), 0, 1) * (.75 + .25 * Math.sin(ctx.currentTime * 2.2 + s.n)) : 0;
        if (s._lv === undefined || Math.abs(s._lv - lv) > .03) {
          s._lv = lv;
          map.setFeatureState({ source: 'sounds', id: s.n }, { level: lv });
          map.setFeatureState({ source: 'pillars', id: s.n }, { level: lv });
        }
      }
    }

    /* ---------- moving ---------- */
    let last = 0;
    function frame(now) {
      raf = requestAnimationFrame(frame);
      const dt = Math.min(.05, (now - (last || now)) / 1000); last = now;
      const on = c => keys.has(c) || tapped.has(c);
      const fwd = (on('ArrowUp') || on('KeyW') ? 1 : 0) - (on('ArrowDown') || on('KeyS') ? 1 : 0) + drag.walk;
      const turn = (on('ArrowRight') || on('KeyD') ? 1 : 0) - (on('ArrowLeft') || on('KeyA') ? 1 : 0);
      const speed = SPEED[view][on('ShiftLeft') || on('ShiftRight') ? 1 : 0];
      tapped.clear();
      // Turning eases in and out; a drag's rotation settles over a few frames
      // instead of snapping, so the street never jerks.
      player.turn += (turn * 70 - player.turn) * (1 - Math.exp(-dt * 7));
      const dyaw = drag.turn * (1 - Math.exp(-dt * 14)); drag.turn -= dyaw;
      player.bearing = ((player.bearing + player.turn * dt + dyaw) % 360 + 360) % 360;
      const b = player.bearing * Math.PI / 180, target = clamp(fwd, -1, 1) * speed;
      const k = 1 - Math.exp(-dt * 6);
      player.vx += (Math.sin(b) * target - player.vx) * k; player.vz += (-Math.cos(b) * target - player.vz) * k;
      if (glide) {
        const t = clamp((now - glide.t0) / glide.ms, 0, 1), e = t < .5 ? 2 * t * t : 1 - Math.pow(-2 * t + 2, 2) / 2;
        player.x = glide.x0 + (glide.x1 - glide.x0) * e; player.z = glide.z0 + (glide.z1 - glide.z0) * e;
        player.bearing = glide.b0 + glide.db * Math.min(1, t * 2.5);
        if (t >= 1) glide = null;
        player.vx = player.vz = 0;
      } else { player.x += player.vx * dt; player.z += player.vz * dt; }
      const spd = Math.hypot(player.vx, player.vz);   // footsteps: a slight sway at walking pace
      bobPhase += dt * spd * .55; bob = reduced || view !== 'street' ? 0 : .03 * Math.sin(bobPhase * 2 * Math.PI) * Math.min(1, spd / 4);
      camera(); setListener();
    }
    function glideTo(x, z) {
      const dist = Math.hypot(x - player.x, z - player.z); if (dist < 1) return;
      const to = Math.atan2(x - player.x, -(z - player.z)) * 180 / Math.PI;
      const db_ = ((to - player.bearing) % 360 + 540) % 360 - 180;
      glide = { t0: performance.now(), ms: clamp(dist / SPEED[view][1] * 1000, 900, 9000), x0: player.x, z0: player.z, x1: x, z1: z, b0: player.bearing, db: db_ };
    }

    const drag = { walk: 0, turn: 0, id: null, x: 0, y: 0, moved: 0 };
    function onKey(e) {
      if (e.target.closest && e.target.closest('input,textarea,select')) return;
      if (['ArrowUp', 'ArrowDown', 'ArrowLeft', 'ArrowRight', 'KeyW', 'KeyA', 'KeyS', 'KeyD', 'ShiftLeft', 'ShiftRight'].includes(e.code)) {
        if (e.type === 'keydown') { keys.add(e.code); tapped.add(e.code); glide = null; } else keys.delete(e.code);
        if (e.code.startsWith('Arrow')) e.preventDefault();
      }
    }
    // A key released while the page is not focused never sends keyup, and the
    // walker would walk on alone (seen in testing: half an hour, 14 sounds).
    function releaseAll() { keys.clear(); tapped.clear(); drag.walk = 0; drag.id = null; }
    function onVisibility() { if (document.visibilityState !== 'visible') releaseAll(); }
    function onDown(e) { if (drag.id !== null) return; drag.id = e.pointerId; drag.x = e.clientX; drag.y = e.clientY; drag.moved = 0; container.setPointerCapture?.(e.pointerId); }
    function onMoveP(e) {
      if (e.pointerId !== drag.id) return;
      const dx = e.clientX - drag.x, dy = e.clientY - drag.y; drag.x = e.clientX; drag.y = e.clientY; drag.moved += Math.abs(dx) + Math.abs(dy);
      drag.turn -= dx * .25;                       // the street follows the finger, like the room
      drag.walk = clamp(drag.walk + dy * .02, -1, 1);
      if (drag.moved > 6) glide = null;
    }
    function onUp(e) {
      if (e.pointerId !== drag.id) return;
      drag.id = null; drag.walk = 0;
      if (drag.moved < 6 && map) {                 // a tap: go there
        const r = container.getBoundingClientRect(), ll = map.unproject([e.clientX - r.left, e.clientY - r.top]);
        const p = toXZ(ll.lng, ll.lat);
        if (Math.hypot(p.x - player.x, p.z - player.z) < 600) glideTo(p.x, p.z);
      }
    }
    function onWheel(e) { e.preventDefault(); const b = player.bearing * Math.PI / 180, step = clamp(-e.deltaY * .05, -6, 6); player.x += Math.sin(b) * step; player.z -= Math.cos(b) * step; }

    /* ---------- lifecycle ---------- */
    return {
      state: { player, found, sounds, voices, get map() { return map; } },
      enter() {
        if (alive) return Promise.resolve();
        alive = true;
        if (!entered) {   // the first time in, face the nearest sound rather than whatever wall is north
          entered = true; const n = nearestUnfound() || { s: sounds[0] };
          player.bearing = ((Math.atan2(n.s.x - player.x, -(n.s.z - player.z)) * 180 / Math.PI) + 360) % 360;
        }
        mapboxgl.accessToken = opts.token;
        const p = fromXZ(player.x, player.z);
        map = new mapboxgl.Map({
          container, style: 'mapbox://styles/mapbox/standard', config: { basemap: styleConfig() },
          center: [p.lng, p.lat], zoom: 19.1, pitch: 74, bearing: player.bearing, maxPitch: 85, maxZoom: 24, antialias: true,
          interactive: false, attributionControl: true, fadeDuration: 0,
        });
        applyView();   // above: the player a little below the middle of the screen
        addEventListener('keydown', onKey); addEventListener('keyup', onKey);
        addEventListener('blur', releaseAll); document.addEventListener('visibilitychange', onVisibility);
        container.addEventListener('pointerdown', onDown); container.addEventListener('pointermove', onMoveP);
        container.addEventListener('pointerup', onUp); container.addEventListener('pointercancel', onUp);
        container.addEventListener('wheel', onWheel, { passive: false });
        return new Promise(resolve => {
          map.on('style.load', () => {
            addLayers(); recolour();
            const t = ctx.currentTime; out.gain.cancelScheduledValues(t); out.gain.setValueAtTime(0, t); out.gain.linearRampToValueAtTime(opts.volume ?? .85, t + 2);
            raf = requestAnimationFrame(frame);
            audioTick(); tickTimer = setInterval(audioTick, 500); levelTimer = setInterval(levelTick, 120);
            lightTimer = setInterval(() => { if (light() !== shownLight) recolour(); }, 60000);   // dusk falls on you
            resolve();
          });
          map.on('error', e => onStatus({ type: 'error', message: e?.error?.message || 'map error' }));
        });
      },
      exit(fade = 1.5) {
        if (!alive) return;
        alive = false;
        cancelAnimationFrame(raf); clearInterval(tickTimer); clearInterval(levelTimer); clearInterval(lightTimer);
        removeEventListener('keydown', onKey); removeEventListener('keyup', onKey);
        removeEventListener('blur', releaseAll); document.removeEventListener('visibilitychange', onVisibility);
        container.removeEventListener('pointerdown', onDown); container.removeEventListener('pointermove', onMoveP);
        container.removeEventListener('pointerup', onUp); container.removeEventListener('pointercancel', onUp); container.removeEventListener('wheel', onWheel);
        keys.clear(); tapped.clear();
        for (const n of [...voices.keys()]) stopVoice(n, fade);
        const t = ctx.currentTime; out.gain.cancelScheduledValues(t); out.gain.setValueAtTime(out.gain.value, t); out.gain.linearRampToValueAtTime(0, t + fade);
        if (map) { map.remove(); map = null; }
      },
      setTheme(t) { theme = t === 'white' ? 'white' : 'black'; recolour(); },
      setPalette(p) { palette = p === 'mono' ? 'mono' : 'colour'; recolour(); },
      setView(v) { view = v === 'above' ? 'above' : 'street'; applyView(); },
      get view() { return view; }, get palette() { return palette; }, light,
      setVolume(v) { out.gain.setTargetAtTime(v, ctx.currentTime, .05); },
      nextSound() { const n = nearestUnfound(); if (n) glideTo(n.s.x + 4, n.s.z + 10); return n; },
      nearestUnfound,
    };
  }

  const api = { create, toXZ, fromXZ, ORIGIN };
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  else root.StreetMode = api;
})(typeof window === 'undefined' ? globalThis : window);
