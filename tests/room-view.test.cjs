/* room-view.test.cjs — dependency-free smoke + geometry tests for room-view.js.
 * Run: node room-view.test.cjs
 */
'use strict';
const assert = require('assert');
const path = require('path');

// ---- minimal browser-ish globals (no real rAF: frames are driven by hand) ----
const winListeners = {};
globalThis.requestAnimationFrame = () => 1;
globalThis.cancelAnimationFrame = () => {};
globalThis.addEventListener = (t, fn) => { (winListeners[t] = winListeners[t] || []).push(fn); };
globalThis.removeEventListener = (t, fn) => { winListeners[t] = (winListeners[t] || []).filter((f) => f !== fn); };
function fire(type, ev) { (winListeners[type] || []).forEach((fn) => fn(ev)); }

const { create } = require(path.join(__dirname, '../client/room-view.js'));

// ---- canvas + recording 2D context stub ----
function makeCanvas(w = 800, h = 600) {
  const calls = [];
  const target = {
    calls,
    measureText: (s) => ({ width: 6 * String(s).length }),
    createLinearGradient: () => ({ addColorStop() {} }),
  };
  const ctx = new Proxy(target, {
    get(t, k) {
      if (k in t) return t[k];
      return (...args) => { calls.push([k, args]); };
    },
    set(t, k, v) { t[k] = v; return true; },
  });
  const attrs = {};
  const listeners = {};
  const canvas = {
    clientWidth: w, clientHeight: h, width: 0, height: 0, style: {},
    getContext: () => ctx,
    setAttribute: (k, v) => { attrs[k] = String(v); },
    getAttribute: (k) => attrs[k],
    hasAttribute: (k) => k in attrs,
    addEventListener: (t, fn) => { (listeners[t] = listeners[t] || []).push(fn); },
    removeEventListener: (t, fn) => { listeners[t] = (listeners[t] || []).filter((f) => f !== fn); },
    getBoundingClientRect: () => ({ left: 0, top: 0, width: w, height: h }),
    focus() {}, setPointerCapture() {}, releasePointerCapture() {},
    attrs, listeners, ctx,
  };
  return canvas;
}
function frames(view, seconds, dt = 1 / 60) { for (let t = 0; t < seconds - 1e-9; t += dt) view._debug.frame(dt); }

let passed = 0;
function test(name, fn) {
  try { fn(); passed++; console.log('  ok  ' + name); }
  catch (e) { console.error('  FAIL ' + name + '\n       ' + (e && e.stack || e)); process.exitCode = 1; }
}

console.log('room-view.js');

test('create / API surface / accessibility attributes', () => {
  const c = makeCanvas();
  const v = create(c, { theme: 'black', bounds: 40, listener: { x: 0, z: 18, yaw: 0 }, onMove() {} });
  for (const m of ['setSources', 'setLevels', 'setTheme', 'setListener', 'setCaption', 'getListener', 'destroy']) {
    assert.strictEqual(typeof v[m], 'function', m);
  }
  assert.strictEqual(c.attrs.tabindex, '0');
  assert.strictEqual(c.attrs.role, 'application');
  assert.ok(c.attrs['aria-label'] && c.attrs['aria-label'].length > 20);
  assert.strictEqual(c.style.touchAction, 'none');
  assert.deepStrictEqual(v.getListener(), { x: 0, z: 18, yaw: 0 });
  v.destroy();
});

test('setSources / setLevels / setTheme / setCaption / setListener / destroy do not throw', () => {
  const c = makeCanvas();
  const v = create(c, {});
  v.setSources([
    { id: 'a', x: 0, z: 0, label: 'Chợ nổi Cái Răng', radius: 10 },
    { id: 2, x: -12, z: 5, label: 'ferry horn', radius: 8 },
    { id: 'c', x: 30, z: -30, label: '', radius: 0 },
    null, { x: 1 },
  ]);
  v.setLevels({ a: 0.8, 2: 0.3, c: 2, nope: 1 });
  v.setTheme('white'); v.setTheme('black'); v.setTheme('nonsense');
  v.setCaption('dawn · 01 / 06'); v.setCaption(null);
  v.setListener({ x: 3, z: 4, yaw: 1 }); v.setListener({ x: 5 }, { glide: true }); v.setListener(null);
  frames(v, 2);
  assert.ok(c.width === 800 && c.height === 600, 'backing store sized');
  v.destroy();
  assert.doesNotThrow(() => { v.setSources([]); v.setLevels({}); v.setTheme('white'); v.setListener({ x: 1 }); v.setCaption('x'); v.getListener(); v.destroy(); });
  assert.strictEqual((c.listeners.pointerdown || []).length, 0, 'canvas listeners removed');
  assert.strictEqual((winListeners.keydown || []).length, 0, 'window listeners removed');
});

test('clamping keeps the listener 1 m inside the bounds', () => {
  const v = create(makeCanvas(), { bounds: 40, listener: { x: 500, z: -500, yaw: 0 } });
  let l = v.getListener();
  assert.strictEqual(l.x, 39); assert.strictEqual(l.z, -39);
  v.setListener({ x: -1e6, z: 1e6, yaw: 0 });
  l = v.getListener();
  assert.strictEqual(l.x, -39); assert.strictEqual(l.z, 39);
  // walk into the north wall for 4 s
  v.setListener({ x: 0, z: -35, yaw: 0 });
  v._debug.key('f', true); v._debug.key('shift', true);
  frames(v, 4);
  v._debug.key('f', false);
  l = v.getListener();
  assert.ok(l.z >= -39 && l.z <= -38.99, 'stopped at the wall, z=' + l.z);
  // glide target outside is clamped too
  v.setListener({ x: 100, z: 0, yaw: 0 }, { glide: true });
  frames(v, 1.5);
  assert.strictEqual(v.getListener().x, 39);
  v.destroy();
});

test('projection: a point straight ahead lands at screen centre (any yaw)', () => {
  const v = create(makeCanvas(800, 600), { listener: { x: 0, z: 18, yaw: 0 } });
  const W = 800;
  let p = v._debug.project(0, 1.6, 8);           // 10 m north of the listener, at eye height
  assert.ok(p, 'projected');
  assert.ok(Math.abs(p.x - W / 2) < 1e-6, 'x=' + p.x);
  assert.ok(Math.abs(p.y - v._debug.size().horizon) < 1e-6, 'on the horizon, y=' + p.y);
  assert.ok(Math.abs(p.depth - 10) < 1e-9);
  // facing east: a point 10 m east is ahead
  v.setListener({ x: 0, z: 0, yaw: Math.PI / 2 });
  p = v._debug.project(10, 0, 0);
  assert.ok(Math.abs(p.x - W / 2) < 1e-6, 'east x=' + p.x);
  assert.ok(p.y > v._debug.size().horizon, 'floor point is below the horizon');
  // and a point to the south is on the right
  p = v._debug.project(5, 0, 5);
  assert.ok(p.x > W / 2, 'south is to the right when facing east');
  // yaw = 0, point to the east is on the right
  v.setListener({ x: 0, z: 0, yaw: 0 });
  p = v._debug.project(5, 0, -10);
  assert.ok(p.x > W / 2, 'east is to the right when facing north');
  v.destroy();
});

test('a point / source behind the camera is not drawn', () => {
  const c = makeCanvas();
  const v = create(c, { listener: { x: 0, z: 0, yaw: 0 } });
  assert.strictEqual(v._debug.project(0, 1.6, 10), null, 'behind -> null');
  assert.strictEqual(v._debug.project(3, 0, 0.05), null, 'beside/behind near plane -> null');
  v.setSources([{ id: 'behind', x: 0, z: 10, label: 'BEHINDXQ', radius: 0 }]);
  v.setLevels({ behind: 1 });
  frames(v, 2.5);
  assert.strictEqual(v._debug.stats.sources, 0, 'no pillar drawn');
  const texts = c.ctx.calls.filter((k) => k[0] === 'fillText').map((k) => k[1][0]).join('');
  assert.ok(!texts.includes('Q'), 'label of the source behind was not drawn');
  // turn around: now it is drawn and its label appears
  c.ctx.calls.length = 0;
  v.setListener({ x: 0, z: 0, yaw: Math.PI });
  v.setLevels({ behind: 1 });
  frames(v, 0.2);
  assert.strictEqual(v._debug.stats.sources, 1, 'pillar drawn when in front');
  const t2 = c.ctx.calls.filter((k) => k[0] === 'fillText').map((k) => k[1][0]).join('');
  assert.ok(t2.includes('BEHINDXQ'), 'label drawn, got ' + t2);
  v.destroy();
});

test('near-plane clipping: no exploding or non-finite coordinates', () => {
  const c = makeCanvas(390, 844);
  const v = create(c, { listener: { x: 0.001, z: 0.001, yaw: 0.3 } });     // standing on a grid crossing
  v.setSources([{ id: 'here', x: 0.2, z: 0.1, label: 'HERE', radius: 6 }, { id: 'b', x: 1, z: -1, label: 'B', radius: 2 }]);
  v.setLevels({ here: 1, b: 1 });
  let bad = 0, n = 0;
  for (let i = 0; i < 360; i++) {
    c.ctx.calls.length = 0;
    v.setListener({ x: (i % 7) * 0.31 - 1, z: (i % 5) * 0.27 - 0.5, yaw: i * 0.0175 * 2.1 });
    v._debug.frame(1 / 60);
    for (const [k, a] of c.ctx.calls) {
      if (k !== 'moveTo' && k !== 'lineTo') continue;
      n++;
      if (!Number.isFinite(a[0]) || !Number.isFinite(a[1]) || Math.abs(a[0]) > 5000 || Math.abs(a[1]) > 5000) bad++;
    }
  }
  assert.ok(n > 1000, 'segments were drawn (' + n + ')');
  assert.strictEqual(bad, 0, bad + ' out-of-range vertices');
  v.destroy();
});

test('keyboard: ArrowUp walks forward at ~4 m/s; ignored while typing in an input', () => {
  const v = create(makeCanvas(), { listener: { x: 0, z: 18, yaw: 0 } });
  const ev = (code, extra = {}) => Object.assign({ code, key: code, preventDefault() {}, target: { tagName: 'CANVAS' } }, extra);
  fire('keydown', ev('ArrowUp'));
  frames(v, 3);
  fire('keyup', ev('ArrowUp'));
  frames(v, 2);
  let l = v.getListener();
  const walked = 18 - l.z;
  assert.ok(walked > 9 && walked < 14, 'walked ' + walked.toFixed(2) + ' m in ~3 s');
  assert.ok(Math.abs(l.x) < 1e-9);
  // turn right with D: yaw increases (clockwise from above)
  fire('keydown', ev('KeyD')); frames(v, 0.5); fire('keyup', ev('KeyD')); frames(v, 1);
  assert.ok(v.getListener().yaw > 0.3, 'turned right, yaw=' + v.getListener().yaw);
  // typing in an input is not hijacked
  const before = v.getListener();
  fire('keydown', ev('KeyW', { target: { tagName: 'INPUT' } }));
  frames(v, 1);
  fire('keyup', ev('KeyW'));
  const after = v.getListener();
  assert.strictEqual(after.z, before.z); assert.strictEqual(after.x, before.x);
  v.destroy();
});

test('tap on the floor glides there; tap above the horizon is ignored', () => {
  const v = create(makeCanvas(800, 600), { listener: { x: 0, z: 18, yaw: 0 } });
  const s = v._debug.size();
  v._debug.tap(400, s.horizon - 50);
  frames(v, 4);
  assert.deepStrictEqual(v.getListener(), { x: 0, z: 18, yaw: 0 });
  // floor point straight ahead: depth = 1.6 * focal / (py - horizon)
  const py = s.horizon + 60, depth = 1.6 * s.focal / 60;
  v._debug.tap(400, py);
  frames(v, 4);
  const l = v.getListener();
  assert.ok(Math.abs(l.x) < 1e-6 && Math.abs(l.z - (18 - depth)) < 1e-6, 'arrived at z=' + l.z + ' expected ' + (18 - depth));
  v.destroy();
});

test('glide takes ~1.2 s; onMove is throttled to <= 30/s and reports the final position', () => {
  const got = [];
  const v = create(makeCanvas(), { listener: { x: 0, z: 0, yaw: 0 }, onMove: (l) => got.push(l) });
  frames(v, 0.1); got.length = 0;
  v.setListener({ x: 10, z: -10, yaw: 1 }, { glide: true });
  frames(v, 0.6);
  const mid = v.getListener();
  assert.ok(mid.x > 2 && mid.x < 8, 'mid-glide x=' + mid.x);
  frames(v, 0.7);
  const end = v.getListener();
  assert.ok(Math.abs(end.x - 10) < 1e-9 && Math.abs(end.z + 10) < 1e-9 && Math.abs(end.yaw - 1) < 1e-9);
  frames(v, 0.2);
  assert.ok(got.length >= 20 && got.length <= 41, got.length + ' onMove calls over 1.3 s');
  const last = got[got.length - 1];
  assert.ok(Math.abs(last.x - 10) < 1e-9 && Math.abs(last.z + 10) < 1e-9, 'final position emitted');
  v.destroy();
});

test('sources fade out (~1.5 s) when dropped and fade in when added', () => {
  const v = create(makeCanvas(), { listener: { x: 0, z: 18, yaw: 0 } });
  v.setSources([{ id: 'a', x: 0, z: 0, label: 'A', radius: 5 }, { id: 'b', x: 5, z: 0, label: 'B', radius: 5 }]);
  frames(v, 2.1);
  v.setSources([{ id: 'b', x: 5, z: 0, label: 'B', radius: 5 }]);
  frames(v, 1.0);
  assert.strictEqual(v._debug.sourceCount(), 2, 'still fading');
  frames(v, 0.7);
  assert.strictEqual(v._debug.sourceCount(), 1, 'removed after fade');
  v.destroy();
});

test('prefers-reduced-motion: glides become instant', () => {
  const prev = globalThis.matchMedia;
  globalThis.matchMedia = (q) => ({ matches: /reduce/.test(q), addEventListener() {}, removeEventListener() {} });
  try {
    const got = [];
    const v = create(makeCanvas(), { listener: { x: 0, z: 0, yaw: 0 }, onMove: (l) => got.push(l) });
    v.setListener({ x: 12, z: -7, yaw: 2 }, { glide: true });
    assert.deepStrictEqual(v.getListener(), { x: 12, z: -7, yaw: 2 }, 'arrived immediately');
    assert.ok(got.length >= 1 && got[got.length - 1].x === 12, 'onMove reported the jump');
    v._debug.tap(400, v._debug.size().horizon + 60);
    const l = v.getListener();
    assert.ok(l.x !== 12 || l.z !== -7, 'tap-to-glide is instant too');
    v.destroy();
  } finally { globalThis.matchMedia = prev; }
});

test('30 sources: frame cost stays small (Node, stub context)', () => {
  const v = create(makeCanvas(1280, 800), { listener: { x: 0, z: 30, yaw: 0 } });
  const list = [];
  for (let i = 0; i < 30; i++) list.push({ id: 'n' + i, x: ((i * 37) % 70) - 35, z: ((i * 53) % 70) - 35, label: 'Source number ' + i, radius: 6 + (i % 8) });
  v.setSources(list);
  const lv = {}; list.forEach((s, i) => { lv[s.id] = (i % 10) / 10; });
  v.setLevels(lv);
  frames(v, 1);
  // A regression guard, not a benchmark: the stub context is far slower than
  // a real canvas (Chrome measured ~0.45 ms/frame), and machine load swings a
  // single timing by +-20%. The median of five batches against a loose
  // ceiling still catches what matters, e.g. a quadratic blow-up.
  const batches = [];
  for (let b = 0; b < 5; b++) {
    const t0 = process.hrtime.bigint();
    for (let i = 0; i < 60; i++) { v.setLevels(lv); v._debug.frame(1 / 60); }
    batches.push(Number(process.hrtime.bigint() - t0) / 1e6 / 60);
  }
  const ms = batches.sort((x, y) => x - y)[2];
  console.log('       ~' + ms.toFixed(3) + ' ms/frame of JS work, ' + v._debug.stats.segments + ' segments, ' + v._debug.stats.sources + ' pillars');
  assert.ok(ms < 20, ms + ' ms/frame (median of 5 batches)');
  v.destroy();
});

console.log(process.exitCode ? '\nFAILED' : '\nall ' + passed + ' tests passed');
