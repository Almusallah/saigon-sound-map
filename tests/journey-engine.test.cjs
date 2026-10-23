const test = require('node:test'), assert = require('node:assert/strict'), path = require('node:path');
const J = require(path.join(__dirname, '../client/journey-engine.js'));
const features = require(path.join(__dirname, '../client/listen-features.json'));
const D = 660;

test('the same seed composes the same journey; another seed composes another', () => {
  const a = JSON.stringify(J.compose(features, { seed: 42, duration: D }));
  assert.equal(a, JSON.stringify(J.compose(features, { seed: 42, duration: D })));
  assert.notEqual(a, JSON.stringify(J.compose(features, { seed: 43, duration: D })));
});

test('the arc runs dawn to dissolve across the whole duration, and every event fits inside it', () => {
  const s = J.compose(features, { seed: 7, duration: D });
  assert.deepEqual(s.sections.map(x => x.key), ['dawn', 'gathering', 'pulse', 'drift', 'night', 'dissolve']);
  assert.equal(s.sections[0].start, 0); assert.equal(s.sections.at(-1).end, D);
  for (const e of s.events) { assert.ok(e.t >= 0 && e.t + e.dur <= D + 1e-6, `${e.type} at ${e.t}`); }
});

test('Music-category recordings are never resampled', () => {
  for (const seed of [1, 2, 3, 4, 5]) {
    const s = J.compose(features, { seed, duration: D });
    assert.ok(s.sources.every(x => x.category !== 'Music'));
  }
});

test('the dawn detail returns unchanged at night', () => {
  const s = J.compose(features, { seed: 9, duration: D });
  const mem = s.events.filter(e => e.memory);
  assert.equal(mem.length, 2);
  assert.equal(mem[0].id, mem[1].id); assert.equal(mem[0].offset, mem[1].offset); assert.equal(mem[0].dur, mem[1].dur);
  assert.ok(mem[0].t < .15 * D && mem[1].t > .8 * D);
});

test('percussion is occasional: nothing in dawn, the pulse in the middle', () => {
  for (const seed of [11, 12, 13]) {
    const s = J.compose(features, { seed, duration: D });
    const perc = s.events.find(e => e.type === 'perc');
    if (!perc) continue;
    assert.ok(perc.hits.every(h => h.t > .3 * D), 'no hits before the pulse');
    const inPulse = perc.hits.filter(h => h.t < .65 * D).length;
    assert.ok(inPulse / perc.hits.length > .7, 'most hits sit in the pulse');
  }
});

test('the night breathes: its synth swells leave silences between them (no drone)', () => {
  const s = J.compose(features, { seed: 21, duration: D });
  const night = s.events.filter(e => e.type === 'pad' && e.t >= .78 * D - 1).sort((a, b) => a.t - b.t);
  assert.ok(night.length >= 2);
  for (let i = 1; i < night.length; i++) assert.ok(night[i - 1].t + night[i - 1].dur < night[i].t, 'gap between swells');
});

test('the map becomes the room: Saigon inside, faraway places near the walls, all within bounds', () => {
  const B = J.BOUNDS;
  const saigon = J.place({ id: 'a', lat: 10.79, lng: 106.70 }), venice = J.place({ id: 'b', lat: 45.44, lng: 12.31 });
  const delta = J.place({ id: 'c', lat: 10.03, lng: 105.78 });
  assert.ok(Math.hypot(saigon.x, saigon.z) < B * .5);
  assert.ok(Math.max(Math.abs(venice.x), Math.abs(venice.z)) > B * .75, 'Venice at the edge');
  assert.ok(delta.x < 0 && delta.z > 0, 'the Mekong Delta lies southwest');
  for (const p of [saigon, venice, delta]) assert.ok(Math.abs(p.x) <= B && Math.abs(p.z) <= B);
});

test('a corpus too thin to compose fails clearly', () => {
  assert.throws(() => J.compose(features.slice(0, 2), { seed: 1 }), /Not enough/);
});
