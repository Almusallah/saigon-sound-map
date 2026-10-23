const test = require('node:test'), assert = require('node:assert/strict'), path = require('node:path');
const S = require(path.join(__dirname, '../client/street-mode.js'));

test('street coordinates round-trip and measure real metres', () => {
  const p = { lng: 106.7165, lat: 10.8186 };              // Thanh Da
  const xz = S.toXZ(p.lng, p.lat), back = S.fromXZ(xz.x, xz.z);
  assert.ok(Math.abs(back.lng - p.lng) < 1e-9 && Math.abs(back.lat - p.lat) < 1e-9);
  const a = S.toXZ(106.7000, 10.7700), b = S.toXZ(106.7000, 10.7790);   // 0.009 deg of latitude ~ 995 m
  assert.ok(Math.abs(Math.hypot(b.x - a.x, b.z - a.z) - 995) < 5);
  assert.ok(b.z < a.z, 'north is -z, as the audio listener expects');
});
