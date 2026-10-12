const test = require('node:test');
const assert = require('node:assert/strict');
const { create } = require('../client/archive-locations');
const locations = create({ Venice: { center: [12.3155, 45.4408] }, Damascus: { center: [36.2765, 33.5138], radius: 60 }, Beirut: { center: [35.5018, 33.8938], radius: 60 } });
const rec = (id, latitude, longitude) => ({ id, latitude, longitude });
test('Saigon, the Delta and Upper Mekong are distinct collections', () => {
  assert.equal(locations.locate(rec('sg', 10.7765, 106.6953)), 'Saigon');
  assert.equal(locations.locate(rec('bt', 10.216051, 106.374212)), 'Mekong Delta');
  assert.equal(locations.locate(rec('mk', 20.268937, 100.406612)), 'Upper Mekong');
});
test('groups conserve every item once and give home collections priority', () => {
  const recordings = [rec('v',45.435393,12.358198),rec('s',10.78,106.70),rec('d',10.3,105.76),rec('u',-33,151)];
  const groups = locations.group(recordings);
  assert.deepEqual(groups.map(g => g.key), ['Saigon','Mekong Delta','Venice','Other locations']);
  assert.deepEqual(groups.flatMap(g => g.recordings.map(r => r.id)).sort(), recordings.map(r => r.id).sort());
});
test('overlapping areas use the nearest centre, not configuration order', () => {
  assert.equal(locations.locate(rec('d',33.511098,36.303699)), 'Damascus');
  assert.equal(locations.locate(rec('b',33.89,35.5)), 'Beirut');
});
test('unknown and malformed coordinates remain discoverable without false city labels', () => {
  for (const r of [rec('x',null,0),rec('y',91,0),rec('z',0,181),rec('s','10.78',106.7),rec('o',-33,151)]) {
    assert.equal(locations.locate(r), 'Other locations');
  }
  assert.deepEqual(locations.group([]), []);
});
test('location keys stay independent of translated display labels', () => {
  const [group] = locations.group([rec('nora',38.990132,9.013511)]);
  assert.equal(group.key, 'Cagliari'); assert.equal(group.label, 'Southern Sardinia');
});
