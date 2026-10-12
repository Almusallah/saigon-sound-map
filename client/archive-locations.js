/* Browsing areas, not administrative boundaries. Coordinates remain canonical. */
(function (root) {
  'use strict';
  const EXTRA = {
    'Saigon': { label: 'Saigon', center: [106.6953, 10.7765], radius: 40 },
    'Mekong Delta': { label: 'Miền Tây · Mekong Delta', center: [105.85, 10.15], radius: 85 },
    'Sapa': { center: [103.84, 22.30], radius: 35 },
    'Seoul': { center: [126.98, 37.56], radius: 50 },
    'Chiang Rai': { center: [99.83, 19.91], radius: 35 },
    'Upper Mekong': { center: [100.41, 20.28], radius: 35 },
    'Lombardy': { center: [10.05, 45.81], radius: 35 },
    'Cagliari': { label: 'Southern Sardinia', center: [9.12, 39.22], radius: 60 },
  };
  function distance(a, b) {
    const rad = Math.PI / 180;
    const h = Math.sin((a[1] - b[1]) * rad / 2) ** 2 +
      Math.cos(a[1] * rad) * Math.cos(b[1] * rad) * Math.sin((a[0] - b[0]) * rad / 2) ** 2;
    return 12742 * Math.asin(Math.sqrt(Math.min(1, h)));
  }
  function create(cities) {
    const areas = Object.entries({ ...cities, ...EXTRA }).map(([key, area]) => ({
      key, label: area.label || key, center: area.center, radius: area.radius || 50,
    }));
    function locate(recording) {
      const { latitude: lat, longitude: lng } = recording;
      if (!Number.isFinite(lat) || !Number.isFinite(lng) || Math.abs(lat) > 90 || Math.abs(lng) > 180) return 'Other locations';
      let key = 'Other locations', nearest = Infinity;
      for (const area of areas) {
        const d = distance([lng, lat], area.center);
        if (d <= area.radius && d < nearest) { key = area.key; nearest = d; }
      }
      return key;
    }
    function label(key) { return areas.find(a => a.key === key)?.label || key; }
    function group(recordings) {
      const buckets = new Map();
      for (const recording of recordings) {
        const key = locate(recording);
        if (!buckets.has(key)) buckets.set(key, { key, label: label(key), recordings: [] });
        buckets.get(key).recordings.push(recording);
      }
      const rank = key => key === 'Saigon' ? 0 : key === 'Mekong Delta' ? 1 : key === 'Other locations' ? 3 : 2;
      return [...buckets.values()].sort((a, b) => rank(a.key) - rank(b.key) || a.label.localeCompare(b.label, 'en'));
    }
    return { locate, label, group };
  }
  if (typeof module !== 'undefined' && module.exports) module.exports = { create };
  else root.ArchiveLocations = { create };
})(typeof window === 'undefined' ? globalThis : window);
