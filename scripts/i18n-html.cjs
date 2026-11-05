#!/usr/bin/env node
/* The page's markup carries its words in the default language (Vietnamese),
   so the first paint needs no script; so does the install manifest. They
   come from client/i18n.js: run this after changing a string there
   (`npm run i18n`), or with --check to list what is out of step
   (tests/i18n.test.cjs does). */
const fs = require('fs');
const path = require('path');
const I18N = require('../client/i18n');
const markup = require('../server/utils/i18n-markup');

const PAGE = path.join(__dirname, '../client/index.html');
const MANIFEST = path.join(__dirname, '../client/manifest.webmanifest');
const t = I18N.make(I18N.DEFAULT);
const sync = html => markup.sync(html, t);
const syncManifest = text => markup.syncManifest(text, t, I18N.DEFAULT);

module.exports = { sync, syncManifest, PAGE, MANIFEST };

if (require.main === module) {
  const before = fs.readFileSync(PAGE, 'utf8');
  const { html, stale } = sync(before);
  const manifestBefore = fs.readFileSync(MANIFEST, 'utf8');
  const manifest = syncManifest(manifestBefore);
  stale.push(...manifest.stale);
  if (process.argv.includes('--check')) {
    if (stale.length) { console.log('Out of step with client/i18n.js:', stale.join(', ')); process.exit(1); }
    console.log('Markup matches client/i18n.js');
  } else {
    if (html !== before) fs.writeFileSync(PAGE, html);
    if (manifest.text !== manifestBefore) fs.writeFileSync(MANIFEST, manifest.text);
    console.log(stale.length ? `Updated: ${stale.join(', ')}` : 'Already in step');
  }
}
