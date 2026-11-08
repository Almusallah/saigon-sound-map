// The sound map and the archive on one page (8 Nov 2026): the markup and
// script hold together. Static checks on client/index.html and client/sw.js.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const page=fs.readFileSync(path.join(__dirname,'../client/index.html'),'utf8');
const sw=fs.readFileSync(path.join(__dirname,'../client/sw.js'),'utf8');
const script=page.slice(page.lastIndexOf('<script>'));
const markup=page.slice(0,page.indexOf('<script src='));
const between=(s,a,b)=>{const i=s.indexOf(a);assert.ok(i>=0,a);const j=s.indexOf(b,i);assert.ok(j>i,b);return s.slice(i,j);};
const tag=id=>{const m=new RegExp(`<\\w+[^>]*\\sid="${id}"[^>]*>`).exec(page);assert.ok(m,id);return m[0];};

test('the page opens on the sound map, marked on body, never with data-view',()=>{
 assert.match(page,/<body data-mode="sound" data-pane="list">/);
 assert.doesNotMatch(page,/<body[^>]*data-view/);
 // data-view only for the viewer: two templates and the capture-phase opener.
 assert.equal((page.match(/data-view="/g)||[]).length,2);
 assert.equal((page.match(/\[data-view\]/g)||[]).length,2); // the opener, and the CSS comment that warns of it
 assert.doesNotMatch(markup,/<[a-z][^>]*\sdata-view/);
});
test('the archive list lives in the map area, under its bar',()=>{
 const wrap=between(page,'<div class="map-wrap">','<!-- Upload form overlay -->');
 assert.match(wrap,/id="archive-panel"/);
 assert.match(wrap,/id="archive-bar"/);
 assert.match(between(wrap,'id="archive-bar"','id="archive-panel"'),/data-show="list"[\s\S]*data-show="map"/);
 assert.doesNotMatch(page,/archive-close|data-i18n="archive\.close"/);
});
test('the door holds both of its labels and no words of its own',()=>{
 const door=between(page,'id="archive-btn"','</button>');
 assert.match(door,/data-i18n="header\.archive"/);assert.match(door,/id="count-badge"/);assert.match(door,/data-i18n="door\.sound"/);
 assert.doesNotMatch(tag('archive-btn'),/data-i18n/);
});
test('the main button has one label per interface',()=>{
 const fab=between(page,'id="upload-btn"','</button>');
 assert.match(fab,/data-i18n="fab\.sound"/);assert.match(fab,/data-i18n="fab\.archive"/);
 assert.doesNotMatch(page,/data-i18n="fab"/);
});
test('one form, two faces',()=>{
 assert.match(tag('form-card'),/data-face="sound"/);
 for(const id of ['kind-formats','form-to-archive','author-field','kind-filter-box'])assert.match(page,new RegExp(`id="${id}"`),id);
 assert.match(tag('kind-pick'),/class="kind-pick face-archive"/);
});
test('the header is two rows on a phone: VI|EN beside the title, Satellite on the map',()=>{
 const header=between(page,'<header>','</header>');
 const actions=between(header,'<div class="header-actions">','<!-- Language');
 assert.doesNotMatch(actions,/lang-switch/);assert.match(header,/class="lang-switch head-lang"/);
 assert.doesNotMatch(header,/id="style-toggle"/);
 assert.match(script,/box\.appendChild\(\$\('style-toggle'\)\)/);
});
test('every element the script looks up exists',()=>{
 const ids=new Set([...markup.matchAll(/\sid="([^"]+)"/g)].map(m=>m[1]));
 const wanted=new Set([...script.matchAll(/\$\('([\w-]+)'\)/g)].map(m=>m[1]));
 const missing=[...wanted].filter(id=>!ids.has(id));
 assert.deepEqual(missing,[]);
 assert.ok(wanted.size>=70,String(wanted.size));
});
test('nothing is remembered behind the visitor\'s back',()=>{
 assert.doesNotMatch(page,/localStorage|sessionStorage/);
});
test('views.js loads before the page script, and works offline',()=>{
 assert.ok(page.indexOf('<script src="/views.js"></script>')>0);
 assert.ok(page.indexOf('<script src="/views.js"></script>')<page.lastIndexOf('<script>'));
 assert.match(sw,/HELPERS = \[[^\]]*'\/views\.js'/);
});
test('Back returns from the archive; nothing else grows the history',()=>{
 assert.match(script,/history\.pushState\(\{ mode, doc: PAGE_LOAD \}/);
 assert.doesNotMatch(script,/replaceState\(null/);
 assert.equal((script.match(/pushState\(/g)||[]).length,2); // the door, and the viewer
});
test('share links are unchanged and never carry the view or the accent',()=>{
 const line=page.split('\n').find(l=>l.includes('/?rec=${btn.dataset.rec}'));
 assert.ok(line);assert.doesNotMatch(line,/view|accent/);
});
test('the lime variants are read once, from an allowlist, and stored nowhere',()=>{
 assert.match(page,/<script>document\.documentElement\.setAttribute\('data-accent', \(\/\[\?&\]accent=\(\[abc\]\)/);
 for(const v of ['a','b','c'])assert.match(page,new RegExp(`\\[data-accent="${v}"\\]`),v);
 assert.match(script,/CLUSTER_PAINT\.stroke/);
 // Lime is never the ink or the paper.
 assert.match(page,/--primary: #121212;/);assert.match(page,/--header-bg: #f3f3f1;/);
});
// Fixes from the review of 8 Nov 2026.
test('on the archive map the FAB stays under popups; only the list sheet lifts it',()=>{
 assert.match(page,/body\[data-mode="archive"\]\[data-pane="list"\] \.fab \{ z-index: 13; \}/);
 assert.doesNotMatch(page,/body\[data-mode="archive"\] \.fab \{ z-index/);
 assert.match(page,/\.mapboxgl-popup[^{]*\{[^}]*z-index: 11/);
});
test('the worker keeps its cache name, so an update never deletes the cached page',()=>{
 assert.match(sw,/const SHELL = 'shell-v2';/);
});
test('a file input keeps its own drop; a dropped sound reaches the recording slot',()=>{
 const h=between(script,"$('form-overlay').addEventListener('drop'",'});');
 assert.match(h,/onFileInput\(e\)/);assert.match(h,/\$\('audio-file'\)\.files = /);
 assert.match(between(script,"$('form-overlay').addEventListener('dragover'",'});'),/!onFileInput\(e\)/);
});
test('the viewer trusts its own flag, and an in-place switch drops it from the state',()=>{
 assert.match(script,/let viewerPushed = false;/);
 assert.doesNotMatch(script,/history\.state\?\.viewer/);
 assert.match(script,/history\.pushState\(\{ viewer: PAGE_LOAD \}/);assert.match(script,/s\.doc === PAGE_LOAD\) history\.back\(\)/);
 assert.match(between(script,'function setMode(','function leaveArchive'),/const \{ viewer, mode: _m, doc: _d, \.\.\.rest \} = history\.state/);
});
test('before the first payload the archive keeps loading and offers no kinds',()=>{
 assert.match(tag('kind-filter-box'),/\shidden/);
 assert.match(between(script,'function updateArchive()','archiveBuiltFrom = lastPayload'),/if \(!lastPayload\) return;/);
});
test('every reason a file was left out is shown, in one toast',()=>{
 const f=between(script,'function addPhotos(','function clearPhotos');
 assert.equal((f.match(/toast\(/g)||[]).length,1);
});
test('the welcome card names the buttons of the interface it opens over',()=>{
 const I=require('../client/i18n.js');
 for(const lang of ['en','vi'])for(const k of ['welcome.p2.archive','welcome.p3.archive'])assert.ok(I.STRINGS[lang][k],`${lang} ${k}`);
 assert.match(page,/<p class="in-sound" data-i18n="welcome\.p2">/);assert.match(page,/<p class="in-archive" data-i18n="welcome\.p2\.archive">/);
 // Gated on the card, set once when it opens: a later switch never rewrites it.
 assert.match(page,/\.welcome-overlay\[data-mode="sound"\] \.in-archive/);assert.match(page,/\.welcome-overlay\[data-mode="archive"\] \.in-sound/);
 assert.match(between(script,'function showWelcome()','overlay.classList.add'),/overlay\.dataset\.mode = mode;/);
});
test('the audio chooser is a pill, and on iOS accepts no type WebKit counts as video',()=>{
 assert.match(tag('audio-file'),/\shidden/);
 assert.match(tag('audio-file-btn'),/class="loc-btn"/);
 const line=script.split('\n').find(l=>l.includes("if (IS_IOS) $('audio-file').accept"));
 assert.ok(line);const acc=/accept = '([^']+)'/.exec(line)[1].split(',');
 for(const bad of ['audio/*','.mp4','.webm','audio/webm','audio/3gpp'])assert.ok(!acc.includes(bad),bad);
 assert.ok(!acc.some(a=>/^video\/|^image\//.test(a)));
 for(const ok of ['.mp3','.m4a','.wav'])assert.ok(acc.includes(ok),ok);
});
test('every inline script of the page parses',()=>{
 const vm=require('node:vm');
 const blocks=[...page.matchAll(/<script>([\s\S]*?)<\/script>/g)].map(m=>m[1]);
 assert.ok(blocks.length>=2);
 blocks.forEach((b,i)=>{ assert.doesNotThrow(()=>new vm.Script(b),`inline script ${i}`); });
});
test('round 2: history entries are tagged per page load; Back closes overlays; popups one at a time',()=>{
 assert.match(script,/history\.pushState\(\{ mode, doc: PAGE_LOAD \}/);
 const pop=between(script,"window.addEventListener('popstate'",'stampUrl();\n  });');
 assert.match(pop,/closeForm\(\)/);assert.match(pop,/s\.viewer === PAGE_LOAD\) \{ selfPop = true; history\.back\(\); return; \}/);
 const fly=between(script,'function flyToEntry(','function ensurePopupVisible');
 assert.match(fly,/p\.remove\(\)/);assert.match(fly,/tok !== flyTok/);
 const add=between(script,'function addPhotos(','function clearPhotos');
 assert.doesNotMatch(add,/else if \(skipped\.length\)/);
 assert.match(page,/\[data-accent="b"\] \.archive-switch button\[aria-pressed="true"\]:focus-visible \{ outline-color: #121212; \}/);
});
test('round 3: the page\'s own Back is not a visitor\'s; a pending pin is closed into the draft',()=>{
 const pop=between(script,"window.addEventListener('popstate'",'stampUrl();\n  });');
 assert.match(pop,/if \(selfPop\) \{ selfPop = false; stampUrl\(\); return; \}/);
 assert.match(pop,/if \(pinning \|\| \$\('form-overlay'\)\.classList\.contains\('open'\)\) closeForm\(\);/);
 assert.match(between(script,'function closeViewer(',"window.addEventListener('popstate'"),/selfPop = true; history\.back\(\);/);
 assert.match(tag('audio-file-btn'),/aria-describedby="audio-file-label audio-file-name"/);
 assert.doesNotMatch(page,/<label for="audio-file-btn"/);
});
