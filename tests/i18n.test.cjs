const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const I18N=require('../client/i18n');
const {sync,syncManifest,MANIFEST}=require('../scripts/i18n-html.cjs');
const page=fs.readFileSync(path.join(__dirname,'../client/index.html'),'utf8');
const server=fs.readFileSync(path.join(__dirname,'../server/index.js'),'utf8');
const {vi,en}=I18N.STRINGS;
const slots=s=>[...String(s).matchAll(/\{(\w+)\}/g)].map(m=>m[1]).sort();
const SAMPLE={n:3,name:'VENICE',max:8,why:'<why>',place:'Saigon',title:'T',i:2,kb:12,size:'1.20 MB',pct:40,a:2,type:'2g',reason:'R',mb:'7.00',lat:'10.1',lng:'106.7',parts:'P',kind:'K',category:'C'};
const render=(lang,key,vars=SAMPLE)=>I18N.make(lang)(key,vars);

test('it opens in Vietnamese; ?lang=en, and only that, switches to English',()=>{
 assert.equal(I18N.DEFAULT,'vi');
 assert.equal(I18N.pick(''),'vi');assert.equal(I18N.pick('?lang=fr'),'vi');assert.equal(I18N.pick('?rec=abcd&lang=en'),'en');
 assert.match(page,/<html lang="vi">/);
});
test('both languages have exactly the same keys',()=>{
 assert.deepEqual(Object.keys(vi).filter(k=>!(k in en)),[],'Vietnamese keys with no English');
 assert.deepEqual(Object.keys(en).filter(k=>!(k in vi)),[],'English keys not yet in Vietnamese');
});
test('every slot survives translation and every value renders',()=>{
 for(const k of Object.keys(en)){
  if(typeof en[k]==='string'&&typeof vi[k]==='string')assert.deepEqual(slots(vi[k]),slots(en[k]),`slots of ${k}`);
  for(const lang of ['vi','en'])for(const n of [1,2]){
   const out=render(lang,k,{...SAMPLE,n});
   assert.equal(typeof out,'string',`${lang} ${k}`);
   assert.doesNotMatch(out,/undefined|\{\w+\}/,`${lang} ${k} rendered "${out}"`);
  }
 }
});
test('Vietnamese is NFC, carries its diacritics, and keeps the project name',()=>{
 for(const [k,v] of Object.entries(vi)){
  const s=typeof v==='function'?v(SAMPLE):v;
  assert.equal(s,s.normalize('NFC'),`${k} is not NFC`);
 }
 assert.match(render('vi','welcome.p1'),/[àáảãạăâđêôơưằắẳẵặầấẩẫậèéẻẽẹềếểễệìíỉĩịòóỏõọồốổỗộờớởỡợùúủũụừứửữựỳýỷỹỵ]/i);
 const NAME=/Saigon_Miền(?:[ \u00a0]|&nbsp;)Tây/;
 for(const lang of ['vi','en'])for(const k of ['about.title','about.p1','og.image','og.text','og.video','og.sound'])assert.match(render(lang,k),NAME,`${lang} ${k}`);
 for(const lang of ['vi','en'])for(const k of ['splash','welcome.title'])assert.match(render(lang,k),/Saigon_Miền&nbsp;Tây/,`${lang} ${k}`);
});
test('the name is Saigon_Miền Tây alone; neither earlier name is left (26 Nov 2026)',()=>{
 // "Sound map" (lower-case m) names the interface; "Sound Map" and "Ghé Lại" were names of the project.
 const SEP='(?:[ _\\u00a0-]|&nbsp;|&#160;|&#x0*a0;|\\\\u00a0|\\\\xa0)';
 const GHE=new RegExp(`(?<![\\p{L}\\p{M}])gh[eé]${SEP}?l[ạa]i(?![\\p{L}\\p{M}])`,'iu'),SM=new RegExp(`Sound${SEP}Map|SOUND${SEP}MAP`,'u');
 const OLD={test:t=>{t=t.normalize('NFC');return SM.test(t)||GHE.test(t);}};
 assert.ok(OLD.test('GHÉ&nbsp;LẠI')&&OLD.test('Ghé\\u00a0Lại')&&OLD.test('Saigon_Miền Tây Sound Map')&&OLD.test('Ghe\u0301 La\u0323i'.normalize('NFD')));
 assert.ok(!OLD.test('Sound map')&&!OLD.test('nghe lại')&&!OLD.test('ghép lại'));
 for(const lang of ['vi','en'])for(const [k,v] of Object.entries(I18N.STRINGS[lang])){
  const out=typeof v==='function'?v(SAMPLE):v;
  assert.ok(!OLD.test(out),`${lang} ${k}: "${out}"`);
  assert.doesNotMatch(out,/âm thanh \{name\}/i,`${lang} ${k}`);
 }
 assert.ok(!('welcome.gloss' in vi)&&!('welcome.gloss' in en),'no gloss: the name needs no explanation');
 assert.equal(en['door.sound'],'Sound map');assert.equal(vi['door.sound'],'Bản đồ âm thanh');
 for(const lang of ['vi','en']){assert.equal(render(lang,'city.title',{name:'BẾN TRE'}),'BẾN TRE');assert.equal(render(lang,'city.docTitle',{name:'Bến Tre'}),'Bến Tre — Saigon_Miền Tây');}
 assert.match(page,/<title>Saigon_Miền Tây<\/title>/);assert.match(page,/<h1 id="site-title">SAIGON_MIỀN&nbsp;TÂY<\/h1>/);
 const m=JSON.parse(fs.readFileSync(MANIFEST,'utf8'));assert.equal(m.name,'Saigon_Miền Tây');assert.equal(m.short_name,'Saigon_Miền Tây');
 const dir=path.join(__dirname,'../client');
 const walk=(d,re)=>fs.readdirSync(d,{withFileTypes:true}).flatMap(e=>e.isDirectory()?(e.name==='node_modules'?[]:walk(path.join(d,e.name),re)):re.test(e.name)?[path.join(d,e.name)]:[]);
 const shipped=[...walk(dir,/\.(html|js|css|webmanifest)$/),...walk(path.join(__dirname,'../server'),/\.js$/),path.join(__dirname,'../package.json')].map(f=>[path.relative(path.join(__dirname,'..'),f),fs.readFileSync(f,'utf8')]);
 for(const [f,txt] of shipped)assert.ok(!OLD.test(txt),f);
});
test('markup in a translation is the same markup',()=>{
 const tags=s=>[...s.matchAll(/<[^>]+>|&\w+;/g)].map(m=>m[0]);
 for(const k of ['splash','welcome.title','about.p1','about.contact'])assert.deepEqual(tags(vi[k]),tags(en[k]),k);
 for(const [k,v] of Object.entries(vi))if(!['splash','welcome.title','about.p1','about.contact'].includes(k)&&typeof v==='string')
  assert.doesNotMatch(v,/<[a-z/]/i,`${k} has markup but is set as text`);
});
test('every key the page and server use exists, including the built ones',()=>{
 const used=new Set();
 for(const m of page.matchAll(/data-i18n(?:-html)?="([^"]+)"/g))used.add(m[1]);
 for(const m of page.matchAll(/data-i18n-attr="([^"]+)"/g))m[1].split(';').forEach(p=>used.add(p.split(':')[1]));
 for(const src of [page,server])for(const m of src.matchAll(/\bt\('([\w.-]+)'/g))used.add(m[1]);
 for(const m of page.matchAll(/\bsay\([^,]+, '([\w.-]+)'/g))used.add(m[1]);
 const kinds=['sound','image','video','text'];
 kinds.forEach(k=>['kind.'+k,'kinds.'+k,`kind.${k}.lower`,'share.'+k,...['photo','title','desc','submit','pin','formats'].map(f=>`ks.${k}.${f}`)].forEach(x=>used.add(x)));
 ['image','video','text','sound'].forEach(k=>used.add('og.'+k));
 ['sound','video','images','text','description'].forEach(w=>used.add('left.'+w));
 ['network','timeout','server'].forEach(e=>used.add('err.'+e));
 ['header.satellite','header.map','mb.zoomIn','mb.zoomOut','mb.resetBearing','mb.findMe','mb.noLocation','photo.count','photo.countCover','up.preparingN','up.preparing1'].forEach(x=>used.add(x));
 const missing=[...used].filter(k=>!k.endsWith('.')&&!(k in en)); // 'cat.' + c and the like are listed above
 assert.deepEqual(missing,[]);
});
test('every category, city and archive area has a label',()=>{
 const block=(start)=>page.slice(page.indexOf(start),page.indexOf('};',page.indexOf(start)));
 const cats=[...block('const CATS = {').matchAll(/^\s*'([^']+)':/gm)].map(m=>m[1]);
 const cities=[...block('const CITIES = {').matchAll(/^\s*'([^']+)':/gm)].map(m=>m[1]);
 assert.equal(cats.length,18);assert.ok(cities.length>=20);
 for(const c of cats)assert.ok(('cat.'+c) in en,`cat.${c}`);
 for(const c of cities)assert.ok(('city.'+c) in en,`city.${c}`);
 const {create}=require('../client/archive-locations');
 const areas=create(Object.fromEntries(cities.map(c=>[c,{center:[0,0]}])));
 const extra=fs.readFileSync(path.join(__dirname,'../client/archive-locations.js'),'utf8');
 for(const m of extra.matchAll(/^\s*'([^']+)': \{/gm)){const k=m[1];assert.ok(('area.'+k) in en||('city.'+k) in en,`area.${k}`);}
 assert.ok('area.Other locations' in en);assert.equal(typeof areas.label,'function');
});
test('upload errors the server sends carry codes the page can say',()=>{
 const codes=new Set([...server.matchAll(/code: '([\w-]+)'/g)].map(m=>m[1]));
 for(const m of server.matchAll(/LIMIT_\w+: '([\w-]+)'/g))codes.add(m[1]);
 for(const f of ['b2.js','video.js'])for(const m of fs.readFileSync(path.join(__dirname,'../server/utils/'+f),'utf8').matchAll(/clientError\([`'][^`']*[`'], '([\w-]+)'\)/g))codes.add(m[1]);
 for(const m of server.matchAll(/\? '([\w-]+)'\s*\n?\s*: \{ LIMIT_/g))codes.add(m[1]); // video-too-large, by field
 for(const c of ['video-too-long','video-unreadable','video-too-large','video-resolution','video-sound-only'])assert.ok(codes.has(c),'scan finds '+c);
 codes.add('server');
 assert.ok(codes.size>=12,[...codes].join());
 for(const c of codes)assert.ok(('err.'+c) in en,`err.${c}`);
});
test('the markup already holds the Vietnamese words (no flash of another language)',()=>{
 const {stale}=sync(page);
 assert.deepEqual(stale,[],'run: node scripts/i18n-html.cjs');
 const manifest=fs.readFileSync(MANIFEST,'utf8');
 assert.deepEqual(syncManifest(manifest).stale,[],'run: node scripts/i18n-html.cjs');
 const m=JSON.parse(manifest);assert.equal(m.lang,'vi');assert.equal(m.description,vi['meta.description']);
});
test('share previews speak the link\'s language',()=>{
 assert.equal(I18N.make('en')('og.sound',{category:'Announcements & Signals'}),'An Announcements & Signals recording from the Saigon_Miền Tây archive.');
 const html='<html lang="vi"><meta name="description" content="VI"><meta property="og:title" content="X"><meta property="og:description" content="VI"><meta property="og:url" content="U"><!--og:image--><meta name="twitter:card" content="summary">';
 const rec={id:'abcd1234-0000',title:'',kind:'sound',category:'Vehicles',body:'',description:''};
 let handler;const ctx=vm.createContext({I18N,CLIENT_INDEX:html,cacheIsFresh:()=>true,cache:[rec],refreshCache:async()=>[rec],SITE_URL:'https://x',THUMB_EDGE:720,console,
  cleanText:(s)=>s,app:{get(p,fn){if(p==='*')handler=fn;}},RegExp,Math,i18nMarkup:require('../server/utils/i18n-markup')});
 const bind=(a,b)=>vm.runInContext(server.slice(server.indexOf(a),server.indexOf(b,server.indexOf(a))),ctx);
 bind('function escAttr','// App download page');bind("app.get('*'",'// Multer errors');
 const run=async query=>{let out;await handler({query},{set(){},type(){return {send:h=>{out=h;}};}});return out;};
 return (async()=>{
  const viHtml=await run({rec:'abcd1234'}),enHtml=await run({rec:'abcd1234',lang:'en'}),plain=await run({lang:'en'});
  assert.match(viHtml,/<html lang="vi">/);assert.ok(viHtml.includes(I18N.make('vi')('untitled')));
  assert.ok(viHtml.includes(I18N.make('vi')('og.sound',{category:I18N.make('vi')('cat.Vehicles')})));
  assert.match(enHtml,/<html lang="en">/);assert.match(enHtml,/content="Untitled — Saigon_Miền Tây"/);
  assert.match(enHtml,/A Vehicles recording from the Saigon_Miền Tây archive\./);assert.match(enHtml,/\?rec=abcd1234&amp;lang=en/);
  assert.match(plain,/<html lang="en">/);assert.ok(plain.includes(`content="${en['meta.description'].replace(/&/g,'&amp;')}"`));
  assert.match(plain,/property="og:url" content="https:\/\/x\/\?lang=en"/);
 })();
});
test('an English link is English before any script runs',()=>{
 let handler;const ctx=vm.createContext({I18N,CLIENT_INDEX:page,cacheIsFresh:()=>true,cache:[],refreshCache:async()=>[],SITE_URL:'https://x',THUMB_EDGE:720,console,
  cleanText:(s)=>s,app:{get(p,fn){if(p==='*')handler=fn;}},RegExp,Math,i18nMarkup:require('../server/utils/i18n-markup')});
 const bind=(a,b)=>vm.runInContext(server.slice(server.indexOf(a),server.indexOf(b,server.indexOf(a))),ctx);
 bind('function escAttr','// App download page');bind("app.get('*'",'// Multer errors');
 const run=async query=>{let out;await handler({query},{set(){},type(){return {send:h=>{out=h;}};}});return out;};
 return (async()=>{
  const enPage=await run({lang:'en'}),viPage=await run({});
  assert.equal(viPage,page);
  assert.match(enPage,/<html lang="en">/);
  assert.deepEqual(require('../server/utils/i18n-markup').sync(enPage,I18N.make('en')).stale,[]);
  assert.ok(enPage.includes(en.splash));
  assert.ok(enPage.includes(en['welcome.title']));
 })();
});
