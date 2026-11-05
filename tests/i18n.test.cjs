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
 for(const k of ['about.title','about.p1','og.image','og.text','og.sound'])assert.match(render('vi',k),/Saigon_Miền Tây Sound Map/,k);
 assert.match(render('vi','welcome.title'),/Saigon_Miền&nbsp;Tây Sound Map/);
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
 const kinds=['sound','image','text'];
 kinds.forEach(k=>['kind.'+k,'kinds.'+k,`kind.${k}.lower`,'share.'+k,...['photo','title','desc','submit','pin'].map(f=>`ks.${k}.${f}`)].forEach(x=>used.add(x)));
 ['image','text','sound'].forEach(k=>used.add('og.'+k));
 ['sound','text','description'].forEach(w=>used.add('left.'+w));
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
 for(const m of fs.readFileSync(path.join(__dirname,'../server/utils/b2.js'),'utf8').matchAll(/clientError\('[^']*', '([\w-]+)'\)/g))codes.add(m[1]);
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
  assert.match(enHtml,/<html lang="en">/);assert.match(enHtml,/content="Untitled — Saigon_Miền Tây Sound Map"/);
  assert.match(enHtml,/A Vehicles recording from the Saigon_Miền Tây Sound Map\./);assert.match(enHtml,/\?rec=abcd1234&amp;lang=en/);
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
 })();
});
