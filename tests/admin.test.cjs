const test=require('node:test'),assert=require('node:assert/strict'),vm=require('node:vm'),fs=require('node:fs'),path=require('node:path');
const {LIMITS,cleanText}=require('../server/utils/entry');
const Real=require('../server/models/Recording');
const source=fs.readFileSync(path.join(__dirname,'../server/index.js'),'utf8');
// Routes run from the real source in a sandbox; the database is a list.
function harness(seed,{missing=new Set(),flaky=new Set()}={}){
 const docs=structuredClone(seed); // writes change this copy, never the fixtures
 let handler;const changes=[],deletedImages=[],saved=[],probes=[],deletedAudio=[];
 class Recording extends Real{async save(){saved.push(this.toObject());return this;}}
 Recording.KINDS=Real.KINDS;Recording.CATEGORIES=Real.CATEGORIES;
 Recording.find=(q)=>{const re=q&&q.$or&&q.$or[0].title.$regex;const hit=q&&q.id&&q.id.$regex?docs.filter(d=>new RegExp(q.id.$regex).test(d.id)):re?docs.filter(d=>new RegExp(re,'i').test(d.title)):docs;
  return {limit:()=>({lean:async()=>hit}),lean:async()=>hit};};
 Recording.updateOne=async(q,u)=>{const set=JSON.parse(JSON.stringify(u.$set)); // out of the sandbox realm
  changes.push({id:q.id,set});const d=docs.find(x=>x.id===q.id);if(d)Object.assign(d,set);};
 Recording.deleteOne=async q=>{const i=docs.findIndex(x=>x.id===q.id);if(i>=0)docs.splice(i,1);return {deletedCount:i>=0?1:0};};
 const sandbox={console:{log(){},error(){}},Buffer,LIMITS,cleanText,Recording,AbortSignal,Date,setTimeout,
  fetch:async u=>{probes.push(u);if(flaky.has(u))return {ok:false,status:503};return missing.has(u)?{ok:false,status:404}:{ok:true,status:200};},refreshCache:async()=>{},requireAdmin:{},imageUrlsOf:()=>[],
  deleteImagesOf:async r=>{deletedImages.push(...r.images.map(i=>i.url));return {deleted:r.images.length,errors:[]};},
  deleteB2Object:async u=>{deletedAudio.push(u);},
 multer:Object.assign(()=>({single:()=>({})}),{memoryStorage:()=>({})}),app:{post(...a){handler=a.at(-1);}}};
 const context=vm.createContext(sandbox);
 const bind=(start,end)=>{vm.runInContext(source.slice(source.indexOf(start),source.indexOf(end,source.indexOf(start))),context);return handler;};
 const res=()=>({statusCode:200,status(n){this.statusCode=n;return this;},json(b){this.body=JSON.parse(JSON.stringify(b));return this;}});
 return {bind,res,changes,deletedImages,saved,probes,docs,deletedAudio};
}
const img=n=>({url:`https://b2.test/images/x-${n}.jpg`,thumbUrl:`https://b2.test/images/x-${n}-thumb.jpg`,width:2400,height:1600});
const poem={id:'abcd1234-0000-4000-a000-000000000001',kind:'text',title:'Mưa',category:'Nature',description:'',body:'line one\n  line two',author:'Lan',latitude:10.7,longitude:106.7};
const drawing={id:'beef5678-0000-4000-a000-000000000002',kind:'image',title:'Section',category:'Home',description:'',images:[img(1),img(2),img(3)],imageUrl:img(1).url,latitude:10.7,longitude:106.7};
const bulk=h=>h.bind("app.post('/api/admin/bulk-update'","app.post('/api/admin/transcode-legacy'");

test('curation edits or removes a credit and a text, keeping every field it was not sent',async()=>{
 const h=harness([poem]),fn=bulk(h),r=h.res();
 await fn({body:{updates:[{idPrefix:'abcd',author:'',body:'line one\r\n  line two, fixed\n\n'}]}},r);
 assert.deepEqual(r.body.errors,[]);
 assert.deepEqual(h.changes[0].set,{title:'Mưa',category:'Nature',description:'',body:'line one\n  line two, fixed',author:''});
 const r2=h.res();await fn({body:{updates:[{idPrefix:'abcd',body:'   '}]}},r2);
 assert.match(r2.body.errors[0].error,/cannot lose its words/);assert.equal(h.changes.length,1);
 const r3=h.res();await fn({body:{updates:[{idPrefix:'abcd',title:''}]}},r3);
 assert.match(r3.body.errors[0].error,/empty title/);
});
test('removing one image updates the entry first, then deletes only that image from storage',async()=>{
 const h=harness([drawing]),fn=bulk(h);
 const dry=h.res();await fn({body:{dryRun:true,updates:[{idPrefix:'beef',removeImages:[2]}]}},dry);
 assert.deepEqual(dry.body.updated[0].removedImages,[img(2).url]);assert.equal(h.changes.length,0);assert.equal(h.deletedImages.length,0);
 const r=h.res();await fn({body:{updates:[{idPrefix:'beef',removeImages:[1]}]}},r);
 assert.deepEqual(h.changes[0].set.images.map(i=>i.url),[img(2).url,img(3).url]);
 assert.equal(h.changes[0].set.imageUrl,img(2).url,'the next image becomes the cover');
 assert.deepEqual(h.deletedImages,[img(1).url]);assert.equal(r.body.updated[0].b2ImagesDeleted,1);
});
test('image removal refuses bad numbers and never empties an image entry',async()=>{
 const h=harness([drawing]),fn=bulk(h);
 for(const removeImages of [[0],[4],['2'],[1.5]]){const r=h.res();await fn({body:{updates:[{idPrefix:'beef',removeImages}]}},r);assert.match(r.body.errors[0].error,/1–3/);}
 const r=h.res();await fn({body:{updates:[{idPrefix:'beef',removeImages:[1,2,3]}]}},r);
 assert.match(r.body.errors[0].error,/at least one image/);
 assert.equal(h.changes.length,0);assert.equal(h.deletedImages.length,0);
});

const restore=h=>h.bind('const backupFile','// One-shot migration');
const feature=(p,c=[106.7,10.7])=>({type:'Feature',geometry:{type:'Point',coordinates:c},properties:p});
const backup=features=>({file:{buffer:Buffer.from(JSON.stringify({type:'FeatureCollection',features}))}});
test('restore adds back missing entries of every kind and leaves present ones alone',async()=>{
 const sound={id:'cafe0000-0000-4000-a000-000000000003',kind:'sound',title:'Rain',category:'Nature',audioUrl:'https://b2.test/a.mp3',imageUrl:'https://b2.test/p.jpg',images:[],duration:12.3,createdAt:'2026-03-01T10:00:00.000Z'};
 const h=harness([{id:poem.id,audioUrl:''}]),fn=restore(h);
 const req=backup([feature(poem),feature(drawing),feature(sound),feature(sound)]);
 const dry=h.res();await fn({...req,body:{}},dry);
 assert.equal(dry.body.dryRun,true,'dry run unless told otherwise');assert.equal(dry.body.alreadyPresent,1);
 assert.deepEqual(dry.body.restored.map(x=>x.kind),['image','sound']);assert.equal(h.saved.length,0);
 const r=h.res();await fn({...req,body:{dryRun:'false'}},r);
 assert.equal(h.saved.length,2);
 const [d,s]=h.saved;
 assert.equal(d.images.length,3);assert.equal(d.audioUrl,'');assert.equal(d.imageUrl,img(1).url);
 assert.equal(s.imageUrl,'https://b2.test/p.jpg','an older single photo comes back');assert.equal(s.duration,12.3);
 assert.equal(new Date(s.createdAt).toISOString(),'2026-03-01T10:00:00.000Z');
});
test('restore drops media deleted since the backup and skips what has nothing left',async()=>{
 const gone=new Set([img(2).url,'https://b2.test/gone.mp3',img(9).url]);
 const h=harness([],{missing:gone}),fn=restore(h);
 const lost={id:'dead0000-0000-4000-a000-000000000004',kind:'sound',title:'x',audioUrl:'https://b2.test/gone.mp3'};
 const empty={...drawing,id:'feed0000-0000-4000-a000-000000000005',images:[img(9)]};
 const r=h.res();await fn({...backup([feature(drawing),feature(lost),feature(empty),feature({...poem,body:' '}),feature({...poem,id:'zz'}),feature({...poem,id:'abcd9999-0000-4000-a000-000000000006'},[200,10])]),body:{dryRun:'false'}},r);
 assert.deepEqual(h.saved.map(x=>x.images.map(i=>i.url)),[[img(1).url,img(3).url]]);
 assert.deepEqual(r.body.skipped.map(x=>x.reason),['its audio is gone','every image is gone','the text is empty']);
 assert.deepEqual(r.body.errors.map(x=>x.error),['No valid id','Invalid coordinates']);
});
test('restore refuses a request without a backup file',async()=>{
 const h=harness([]),fn=restore(h),r=h.res();
 await fn({body:{}},r);assert.equal(r.statusCode,400);
 const r2=h.res();await fn({file:{buffer:Buffer.from('not json')},body:{}},r2);assert.equal(r2.statusCode,400);
});

test('one update per entry per request, so a dry run lists what the real run deletes',async()=>{
 const h=harness([drawing]),fn=bulk(h);
 for(const dryRun of [true,false]){
  const r=h.res();await fn({body:{dryRun,updates:[{idPrefix:'beef',removeImages:[1]},{idPrefix:'beef5678',removeImages:[1]}]}},r);
  assert.equal(r.body.updated.length,1);assert.match(r.body.errors[0].error,/already has an update/);
 }
 assert.deepEqual(h.deletedImages,[img(1).url]);
});
test('deleteByMatch refuses tiny terms and broad real runs without confirmCount; malformed bodies are a 400',async()=>{
 const many=Array.from({length:7},(_,i)=>({...drawing,id:`beef000${i}-0000-4000-a000-000000000000`,title:'Test upload '+i}));
 const h=harness(many),fn=bulk(h);
 const r=h.res();await fn({body:{deleteByMatch:['',' a ']}},r);
 assert.equal(r.body.errors.length,2);assert.equal(r.body.deleted.length,0);
 const r2=h.res();await fn({body:{updates:[{idPrefix:'beef0001',author:'x'}],deleteByMatch:['Test upload']}},r2);
 assert.equal(r2.statusCode,400);assert.equal(r2.body.matchCount,7);assert.equal(h.changes.length,0,'a refusal changes nothing');
 const dry=h.res();await fn({body:{dryRun:true,deleteByMatch:['Test upload']}},dry);
 assert.equal(dry.body.deleted.length,7,'a dry run still lists them');assert.equal(dry.body.matchCount,7);
 for(const body of [{updates:[null]},{deleteByPrefix:[{}]},{deleteByMatch:'Test'}]){
  const bad=h.res();await fn({body},bad);assert.equal(bad.statusCode,400);
 }
 assert.equal(h.deletedImages.length,0);
 const ok=h.res();await fn({body:{deleteByMatch:['Test upload'],confirmCount:7}},ok);
 assert.equal(ok.body.deleted.length,7);assert.equal(h.docs.length,0);
});
test('deletes are what the dry run listed: worked out before updates, each entry once',async()=>{
 const mk=(n,title)=>({...drawing,id:`c0ffee0${n}-0000-4000-a000-000000000000`,title,images:[img(10+n)]});
 const seed=()=>[mk(0,'Market at dawn'),mk(1,'Test upload 1'),mk(2,'Test upload 2'),mk(3,'Test upload 3'),mk(4,'Test upload 4'),mk(5,'Test upload 5'),mk(6,'Test upload 6')];
 // An update renaming an entry INTO a delete term no longer gets it deleted.
 const body={updates:[{idPrefix:'c0ffee00',title:'Contest of kites'}],deleteByMatch:['test upload 1','test upload 2']};
 const hd=harness(seed()),dry=hd.res();await bulk(hd)({body:{...body,dryRun:true}},dry);
 const hr=harness(seed()),real=hr.res();await bulk(hr)({body},real);
 assert.deepEqual(real.body.deleted.map(x=>x.title),dry.body.deleted.map(x=>x.title));
 assert.deepEqual(real.body.deleted.map(x=>x.title),['Test upload 1','Test upload 2']);
 // A prefix and an overlapping broad term count each entry once; confirmCount is that total.
 const h=harness(seed()),fn=bulk(h),d2=h.res();
 await fn({body:{dryRun:true,deleteByPrefix:['c0ffee01'],deleteByMatch:['Test upload','upload 2']}},d2);
 assert.equal(d2.body.deleted.length,6);assert.equal(d2.body.matchCount,5,'the prefix target is not counted again');
 const r=h.res();await fn({body:{deleteByPrefix:['c0ffee01'],deleteByMatch:['Test upload','upload 2']}},r);
 assert.equal(r.body.deleted.length,6,'5 matches need no confirmCount');
 // An entry both updated and deleted in one request is only deleted.
 const h2=harness(seed()),r3=h2.res();await bulk(h2)({body:{updates:[{idPrefix:'c0ffee01',author:'x'}],deleteByPrefix:['c0ffee01']}},r3);
 assert.match(r3.body.errors[0].error,/deleted in this request/);assert.equal(h2.changes.length,0);
});
test('restore keeps an entry for a rerun when storage cannot answer, and drops a dead thumbnail only',async()=>{
 const thumbGone={...drawing,id:'babe0000-0000-4000-a000-000000000007',images:[img(4)]};
 const busy={...drawing,id:'babe0000-0000-4000-a000-000000000008',images:[img(5)]};
 const h=harness([],{missing:new Set([img(4).thumbUrl]),flaky:new Set([img(5).url])}),fn=restore(h);
 const r=h.res();await fn({...backup([feature(thumbGone),feature(busy)]),body:{dryRun:'false'}},r);
 assert.equal(h.saved.length,1);assert.equal(h.saved[0].images[0].thumbUrl,'');
 assert.deepEqual(r.body.restored[0].droppedMedia,[img(4).thumbUrl]);
 assert.match(r.body.errors[0].error,/could not check .* run the restore again/);
 assert.equal(h.probes.filter(u=>u===img(5).url).length,3,'retried before giving up');
});
test('an entry another request deleted meanwhile is reported as such; its audio still goes when asked',async()=>{
 const sound={...drawing,id:'5a5a0000-0000-4000-a000-000000000009',kind:'sound',audioUrl:'https://b2.test/s.mp3',images:[img(7)]};
 const h=harness([sound]),fn=bulk(h);
 const realDelete=h.docs.splice.bind(h.docs);
 // The other request lands between this one's snapshot and its delete.
 const r=h.res();const p=fn({body:{deleteByPrefix:['5a5a'],alsoDeleteB2:true,updates:[]}},r);
 realDelete(0,1);await p;
 assert.equal(r.body.deleted[0].alreadyDeleted,true);assert.equal(r.body.deleted[0].b2ImagesDeleted,undefined);
 assert.deepEqual(h.deletedImages,[]);assert.deepEqual(h.deletedAudio,['https://b2.test/s.mp3']);
});
