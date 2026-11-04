const test=require('node:test'),assert=require('node:assert/strict'),vm=require('node:vm'),fs=require('node:fs'),path=require('node:path');
const source=fs.readFileSync(path.join(__dirname,'../client/draft-store.js'),'utf8');
// A fake IndexedDB: requests and commits complete on later timers, like the real one.
function load({slow=5,data=new Map(),local=new Map(),failOpen=false}={}){ // share data/local to model the next page load
 const log=[];let opens=0,closed=false;
 const conn={transaction(name,mode){
  if(closed)throw new Error('InvalidStateError: the connection is closing');
  log.push('tx:'+mode);const tx={};let pending=0;
  const req=(fn,ms=5)=>{const r={};pending++;setTimeout(()=>{r.result=fn();if(--pending===0)setTimeout(()=>tx.oncomplete(),0);},ms);return r;};
  const size=v=>typeof v==='object'&&v&&v.audio?slow:5; // a draft carrying media takes longer
  tx.objectStore=()=>({get:k=>req(()=>data.get(k)),
   put:(v,k)=>req(()=>{data.set(k,v);log.push(`put:${k}`);},size(v)),
   delete:k=>req(()=>{data.delete(k);log.push(`delete:${k}`);})});
  return tx;},close(){}};
 const indexedDB={open(){opens++;closed=false;const r={result:conn};
  setTimeout(()=>{if(failOpen){r.error=new Error('Connection to Indexed Database server lost');r.onerror();}else r.onsuccess();},5);return r;}};
 const localStorage={getItem:k=>local.has(k)?local.get(k):null,setItem:(k,v)=>local.set(k,String(v)),removeItem:k=>local.delete(k)};
 const window={};vm.runInNewContext(source,{window,indexedDB,localStorage,Promise,setTimeout,Date,JSON,Math});
 return {store:window.DraftStore,data,log,local,get opens(){return opens;},close(notify=true){closed=true;if(notify)conn.onversionchange?.();}};
}
const plain=v=>JSON.parse(JSON.stringify(v)); // out of the sandbox realm
test('writes land in the order they were made',async()=>{
 const s=load();
 await Promise.all([s.store.save({v:'a'}),s.store.clear(),s.store.save({v:'b'})]);
 assert.deepEqual(s.log.filter(x=>!x.startsWith('tx')),['put:current','delete:current','put:current']);
 assert.equal(s.data.get('current').v,'b');
});
test('an idle, open store starts the write in the same task (pagehide saves)',async()=>{
 const s=load();
 await s.store.read(); // opens the database
 const before=s.log.length;
 const job=s.store.save({v:'late words'});
 assert.equal(s.log[before],'tx:readwrite','transaction created before save() returned');
 await job;assert.equal(s.data.get('current').v,'late words');
});
test('a closed connection reopens instead of failing the save',async()=>{
 for(const notify of [true,false]){ // told by versionchange, or found out by a throw
  const s=load();
  await s.store.read();
  s.close(notify);
  await s.store.save({v:'after close'});
  assert.equal(s.data.get('current').v,'after close');assert.equal(s.opens,2);
 }
});
test('the closing page\'s words are kept at once, survive an older slow save, and are laid over the draft',async()=>{
 const s=load({slow:60});
 await s.store.save({values:{t:'old'},key:'k',audio:'blob',photos:['p1']});
 const slow=s.store.save({values:{t:'mid'},key:'k',audio:'blob',photos:['p1']}); // older snapshot, in flight
 s.store.saveText({values:{t:'newest'},kind:'text',key:'k'});
 assert.ok(s.local.get('saigon-map-draft-text'),'written before saveText returned');
 await slow; // its commit must not remove the newer words
 const d=plain(await s.store.read());delete d.seq;
 assert.deepEqual(d,{values:{t:'newest'},kind:'text',key:'k',audio:'blob',photos:['p1']},'media kept, newest words on top');
});
test('a newer full save or a clear removes the words; an older overlay never wins',async()=>{
 const s=load();
 await s.store.save({values:{t:'a'},key:'k'});s.store.saveText({values:{t:'b'},key:'k'});
 await s.store.save({values:{t:'c'},key:'k'});
 assert.equal(s.local.size,0);assert.equal(plain(await s.store.read()).values.t,'c');
 // An overlay left behind by a page that died between commit and cleanup.
 s.local.set('saigon-map-draft-text',JSON.stringify({values:{t:'stale'},seq:1}));
 assert.equal(plain(await s.store.read()).values.t,'c');
 s.store.saveText({values:{t:'d'},key:'k'});await s.store.clear();
 assert.equal(s.local.size,0);assert.equal(await s.store.read(),undefined);
});
test('stamps start above what is stored, so a clock gone back never lets old words win',async()=>{
 const s=load(),future=Date.now()+3600e3;
 s.data.set('current',{values:{t:'saved'},key:'k',seq:future});
 s.local.set('saigon-map-draft-text',JSON.stringify({values:{t:'left behind'},key:'k',seq:future-1}));
 assert.equal(plain(await s.store.read()).values.t,'saved','an older overlay is ignored');
 await s.store.save({values:{t:'newer'},key:'k'});
 assert.ok(s.data.get('current').seq>future);assert.equal(s.local.size,0,'and removed by the next save');
 assert.equal(plain(await s.store.read()).values.t,'newer');
});
test('words from another contribution are not laid over this one\'s media',async()=>{
 const s=load();
 await s.store.read(); // the page saw this contribution before its user discarded it
 await s.store.save({values:{t:'k1 words'},key:'k1',photos:['p1']});
 s.store.saveText({values:{t:'a new poem'},kind:'text',key:'k2'});
 const d=plain(await s.store.read());
 assert.equal(d.values.t,'a new poem');assert.equal(d.key,'k2');assert.equal(d.photos,undefined);
});
test('a page that could not read the store claims no contribution: its words go over the stored media',async()=>{
 const first=load();
 await first.store.read();await first.store.save({values:{t:'canal sketches'},key:'k1',photos:['p1','p2']});
 const blind=load({data:first.data,local:first.local,failOpen:true}); // storage failed on this load
 await assert.rejects(blind.store.read());
 blind.store.saveText({values:{t:'typed while storage failed'},key:'k2'});
 const next=load({data:first.data,local:first.local}); // and works again on the next
 const d=plain(await next.store.read());delete d.seq;
 assert.deepEqual(d,{values:{t:'typed while storage failed'},key:'k1',photos:['p1','p2']},'media kept, under the stored key');
});
