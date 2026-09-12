const test=require('node:test'),assert=require('node:assert/strict'),vm=require('node:vm'),fs=require('node:fs'),path=require('node:path');
const {uploadId}=require('../server/utils/upload-id');
const source=fs.readFileSync(path.join(__dirname,'../server/index.js'),'utf8');
function routeHarness(){
 let handler,calls=0;const stored=new Map(),changes=[];
 const sandbox={console,refreshCache:async()=>{},uploadId,pendingUploads:new Map(),cache:[],cacheTime:0,uploadLimiter:{},requireAdmin:{},upload:{fields:()=>({})},
 app:{post(...args){handler=args.at(-1);}},
 Recording:{find(){return {limit(){return {lean:async()=>[{id:'abc',title:'Before',category:'Nature',description:'Keep these field notes'}]};}};},updateOne:async(q,u)=>changes.push(u.$set)},
 uploadRecording:async(bytes,type,meta)=>{calls++;await new Promise(r=>setTimeout(r,5));if(stored.has(meta.id))return stored.get(meta.id);const doc={id:meta.id,...meta,toObject(){return {id:this.id};}};stored.set(meta.id,doc);return doc;}};
 const context=vm.createContext(sandbox);
 function bind(start,end){vm.runInContext(source.slice(source.indexOf(start),source.indexOf(end,source.indexOf(start))),context);return handler;}
 const response=()=>({statusCode:200,status(n){this.statusCode=n;return this;},json(body){this.body=body;return this;}});
 return {bind,response,get calls(){return calls;},changes};
}
test('same draft and payload keep one identity; different material gets a different identity',()=>{
 const id=uploadId('draft-key-12345678',Buffer.from('audio'),['title'],null);
 assert.equal(id,uploadId('draft-key-12345678',Buffer.from('audio'),['title'],null));
 assert.notEqual(id,uploadId('draft-key-12345678',Buffer.from('different'),['title'],null));
 assert.equal(uploadId(null,Buffer.from('audio'),[],null),undefined);
});
test('concurrent upload retries share one job and return the same recording',async()=>{
 const h=routeHarness(),fn=h.bind("app.post('/api/upload'","app.delete('/api/recordings/:id'");
 const req={files:{audioFile:[{buffer:Buffer.from('audio'),mimetype:'audio/mpeg'}]},body:{uploadKey:'draft-key-12345678',latitude:'10.8',longitude:'106.7',title:'Test'}};
 const a=h.response(),b=h.response();await Promise.all([fn(req,a),fn(req,b)]);
 assert.equal(h.calls,1);assert.equal(a.body.recording.id,b.body.recording.id);
});
test('invalid draft keys return a client error before uploading',async()=>{
 const h=routeHarness(),fn=h.bind("app.post('/api/upload'","app.delete('/api/recordings/:id'");const res=h.response();
 await fn({files:{audioFile:[{buffer:Buffer.from('a')}]},body:{uploadKey:{bad:true},latitude:10,longitude:106}},res);
 assert.equal(res.statusCode,400);assert.equal(h.calls,0);
});
test('curation preserves omitted descriptions and permits explicit edits or clearing',async()=>{
 const h=routeHarness(),fn=h.bind("app.post('/api/admin/bulk-update'","app.post('/api/admin/transcode-legacy'");
 for(const value of [undefined,'Updated notes','']) {
  const entry={idPrefix:'abc',title:'After',category:'Nature'};if(value!==undefined)entry.description=value;
  const res=h.response(); await fn({body:{updates:[entry]}},res); assert.equal(res.statusCode,200);
 }
 assert.deepEqual(h.changes.map(c=>c.description),['Keep these field notes','Updated notes','']);
});
