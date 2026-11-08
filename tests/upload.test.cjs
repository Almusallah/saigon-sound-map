const test=require('node:test'),assert=require('node:assert/strict'),vm=require('node:vm'),fs=require('node:fs'),path=require('node:path');
const {uploadId}=require('../server/utils/upload-id');
const {LIMITS,cleanText,entryKind}=require('../server/utils/entry');
const source=fs.readFileSync(path.join(__dirname,'../server/index.js'),'utf8');
function routeHarness(existingDescription='Keep these field notes',{fail}={}){
 let handler,calls=0,last;const stored=new Map(),changes=[],pendingUploads=new Map();
 const sandbox={console:{log(){},error(){}},Buffer,LIMITS,cleanText,entryKind,deleteImagesOf:async()=>({deleted:0,errors:[]}),
 refreshCache:async()=>{},uploadId,fs,fileDigest:async p=>require('node:crypto').createHash('sha256').update(fs.readFileSync(p)).digest(),pendingUploads,cache:[],cacheTime:0,uploadLimiter:{},boundUpload:{},requireAdmin:{},upload:{fields:()=>({})},imageUrlsOf:()=>[],
 app:{post(...args){handler=args.at(-1);}},
 Recording:{find(){return {limit(){return {lean:async()=>[{id:'abc',title:'Before',category:'Nature',description:existingDescription}]};}};},updateOne:async(q,u)=>changes.push(u.$set)},
 uploadRecording:async(bytes,type,meta)=>{calls++;last={bytes,type,meta};await new Promise(r=>setTimeout(r,5));if(fail)throw fail;if(stored.has(meta.id))return stored.get(meta.id);const doc={id:meta.id,...meta,toObject(){return {id:this.id};}};stored.set(meta.id,doc);return doc;}};
 const context=vm.createContext(sandbox);
 function bind(start,end){vm.runInContext(source.slice(source.indexOf(start),source.indexOf(end,source.indexOf(start))),context);return handler;}
 const response=()=>({statusCode:200,status(n){this.statusCode=n;return this;},json(body){this.body=body;return this;}});
 return {bind,response,get calls(){return calls;},get last(){return last;},changes,pendingUploads};
}
const uploadRoute=(opts)=>{const h=routeHarness(undefined,opts);return [h,h.bind("app.post('/api/upload'","app.delete('/api/recordings/:id'")];};
const at={latitude:'10.78',longitude:'106.7'},img=s=>({buffer:Buffer.from(s),mimetype:'image/jpeg'}),audioFile=[{buffer:Buffer.from('audio'),mimetype:'audio/mpeg',originalname:'a.mp3'}];
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
 assert.equal(res.statusCode,400);assert.equal(res.body.success,false);assert.equal(h.calls,0);
});
test('curation preserves omitted descriptions and permits explicit edits or clearing',async()=>{
 const h=routeHarness(),fn=h.bind("app.post('/api/admin/bulk-update'","app.post('/api/admin/transcode-legacy'");
 for(const value of [undefined,'Updated notes','']) {
  const entry={idPrefix:'abc',title:'After',category:'Nature'};if(value!==undefined)entry.description=value;
  const res=h.response(); await fn({body:{updates:[entry]}},res); assert.equal(res.statusCode,200);
 }
 assert.deepEqual(h.changes.map(c=>c.description),['Keep these field notes','Updated notes','']);
});
test('curation clears the resync placeholder when no description is sent',async()=>{
 const h=routeHarness('Auto-discovered recording'),fn=h.bind("app.post('/api/admin/bulk-update'","app.post('/api/admin/transcode-legacy'");
 const res=h.response(); await fn({body:{updates:[{idPrefix:'abc',title:'After',category:'Nature'}]}},res);
 assert.equal(res.statusCode,200); assert.deepEqual(h.changes.map(c=>c.description),['']);
});
test('image-only upload sends no audio, kind image and every image buffer',async()=>{
 const [h,fn]=uploadRoute(),res=h.response();
 await fn({files:{imageFile:[img('one'),img('two')]},body:{...at,title:'Bản vẽ chợ',uploadKey:'draft-key-12345678'}},res);
 assert.equal(res.statusCode,200);assert.equal(h.calls,1);
 assert.equal(h.last.bytes,null);assert.equal(h.last.type,'');assert.equal(h.last.meta.kind,'image');assert.equal(h.last.meta.originalFilename,'');
 assert.equal(h.last.meta.imageBuffers.length,2);assert.ok(h.last.meta.imageBuffers[1].equals(Buffer.from('two')));
 const first=h.last.meta.id;await fn({files:{imageFile:[img('one'),img('three')]},body:{...at,title:'Bản vẽ chợ',uploadKey:'draft-key-12345678'}},h.response());
 assert.notEqual(h.last.meta.id,first);
});
test('text-only upload keeps the poem shape and drops only trailing whitespace',async()=>{
 const poem='  Mưa rơi trên mái tôn\n\n      tiếng xe máy   \n\tvọng lại\n\n \n';
 const [h,fn]=uploadRoute(),res=h.response();
 await fn({body:{...at,title:'Mưa',kind:'text',body:poem}},res);
 assert.equal(res.statusCode,200);assert.equal(h.last.bytes,null);assert.equal(h.last.meta.kind,'text');
 assert.equal(h.last.meta.body,'  Mưa rơi trên mái tôn\n\n      tiếng xe máy   \n\tvọng lại');assert.equal(h.last.meta.imageBuffers.length,0);
});
test('a text with illustrations stays a text when the contributor says so',async()=>{
 const [h,fn]=uploadRoute(),res=h.response();
 await fn({files:{imageFile:[img('sketch')]},body:{...at,title:'Story',kind:'text',body:'Once, on Bùi Viện…'}},res);
 assert.equal(res.statusCode,200);assert.equal(h.last.meta.kind,'text');assert.equal(h.last.meta.imageBuffers.length,1);
});
test('an upload with no sound, image or text is refused before uploading',async()=>{
 for(const req of [{body:{...at,title:'Empty'}},{files:{},body:{...at,title:'Empty',body:'  \n\t \n'}},{files:{imageFile:[]},body:{...at,title:'Empty',kind:'text',body:' '}}]){
  const [h,fn]=uploadRoute(),res=h.response();await fn(req,res);
  assert.equal(res.statusCode,400);assert.equal(res.body.success,false);assert.equal(h.calls,0);
  assert.equal(res.body.message,'Add a sound, an image or a text');
 }
 // Images were sent but none had an image type: say so, not "add something".
 const [h,fn]=uploadRoute(),res=h.response();
 await fn({files:{},imagesRefused:2,body:{...at,title:'Scans',kind:'image'}},res);
 assert.equal(res.statusCode,400);assert.match(res.body.message,/None of the images could be read/);assert.equal(h.calls,0);
});
test('a missing or blank title is refused',async()=>{
 for(const title of [undefined,'','   \n\t ']){
  const [h,fn]=uploadRoute(),res=h.response();await fn({files:{audioFile},body:{...at,title}},res);
  assert.equal(res.statusCode,400);assert.equal(res.body.message,'A title is needed');assert.equal(h.calls,0);
 }
});
test('out-of-range or missing coordinates are refused',async()=>{
 for(const pos of [{latitude:'95',longitude:'106.7'},{latitude:'10.7',longitude:'181'},{latitude:'',longitude:'106.7'},{latitude:'10.7'}]){
  const [h,fn]=uploadRoute(),res=h.response();await fn({files:{audioFile},body:{...pos,title:'T'}},res);
  assert.equal(res.statusCode,400);assert.equal(res.body.message,'Invalid coordinates');assert.equal(h.calls,0);
 }
});
test('client errors from the upload pass through as 400; anything else is a generic 500',async()=>{
 const bad=Object.assign(new Error('None of the images could be read. Try a JPEG or PNG.'),{status:400});
 let [h,fn]=uploadRoute({fail:bad}),res=h.response();
 await fn({files:{imageFile:[img('x')]},body:{...at,title:'T',uploadKey:'draft-key-12345678'}},res);
 assert.equal(res.statusCode,400);assert.equal(res.body.message,bad.message);assert.equal(h.pendingUploads.size,0);
 [h,fn]=uploadRoute({fail:new Error('B2 unreachable')});res=h.response();
 await fn({files:{audioFile},body:{...at,title:'T'}},res);
 assert.equal(res.statusCode,500);assert.equal(res.body.message,'Upload failed');
});
test('non-string fields never crash the route',async()=>{
 for(const title of [['a','b'],{x:'y'},42]){
  const [h,fn]=uploadRoute(),res=h.response();await fn({files:{audioFile},body:{...at,title}},res);
  assert.equal(res.statusCode,400);assert.equal(res.body.message,'A title is needed');assert.equal(h.calls,0);
 }
 let [h,fn]=uploadRoute(),res=h.response();
 await fn({body:{...at,title:'T',body:['a poem']}},res);
 assert.equal(res.statusCode,400);assert.equal(h.calls,0);
 [h,fn]=uploadRoute();res=h.response();
 await fn({files:{audioFile},body:{...at,title:'T',kind:['text'],body:{a:1},description:['d'],author:['me'],category:{c:1}}},res);
 assert.equal(res.statusCode,200);const m=h.last.meta;
 assert.equal(m.kind,'sound');assert.equal(m.body,'');assert.equal(m.description,'');assert.equal(m.author,'');assert.equal(m.category,'');
});
test('uploadId hashes a list of images exactly as the joined bytes, and one buffer as before',()=>{
 const k='draft-key-12345678',a=Buffer.from('audio'),imgs=[Buffer.from('one'),Buffer.from('two')];
 assert.equal(uploadId(k,a,['t'],imgs),uploadId(k,a,['t'],Buffer.concat(imgs)));
 assert.equal(uploadId(k,a,['t'],[imgs[0]]),uploadId(k,a,['t'],imgs[0]));
 assert.equal(uploadId(k,a,['t'],[]),uploadId(k,a,['t'],null));
});
test('a plain sound keeps the identity the previous upload form gave it',async()=>{
 const key='draft-key-12345678',body={latitude:'10.78',longitude:'106.7',title:'  Chợ   Bến Thành ',description:'Sáng',category:'Markets',uploadKey:key};
 const photo=img('photo');
 for(const files of [{audioFile},{audioFile,imageFile:[photo]}]){
  const [h,fn]=uploadRoute();await fn({files,body:{...body}},h.response());
  // The pre-archive server: raw fields, parsed coordinates, the one photo's bytes.
  const old=uploadId(key,audioFile[0].buffer,[body.title,body.description,body.category,10.78,106.7],files.imageFile?photo.buffer:null);
  assert.equal(h.last.meta.id,old);
 }
 const [h,fn]=uploadRoute();await fn({files:{audioFile},body:{...body,author:'Lan'}},h.response());
 assert.notEqual(h.last.meta.id,uploadId(key,audioFile[0].buffer,[body.title,body.description,body.category,10.78,106.7],null));
});
test('one upload request is bounded before multer reads it',()=>{
 const ctx=vm.createContext({});vm.runInContext(source.slice(source.indexOf('const MAX_UPLOAD_BYTES'),source.indexOf('// ── Cache')),ctx);
 const run=headers=>{let next=false;const res={statusCode:200,status(n){this.statusCode=n;return this;},json(b){this.body=b;return this;}};vm.runInContext('boundUpload',ctx)({headers},res,()=>{next=true;});return {next,res};};
 assert.equal(run({'content-length':String(40*1024*1024)}).next,true);
 const big=run({'content-length':String(61*1024*1024)});assert.equal(big.next,false);assert.equal(big.res.statusCode,413);assert.equal(big.res.body.success,false);
 assert.equal(run({}).res.statusCode,411);
});
test('an unknown API path is a JSON 404, not the map page',()=>{
 let path,handler;const ctx=vm.createContext({app:{all(p,h){path=p;handler=h;}}});
 const start=source.indexOf("app.all('/api/*'");vm.runInContext(source.slice(start,source.indexOf('\n',start)),ctx);
 const res={status(n){this.statusCode=n;return this;},json(b){this.body=b;return this;}};handler({},res);
 assert.equal(path,'/api/*');assert.equal(res.statusCode,404);assert.equal(res.body.error,'Not found');
 assert.ok(start<source.indexOf("app.get('*'")&&start>source.lastIndexOf("app.post('/api/"),'registered after every API route, before the page fallback');
});
test('a video goes up from its temp file: kind video, no images, its bytes in the id, the temp file removed',async()=>{
 const os=require('node:os'),tmp=n=>{const f=path.join(os.tmpdir(),`up-test-${process.pid}-${n}`);fs.writeFileSync(f,'clip '+n);return f;};
 const [h,fn]=uploadRoute();
 const a=tmp('a'),res=h.response();
 await fn({files:{videoFile:[{path:a,size:6}],imageFile:[img('x')]},body:{...at,title:'Bến Tre',kind:'video',uploadKey:'draft-key-12345678'}},res);
 assert.equal(res.statusCode,200);assert.equal(h.last.bytes,null);assert.equal(h.last.type,'');
 assert.equal(h.last.meta.kind,'video');assert.equal(h.last.meta.videoPath,a);assert.equal(h.last.meta.imageBuffers.length,0);
 await new Promise(r=>setTimeout(r,20));assert.equal(fs.existsSync(a),false,'temp file removed');
 // Another clip from the same draft, same words: a new entry, not the first one returned.
 const first=h.last.meta.id,b=tmp('b');
 await fn({files:{videoFile:[{path:b,size:6}]},body:{...at,title:'Bến Tre',kind:'video',uploadKey:'draft-key-12345678'}},h.response());
 assert.notEqual(h.last.meta.id,first);
 // A sound sent with it leads, as always: the video is not taken, and its file still goes.
 const c=tmp('c');
 await fn({files:{videoFile:[{path:c,size:6}],audioFile},body:{...at,title:'Bến Tre',uploadKey:'draft-key-12345678'}},h.response());
 assert.equal(h.last.meta.kind,'sound');assert.equal(h.last.meta.videoPath,null);
 await new Promise(r=>setTimeout(r,20));assert.equal(fs.existsSync(c),false);
});
test('a video refused by the checks is a 400 with its code, and leaves no temp file',async()=>{
 const os=require('node:os'),f=path.join(os.tmpdir(),`up-test-${process.pid}-long`);fs.writeFileSync(f,'long clip');
 const [h,fn]=uploadRoute({fail:Object.assign(new Error('This video is 75 seconds long.'),{status:400,code:'video-too-long'})}),res=h.response();
 await fn({files:{videoFile:[{path:f,size:9}]},body:{...at,title:'Dài quá',kind:'video'}},res);
 assert.equal(res.statusCode,400);assert.equal(res.body.code,'video-too-long');assert.match(res.body.message,/75 seconds/);
 await new Promise(r=>setTimeout(r,20));assert.equal(fs.existsSync(f),false);
});
