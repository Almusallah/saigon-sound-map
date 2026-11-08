// The upload storage engine (server/index.js): a video streams to a temp
// file, and an upload that never completes leaves no file behind — even
// when its video part had already finished (8 Nov 2026 review).
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),os=require('node:os'),path=require('node:path'),http=require('node:http'),net=require('node:net'),vm=require('node:vm'),crypto=require('node:crypto');
const multer=require('multer');
const source=fs.readFileSync(path.join(__dirname,'../server/index.js'),'utf8');
const start=source.indexOf('const uploadStorage = {'),end=source.indexOf('\nconst upload = multer(',start);
assert.ok(start>0&&end>start,'storage engine found');
const tmp=fs.mkdtempSync(path.join(os.tmpdir(),'upstore-'));
const ctx=vm.createContext({fs,path,crypto,multer,Buffer,MEMORY_FILE_BYTES:25*1024*1024,os:{tmpdir:()=>tmp}});
vm.runInContext(source.slice(start,end)+';this.uploadStorage=uploadStorage;',ctx);
const upload=multer({storage:ctx.uploadStorage,limits:{fileSize:50*1024*1024,fieldSize:1024}}).fields([{name:'videoFile',maxCount:1},{name:'imageFile',maxCount:2}]);
const files=()=>fs.readdirSync(tmp).filter(n=>n.startsWith('up-'));
const settle=async()=>{for(let i=0;i<40&&files().length;i++)await new Promise(r=>setTimeout(r,25));};
let server,port,seen=[];
test.before(async()=>{
 server=http.createServer((req,res)=>{req.res=res;upload(req,res,err=>{
  if(err){res.statusCode=400;return res.end(err.code||'error');}
  const v=req.files?.videoFile?.[0];seen.push(v&&v.path);
  // The route's own cleanup, as in server/index.js.
  if(v?.path)fs.unlinkSync(v.path);res.end('ok');});});
 await new Promise(r=>server.listen(0,'127.0.0.1',r));port=server.address().port;
});
test.after(()=>{server.close();fs.rmSync(tmp,{recursive:true,force:true});});
const B='----x1';
const part=(name,file,body,type='video/mp4')=>Buffer.concat([Buffer.from(`--${B}\r\nContent-Disposition: form-data; name="${name}"${file?`; filename="${file}"`:''}\r\n${file?`Content-Type: ${type}\r\n`:''}\r\n`),body,Buffer.from('\r\n')]);
const send=(body,{cut=0}={})=>new Promise(resolve=>{
 const s=net.connect(port,'127.0.0.1',()=>{
  const sent=cut?body.subarray(0,body.length-cut):body;
  s.write(`POST /api/upload HTTP/1.1\r\nHost: x\r\nContent-Type: multipart/form-data; boundary=${B}\r\nContent-Length: ${body.length}\r\n\r\n`);
  s.write(sent);
  if(cut)setTimeout(()=>{s.destroy();resolve('cut');},150);
 });
 let out='';s.on('data',d=>{out+=d;if(/\r\n\r\n(ok|\w[\w-]*)$/.test(out)){s.destroy();resolve(out.split('\r\n\r\n')[1]);}});
 s.on('error',()=>resolve('error'));
});
const clip=Buffer.alloc(200*1024,7),end_=Buffer.from(`--${B}--\r\n`);
test('a whole upload reaches the route with its temp file, which the route then removes',async()=>{
 seen=[];assert.equal(await send(Buffer.concat([part('title',null,Buffer.from('Bến Tre')),part('videoFile','clip.mov',clip),end_])),'ok');
 assert.equal(seen.length,1);assert.ok(seen[0]&&seen[0].startsWith(tmp));assert.deepEqual(files(),[]);
});
test('cut after the video part ended (only the closing boundary missing): no file left',async()=>{
 await send(Buffer.concat([part('title',null,Buffer.from('a')),part('videoFile','clip.mov',clip),end_]),{cut:4});
 await settle();assert.deepEqual(files(),[]);
});
test('cut in the middle of the video: no file left',async()=>{
 await send(Buffer.concat([part('videoFile','clip.mov',clip),end_]),{cut:100*1024});
 await settle();assert.deepEqual(files(),[]);
});
test('an earlier part refused in the same request: the video is never written',async()=>{
 const r=await send(Buffer.concat([part('title',null,Buffer.alloc(4096,97)),part('videoFile','clip.mov',clip),end_]));
 assert.equal(r,'LIMIT_FIELD_VALUE');await settle();assert.deepEqual(files(),[]);
});
