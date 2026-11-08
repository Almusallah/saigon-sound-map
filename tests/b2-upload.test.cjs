const test=require('node:test'),assert=require('node:assert/strict');
Object.assign(process.env,{B2_ENDPOINT:'s3.test.invalid',B2_BUCKET_NAME:'test-bucket',B2_APPLICATION_KEY_ID:'test',B2_APPLICATION_KEY:'test'});
// In-memory bucket: Upload writes, DeleteObjectCommand removes; failPut decides which keys refuse.
const store=new Map();let failPut=()=>false;
const ls=require.resolve('@aws-sdk/lib-storage');
require.cache[ls]={id:ls,filename:ls,loaded:true,exports:{Upload:class{constructor({params}){this.p=params;}async done(){if(failPut(this.p.Key))throw new Error('storage cap exceeded');store.set(this.p.Key,this.p.Body);return {};}}}};
const s3=require('@aws-sdk/client-s3');
s3.S3Client.prototype.send=async function(cmd){if(cmd.constructor.name==='DeleteObjectCommand'){store.delete(cmd.input.Key);return {};}throw new Error('unexpected '+cmd.constructor.name);};
const sharp=require('sharp');
const Recording=require('../server/models/Recording');
Recording.findOne=async()=>null;
Recording.prototype.save=async function(){const e=this.validateSync();if(e)throw e;return this;};
const {uploadRecording}=require('../server/utils/b2');
const jpeg=()=>sharp({create:{width:40,height:30,channels:3,background:'#c0ffee'}}).jpeg().toBuffer();
const base={title:'Bản vẽ',description:'',category:'Home',latitude:10.77,longitude:106.7};
test.beforeEach(()=>{store.clear();failPut=()=>false;});
test('an unreadable image is skipped and the rest are kept',async()=>{
 const doc=await uploadRecording(null,'',{...base,kind:'image',imageBuffers:[Buffer.from('not an image'),await jpeg()],id:'11111111-aaaa-4aaa-aaaa-111111111111'});
 assert.equal(doc.kind,'image');assert.equal(doc.images.length,1);assert.equal(doc.imageUrl,doc.images[0].url);
 assert.equal(store.size,2); // one full image + its thumb
});
test('a storage failure fails the upload and leaves nothing behind',async()=>{
 for(const [kind,extra] of [['image',{}],['text',{body:'Mưa'}]]){
  store.clear();let n=0;failPut=()=>++n===2; // the first thumbnail write refuses
  await assert.rejects(uploadRecording(null,'',{...base,...extra,kind,imageBuffers:[await jpeg(),await jpeg()],id:'22222222-aaaa-4aaa-aaaa-22222222222'+(kind==='image'?'2':'3')}),
   e=>e.status!==400&&/storage/.test(e.message),kind);
  assert.equal(store.size,0,`${kind}: the full image written before the failure was rolled back`);
 }
});
test('an image entry with nothing readable is a 400',async()=>{
 await assert.rejects(uploadRecording(null,'',{...base,kind:'image',imageBuffers:[Buffer.from('junk')],id:'33333333-aaaa-4aaa-aaaa-333333333333'}),e=>e.status===400);
 assert.equal(store.size,0);
});
test('a scan over the pixel limit says how to fix it, not "try a JPEG"',async()=>{
 // A 1-bit 8000×8000 PNG (64 MP): tiny on disk, refused from its header.
 const zlib=require('node:zlib');
 const chunk=(t,d)=>{const n=Buffer.alloc(4),c=Buffer.alloc(4),td=Buffer.concat([Buffer.from(t),d]);n.writeUInt32BE(d.length);c.writeUInt32BE(zlib.crc32(td));return Buffer.concat([n,td,c]);};
 const ihdr=Buffer.alloc(13);ihdr.writeUInt32BE(8000,0);ihdr.writeUInt32BE(8000,4);ihdr[8]=1;
 const png=Buffer.concat([Buffer.from([137,80,78,71,13,10,26,10]),chunk('IHDR',ihdr),chunk('IDAT',zlib.deflateSync(Buffer.alloc(1001))),chunk('IEND',Buffer.alloc(0))]);
 await assert.rejects(uploadRecording(null,'',{...base,kind:'image',imageBuffers:[png],id:'44444444-aaaa-4aaa-aaaa-444444444444'}),
  e=>e.status===400&&/50 megapixels/.test(e.message));
});
test('a video: the clip under videos/<id>.mp4, its poster as the cover, no audio; a failure leaves nothing',{skip:(()=>{try{require('node:child_process').execFileSync('ffmpeg',['-version'],{stdio:'ignore'});return false;}catch{return true;}})()},async()=>{
 const fs=require('node:fs'),os=require('node:os'),path=require('node:path'),{execFileSync}=require('node:child_process');
 const clip=path.join(os.tmpdir(),'b2test-clip.mp4');
 execFileSync('ffmpeg',['-hide_banner','-loglevel','error','-y','-f','lavfi','-i','testsrc2=size=320x240:rate=10:duration=3','-c:v','libx264','-preset','ultrafast','-pix_fmt','yuv420p',clip]);
 try{
  const id='33333333-aaaa-4aaa-aaaa-333333333333';
  // Images sent alongside are not taken: the poster is the cover.
  const doc=await uploadRecording(null,'',{...base,kind:'video',videoPath:clip,imageBuffers:[await jpeg()],id});
  assert.equal(doc.kind,'video');assert.equal(doc.audioUrl,'');
  assert.equal(doc.videoUrl,`https://s3.test.invalid/test-bucket/videos/${id}.mp4`);
  assert.equal(doc.images.length,1);assert.equal(doc.imageUrl,doc.images[0].url);
  assert.ok(doc.images[0].width>0&&doc.images[0].height>0);
  assert.ok(doc.duration>2.5&&doc.duration<3.5);assert.ok(doc.fileSize>0);
  assert.deepEqual([...store.keys()].sort(),[`images/${id}-0-t_10.77_106.7.jpg`,`images/${id}_10.77_106.7.jpg`,`videos/${id}.mp4`]);
  assert.ok(fs.existsSync(clip),'the upload\'s own temp file is the route\'s to remove');
  // The poster's write fails: the clip already written is rolled back.
  store.clear();failPut=k=>k.startsWith('images/');
  await assert.rejects(uploadRecording(null,'',{...base,kind:'video',videoPath:clip,id:'44444444-aaaa-4aaa-aaaa-444444444444'}),/storage/);
  assert.equal(store.size,0);
 }finally{fs.rmSync(clip,{force:true});}
});
