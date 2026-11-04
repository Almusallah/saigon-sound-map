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
