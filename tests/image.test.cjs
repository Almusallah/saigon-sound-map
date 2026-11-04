const test=require('node:test'),assert=require('node:assert/strict'),sharp=require('sharp');
const {processImage,PHOTO_EDGE,DRAWING_EDGE,THUMB_EDGE}=require('../server/utils/image');
async function drawing(){ // 3000×1000 transparent PNG, opaque black 500×500 square top-left
 const square=await sharp({create:{width:500,height:500,channels:4,background:{r:0,g:0,b:0,alpha:1}}}).png().toBuffer();
 return sharp({create:{width:3000,height:1000,channels:4,background:{r:0,g:0,b:0,alpha:0}}}).composite([{input:square,left:0,top:0}]).png().toBuffer();
}
async function pixel(buf,x,y){const {data,info}=await sharp(buf).raw().toBuffer({resolveWithObject:true});const i=(y*info.width+x)*info.channels;return [...data.subarray(i,i+info.channels)];}
test('a transparent drawing becomes a JPEG on white at the drawing edge, with a small thumb',async()=>{
 const out=await processImage(await drawing(),{maxEdge:DRAWING_EDGE});
 assert.equal(DRAWING_EDGE,2400);assert.equal(out.ext,'jpg');assert.equal(out.mime,'image/jpeg');
 assert.deepEqual([...out.buffer.subarray(0,2)],[0xff,0xd8]);
 assert.equal(out.width,2400);assert.equal(out.height,800);
 const meta=await sharp(out.buffer).metadata();assert.equal(meta.format,'jpeg');assert.equal(meta.width,2400);assert.equal(meta.height,800);assert.equal(meta.hasAlpha,false);
 for(const c of await pixel(out.buffer,2000,400)) assert.ok(c>=250,`transparent area not white: ${c}`);
 for(const c of await pixel(out.buffer,100,100)) assert.ok(c<=10,`black square lost: ${c}`);
 const t=await sharp(out.thumb).metadata();assert.equal(t.format,'jpeg');assert.ok(Math.max(t.width,t.height)<=THUMB_EDGE);assert.equal(t.width,720);
 for(const c of await pixel(out.thumb,600,120)) assert.ok(c>=250,`thumb background not white: ${c}`);
});
test('photos default to a 1600 px long edge and are never enlarged',async()=>{
 assert.equal(PHOTO_EDGE,1600);
 const out=await processImage(await drawing());assert.equal(out.width,1600);assert.equal(Math.max(out.width,out.height),1600);
 const small=await processImage(await sharp({create:{width:200,height:100,channels:3,background:'#336699'}}).png().toBuffer(),{maxEdge:DRAWING_EDGE});
 assert.equal(small.width,200);assert.equal(small.height,100);
 const t=await sharp(small.thumb).metadata();assert.equal(t.width,200);assert.equal(t.height,100);
});
test('EXIF orientation is applied and all metadata (incl. GPS) is stripped',async()=>{
 const input=await sharp({create:{width:400,height:200,channels:3,background:'#808080'}}).jpeg()
  .withMetadata({orientation:6}).withExifMerge({IFD0:{Make:'TestCam'},IFD3:{GPSLatitudeRef:'N',GPSLatitude:'10/1 46/1 0/1'}}).toBuffer();
 const before=await sharp(input).metadata();assert.equal(before.orientation,6);assert.ok(before.exif&&before.exif.includes('TestCam'));
 const out=await processImage(input);
 assert.equal(out.width,200);assert.equal(out.height,400);
 for(const buf of [out.buffer,out.thumb]){
  const m=await sharp(buf).metadata();
  assert.equal(m.exif,undefined);assert.equal(m.orientation,undefined);assert.equal(m.xmp,undefined);assert.equal(m.iptc,undefined);
 }
});
test('unreadable or oversized input rejects',async()=>{
 await assert.rejects(processImage(Buffer.from('definitely not an image')));
 await assert.rejects(processImage(Buffer.alloc(0)));
 const bomb=await sharp({create:{width:7100,height:7100,channels:3,background:'#ffffff'}}).png({compressionLevel:1}).toBuffer();
 await assert.rejects(processImage(bomb),/pixel limit/);
});
