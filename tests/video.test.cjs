// Short videos (8 Nov 2026): probe, limits, repackaging vs re-encoding,
// the poster frame. Real ffmpeg on tiny generated clips; skipped where
// ffmpeg is missing.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),os=require('node:os'),path=require('node:path');
const {execFileSync}=require('node:child_process');
const {prepareVideo,probeVideo,encodeArgs,remuxArgs,keepsPicture,VIDEO_LIMITS,MAX_PIXELS}=require('../server/utils/video');
let ff=true;try{execFileSync('ffmpeg',['-version'],{stdio:'ignore'});}catch{ff=false;}
const dir=fs.mkdtempSync(path.join(os.tmpdir(),'vidtest-'));
const make=(name,args)=>{const f=path.join(dir,name);execFileSync('ffmpeg',['-hide_banner','-loglevel','error','-y',...args,f]);return f;};
const probe=f=>JSON.parse(execFileSync('ffprobe',['-v','error','-print_format','json','-show_entries','stream=codec_type,codec_name,width,height,pix_fmt,avg_frame_rate:stream_side_data=rotation:format=duration',f]).toString());
const leftovers=()=>fs.readdirSync(os.tmpdir()).filter(n=>/^vid-/.test(n));
test.after(()=>fs.rmSync(dir,{recursive:true,force:true}));

test('limits: 50 MB, 60 seconds, 4K either way up',()=>{assert.equal(VIDEO_LIMITS.bytes,50*1024*1024);assert.equal(VIDEO_LIMITS.seconds,60);assert.equal(MAX_PIXELS,4096*2160);});
test('past 1080p the decoder runs on one thread (memory), below on two',()=>{
 const th=a=>a[a.indexOf('-threads')+1];
 assert.equal(th(encodeArgs('in','out',{tonemap:false,hasAudio:false,bigFrame:true})),'1');
 assert.equal(th(encodeArgs('in','out',{tonemap:false,hasAudio:false})),'2');
});
test('only plain H.264 at most 1920 px is kept as it is',()=>{
 const base={codec:'h264',pixFmt:'yuv420p',hdr:false,width:1920,height:1080};
 assert.equal(keepsPicture(base),true);
 assert.equal(keepsPicture({...base,codec:'hevc'}),false);
 assert.equal(keepsPicture({...base,pixFmt:'yuv420p10le'}),false);
 assert.equal(keepsPicture({...base,hdr:true}),false);
 assert.equal(keepsPicture({...base,width:3840,height:2160}),false);
 assert.equal(keepsPicture({...base,width:1080,height:1920}),true);
});
test('every output is faststart, stripped of metadata (no GPS), and cut at the limit',()=>{
 for(const args of [encodeArgs('in','out',{tonemap:false,hasAudio:true}),remuxArgs('in','out',{hasAudio:true,audioCodec:'opus'})]){
  assert.ok(args.includes('+faststart'));assert.equal(args[args.indexOf('-map_metadata')+1],'-1');
  assert.equal(args[args.indexOf('-t')+1],'60.5');assert.equal(args.at(-1),'out');
 }
 const enc=encodeArgs('in','out',{tonemap:true,hasAudio:false});
 assert.ok(!enc.includes('0:a:0'),'no audio map without sound');
 assert.match(enc[enc.indexOf('-vf')+1],/^scale=.*zscale/,'scaled before tone-mapping');
 assert.equal(enc[enc.indexOf('-c:v')+1],'libx264');assert.ok(enc.indexOf('-threads')<enc.indexOf('-i'),'decoder threads bounded');
 const rem=remuxArgs('in','out',{hasAudio:true,audioCodec:'aac'});
 assert.equal(rem[rem.indexOf('-c:v')+1],'copy');assert.equal(rem[rem.indexOf('-c:a')+1],'copy');
 assert.equal(remuxArgs('in','out',{hasAudio:true,audioCodec:'pcm_s16le'}).includes('aac'),true);
});
test('plain H.264 is repackaged; its rotation and sound survive; the poster stands upright',{skip:!ff},async()=>{
 const src=make('h264.mp4',['-f','lavfi','-i','testsrc2=size=640x360:rate=30:duration=3','-f','lavfi','-i','sine=duration=3','-c:v','libx264','-preset','ultrafast','-pix_fmt','yuv420p','-c:a','aac']);
 const rot=make('rot.mov',['-display_rotation','90','-i',src,'-c','copy']);
 const v=await prepareVideo(rot);
 try{
  assert.equal(v.remuxed,true);
  const p=probe(v.mp4),vs=p.streams.find(s=>s.codec_type==='video');
  assert.equal(vs.codec_name,'h264');assert.ok(p.streams.some(s=>s.codec_type==='audio'));
  assert.equal(Math.abs(vs.side_data_list?.[0]?.rotation),90);
  const pp=probe(v.poster).streams[0];assert.ok(pp.height>pp.width,`poster ${pp.width}x${pp.height}`);
  assert.ok(v.duration>2.5&&v.duration<3.5,String(v.duration));
 }finally{await v.cleanup();}
 assert.equal(fs.existsSync(v.mp4),false);
});
test('HEVC 10-bit HDR at 60 fps is re-encoded to H.264, 8-bit, ≤1280 px, ≤30 fps',{skip:!ff},async()=>{
 let src;try{src=make('hevc.mov',['-f','lavfi','-i','testsrc2=size=1440x810:rate=60:duration=1','-c:v','libx265','-preset','ultrafast','-tag:v','hvc1','-pix_fmt','yuv420p10le','-x265-params','transfer=arib-std-b67:colorprim=bt2020:colormatrix=bt2020nc:log-level=error','-an']);}
 catch{return;} // no libx265 here
 const info=await probeVideo(src);assert.equal(info.hdr,true);assert.equal(info.hasAudio,false);
 const v=await prepareVideo(src);
 try{
  assert.equal(v.remuxed,false);
  const p=probe(v.mp4),vs=p.streams.find(s=>s.codec_type==='video');
  assert.equal(vs.codec_name,'h264');assert.equal(vs.pix_fmt,'yuv420p');assert.equal(vs.width,1280);assert.equal(vs.height,720);
  const [num,den]=vs.avg_frame_rate.split('/').map(Number);assert.ok(num/den<=30.01,vs.avg_frame_rate);
  assert.ok(!p.streams.some(s=>s.codec_type==='audio'));assert.ok(v.poster);
 }finally{await v.cleanup();}
});
test('too long, too large a frame, not a video, and sound alone are the contributor\'s to fix (400), leaving nothing behind',{skip:!ff},async()=>{
 const before=leftovers().length;
 const long=make('long.mp4',['-f','lavfi','-i','color=c=gray:size=64x64:rate=5:duration=62','-c:v','libx264','-preset','ultrafast','-pix_fmt','yuv420p']);
 await assert.rejects(prepareVideo(long),e=>e.status===400&&e.code==='video-too-long');
 const fake=path.join(dir,'fake.mp4');fs.writeFileSync(fake,'not a video');
 await assert.rejects(prepareVideo(fake),e=>e.status===400&&e.code==='video-unreadable');
 const jpg=make('cover.jpg',['-f','lavfi','-i','color=c=red:size=64x64','-frames:v','1']);
 const mp3=make('song.mp3',['-f','lavfi','-i','sine=duration=1','-i',jpg,'-map','0:a','-map','1:v','-c:v','mjpeg','-disposition:v','attached_pic','-c:a','libmp3lame']);
 // Sound with cover art, or a voice memo saved as .mp4: a sound, not a video.
 await assert.rejects(prepareVideo(mp3),e=>e.status===400&&e.code==='video-sound-only');
 const memo=make('memo.mp4',['-f','lavfi','-i','sine=duration=1','-c:a','aac']);
 await assert.rejects(prepareVideo(memo),e=>e.status===400&&e.code==='video-sound-only');
 // Over 4K (8K phones): refused before any decoding.
 const huge=make('huge.mp4',['-f','lavfi','-i','color=c=gray:size=4352x2240:rate=1:duration=1','-frames:v','1','-c:v','libx264','-preset','ultrafast','-pix_fmt','yuv420p']);
 await assert.rejects(prepareVideo(huge),e=>e.status===400&&e.code==='video-resolution');
 assert.equal(leftovers().length,before);
});
test('videos are prepared one at a time',{skip:!ff},async()=>{
 const src=make('a.mp4',['-f','lavfi','-i','testsrc2=size=320x240:rate=10:duration=1','-c:v','libx264','-preset','ultrafast','-pix_fmt','yuv420p']);
 const order=[];
 const [a,b]=await Promise.all([prepareVideo(src).then(v=>{order.push('a');return v;}),prepareVideo(src).then(v=>{order.push('b');return v;})]);
 assert.deepEqual(order,['a','b']);await a.cleanup();await b.cleanup();
 // A failure does not jam the queue for the next one.
 const fake=path.join(dir,'bad.mp4');fs.writeFileSync(fake,'x');
 await assert.rejects(prepareVideo(fake));const c=await prepareVideo(src);await c.cleanup();
});
test('the sound kept is a track ffmpeg can decode: AAC first, past any other',{skip:!ff},async()=>{
 // Two tracks, the first one not AAC: the AAC one is chosen and mapped by its place.
 const src=make('two-tracks.mov',['-f','lavfi','-i','testsrc2=size=320x240:rate=10:duration=2','-f','lavfi','-i','sine=f=300:duration=2','-f','lavfi','-i','sine=f=600:duration=2',
  '-map','0','-map','1','-map','2','-c:v','libx264','-preset','ultrafast','-pix_fmt','yuv420p','-c:a:0','pcm_s16le','-c:a:1','aac']);
 const info=await probeVideo(src);
 assert.equal(info.hasAudio,true);assert.equal(info.audioCodec,'aac');assert.equal(info.audioIndex,1);
 assert.ok(remuxArgs('in','out',info).includes('0:a:1'));
 const v=await prepareVideo(src);
 try{const p=probe(v.mp4);assert.deepEqual(p.streams.filter(s=>s.codec_type==='audio').map(s=>s.codec_name),['aac']);}
 finally{await v.cleanup();}
});
