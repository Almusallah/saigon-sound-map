const test = require('node:test');
const assert = require('node:assert/strict');
const vm = require('node:vm');
const fs = require('node:fs');
const path = require('node:path');
const voiceLoop = require('../client/voice-loop');
const source = fs.readFileSync(process.env.ENGINE_PATH || path.join(__dirname, '../client/listen-engine.js'), 'utf8');
const features = [
  { id:'bed00001', role:'bed', category:'Nature', lat:10.837, lng:106.727, hour:12, lufs:-28 },
  { id:'tex00001', role:'texture', category:'Vehicles', lat:10.838, lng:106.729, hour:19, lufs:-30 },
  { id:'voice001', role:'voice', category:'Conversations', lat:10.84, lng:106.73, hour:2, lufs:-29 },
  { id:'tonal001', role:'tonal', category:'Music', lat:10.78, lng:106.69, hour:10, lufs:-25 },
].map(r => ({...r,title:r.id,duration:60,file:r.id+'.mp3'}));
function harness({search='', failAPI=false, failAudio=false, records=features, holdAudio=false}={}) {
  const intervals = new Map(), calls = [], nodes = [], events = [], pending = [];
  let timerId=0;
  class Param { constructor(){this.value=0;} setTargetAtTime(v){this.value=v;} setValueAtTime(v){this.value=v;} linearRampToValueAtTime(v){this.value=v;} exponentialRampToValueAtTime(v){this.value=v;} cancelScheduledValues(){} }
  class Node {
    constructor(){for(const k of ['pan','gain','frequency','Q','threshold','ratio','attack','release','delayTime','playbackRate','detune'])this[k]=new Param();this.connections=[];nodes.push(this);}
    connect(n){this.connections.push(n);return n;} disconnect(){this.disconnected=true;}
    start(t=0){if(t && t<this.ctx.currentTime)throw new Error('Scheduled in the past');this.startTime=t;this.started=true;}
    stop(t){this.stopTime=t;}
  }
  class Context {
    constructor(){this.currentTime=0;this.state='suspended';this.sampleRate=10;this.destination={};}
    async resume(){this.state='running';this.onstatechange?.();} async suspend(){this.state='suspended';this.onstatechange?.();} async close(){this.state='closed';this.onstatechange?.();}
    make(){const n=new Node();n.ctx=this;return n;} createGain(){return this.make();} createAnalyser(){return this.make();} createStereoPanner(){return this.make();} createBiquadFilter(){return this.make();} createDynamicsCompressor(){return this.make();} createDelay(){return this.make();} createConvolver(){return this.make();} createBufferSource(){return this.make();} createOscillator(){return this.make();}
    createBuffer(ch,len,rate){const channels=Array.from({length:ch},()=>new Float32Array(len));return {length:len,numberOfChannels:ch,sampleRate:rate,duration:len/rate,getChannelData:i=>channels[i]};}
    async decodeAudioData(){return this.createBuffer(2,600,10);}
  }
  const sandbox={ VoiceLoop:voiceLoop, console,URLSearchParams,AbortController,Event,location:{hostname:'localhost',port:'8343',search},
    setInterval:(fn)=>{const id=++timerId;intervals.set(id,fn);return id;},clearInterval:id=>intervals.delete(id),setTimeout:()=>++timerId,clearTimeout:()=>{},
    fetch:async url=>{
      calls.push(url);
      if(url==='listen-features.json')return {ok:true,json:async()=>features};
      if(url==='/api/recordings')return {ok:!failAPI,status:503,json:async()=>({recordings:records.map(r=>({...r,latitude:r.lat,longitude:r.lng,id:r.id+'-full-id',createdAt:'2026-09-05T03:00:00Z',audioUrl:'/audio/'+r.id}))})};
      if(holdAudio)await new Promise(resolve=>pending.push(resolve));
      return {ok:!failAudio,status:404,arrayBuffer:async()=>new ArrayBuffer(4)};
    },
  };
  sandbox.window={AudioContext:Context,dispatchEvent:e=>events.push(e.type),addEventListener:()=>{}};
  const context=vm.createContext(sandbox);vm.runInContext(source,context);
  return {Room:sandbox.window.Room,run:code=>vm.runInContext(code,context),calls,nodes,intervals,events,pending};
}
test('starts once, resumes audio in the entry gesture and installs one scheduler set',async()=>{
 const h=harness();await Promise.all([h.Room.start(),h.Room.start()]);assert.equal(h.Room.state.ctx.state,'running');assert.equal(h.intervals.size,5);assert.equal(h.calls.filter(x=>x==='/api/recordings').length,1);assert.equal(h.Room.state.nowPlaying.size,2);
});
test('an unavailable production archive fails visibly without invalid local-audio fallback',async()=>{
 const h=harness({failAPI:true});await assert.rejects(h.Room.start(),/archive is unavailable/);assert.equal(h.Room.state.started,false);assert.equal(h.Room.state.ctx.state,'closed');assert.equal(h.intervals.size,0);assert.equal(h.calls.filter(x=>x.startsWith('/audio')).length,0);
});
test('a one-recording room stays filtered, and an empty filter fails rather than playing the whole city',async()=>{
 const h=harness({search:'?cat=Conversations'});await h.Room.start();assert.equal(h.Room.state.recs.length,1);assert.equal(h.Room.state.roomFilter.count,1);assert.equal(h.Room.state.nowPlaying.size,1);
 const empty=harness({search:'?cat=nonexistent'});await assert.rejects(empty.Room.start(),/No recordings match/);assert.equal(empty.Room.state.started,false);
});
test('Thanh Đa filtering excludes distant sounds and keeps its own listener bounds',async()=>{
 const h=harness({search:'?walk=thanh-da'});await h.Room.start();assert.equal(h.Room.state.recs.length,3);assert.equal(h.Room.state.roomFilter.label,'Thanh Đa');h.Room.moveListener(0,0);assert.equal(h.Room.state.listener.lat,h.Room.state.roomFilter.bounds.latMin);
});
test('an unanalysed recording does not inherit its upload hour as a capture hour',async()=>{
 const h=harness({records:[{id:'new00001',category:'Nature',lat:10.8,lng:106.7}]});await h.run('loadCorpus()');assert.equal(h.Room.state.recs[0].hour,null);
});
test('concurrent buffer requests share one download and one decode',async()=>{
 const h=harness({holdAudio:true});h.Room.state.ctx=h.run('new window.AudioContext()');
 const rec={id:'same',audioUrl:'/audio/same'};h.Room.state.recs=[rec];
 const a=h.run('getBuffer(state.recs[0])'),b=h.run('getBuffer(state.recs[0])');await new Promise(setImmediate);assert.equal(h.calls.length,1);h.pending.shift()();assert.equal(await a,await b);assert.equal(h.Room.state.buffers.size,1);
});
test('pointer movement does not start downloads; overlapping refreshes are serialized',async()=>{
 const h=harness();await h.Room.start();const before=h.calls.length;for(let i=0;i<200;i++)h.Room.moveListener(10.8+i/10000,106.7);assert.equal(h.calls.length,before);
 await Promise.all(Array.from({length:20},()=>h.Room.refresh()));assert.ok(h.Room.state.nowPlaying.size<=4);assert.equal(new Set([...h.Room.state.nowPlaying.values()].map(r=>r.id)).size,h.Room.state.nowPlaying.size);
});
test('expired surroundings stay audible until a replacement is ready; resume keeps one scheduler',async()=>{
 const h=harness();await h.Room.start();const layer=h.Room.state.slots.bedA;await h.Room.pause();h.Room.state.ctx.currentTime=layer.endsAt+1;h.run('tick()');assert.equal(h.Room.state.slots.bedA,layer);await h.Room.resume();h.run('tick()');assert.equal(h.Room.state.slots.bedA,layer);assert.equal(layer.src.stopTime,undefined);assert.equal(h.intervals.size,5);
});
test('natural source completion removes only its own layer and disconnects all layer nodes',async()=>{
 const h=harness();await h.Room.start();const layer=h.Room.state.slots.bedA;layer.src.onended();assert.equal(h.Room.state.slots.bedA,undefined);assert.equal(h.Room.state.nowPlaying.has('bedA'),false);for(const n of [layer.src,layer.g,layer.spatial,layer.panner,layer.dSend,layer.rSend])assert.equal(n.disconnected,true);
});
test('a delayed background scheduler skips missed beats',async()=>{
 const h=harness();await h.Room.start();h.Room.state.ctx.currentTime=3600;h.run('tick()');assert.ok(h.run('stepCount')<3);assert.ok(h.run('nextStepTime')>3600);
});
test('master mute and volume affect the post-compressor output, including effect returns',async()=>{
 const h=harness();await h.Room.start();h.Room.setVolume(.42);assert.equal(h.run('output.gain.value'),.42);h.Room.toggleMute();assert.equal(h.run('output.gain.value'),0);h.Room.setVolume(.3);assert.equal(h.run('output.gain.value'),0);h.Room.toggleMute();assert.equal(h.run('output.gain.value'),.3);assert.equal(h.run('comp.connections[0] === output && output.connections[0] === state.ctx.destination'),true);
});
test('reset restores real defaults and the live clock; manual time wraps at midnight',async()=>{
 const h=harness();await h.Room.start();h.Room.set('sub',1);h.Room.set('reso',1);h.Room.setHour(24);assert.equal(h.Room.state.hour,0);h.Room.reset();assert.equal(h.Room.state.sub,.5);assert.equal(h.Room.state.reso,.08);assert.equal(h.Room.state.hour,null);assert.equal(h.Room.state.percDensity,.28);
});
test('failed recordings are temporarily excluded and do not crash the room',async()=>{
 const h=harness({failAudio:true});await h.Room.start();assert.equal(h.Room.state.nowPlaying.size,0);assert.match(h.Room.state.message,/No audio loaded/);await h.Room.refresh();const audioCalls=h.calls.filter(x=>x.startsWith('/audio'));assert.equal(new Set(audioCalls).size,audioCalls.length);
});
test('decoded buffer cache stays within the byte budget',async()=>{
 const h=harness();h.Room.state.ctx=h.run('new window.AudioContext()');for(let i=0;i<20;i++){h.Room.state.recs=[{id:String(i),audioUrl:'/audio/'+i}];await h.run('getBuffer(state.recs[0])');}assert.ok(h.Room.state.bufferBytes<=64*1024*1024);assert.ok(h.Room.state.buffers.size<20);
});

test('spatial movement changes stereo position and proximity without overwriting the layer fade',async()=>{
 const h=harness();await h.Room.start();const layer=h.Room.state.slots.bedA;
 const envelope=layer.g.gain.value;h.Room.moveListener(layer.rec.lat,layer.rec.lng);
 assert.equal(layer.panner.pan.value,0);assert.equal(layer.spatial.gain.value,1);h.Room.moveListener(layer.rec.lat,layer.rec.lng+.01);
 assert.ok(layer.panner.pan.value<0);assert.ok(layer.spatial.gain.value<1);assert.equal(layer.g.gain.value,envelope);
});

test('percussion replacement publishes buffers and hit offsets atomically',async()=>{
 const h=harness({holdAudio:true});h.Room.state.ctx=h.run('new window.AudioContext()');
 h.Room.state.recs=['rhythm01','rhythm02'].map(id=>({id,role:'rhythm',onset:3,lufs:-28,audioUrl:'/audio/'+id}));
 h.run('percBufA={duration:120};percOffA=[100];');const previous=h.run('percBufA');
 const load=h.run('loadPerc()');await new Promise(setImmediate);assert.equal(h.pending.length,2);
 h.pending.shift()();await new Promise(setImmediate);assert.equal(h.run('percBufA'),previous);assert.equal(h.run('percOffA[0]'),100);
 h.pending.shift()();await load;assert.equal(h.run('percBufA.duration'),60);assert.ok(h.run('percOffA.every(offset=>offset<percBufA.duration)'));
});


test('foundation repeats a real voice and survives many foreground changes',async()=>{
 const h=harness();await h.Room.start();const anchor=h.Room.state.slots.anchor;
 assert.equal(anchor.rec.category,'Conversations');assert.equal(anchor.src.loop,true);
 assert.ok(anchor.loopSeconds>10 && anchor.loopSeconds<15);
 for(let i=0;i<8;i++)await h.Room.refresh();assert.equal(h.Room.state.slots.anchor,anchor);
 assert.ok(h.Room.state.nowPlaying.size<=3);
});
test('holding the voice prevents timed replacement',async()=>{
 const h=harness();await h.Room.start();const anchor=h.Room.state.slots.anchor;
 h.Room.holdAnchor(true);h.Room.state.ctx.currentTime=1000;await h.run('renewAnchor()');
 assert.equal(h.Room.state.slots.anchor,anchor);assert.equal(anchor.src.stopTime,undefined);
});
test('voice replacement loads before starting a 24-second crossfade',async()=>{
 const records=[...features,{id:'voice002',category:'Conversations',role:'voice',duration:60,lat:10.839,lng:106.727}];
 const h=harness({records});await h.Room.start();const old=h.Room.state.slots.anchor;
 await h.Room.nextAnchor();assert.notEqual(h.Room.state.slots.anchor,old);
 assert.equal(old.src.stopTime,h.Room.state.ctx.currentTime+24.1);
 assert.notEqual(old.rec.id,h.Room.state.slots.anchor.rec.id);
});
test('a room without speech uses a labelled texture without leaving the filter',async()=>{
 const h=harness({search:'?cat=Nature'});await h.Room.start();
 assert.equal(h.Room.state.slots.anchor.rec.category,'Nature');assert.equal(h.Room.state.recs.length,1);
});
test('constant PCM remains constant across the loop seam',()=>{
 const pcm=new Float32Array(2000).fill(.2), loop=voiceLoop.prepare(pcm,100,14,1.8,3);
 assert.equal(loop.length,1220);assert.ok(loop.every(v=>Math.abs(v-.2)<1e-6));
});

test('effects start dry and zero means zero wet signal, including feedback',async()=>{
 const h=harness();await h.Room.start();assert.equal(h.run('reverbSend.gain.value'),0);assert.equal(h.run('delaySend.gain.value'),0);
 h.Room.toggleEffects();assert.ok(h.run('reverbSend.gain.value')>0);
 h.Room.set('space',0);h.Room.set('echo',0);assert.equal(h.run('reverbSend.gain.value'),0);assert.equal(h.run('delaySend.gain.value'),0);assert.equal(h.run('delayFb.gain.value'),0);
 h.Room.set('synthOn',true);h.Room.set('synthOn',false);assert.equal(h.run('synthWet.gain.value'),0);
});

test('bass and percussion controls reach true silence independently of the voice',async()=>{
 const h=harness();await h.Room.start();const voice=h.Room.state.slots.anchor;
 assert.equal(h.run('bassLoop.src.loop'),true);assert.ok(h.run('bassBus.gain.value')>0);
 h.Room.set('bassLevel',0);h.Room.set('percDensity',0);
 assert.equal(h.run('bassBus.gain.value'),0);assert.equal(h.run('percBus.gain.value'),0);
 assert.equal(h.Room.state.slots.anchor,voice);
});
test('ambience defaults to silence on both dry and effect paths; solo restores the chosen mix',async()=>{
 const h=harness();await h.Room.start();
 for(const bus of ['fieldBus','cityDelayGate','cityReverbGate'])assert.equal(h.run(bus+'.gain.value'),0);
 h.Room.set('ambienceLevel',.2);h.Room.set('effectsOn',true);h.Room.set('synthOn',true);
 const voice=h.Room.state.slots.anchor;
 h.Room.set('voiceSolo',true);
 for(const bus of ['fieldBus','cityDelayGate','cityReverbGate','bassBus','percBus','synthBus','synthWet','delaySend','delayFb','reverbSend'])assert.equal(h.run(bus+'.gain.value'),0,bus);
 assert.equal(h.Room.state.slots.anchor,voice);
 h.Room.set('voiceSolo',false);
 assert.equal(h.run('fieldBus.gain.value'),.2);assert.ok(h.run('bassBus.gain.value')>0);assert.ok(h.run('percBus.gain.value')>0);
 assert.equal(h.Room.state.slots.anchor,voice);
});
