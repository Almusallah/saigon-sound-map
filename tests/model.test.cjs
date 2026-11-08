const test=require('node:test'),assert=require('node:assert/strict');
const Recording=require('../server/models/Recording');
const {LIMITS,cleanText}=require('../server/utils/entry');
const base={id:'t-1',title:'T',latitude:10.78,longitude:106.7};
const errorsOf=doc=>Object.keys(new Recording(doc).validateSync()?.errors||{});
test('image and text entries validate without audio',()=>{
 assert.deepEqual(errorsOf({...base,kind:'text',body:'  a poem\n\n  in two stanzas'}),[]);
 assert.deepEqual(errorsOf({...base,kind:'image',images:[{url:'https://b2/images/a.jpg'}]}),[]);
 assert.deepEqual(errorsOf({...base,kind:'image',images:[{url:'a'},{url:'b',thumbUrl:'b-t',width:10,height:5}]}),[]);
});
test('a sound, explicit or by default, needs its audioUrl',()=>{
 assert.deepEqual(errorsOf({...base,kind:'sound'}),['audioUrl']);
 assert.deepEqual(errorsOf({...base}),['audioUrl']);
 assert.deepEqual(errorsOf({...base,audioUrl:'https://b2/recordings/a.mp3'}),[]);
 assert.equal(new Recording({...base,audioUrl:'x'}).kind,'sound');
});
test('kind, body, author and images are bounded',()=>{
 assert.deepEqual(errorsOf({...base,kind:'gif',audioUrl:'x'}),['kind']);
 assert.deepEqual(errorsOf({...base,kind:'text',body:'x'.repeat(20000)}),[]);
 assert.deepEqual(errorsOf({...base,kind:'text',body:'x'.repeat(20001)}),['body']);
 assert.deepEqual(errorsOf({...base,kind:'text',body:'b',author:'x'.repeat(80)}),[]);
 assert.deepEqual(errorsOf({...base,kind:'text',body:'b',author:'x'.repeat(81)}),['author']);
 assert.ok(errorsOf({...base,kind:'image',images:[{thumbUrl:'t'}]}).includes('images.0.url'));
 assert.equal(LIMITS.body,Recording.schema.path('body').options.maxlength);
 assert.equal(LIMITS.author,Recording.schema.path('author').options.maxlength);
});
test('text capped by cleanText always fits the model limits',()=>{
 const author=cleanText('🌸'.repeat(100),LIMITS.author),body=cleanText('😀'.repeat(LIMITS.body+1),LIMITS.body,{multiline:true});
 assert.deepEqual(errorsOf({...base,kind:'text',body,author}),[]);
});

test('a video needs its clip, never audio; its poster is its cover',()=>{
 assert.deepEqual(errorsOf({...base,kind:'video'}),['videoUrl']);
 assert.deepEqual(errorsOf({...base,kind:'video',videoUrl:'https://b2/videos/a.mp4'}),[]);
 assert.deepEqual(errorsOf({...base,kind:'video',videoUrl:'v',imageUrl:'p',images:[{url:'p',thumbUrl:'pt',width:720,height:1280}]}),[]);
 assert.ok(Recording.KINDS.includes('video'));
 // A sound never needs a video, and keeps validating as before.
 assert.deepEqual(errorsOf({...base,kind:'sound',audioUrl:'a'}),[]);
});
