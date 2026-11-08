const test=require('node:test'),assert=require('node:assert/strict');
const {LIMITS,cleanText,entryKind,isSound,imageUrlsOf}=require('../server/utils/entry');
const ml={multiline:true};
test('cleanText normalises line endings, control characters and Unicode form',()=>{
 assert.equal(cleanText('a\r\nb\rc',100,ml),'a\nb\nc');
 assert.equal(cleanText('a\u0000b\u0007c\u000Bd\u000Ce\u001Bf\u007Fg',100),'abcdefg');
 assert.equal(cleanText('Café',100),'Café');
 assert.equal(cleanText('Sài Gòn',100),'Sài Gòn');
 assert.equal(cleanText('Sài Gòn',100).length,7);
});
test('cleanText collapses single-line whitespace',()=>{
 assert.equal(cleanText('  Chợ \n\t Bến   Thành  ',100),'Chợ Bến Thành');
 assert.equal(cleanText(' \n\t ',100),'');
});
test('cleanText keeps the shape of multiline text',()=>{
 assert.equal(cleanText('\n\n  first line\n\n\n    indented\tline  \n\n \t\n',1000,ml),'  first line\n\n\n    indented\tline');
 assert.equal(cleanText('\r\n\r\n  one\r\n\r\n  two\r\n',1000,ml),'  one\n\n  two');
 assert.equal(cleanText('\n \n\t',1000,ml),'');
});
test('cleanText caps in UTF-16 units (as the model counts) and never splits a surrogate pair',()=>{
 assert.equal(cleanText('ab😀cd',3),'ab');
 assert.equal(cleanText('ab😀cd',4),'ab😀');
 assert.equal(cleanText('😀😀😀',4),'😀😀');
 assert.equal(cleanText('🌸'.repeat(100),80).length,80);
 assert.equal(cleanText('abc def',4),'abc');
 assert.equal(cleanText('line one\n\nline two',10,ml),'line one');
 assert.equal(cleanText('x'.repeat(LIMITS.title+50),LIMITS.title).length,LIMITS.title);
 assert.ok(!/[\uD800-\uDBFF]$/.test(cleanText('a'+'😀'.repeat(10),4)));
});
test('cleanText returns an empty string for anything that is not a string',()=>{
 for(const v of [undefined,null,42,true,['a'],{toString(){return 'x';}}]) assert.equal(cleanText(v,100),'');
 assert.equal(cleanText(['poem'],100,ml),'');
});
test('entryKind: audio leads, a text hint keeps a text, then images, then body',()=>{
 const cases=[
  [{hasAudio:true,imageCount:0,body:'',hint:undefined},'sound'],
  [{hasAudio:true,imageCount:3,body:'poem',hint:'text'},'sound'],
  [{hasAudio:true,imageCount:0,body:'',hint:'image'},'sound'],
  [{hasAudio:false,imageCount:2,body:'',hint:undefined},'image'],
  [{hasAudio:false,imageCount:2,body:'',hint:'text'},'image'],
  [{hasAudio:false,imageCount:2,body:'poem',hint:undefined},'image'],
  [{hasAudio:false,imageCount:2,body:'poem',hint:'image'},'image'],
  [{hasAudio:false,imageCount:2,body:'poem',hint:'text'},'text'],
  [{hasAudio:false,imageCount:0,body:'poem',hint:'text'},'text'],
  [{hasAudio:false,imageCount:0,body:'poem',hint:undefined},'text'],
  [{hasAudio:false,imageCount:0,body:'poem',hint:'sound'},'text'],
  [{hasAudio:false,imageCount:0,body:'poem',hint:'image'},'text'],
  [{hasAudio:false,imageCount:0,body:'',hint:'text'},null],
  [{hasAudio:false,imageCount:0,body:'',hint:'sound'},null],
  [{hasAudio:false,imageCount:0,body:'',hint:undefined},null],
  [{hasAudio:false,imageCount:2,body:'poem',hint:['text']},'image'],
 ];
 for(const [input,kind] of cases) assert.equal(entryKind(input),kind,JSON.stringify(input));
});
test('isSound only accepts entries with an audioUrl',()=>{
 assert.equal(isSound({audioUrl:'https://b2/x.mp3'}),true);
 for(const r of [{audioUrl:''},{kind:'text'},{},null,undefined]) assert.equal(isSound(r),false);
});
test('imageUrlsOf lists the cover, every image and thumb once',()=>{
 assert.deepEqual(imageUrlsOf({imageUrl:'a',images:[{url:'a',thumbUrl:'a-t'},{url:'b',thumbUrl:'b-t'},{url:'b'},null,{}]}),['a','a-t','b','b-t']);
 assert.deepEqual(imageUrlsOf({imageUrl:'legacy.jpg'}),['legacy.jpg']);
 assert.deepEqual(imageUrlsOf({imageUrl:'',images:[{url:'c',thumbUrl:''}]}),['c']);
 for(const r of [{},null,undefined,{audioUrl:'x.mp3'}]) assert.deepEqual(imageUrlsOf(r),[]);
});
test('cleanText stays linear on long whitespace runs (one crafted field once froze the server)',()=>{
 for(const s of ['a'+' '.repeat(1e6)+'b','\n'+' '.repeat(1e6)+'b',(' \n').repeat(5e5)+'x','x'+'\t'.repeat(1e6)])
  for(const multiline of [true,false]){const t=Date.now();cleanText(s,LIMITS.body,{multiline});assert.ok(Date.now()-t<250,`${multiline} ${JSON.stringify(s.slice(0,3))}`);}
 assert.equal(cleanText('poem \n\t \n',100,ml),'poem');
});
test('entryKind: a video comes after audio and before everything else',()=>{
 assert.equal(entryKind({hasAudio:false,hasVideo:true,imageCount:0,body:''}),'video');
 assert.equal(entryKind({hasAudio:false,hasVideo:true,imageCount:3,body:'poem',hint:'text'}),'video');
 assert.equal(entryKind({hasAudio:true,hasVideo:true,imageCount:0,body:''}),'sound');
 // Callers that know nothing of video keep their answers.
 assert.equal(entryKind({hasAudio:false,imageCount:2,body:''}),'image');
});
test('imageUrlsOf also lists a video\'s clip, so deleting an entry deletes it',()=>{
 assert.deepEqual(imageUrlsOf({kind:'video',videoUrl:'v.mp4',imageUrl:'p.jpg',images:[{url:'p.jpg',thumbUrl:'p-t.jpg'}]}),['v.mp4','p.jpg','p-t.jpg']);
 assert.deepEqual(imageUrlsOf({kind:'video',videoUrl:'v.mp4'}),['v.mp4']);
 // A sound's audio is never in the list: DELETE leaves it for the resync.
 assert.deepEqual(imageUrlsOf({audioUrl:'a.mp3',videoUrl:''}),[]);
});
