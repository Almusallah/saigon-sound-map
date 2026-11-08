const test=require('node:test'),assert=require('node:assert/strict');
const Views=require('../client/views');

test('only ?view=archive opens the archive',()=>{
 assert.equal(Views.pick(''),'sound');assert.equal(Views.pick('?view=foo'),'sound');assert.equal(Views.pick('?view=ARCHIVE'),'sound');
 assert.equal(Views.pick('?rec=ab12&view=archive&lang=en'),'archive');
});
test('images and texts live in the archive; a sound stays where it is',()=>{
 assert.equal(Views.forLink('sound','image'),'archive');assert.equal(Views.forLink('sound','text'),'archive');
 assert.equal(Views.forLink('sound','sound'),'sound');assert.equal(Views.forLink('archive','sound'),'archive');
});
test('the sound map shows sounds only, whatever the kind toggle says',()=>{
 assert.equal(Views.shows('sound','image',''),false);assert.equal(Views.shows('sound','text',''),false);
 assert.equal(Views.shows('sound','sound','image'),true);
 assert.equal(Views.shows('archive','image',''),true);assert.equal(Views.shows('archive','image','sound'),false);
});
test('kinds present come in a fixed order, once each',()=>{
 assert.deepEqual(Views.kindsPresent(['sound','sound']),['sound']);
 assert.deepEqual(Views.kindsPresent(['text','sound']),['sound','text']);
 assert.deepEqual(Views.kindsPresent([]),[]);
});
test('switching view keeps every other parameter',()=>{
 const u=new URL(Views.withView('https://x/?rec=ab&lang=en','archive'));
 assert.equal(u.searchParams.get('rec'),'ab');assert.equal(u.searchParams.get('lang'),'en');assert.equal(u.searchParams.get('view'),'archive');
 assert.equal(Views.withView('https://x/?view=archive&city=Milan','sound'),'https://x/?city=Milan');
 assert.equal(Views.withView('https://x/?view=archive','sound'),'https://x/');
});
test('a video lives in the archive only, between images and texts',()=>{
 assert.equal(Views.forLink('sound','video'),'archive');assert.equal(Views.forLink('archive','video'),'archive');
 assert.equal(Views.shows('sound','video',''),false);assert.equal(Views.shows('sound','video','video'),false);
 assert.equal(Views.shows('archive','video',''),true);assert.equal(Views.shows('archive','video','video'),true);
 assert.equal(Views.shows('archive','video','image'),false);
 assert.deepEqual(Views.kindsPresent(['text','video','sound','image']),['sound','image','video','text']);
});
