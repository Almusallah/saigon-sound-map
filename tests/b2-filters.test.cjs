const test=require('node:test'),assert=require('node:assert/strict');
Object.assign(process.env,{B2_ENDPOINT:'s3.test.invalid',B2_BUCKET_NAME:'test-bucket',B2_APPLICATION_KEY_ID:'test',B2_APPLICATION_KEY:'test'});
const Recording=require('../server/models/Recording');
const b2=require('../server/utils/b2');
const HAS_AUDIO={$type:'string',$ne:''};
test('maintenance jobs only ever read entries with audio',async t=>{
 const calls=[],own=Object.getOwnPropertyDescriptor(Recording,'find');
 Recording.find=(filter,fields)=>{calls.push({filter,fields});return {lean:async()=>[]};};
 t.after(()=>{if(own)Object.defineProperty(Recording,'find',own);else delete Recording.find;});
 assert.equal(await b2.cleanupOrphans(),0);
 assert.deepEqual(await b2.transcodeLegacyToMp3({dryRun:true}),{dryRun:true,remaining:0,files:[]});
 assert.deepEqual(await b2.backfillDurations({limit:1}),{updated:[],errors:[],remaining:0});
 assert.equal(calls.length,3);
 for(const {filter} of calls) assert.deepEqual(filter.audioUrl,HAS_AUDIO);
 assert.match(calls[0].fields,/\bimages\b/); // orphans take their images with them
 assert.deepEqual(calls[2].filter.$or,[{duration:{$exists:false}},{duration:0}]);
});
test('the resync never takes a video\'s clip for a recording',async t=>{
 const s3=require('@aws-sdk/client-s3');const own=s3.S3Client.prototype.send;
 t.after(()=>{s3.S3Client.prototype.send=own;});
 s3.S3Client.prototype.send=async()=>({Contents:[
  {Key:'recordings/aaaa_10.7_106.6.mp4',Size:1},{Key:'videos/bbbb.mp4',Size:1},{Key:'videos/cccc_10.7_106.6.mp4',Size:1},{Key:'images/dddd_10.7_106.6.jpg',Size:1}],IsTruncated:false});
 const files=await b2.listAllAudioFiles();
 assert.deepEqual(files.map(f=>f.key),['recordings/aaaa_10.7_106.6.mp4']);
});
