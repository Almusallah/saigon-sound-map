/**
 * Short videos for the archive (8 Nov 2026).
 *
 * A phone video arrives as whatever the phone made — HEVC .mov from an
 * iPhone, H.264 .mp4 from most Androids, sometimes 4K, 60 fps, HDR — and
 * not every browser can play every one of those (Chrome on most Android
 * and Windows machines cannot play HEVC). What every browser plays is
 * H.264 + AAC in an MP4 whose index sits at the front (+faststart), so
 * playback starts before the whole file has arrived.
 *
 * - Already H.264, 8-bit, ordinary colour, at most 1920 px: the picture
 *   is kept as it is and only repackaged (and the sound made AAC) —
 *   a fraction of a second.
 * - Anything else is re-encoded: longest side 1280 px, at most 30 frames
 *   a second. On the 0.5-CPU instance a 30-second iPhone HEVC clip takes
 *   a few minutes, so ffmpeg runs at low priority (nice) and the page
 *   keeps answering everyone else meanwhile.
 *
 * The upload arrives as a temp file (never a 50 MB buffer in a 512 MB
 * instance) and the work runs one video at a time: two x264 encodes at
 * once on a 0.5-CPU instance would each take twice as long and together
 * risk the memory limit.
 */

const fs = require('fs');
const os = require('os');
const path = require('path');
const { spawn } = require('child_process');
const { v4: uuidv4 } = require('uuid');

const VIDEO_LIMITS = { bytes: 50 * 1024 * 1024, seconds: 60 };
// 4K, cinema width included (4096 × 2160), either way up.
const MAX_PIXELS = 4096 * 2160;
// A phone's "60-second" clip measures a few frames over.
const SECONDS_SLACK = 0.5;

function clientError(message, code) {
  return Object.assign(new Error(message), { status: 400, code });
}

function run(cmd, args) {
  return new Promise((resolve, reject) => {
    const p = spawn(cmd, args);
    let out = '', err = '';
    p.stdout.on('data', d => { out += d; });
    p.stderr.on('data', d => { err += d; });
    p.on('error', reject);
    p.on('close', code => (code === 0 ? resolve(out) : reject(new Error(`${cmd} exit ${code}: ${err.slice(-400)}`))));
  });
}

// The heavy work at low priority: on a 0.5-CPU instance an encode at
// normal priority would hold up every page and API request meanwhile.
// `nice` is in every Debian image; without it, ffmpeg runs as it is.
let niceMissing = false;
async function runLow(cmd, args) {
  if (niceMissing) return run(cmd, args);
  try { return await run('nice', ['-n', '10', cmd, ...args]); }
  catch (err) {
    if (err.code !== 'ENOENT') throw err;
    niceMissing = true;
    return run(cmd, args);
  }
}

const DECODABLE_AUDIO = /^(aac|alac|mp3|mp2|opus|vorbis|flac|ac3|eac3|amr_nb|amr_wb|pcm_\w+)$/;

// What the file holds: its first video stream (codec, size, pixel format,
// transfer curve for HDR), whether it has sound, and its length. A file
// with no decodable video stream is the contributor's to fix, not a
// server error. Cover art in an audio file (attached_pic) is not a video.
async function probeVideo(file) {
  let info;
  try {
    info = JSON.parse(await run('ffprobe', ['-v', 'error', '-print_format', 'json',
      '-show_entries', 'stream=codec_type,codec_name,width,height,pix_fmt,color_transfer:stream_disposition=attached_pic:format=duration', file]));
  } catch {
    throw clientError('This video could not be read. Try an MP4 or MOV file.', 'video-unreadable');
  }
  const streams = info.streams || [];
  const v = streams.find(s => s.codec_type === 'video' && s.width > 0 && s.height > 0 && !(s.disposition && s.disposition.attached_pic));
  if (!v) {
    // A voice memo saved as .mp4 or .webm: it belongs with the sounds.
    throw streams.some(s => s.codec_type === 'audio')
      ? clientError('This file has only sound. Add it as a Sound.', 'video-sound-only')
      : clientError('This file has no video we can read. Try an MP4 or MOV file.', 'video-unreadable');
  }
  // The sound track to keep: AAC first, else the first one ffmpeg can
  // decode. A phone may add a second track in a format this ffmpeg build
  // has no decoder for; mapping that one would fail the whole encode,
  // while no track at all still leaves a silent clip.
  const audios = streams.filter(s => s.codec_type === 'audio');
  const a = audios.find(s => s.codec_name === 'aac') || audios.find(s => DECODABLE_AUDIO.test(s.codec_name || ''));
  const duration = parseFloat(info.format && info.format.duration);
  return {
    width: v.width, height: v.height, codec: v.codec_name || '', pixFmt: v.pix_fmt || '',
    duration: Number.isFinite(duration) && duration > 0 ? Math.round(duration * 10) / 10 : 0,
    hdr: /^(arib-std-b67|smpte2084)$/.test(v.color_transfer || ''),
    hasAudio: !!a, audioCodec: a ? a.codec_name || '' : '', audioIndex: a ? audios.indexOf(a) : -1,
  };
}

// Plays everywhere as it is: only the box around it changes.
const keepsPicture = info => info.codec === 'h264' && info.pixFmt === 'yuv420p' && !info.hdr &&
  Math.max(info.width, info.height) <= 1920;

function remuxArgs(input, output, { hasAudio, audioCodec, audioIndex = 0 }) {
  return ['-hide_banner', '-loglevel', 'error', '-y',
    '-i', input,
    '-t', String(VIDEO_LIMITS.seconds + SECONDS_SLACK),
    '-map', '0:v:0', ...(hasAudio ? ['-map', `0:a:${audioIndex}`] : []),
    '-c:v', 'copy',
    ...(hasAudio ? (audioCodec === 'aac' ? ['-c:a', 'copy'] : ['-c:a', 'aac', '-b:a', '128k', '-ac', '2']) : []),
    '-movflags', '+faststart',
    '-map_metadata', '-1',
    output];
}

// Longest side at most 1280 px, never enlarged; even dimensions (x264).
// ffmpeg turns an upright phone video the right way round on its own
// (autorotate) before this filter sees it.
const SCALE = "scale=w='min(1280,iw)':h='min(1280,ih)':force_original_aspect_ratio=decrease:force_divisible_by=2";
// An HDR clip (iPhone 12 and later film HDR by default) mapped down to
// ordinary colour; without it the picture plays grey and washed out.
const TONEMAP = 'zscale=t=linear:npl=100,format=gbrpf32le,zscale=p=bt709,tonemap=tonemap=hable:desat=0,zscale=t=bt709:m=bt709:r=tv,format=yuv420p';

function encodeArgs(input, output, { tonemap, hasAudio, audioIndex = 0, bigFrame = false }) {
  return ['-hide_banner', '-loglevel', 'error', '-y',
    // Decoder threads too: left alone, ffmpeg sizes them by the host's
    // cores, not the container's share, and HEVC frames add up in memory —
    // past 1080p, one thread (each holds its own frames).
    '-threads', bigFrame ? '1' : '2',
    '-i', input,
    '-t', String(VIDEO_LIMITS.seconds + SECONDS_SLACK),
    '-map', '0:v:0', ...(hasAudio ? ['-map', `0:a:${audioIndex}`] : []),
    // Scaled first: the tone-mapping works in floating point, per pixel,
    // and costs far less on 1280 px than on a 4K frame.
    '-vf', tonemap ? `${SCALE},${TONEMAP}` : `${SCALE},format=yuv420p`,
    '-fpsmax', '30',
    '-c:v', 'libx264', '-preset', 'veryfast', '-crf', '26', '-profile:v', 'high',
    '-threads', '2',
    ...(hasAudio ? ['-c:a', 'aac', '-b:a', '128k', '-ac', '2'] : []),
    '-movflags', '+faststart',
    '-map_metadata', '-1', // no GPS or device tags in a public file
    output];
}

// One video at a time, in arrival order.
let queue = Promise.resolve();
function serialised(job) {
  const next = queue.then(job, job);
  queue = next.catch(() => {});
  return next;
}

/**
 * Probe, check and re-encode one uploaded video. Returns the MP4 and a
 * JPEG poster frame as temp files the caller uploads and then removes
 * with `cleanup()`.
 */
function prepareVideo(inputPath) {
  return serialised(async () => {
    const info = await probeVideo(inputPath);
    if (info.duration > VIDEO_LIMITS.seconds + SECONDS_SLACK) {
      throw clientError(`This video is ${Math.round(info.duration)} seconds long. Trim it to ${VIDEO_LIMITS.seconds} seconds or less and try again.`, 'video-too-long');
    }
    // Every frame is decoded at full size before it is scaled down: an 8K
    // frame (some phones film it) takes the 512 MB instance past its memory.
    if (info.width * info.height > MAX_PIXELS) {
      throw clientError('This video is larger than 4K. Record or export it at 4K or lower and try again.', 'video-resolution');
    }
    const stamp = uuidv4();
    const mp4 = path.join(os.tmpdir(), `vid-${stamp}.mp4`);
    const poster = path.join(os.tmpdir(), `vid-${stamp}.jpg`);
    const cleanup = () => Promise.all([mp4, poster].map(f => fs.promises.unlink(f).catch(() => {})));
    try {
      let remuxed = false;
      if (keepsPicture(info)) {
        try { await run('ffmpeg', remuxArgs(inputPath, mp4, info)); remuxed = true; }
        catch (err) { console.error('[video] repackaging failed, re-encoding:', err.message.slice(0, 200)); }
      }
      if (!remuxed) {
        try {
          await runLow('ffmpeg', encodeArgs(inputPath, mp4, { tonemap: info.hdr, hasAudio: info.hasAudio, audioIndex: info.audioIndex, bigFrame: info.width * info.height > 1920 * 1080 }));
        } catch (err) {
          // A build without zimg cannot tone-map: an HDR clip in flat colour
          // still beats no clip at all.
          if (!info.hdr) throw err;
          console.error('[video] tone-mapping failed, encoding without it:', err.message.slice(0, 200));
          await runLow('ffmpeg', encodeArgs(inputPath, mp4, { tonemap: false, hasAudio: info.hasAudio, audioIndex: info.audioIndex, bigFrame: info.width * info.height > 1920 * 1080 }));
        }
      }
      // The poster: a frame a moment in, past a fade-in or a black first frame.
      let hasPoster = true;
      try {
        const at = info.duration > 2 ? 1 : 0;
        await run('ffmpeg', ['-hide_banner', '-loglevel', 'error', '-y', '-ss', String(at), '-i', mp4,
          '-frames:v', '1', '-q:v', '3', poster]);
      } catch (err) {
        console.error('[video] no poster frame:', err.message.slice(0, 200));
        hasPoster = false;
      }
      const out = await probeVideo(mp4);
      const { size } = await fs.promises.stat(mp4);
      return { mp4, poster: hasPoster ? poster : null, size, duration: out.duration || info.duration,
        width: out.width, height: out.height, remuxed, cleanup };
    } catch (err) {
      await cleanup();
      throw err;
    }
  });
}

module.exports = { prepareVideo, probeVideo, VIDEO_LIMITS, MAX_PIXELS, encodeArgs, remuxArgs, keepsPicture };
