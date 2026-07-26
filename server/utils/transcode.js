/**
 * ffmpeg transcode helpers.
 *
 * Why MP3 everywhere: in-browser recordings arrive as WebM/Opus (Chrome,
 * Android) or M4A/AAC (iOS Safari). Safari — macOS and iOS — cannot decode
 * Opus inside WebM, so those recordings play silently for every Apple
 * visitor. MP3 is the one format every browser decodes natively, so all
 * audio is normalised to MP3 192 kbps before it reaches B2.
 *
 * Output goes through a temp file, not a pipe: when ffmpeg writes MP3 to a
 * non-seekable pipe it cannot seek back to write the Xing/Info header, and
 * without that header browsers can't report duration (the old "--:--" bug).
 */

const fs   = require('fs');
const os   = require('os');
const path = require('path');
const { spawn } = require('child_process');
const { v4: uuidv4 } = require('uuid');

function isMp3Buffer(buf) {
  if (!buf || buf.length < 3) return false;
  // ID3 tag or raw MPEG frame sync
  if (buf[0] === 0x49 && buf[1] === 0x44 && buf[2] === 0x33) return true;
  return buf[0] === 0xff && (buf[1] & 0xe0) === 0xe0;
}

async function transcodeToMp3(inputBuffer, panFilter = null) {
  const stamp  = uuidv4();
  const tmpIn  = path.join(os.tmpdir(), `tc-in-${stamp}`);
  const tmpOut = path.join(os.tmpdir(), `tc-out-${stamp}.mp3`);
  await fs.promises.writeFile(tmpIn, inputBuffer);
  try {
    await new Promise((resolve, reject) => {
      const ff = spawn('ffmpeg', [
        '-hide_banner', '-loglevel', 'error', '-y',
        '-i', tmpIn,
        '-vn',                    // mp4/m4a uploads sometimes carry a video track
        ...(panFilter ? ['-af', panFilter] : []),
        '-acodec', 'libmp3lame',
        '-b:a', '192k',
        '-ar', '44100',
        tmpOut,
      ]);
      let errOut = '';
      ff.stderr.on('data', d => { errOut += d; });
      ff.on('error', reject);
      ff.on('close', code => code === 0
        ? resolve()
        : reject(new Error(`ffmpeg exit ${code}: ${errOut.toString().slice(0, 300)}`)));
    });
    return await fs.promises.readFile(tmpOut);
  } finally {
    fs.promises.unlink(tmpIn).catch(() => {});
    fs.promises.unlink(tmpOut).catch(() => {});
  }
}

// Detect "fake stereo": a 2-channel file where exactly one channel is
// digitally dead. Some phones (observed on iPhone 12 Safari, some
// Androids) hand the browser a stereo stream with only one live mic
// channel — stored as-is it plays in one earbud only. Returns a pan
// filter selecting the live channel, or null for mono / true stereo /
// any analysis failure (never block an upload over this).
// 76 of the archive's first 155 files had this defect before the guard.
const DEAD_CHANNEL_DB = -100;
async function detectFakeStereoPan(inputBuffer) {
  const stamp = uuidv4();
  const tmpIn = path.join(os.tmpdir(), `cs-${stamp}`);
  await fs.promises.writeFile(tmpIn, inputBuffer);
  try {
    const probe = await new Promise((resolve) => {
      const fp = spawn('ffprobe', ['-v', 'error', '-select_streams', 'a:0',
        '-show_entries', 'stream=channels', '-of', 'csv=p=0', tmpIn]);
      let out = ''; fp.stdout.on('data', d => out += d);
      fp.on('error', () => resolve('')); fp.on('close', () => resolve(out.trim()));
    });
    if (parseInt(probe, 10) < 2) return null;

    const stats = await new Promise((resolve) => {
      const ff = spawn('ffmpeg', ['-i', tmpIn,
        '-af', 'astats=measure_perchannel=RMS_level:measure_overall=none',
        '-f', 'null', '-']);
      let err = ''; ff.stderr.on('data', d => err += d);
      ff.on('error', () => resolve('')); ff.on('close', () => resolve(err));
    });
    const rms = [...stats.matchAll(/Channel: (\d+)\s[\s\S]*?RMS level dB: (-?[\d.]+|-inf)/g)]
      .map(m => ({ ch: parseInt(m[1], 10) - 1, db: m[2] === '-inf' ? -400 : parseFloat(m[2]) }));
    if (rms.length < 2) return null;
    const live = rms.filter(r => r.db >= DEAD_CHANNEL_DB);
    if (live.length !== 1) return null; // mono-ish silence or true stereo
    return `pan=mono|c0=c${live[0].ch}`;
  } catch {
    return null;
  } finally {
    fs.promises.unlink(tmpIn).catch(() => {});
  }
}

// Audio length in seconds (float), measured with ffprobe via a temp file.
// Returns 0 on any failure — duration is cosmetic, never worth failing an
// upload over.
async function probeDuration(buffer) {
  const tmp = path.join(os.tmpdir(), `probe-${uuidv4()}`);
  await fs.promises.writeFile(tmp, buffer);
  try {
    const out = await new Promise((resolve) => {
      const fp = spawn('ffprobe', [
        '-v', 'error',
        '-show_entries', 'format=duration',
        '-of', 'csv=p=0',
        tmp,
      ]);
      let stdout = '';
      fp.stdout.on('data', d => { stdout += d; });
      fp.on('error', () => resolve(''));
      fp.on('close', () => resolve(stdout));
    });
    const dur = parseFloat(out);
    return isFinite(dur) && dur > 0 ? Math.round(dur * 10) / 10 : 0;
  } finally {
    fs.promises.unlink(tmp).catch(() => {});
  }
}

module.exports = { transcodeToMp3, isMp3Buffer, probeDuration, detectFakeStereoPan };
