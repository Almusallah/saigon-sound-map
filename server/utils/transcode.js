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

async function transcodeToMp3(inputBuffer) {
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

module.exports = { transcodeToMp3, isMp3Buffer };
