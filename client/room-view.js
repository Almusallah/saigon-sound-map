/*!
 * room-view.js — the visual + navigation layer of the Phòng nghe (listening room).
 * Saigon_Miền Tây Sound Map.
 *
 * Vanilla JS, Canvas 2D, own perspective projection. No dependencies.
 *
 *   const view = RoomView.create(canvas, {
 *     theme: 'black' | 'white',
 *     bounds: 40,                         // room = square x,z in [-bounds, bounds] metres, y up
 *     listener: { x: 0, z: 18, yaw: 0 }, // yaw 0 looks toward -z (north); +yaw turns right
 *     onMove(listener) {},                // {x, z, yaw}, throttled to <= 30/s
 *   });
 *   view.setSources([{ id, x, z, label, radius }]);
 *   view.setLevels({ [id]: 0..1 });
 *   view.setTheme('white');
 *   view.setListener({ x, z, yaw }, { glide: true });
 *   view.setCaption('DAWN · 01 / 06');
 *   view.getListener();
 *   view.destroy();
 *
 * Coordinates: x = east (+) / west (-), z = south (+) / north (-). Eye height 1.6 m.
 * Forward vector = (sin yaw, -cos yaw); right vector = (cos yaw, sin yaw).
 */
(function (root) {
  'use strict';

  var PI = Math.PI, TAU = PI * 2;

  // ---- world --------------------------------------------------------------
  var EYE = 1.6;            // eye height (m)
  var NEAR = 0.1;           // near plane (m)
  var WALL_H = 14;          // suggested wall height (m)
  var GRID = 4;             // floor grid pitch (m)
  var SUB = 2;              // grid segment length used for fog shading (m)
  var WALL_MARGIN = 1;      // listener keeps this far from the walls (m)

  // ---- lens ---------------------------------------------------------------
  var VFOV = 70 * PI / 180;      // vertical field of view
  var MIN_HFOV = 56 * PI / 180;  // never narrower than this horizontally (portrait phones)
  var HORIZON = 0.5;             // horizon height as a fraction of the canvas (no pitch)

  // ---- motion -------------------------------------------------------------
  var WALK = 4;             // m/s
  var TURN = 1.45;          // rad/s
  var FAST = 2.2;           // Shift multiplier (walk); turn uses 1.6x
  var GLIDE_T = 1.2;        // setListener({glide:true}) duration (s)

  // ---- look ---------------------------------------------------------------
  var GRID_A = 0.22, WALL_A = 0.15, HORIZON_A = 0.10, RING_A = 0.085, ONSET_A = 0.2;
  var FOG_GRID = 1 / (23.5 * 23.5); // grid fades to background by ~45 m
  var FOG_RING = 1 / (26 * 26);
  var FOG_WALL = 1 / (78 * 78);   // walls: faint but present across the whole room
  var FOG_SRC = 1 / (46 * 46);
  var LABEL_FAR = 18;             // labels only within this distance (m)
  var FONT_PX = 10;
  var FONT_STACK = 'ui-monospace, "SF Mono", SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace';
  var THEMES = {
    black: { bg: [0, 0, 0], ink: [255, 255, 255] },
    white: { bg: [243, 243, 241], ink: [18, 18, 18] }
  };

  // ---- segment batching (alpha buckets) -----------------------------------
  var NB = 64, AMAX = 0.64, ASTEP = AMAX / NB, AMIN = ASTEP * 0.5;
  var MAXSEG = 12000, MAXPT = 4096;
  var POINT_A = 0.42;
  var CIRC = 96;   // ring resolution near the listener; far rings use every other vertex
  var CCOS = new Float32Array(CIRC + 1), CSIN = new Float32Array(CIRC + 1);
  for (var ci = 0; ci <= CIRC; ci++) { CCOS[ci] = Math.cos(ci / CIRC * TAU); CSIN[ci] = Math.sin(ci / CIRC * TAU); }

  var KEYMAP = {
    ArrowUp: 'f', KeyW: 'f', Up: 'f',
    ArrowDown: 'b', KeyS: 'b', Down: 'b',
    ArrowLeft: 'l', KeyA: 'l', Left: 'l',
    ArrowRight: 'r', KeyD: 'r', Right: 'r'
  };

  function clamp(v, a, b) { return v < a ? a : v > b ? b : v; }
  function smooth01(t) { t = t < 0 ? 0 : t > 1 ? 1 : t; return t * t * (3 - 2 * t); }
  function easeInOut(p) { return p < 0.5 ? 4 * p * p * p : 1 - Math.pow(-2 * p + 2, 3) / 2; }
  function easeOut(p) { return 1 - (1 - p) * (1 - p) * (1 - p); }
  function normYaw(a) {
    a = a % TAU;
    if (a > PI) a -= TAU; else if (a <= -PI) a += TAU;
    return a;
  }
  function isNum(v) { return typeof v === 'number' && isFinite(v); }

  // Split an upper-cased label into drawable clusters (keeps combining marks attached).
  function clusters(str) {
    var out = [];
    if (str == null) return out;
    str = String(str);
    if (str.normalize) str = str.normalize('NFC');
    str = str.toUpperCase();
    for (var i = 0; i < str.length; i++) {
      var c = str.charCodeAt(i), ch = str[i];
      if (c >= 0xD800 && c <= 0xDBFF && i + 1 < str.length) { ch += str[++i]; }
      var comb = (c >= 0x0300 && c <= 0x036F) || (c >= 0x1AB0 && c <= 0x1AFF) ||
                 (c >= 0x1DC0 && c <= 0x1DFF) || (c >= 0x20D0 && c <= 0x20FF) || (c >= 0xFE20 && c <= 0xFE2F);
      if (comb && out.length) out[out.length - 1] += ch; else out.push(ch);
    }
    return out;
  }

  function create(canvas, opts) {
    if (!canvas || typeof canvas.getContext !== 'function') throw new TypeError('RoomView.create: a <canvas> is required');
    opts = opts || {};

    var win = root;
    var doc = root.document || null;
    var ctx = canvas.getContext('2d', { alpha: false }) || canvas.getContext('2d');
    if (!ctx) throw new Error('RoomView.create: 2D context unavailable');

    var B = isNum(opts.bounds) && opts.bounds > 2 ? opts.bounds : 40;
    var LABEL_TOP = isNum(opts.labelTop) ? opts.labelTop : 30;   // keep labels clear of a page's header
    var LO = -B + WALL_MARGIN, HI = B - WALL_MARGIN;
    var onMove = typeof opts.onMove === 'function' ? opts.onMove : null;
    var fontStack = typeof opts.fontFamily === 'string' && opts.fontFamily ? opts.fontFamily : FONT_STACK;
    var FONT = '500 ' + FONT_PX + 'px ' + fontStack;

    function clampL(v) { return v < LO ? LO : v > HI ? HI : v; }

    // ---- listener & motion state ----
    var lis = { x: 0, z: 18, yaw: 0 };
    if (opts.listener) {
      if (isNum(opts.listener.x)) lis.x = opts.listener.x;
      if (isNum(opts.listener.z)) lis.z = opts.listener.z;
      if (isNum(opts.listener.yaw)) lis.yaw = opts.listener.yaw;
    }
    lis.x = clampL(lis.x); lis.z = clampL(lis.z); lis.yaw = normYaw(lis.yaw);

    var vel = 0, angVel = 0;
    var keys = { f: 0, b: 0, l: 0, r: 0, shift: false };
    var latch = { f: 0, b: 0, l: 0, r: 0 };   // a press shorter than a frame still counts once
    var gliding = false, gT = 0, gDur = 1, gx0 = 0, gz0 = 0, gy0 = 0, gx1 = 0, gz1 = 0, gdy = 0;
    var drag = { id: null, active: false, moved: false, axis: 0, x0: 0, y0: 0, lx: 0, ly: 0, t0: 0, lt: 0, v: 0, w: 0, type: '' };
    var emX = NaN, emZ = NaN, emYaw = NaN, emT = -1, emitting = false;

    // ---- view state ----
    var clock = 0;
    var themeTarget = opts.theme === 'white' ? 1 : 0, themeT = themeTarget, themeDirty = true;
    var bgStr = '#000', inkStr = '#fff', inkRGB = [255, 255, 255], aScale = 1, glowK = 1, tE = 0;
    var W = 0, H = 0, dpr = 1, focal = 1, cx = 0, hy = 0, needMeasure = true;
    var kLp = 0, kRp = 0, kTp = 0, kBp = 0;     // frustum plane constants
    var camX = 0, camZ = 0, camYaw = 0, eyeY = EYE, sinY = 0, cosY = 1;
    var adv = 0, SP = FONT_PX * 0.12;            // monospace advance + letter-spacing
    var hazeGrad = null, hazeDirty = true, hazeY0 = 0, hazeY1 = 0;
    var mapPref = null, mapA = 0, mapRect = [0, 0, 0, 0], chipRect = [0, 0, 0, 0], chipOn = false, mapShown = false;
    var capCur = [], capPrev = [], capT = 1, capRaw = '';
    var reduced = false, destroyed = false;
    var stats = { sources: 0, labels: 0, segments: 0, frames: 0 };

    // ---- sources ----
    var sources = [], byId = Object.create(null), gen = 0;
    var visI = new Int32Array(64), visD = new Float32Array(64), nVis = 0;
    var claim = new Float32Array(64 * 4), nClaim = 0;

    // ---- segment buffers ----
    var SEG = new Float32Array(MAXSEG * 4), SEGB = new Uint8Array(MAXSEG), ORDER = new Uint16Array(MAXSEG);
    var CNT = new Int32Array(NB + 1), START = new Int32Array(NB + 1), CUR = new Int32Array(NB + 1), segN = 0;
    var T0 = 0, T1 = 1;
    var PT = new Float32Array(MAXPT * 3), PTB = new Uint8Array(MAXPT), PCNT = new Int32Array(NB + 1), ptN = 0;

    // grid line positions
    var gridVals = [];
    (function () {
      var v = Math.ceil(-B / GRID) * GRID;
      if (v > -B + 1e-6) gridVals.push(-B);
      for (; v <= B + 1e-6; v += GRID) gridVals.push(v);
      if (gridVals[gridVals.length - 1] < B - 1e-6) gridVals.push(B);
    })();

    // ---- glow sprites (browser only; strokes are used as a fallback) ----
    var sprGlow = null, sprPool = null, sprRefl = null;
    function makeCanvas(w, h) {
      if (!doc || !doc.createElement) return null;
      var c = doc.createElement('canvas');
      if (!c || !c.getContext) return null;
      c.width = w; c.height = h;
      return c;
    }
    function paintSprite(c, fn) {
      if (!c) return;
      var g = c.getContext('2d'); if (!g || !g.createImageData) return;
      var w = c.width, h = c.height, img = g.createImageData(w, h), d = img.data;
      for (var y = 0; y < h; y++) for (var x = 0; x < w; x++) {
        var a = fn((x + 0.5) / w, (y + 0.5) / h), o = (y * w + x) * 4;
        d[o] = inkRGB[0]; d[o + 1] = inkRGB[1]; d[o + 2] = inkRGB[2]; d[o + 3] = Math.round(255 * clamp(a, 0, 1));
      }
      g.putImageData(img, 0, 0);
    }
    function buildSprites() {
      if (!sprGlow) { sprGlow = makeCanvas(32, 64); sprPool = makeCanvas(64, 64); sprRefl = makeCanvas(16, 64); }
      if (!sprGlow) return;
      paintSprite(sprGlow, function (u, v) {      // vertical halo, soft ends
        var x = (u - 0.5) * 4.2;
        var ends = smooth01(v / 0.14) * smooth01((1 - v) / 0.06);
        return Math.exp(-x * x) * ends;
      });
      paintSprite(sprPool, function (u, v) {      // round pool of light (drawn flattened)
        var x = (u - 0.5) * 2, y = (v - 0.5) * 2, r2 = x * x + y * y;
        return r2 >= 1 ? 0 : Math.exp(-r2 * 3.2) * (1 - r2);
      });
      paintSprite(sprRefl, function (u, v) {      // reflection: narrow, fading downward
        var x = (u - 0.5) * 5;
        return Math.exp(-x * x) * Math.pow(1 - v, 2.2);
      });
    }

    // ---- canvas setup ----
    if (canvas.setAttribute) {
      if (!canvas.hasAttribute || !canvas.hasAttribute('tabindex')) canvas.setAttribute('tabindex', '0');
      canvas.setAttribute('role', 'application');
      if (!canvas.hasAttribute || !canvas.hasAttribute('aria-label')) {
        canvas.setAttribute('aria-label',
          'Listening room. Arrow keys or W A S D: walk forward and back, turn left and right; hold Shift to move faster. ' +
          'Drag sideways to turn, drag up or down to walk; click or tap the floor to glide there; scroll to walk. M toggles the plan.');
      }
    }
    if (canvas.style) {
      canvas.style.touchAction = 'none';
      canvas.style.userSelect = 'none';
      canvas.style.webkitUserSelect = 'none';
      canvas.style.webkitTapHighlightColor = 'transparent';
    }

    function now() { return (win.performance && win.performance.now) ? win.performance.now() : Date.now(); }

    // =========================================================================
    // Measure / projection
    // =========================================================================
    function measure() {
      var cw = canvas.clientWidth || 0, ch = canvas.clientHeight || 0;
      if ((!cw || !ch) && canvas.getBoundingClientRect) {
        var r = canvas.getBoundingClientRect(); cw = r.width; ch = r.height;
      }
      var d = Math.min(win.devicePixelRatio || 1, 2);
      needMeasure = false;
      if (cw === W && ch === H && d === dpr) return;
      W = cw; H = ch; dpr = d;
      canvas.width = Math.max(1, Math.round(W * dpr));
      canvas.height = Math.max(1, Math.round(H * dpr));
      projection();
      hazeDirty = true;
    }
    function projection() {
      cx = W / 2; hy = Math.round(H * HORIZON);
      var fV = (H / 2) / Math.tan(VFOV / 2), fH = (W / 2) / Math.tan(MIN_HFOV / 2);
      focal = Math.max(1, Math.min(fV, fH));
      var m = 4;   // margin beyond the canvas edge, px
      kLp = cx + m; kRp = W + m - cx; kTp = hy + m; kBp = H + m - hy;
    }
    function setupCamera(sway) {
      var se = 0, sy = 0;
      if (sway && !reduced) {
        se = 0.012 * Math.sin(clock * 0.41);
        sy = 0.0022 * Math.sin(clock * 0.23 + 1.3);
      }
      camX = lis.x; camZ = lis.z; camYaw = lis.yaw + sy; eyeY = EYE + se;
      sinY = Math.sin(camYaw); cosY = Math.cos(camYaw);
    }

    // =========================================================================
    // Theme
    // =========================================================================
    function updateColors() {
      tE = smooth01(themeT);
      var a = THEMES.black, b = THEMES.white;
      var br = Math.round(a.bg[0] + (b.bg[0] - a.bg[0]) * tE), bgc = Math.round(a.bg[1] + (b.bg[1] - a.bg[1]) * tE), bb = Math.round(a.bg[2] + (b.bg[2] - a.bg[2]) * tE);
      inkRGB = [Math.round(a.ink[0] + (b.ink[0] - a.ink[0]) * tE), Math.round(a.ink[1] + (b.ink[1] - a.ink[1]) * tE), Math.round(a.ink[2] + (b.ink[2] - a.ink[2]) * tE)];
      bgStr = 'rgb(' + br + ',' + bgc + ',' + bb + ')';
      inkStr = 'rgb(' + inkRGB[0] + ',' + inkRGB[1] + ',' + inkRGB[2] + ')';
      aScale = 1 + 0.45 * tE;          // dark ink on light ground reads weaker: lift it a little
      glowK = 1 - 0.55 * tE;           // halos read as smudges on white: keep them subtler
      buildSprites();
      hazeDirty = true;
      themeDirty = false;
    }
    function buildHaze() {
      hazeDirty = false;
      if (!ctx.createLinearGradient || !H) { hazeGrad = null; return; }
      hazeY0 = hy - H * 0.22; hazeY1 = hy + H * 0.09;
      var g = ctx.createLinearGradient(0, hazeY0, 0, hazeY1);
      var ha = 0.075 + (0.9 - 0.075) * tE;    // black: faint luminous band; white: a brighter band of light
      var mid = (hy - hazeY0) / (hazeY1 - hazeY0);
      g.addColorStop(0, 'rgba(255,255,255,0)');
      g.addColorStop(mid * 0.45, 'rgba(255,255,255,' + (ha * 0.12).toFixed(4) + ')');
      g.addColorStop(mid * 0.8, 'rgba(255,255,255,' + (ha * 0.45).toFixed(4) + ')');
      g.addColorStop(mid, 'rgba(255,255,255,' + ha.toFixed(4) + ')');
      g.addColorStop(mid + (1 - mid) * 0.4, 'rgba(255,255,255,' + (ha * 0.3).toFixed(4) + ')');
      g.addColorStop(1, 'rgba(255,255,255,0)');
      hazeGrad = g;
    }

    // =========================================================================
    // Segment pipeline: world -> camera -> clip (near + frustum) -> screen -> alpha bucket
    // =========================================================================
    function cp(fa, fb) {
      var t;
      if (fa < 0) { if (fb < 0) return false; t = fa / (fa - fb); if (t > T0) T0 = t; }
      else if (fb < 0) { t = fa / (fa - fb); if (t < T1) T1 = t; }
      return T0 <= T1;
    }
    function camSeg(ar, au, ad, br, bu, bd, a) {
      T0 = 0; T1 = 1;
      if (!cp(ad - NEAR, bd - NEAR)) return;
      if (!cp(focal * ar + kLp * ad, focal * br + kLp * bd)) return;
      if (!cp(kRp * ad - focal * ar, kRp * bd - focal * br)) return;
      if (!cp(kTp * ad - focal * au, kTp * bd - focal * bu)) return;
      if (!cp(kBp * ad + focal * au, kBp * bd + focal * bu)) return;
      var dr = br - ar, du = bu - au, dd = bd - ad;
      var r0 = ar + dr * T0, u0 = au + du * T0, d0 = ad + dd * T0;
      var r1 = ar + dr * T1, u1 = au + du * T1, d1 = ad + dd * T1;
      if (d0 < NEAR) d0 = NEAR; if (d1 < NEAR) d1 = NEAR;   // float guard
      pushSeg(cx + focal * r0 / d0, hy - focal * u0 / d0, cx + focal * r1 / d1, hy - focal * u1 / d1, a);
    }
    function worldSeg(ax, ay, az, bx, by, bz, a) {
      if (a < AMIN) return;
      var dx = ax - camX, dz = az - camZ;
      var ar = dx * cosY + dz * sinY, ad = dx * sinY - dz * cosY;
      dx = bx - camX; dz = bz - camZ;
      var br = dx * cosY + dz * sinY, bd = dx * sinY - dz * cosY;
      camSeg(ar, ay - eyeY, ad, br, by - eyeY, bd, a);
    }
    function pushSeg(x0, y0, x1, y1, a) {
      var b = Math.round(a / ASTEP);
      if (b < 1 || segN >= MAXSEG) return;
      if (b > NB) b = NB;
      var o = segN * 4;
      SEG[o] = x0; SEG[o + 1] = y0; SEG[o + 2] = x1; SEG[o + 3] = y1;
      SEGB[segN++] = b; CNT[b]++;
    }
    function flushSegs() {
      var off = 0, b, i, j, n, o;
      for (b = 1; b <= NB; b++) { START[b] = off; CUR[b] = off; off += CNT[b]; }
      for (i = 0; i < segN; i++) ORDER[CUR[SEGB[i]]++] = i;
      ctx.lineWidth = 1; ctx.lineCap = 'butt'; ctx.strokeStyle = inkStr;
      for (b = 1; b <= NB; b++) {
        n = CNT[b]; if (!n) continue;
        ctx.globalAlpha = b * ASTEP;
        ctx.beginPath();
        for (j = START[b]; j < START[b] + n; j++) {
          o = ORDER[j] * 4;
          ctx.moveTo(SEG[o], SEG[o + 1]); ctx.lineTo(SEG[o + 2], SEG[o + 3]);
        }
        ctx.stroke();
        CNT[b] = 0;
      }
      stats.segments = segN;
      segN = 0;
    }

    function addGrid() {
      var base = GRID_A * aScale, g, v, p, s, e, m, d2, a;
      for (g = 0; g < gridVals.length; g++) {
        v = gridVals[g];
        p = v - camZ;                      // line z = v, running east-west
        if (p * p < 2600) {
          for (s = -B; s < B - 1e-6; s += SUB) {
            e = s + SUB; if (e > B) e = B;
            m = (s + e) * 0.5 - camX; d2 = m * m + p * p;
            a = base * Math.exp(-d2 * FOG_GRID);
            if (a >= AMIN) worldSeg(s, 0, v, e, 0, v, a);
          }
        }
        p = v - camX;                      // line x = v, running north-south
        if (p * p < 2600) {
          for (s = -B; s < B - 1e-6; s += SUB) {
            e = s + SUB; if (e > B) e = B;
            m = (s + e) * 0.5 - camZ; d2 = m * m + p * p;
            a = base * Math.exp(-d2 * FOG_GRID);
            if (a >= AMIN) worldSeg(v, 0, s, v, 0, e, a);
          }
        }
      }
    }
    function addPoints() {                 // points at the grid crossings
      var base = POINT_A * aScale, i, j, x, z, dx, dz, d2, a, cr, cd, inv, sx, sy, b;
      for (i = 0; i < gridVals.length; i++) {
        x = gridVals[i]; dx = x - camX;
        if (dx * dx > 2600) continue;
        for (j = 0; j < gridVals.length; j++) {
          z = gridVals[j]; dz = z - camZ; d2 = dx * dx + dz * dz;
          if (d2 > 2600) continue;
          cd = dx * sinY - dz * cosY;
          if (cd < 0.5) continue;
          a = base * Math.exp(-d2 * FOG_GRID);
          b = Math.round(a / ASTEP); if (b < 1 || ptN >= MAXPT) continue; if (b > NB) b = NB;
          cr = dx * cosY + dz * sinY; inv = focal / cd;
          sx = cx + cr * inv; sy = hy + eyeY * inv;
          if (sx < -4 || sx > W + 4 || sy > H + 4) continue;
          PT[ptN * 3] = sx; PT[ptN * 3 + 1] = sy; PT[ptN * 3 + 2] = clamp(inv * 0.035, 0.9, 1.8);
          PTB[ptN++] = b; PCNT[b]++;
        }
      }
    }
    function flushPoints() {
      var b, i, o, r;
      ctx.fillStyle = inkStr;
      for (b = 1; b <= NB; b++) {
        if (!PCNT[b]) continue;
        ctx.globalAlpha = b * ASTEP;
        ctx.beginPath();
        for (i = 0; i < ptN; i++) {
          if (PTB[i] !== b) continue;
          o = i * 3; r = PT[o + 2];
          ctx.rect(PT[o] - r * 0.5, PT[o + 1] - r * 0.5, r, r);
        }
        ctx.fill();
        PCNT[b] = 0;
      }
      ptN = 0;
    }
    function addWalls() {
      var base = WALL_A * aScale, k, x, z, dx, dz, a, s, e, m;
      for (k = 0; k < 4; k++) {            // the four corners: vertical edges
        x = (k & 1) ? B : -B; z = (k & 2) ? B : -B;
        dx = x - camX; dz = z - camZ;
        a = base * Math.exp(-(dx * dx + dz * dz) * FOG_WALL);
        worldSeg(x, 0, z, x, WALL_H * 0.5, z, a);
        worldSeg(x, WALL_H * 0.5, z, x, WALL_H, z, a * 0.85);
      }
      for (k = 0; k < 4; k++) {            // top edges (and a fainter base line)
        for (s = -B; s < B - 1e-6; s += GRID) {
          e = s + GRID; if (e > B) e = B; m = (s + e) * 0.5;
          if (k < 2) { z = k ? B : -B; dx = m - camX; dz = z - camZ; }
          else { x = (k === 3) ? B : -B; dx = x - camX; dz = m - camZ; }
          a = base * Math.exp(-(dx * dx + dz * dz) * FOG_WALL);
          if (a < AMIN) continue;
          if (k < 2) { worldSeg(s, WALL_H, z, e, WALL_H, z, a * 0.85); worldSeg(s, 0, z, e, 0, z, a * 0.5); }
          else { worldSeg(x, WALL_H, s, x, WALL_H, e, a * 0.85); worldSeg(x, 0, s, x, 0, e, a * 0.5); }
        }
      }
    }
    function addCircle(x0, z0, r, a, fog) {
      var k, ax, az, bx, bz, mx, mz, dx = x0 - camX, dz = z0 - camZ;
      var st = Math.sqrt(dx * dx + dz * dz) - r < 9 ? 1 : 2;
      for (k = 0; k < CIRC; k += st) {
        ax = x0 + r * CCOS[k]; az = z0 + r * CSIN[k];
        bx = x0 + r * CCOS[k + st]; bz = z0 + r * CSIN[k + st];
        mx = (ax + bx) * 0.5 - camX; mz = (az + bz) * 0.5 - camZ;
        worldSeg(ax, 0, az, bx, 0, bz, a * Math.exp(-(mx * mx + mz * mz) * fog));
      }
    }
    function addRings() {
      for (var i = 0; i < sources.length; i++) {
        var s = sources[i]; if (s.pres <= 0.002) continue;
        var pr = smooth01(s.pres);
        var dx = s.x - camX, dz = s.z - camZ, dc = Math.sqrt(dx * dx + dz * dz);
        if (s.radius > 0.3 && dc - s.radius < 60) addCircle(s.x, s.z, s.radius, RING_A * aScale * pr * (0.6 + 0.4 * s.lvl), FOG_RING);
        if (s.ring >= 0) {
          var p = s.ring, rMax = clamp(s.radius > 0 ? s.radius * 0.7 : 8, 3, 14);
          var q = 1 - p;
          addCircle(s.x, s.z, 0.3 + (rMax - 0.3) * easeOut(p), ONSET_A * aScale * pr * q * q, FOG_RING);
        }
      }
    }

    // =========================================================================
    // Sources: pillars of light
    // =========================================================================
    // canvas silently ignores globalAlpha outside [0, 1] — always clamp
    function ga(v) { ctx.globalAlpha = v > 1 ? 1 : v > 0 ? v : 0; }
    function line(x0, y0, x1, y1) { ctx.beginPath(); ctx.moveTo(x0, y0); ctx.lineTo(x1, y1); ctx.stroke(); }

    function drawSources() {
      var i, k, j, s, dx, dz, sx, n = sources.length;
      if (visI.length < n) { var cap = Math.max(n, visI.length * 2); visI = new Int32Array(cap); visD = new Float32Array(cap); claim = new Float32Array(cap * 4); }
      nVis = 0;
      for (i = 0; i < n; i++) {
        s = sources[i];
        dx = s.x - camX; dz = s.z - camZ;
        s.cd = dx * sinY - dz * cosY;
        s.cr = dx * cosY + dz * sinY;
        s.dist = Math.sqrt(dx * dx + dz * dz);
        s.vis = false;
        if (s.pres <= 0.002 || s.cd < 0.3) continue;      // behind the camera (or standing inside it)
        sx = cx + focal * s.cr / s.cd;
        if (sx < -60 || sx > W + 60) continue;
        s.sx = sx; s.vis = true;
        visI[nVis] = i; visD[nVis] = s.cd; nVis++;
      }
      for (k = 1; k < nVis; k++) {                          // far -> near (insertion sort, no allocation)
        var vi = visI[k], vd = visD[k]; j = k - 1;
        while (j >= 0 && visD[j] < vd) { visI[j + 1] = visI[j]; visD[j + 1] = visD[j]; j--; }
        visI[j + 1] = vi; visD[j + 1] = vd;
      }
      ctx.strokeStyle = inkStr; ctx.fillStyle = inkStr;
      var drawn = 0;
      for (k = 0; k < nVis; k++) if (drawPillar(sources[visI[k]])) drawn++;
      stats.sources = drawn;
    }

    function drawPillar(s) {
      var inv = focal / s.cd, sx = s.sx, lvl = s.lvl;
      var h = 1.5 + 9 * lvl;
      var yB = hy + eyeY * inv, yT = hy - (h - eyeY) * inv;
      var pr = smooth01(s.pres);
      var fog = 0.35 + 0.65 * Math.exp(-s.dist * s.dist * FOG_SRC);
      var a = (0.25 + 0.75 * lvl) * pr * fog;
      s.yT = yT; s.yB = yB; s.a = a;
      if (a < 0.004) return false;
      // keep geometry near the viewport (a source a step away would otherwise span 10 000 px)
      var topCut = yT < -40, botCut = yB > H + 40;
      var yTc = topCut ? -40 - H * 0.2 : yT, yBc = botCut ? H + 40 : yB;
      var len = yBc - yTc;
      if (sprGlow) {
        // pool of light on the floor (a flattened disc, perspective-correct aspect)
        var rho = 0.7 + 1.7 * lvl, rw = rho * inv, rh = Math.max(0.8, rw * eyeY / s.cd);
        if (!botCut && rw < W * 3) {
          ctx.globalAlpha = a * 0.30 * glowK;
          ctx.drawImage(sprPool, sx - rw, yB - rh, rw * 2, rh * 2);
          // reflection in the floor
          var rl = Math.min(H, (yB - yT) * 0.34), gw0 = clamp(inv * 0.10, 3, 10);
          ctx.globalAlpha = a * 0.42;
          ctx.drawImage(sprRefl, sx - gw0 / 2, yB, gw0, rl);
        }
        // halo
        var gw = clamp(inv * 0.55, 7, 46), pad = topCut ? 0 : gw * 0.25;
        ctx.globalAlpha = a * 0.5 * glowK;
        ctx.drawImage(sprGlow, sx - gw / 2, yTc - pad, gw, len + pad * 1.2);
      } else {
        ctx.lineCap = 'round';
        ctx.globalAlpha = a * 0.06 * glowK; ctx.lineWidth = clamp(inv * 0.4, 6, 24); line(sx, yBc, sx, yTc);
        ctx.globalAlpha = a * 0.16 * glowK; ctx.lineWidth = clamp(inv * 0.12, 2.5, 7); line(sx, yBc, sx, yTc);
        ctx.lineCap = 'butt';
      }
      // the line itself
      ctx.globalAlpha = a; ctx.lineWidth = 1.15;
      line(sx, yBc, sx, yTc);
      // base point and a fainter crown point
      ctx.globalAlpha = Math.min(1, a * 1.25);
      if (!botCut) { ctx.beginPath(); ctx.arc(sx, yB, clamp(inv * 0.05, 1.1, 2.4), 0, TAU); ctx.fill(); }
      ctx.globalAlpha = a * 0.7;
      if (!topCut) { ctx.beginPath(); ctx.arc(sx, yT, 1, 0, TAU); ctx.fill(); }
      return true;
    }

    function drawLabels() {
      if (!nVis) { stats.labels = 0; return; }
      ctx.font = FONT; ctx.textBaseline = 'alphabetic'; ctx.textAlign = 'left'; ctx.fillStyle = inkStr;
      if (!adv) { var mt = ctx.measureText('M'); adv = (mt && mt.width) || FONT_PX * 0.6; }
      nClaim = 0;
      var drawn = 0, k, s, c;
      for (k = nVis - 1; k >= 0; k--) {                  // nearest first: they win collisions
        s = sources[visI[k]];
        var nC = s.label.length;
        if (!nC || s.dist > LABEL_FAR || !(s.a > 0.004)) continue;
        var la = smooth01((LABEL_FAR - s.dist) / 7) * (0.5 + 0.5 * s.lvl) * smooth01(s.pres) * 0.85;
        if (la < 0.02) continue;
        var hp = 1.5 + 9 * s.peak;
        var yTop = hy - (hp - eyeY) * focal / s.cd;
        var y = Math.round(Math.max(LABEL_TOP, Math.min(yTop, s.yT) - 11));
        var w = nC * adv + (nC - 1) * SP;
        var x0 = Math.round(s.sx - w / 2);
        var rx0 = x0 - 6, rx1 = x0 + w + 6, ry0 = y - FONT_PX - 3, ry1 = y + 4, hit = false;
        for (c = 0; c < nClaim; c++) {
          var o = c * 4;
          if (rx0 < claim[o + 2] && rx1 > claim[o] && ry0 < claim[o + 3] && ry1 > claim[o + 1]) { hit = true; break; }
        }
        if (hit) continue;
        var q = nClaim * 4; claim[q] = rx0; claim[q + 1] = ry0; claim[q + 2] = rx1; claim[q + 3] = ry1; nClaim++;
        ga(la * Math.min(1, aScale));
        for (c = 0; c < nC; c++) { var ch = s.label[c]; if (ch !== ' ') ctx.fillText(ch, x0 + c * (adv + SP), y); }
        drawn++;
      }
      stats.labels = drawn;
    }

    // =========================================================================
    // Overlay: caption + plan view
    // =========================================================================
    function spaced(cl, x, y) {
      for (var c = 0; c < cl.length; c++) { var ch = cl[c]; if (ch !== ' ') ctx.fillText(ch, x + c * (adv + SP), y); }
    }
    function drawCaption() {
      if (!capCur.length && !capPrev.length) return;
      ctx.font = FONT; ctx.textBaseline = 'alphabetic'; ctx.textAlign = 'left'; ctx.fillStyle = inkStr;
      if (!adv) { var mt = ctx.measureText('M'); adv = (mt && mt.width) || FONT_PX * 0.6; }
      var x = 20, y = H - 22;
      var aOut = 1 - smooth01(capT / 0.45), aIn = smooth01((capT - 0.35) / 0.5);
      if (capPrev.length && aOut > 0.01) { ctx.globalAlpha = 0.62 * aOut; spaced(capPrev, x, y); }
      if (capCur.length && aIn > 0.01) { ctx.globalAlpha = 0.62 * aIn; spaced(capCur, x, y); }
    }
    function drawMap() {
      var small = W < 400;
      mapShown = mapPref === null ? !small : mapPref;
      chipOn = small;
      var m = 20, S = Math.round(Math.min(110, Math.min(W, H) * 0.3));
      ctx.font = FONT; ctx.textBaseline = 'alphabetic'; ctx.textAlign = 'left'; ctx.fillStyle = inkStr; ctx.strokeStyle = inkStr;
      if (!adv) { var mt = ctx.measureText('M'); adv = (mt && mt.width) || FONT_PX * 0.6; }
      var chipH = 0;
      if (chipOn) {
        var cw = 4 * adv + 3 * SP;
        chipRect[0] = W - m - cw - 12; chipRect[1] = H - m - 22; chipRect[2] = cw + 24; chipRect[3] = 44;
        ctx.globalAlpha = mapShown ? 0.62 : 0.38;
        spaced(PLAN, W - m - cw, H - 22);
        chipH = 22;
      } else { chipRect[2] = 0; }
      if (mapA < 0.01) { mapRect[2] = 0; return; }
      var mx = W - m - S, my = H - m - S - chipH, sc = S / (2 * B);
      mapRect[0] = mx; mapRect[1] = my; mapRect[2] = S; mapRect[3] = S;
      var A = mapA * Math.min(1.25, aScale);
      ctx.lineWidth = 1;
      ga(0.92 * mapA); ctx.fillStyle = bgStr; ctx.fillRect(mx, my, S, S); ctx.fillStyle = inkStr;
      ga(0.16 * A); ctx.strokeRect(mx + 0.5, my + 0.5, S - 1, S - 1);
      var i, s, px, py;
      for (i = 0; i < sources.length; i++) {           // sources
        s = sources[i]; if (s.pres < 0.01) continue;
        ga((0.24 + 0.5 * s.lvl) * smooth01(s.pres) * A);
        ctx.beginPath(); ctx.arc(mx + (s.x + B) * sc, my + (s.z + B) * sc, 1 + 1.4 * s.lvl, 0, TAU); ctx.fill();
      }
      px = mx + (lis.x + B) * sc; py = my + (lis.z + B) * sc;
      var fx = Math.sin(lis.yaw), fy = -Math.cos(lis.yaw), rx = Math.cos(lis.yaw), ry = Math.sin(lis.yaw);
      var half = Math.atan((W / 2) / focal), L = 22;
      ga(0.13 * A);                                    // field of view
      ctx.beginPath();
      ctx.moveTo(px + Math.sin(lis.yaw - half) * L, py - Math.cos(lis.yaw - half) * L);
      ctx.lineTo(px, py);
      ctx.lineTo(px + Math.sin(lis.yaw + half) * L, py - Math.cos(lis.yaw + half) * L);
      ctx.stroke();
      ga(0.72 * A);                                    // listener
      ctx.beginPath();
      ctx.moveTo(px + fx * 6, py + fy * 6);
      ctx.lineTo(px - fx * 3 + rx * 3.4, py - fy * 3 + ry * 3.4);
      ctx.lineTo(px - fx * 3 - rx * 3.4, py - fy * 3 - ry * 3.4);
      ctx.closePath(); ctx.fill();
    }
    var PLAN = clusters('PLAN');

    // =========================================================================
    // Frame
    // =========================================================================
    function render() {
      if (needMeasure) measure();
      else if (Math.min(win.devicePixelRatio || 1, 2) !== dpr) measure();
      if (!W || !H) return;
      if (themeDirty) updateColors();
      if (hazeDirty) buildHaze();
      stats.frames++;

      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      ctx.globalCompositeOperation = 'source-over';
      ctx.globalAlpha = 1;
      ctx.fillStyle = bgStr; ctx.fillRect(0, 0, W, H);
      if (hazeGrad) { ctx.fillStyle = hazeGrad; ctx.fillRect(0, hazeY0, W, hazeY1 - hazeY0); }

      setupCamera(true);

      ctx.strokeStyle = inkStr; ctx.lineWidth = 1;
      ga(HORIZON_A * aScale);
      line(0, hy + 0.5, W, hy + 0.5);

      addGrid(); addWalls(); addRings();
      flushSegs();
      addPoints(); flushPoints();
      drawSources();
      drawLabels();
      ctx.globalAlpha = 1;
      drawCaption();
      drawMap();
      ctx.globalAlpha = 1;
    }

    function step(dt) {
      clock += dt;
      if (themeT !== themeTarget) {
        var k = dt / 0.6;
        themeT = themeT < themeTarget ? Math.min(themeTarget, themeT + k) : Math.max(themeTarget, themeT - k);
        themeDirty = true;
      }

      // --- motion ---
      var fIn = (keys.f || latch.f ? 1 : 0) - (keys.b || latch.b ? 1 : 0), tIn = (keys.r || latch.r ? 1 : 0) - (keys.l || latch.l ? 1 : 0);
      latch.f = latch.b = latch.l = latch.r = 0;
      if (fIn || tIn) gliding = false;
      if (gliding) {
        gT += dt / gDur;
        var p = gT >= 1 ? 1 : gT, e = easeInOut(p);
        lis.x = clampL(gx0 + (gx1 - gx0) * e);
        lis.z = clampL(gz0 + (gz1 - gz0) * e);
        lis.yaw = normYaw(gy0 + gdy * e);
        if (p >= 1) gliding = false;
        vel = 0; angVel = 0;
      } else if (!drag.active) {
        var fast = keys.shift ? FAST : 1;
        var tv = fIn * WALK * fast, tw = tIn * TURN * (keys.shift ? 1.6 : 1);
        vel += (tv - vel) * (1 - Math.exp(-dt / (fIn ? 0.3 : 0.5)));
        angVel += (tw - angVel) * (1 - Math.exp(-dt / (tIn ? 0.16 : 0.3)));
        if (!fIn && Math.abs(vel) < 0.004) vel = 0;
        if (!tIn && Math.abs(angVel) < 0.0008) angVel = 0;
        if (angVel) lis.yaw = normYaw(lis.yaw + angVel * dt);
        if (vel) {
          lis.x = clampL(lis.x + Math.sin(lis.yaw) * vel * dt);
          lis.z = clampL(lis.z - Math.cos(lis.yaw) * vel * dt);
        }
      }

      // --- sources ---
      var kL = 1 - Math.exp(-dt / 0.12), kF = 1 - Math.exp(-dt / 0.05), kS = 1 - Math.exp(-dt / 1.6);
      var kP = 1 - Math.exp(-dt / 0.4), dec = Math.exp(-dt / 0.6);
      for (var i = sources.length - 1; i >= 0; i--) {
        var s = sources[i];
        if (s.leaving) {
          s.pres -= dt / 1.5;
          if (s.pres <= 0) { if (byId[s.id] === s) delete byId[s.id]; sources.splice(i, 1); continue; }
        } else if (s.pres < 1) s.pres = Math.min(1, s.pres + dt / 2);
        if (clock - s.stamp > 0.5) s.tgt *= dec;              // no fresh level: let it settle
        s.lvl += (s.tgt - s.lvl) * kL;
        s.peak += (s.lvl - s.peak) * kP;
        s.fast += (s.tgt - s.fast) * kF; s.slow += (s.tgt - s.slow) * kS;
        if (s.ring < 0 && !reduced && s.pres > 0.9 && s.fast - s.slow > 0.3 && s.fast > 0.45 && clock - s.onset > 9) {
          s.ring = 0; s.onset = clock;
        }
        if (s.ring >= 0) { s.ring += dt / 4.8; if (s.ring >= 1) s.ring = -1; }
      }

      // --- overlay fades ---
      if (capT < 1) capT = Math.min(1, capT + dt);
      mapA += ((mapShown ? 1 : 0) - mapA) * (1 - Math.exp(-dt / 0.18));
      if (Math.abs(mapA - (mapShown ? 1 : 0)) < 0.002) mapA = mapShown ? 1 : 0;

      // --- notify (<= 30/s) ---
      if (clock - emT >= 1 / 30 - 1e-4) emitMove(false);
    }

    function emitMove(force) {
      if (emitting) return;
      if (!force && lis.x === emX && lis.z === emZ && lis.yaw === emYaw) return;
      emX = lis.x; emZ = lis.z; emYaw = lis.yaw; emT = clock;
      if (!onMove) return;
      emitting = true;
      try { onMove({ x: lis.x, z: lis.z, yaw: lis.yaw }); }
      catch (err) { if (root.console) root.console.error(err); }
      finally { emitting = false; }
    }

    // ---- loop ----
    var raf = 0, lastTs = 0;
    var rAF = typeof win.requestAnimationFrame === 'function' ? function (cb) { return win.requestAnimationFrame(cb); }
      : function (cb) { return setTimeout(function () { cb(now()); }, 16); };
    var cAF = typeof win.cancelAnimationFrame === 'function' ? function (id) { win.cancelAnimationFrame(id); } : clearTimeout;
    function hidden() { return !!(doc && doc.hidden); }
    function schedule() { if (!raf && !destroyed && !hidden()) raf = rAF(tick); }
    function tick() {
      raf = 0;
      if (destroyed) return;
      var t = now();
      var dt = lastTs ? (t - lastTs) / 1000 : 1 / 60;
      lastTs = t;
      if (dt > 0.1) dt = 0.1; else if (dt < 0) dt = 0;
      step(dt);
      render();
      schedule();
    }

    // =========================================================================
    // Navigation
    // =========================================================================
    function startGlide(x, z, yaw, dur) {
      vel = 0; angVel = 0;
      if (reduced || dur <= 0) {
        gliding = false; lis.x = x; lis.z = z; lis.yaw = normYaw(yaw);
        emitMove(false);
        return;
      }
      gliding = true; gT = 0; gDur = dur;
      gx0 = lis.x; gz0 = lis.z; gy0 = lis.yaw;
      gx1 = x; gz1 = z; gdy = normYaw(yaw - lis.yaw);
      schedule();
    }
    function glideTo(x, z, face) {
      x = clampL(x); z = clampL(z);
      var dx = x - lis.x, dz = z - lis.z, dist = Math.sqrt(dx * dx + dz * dz);
      if (dist < 0.05) return;
      var yaw = (face && dist > 3) ? Math.atan2(dx, -dz) : lis.yaw;
      startGlide(x, z, yaw, clamp(1.2 + dist / 14, 1.2, 3.4));
    }
    function inRect(r, x, y) { return r[2] > 0 && x >= r[0] && x <= r[0] + r[2] && y >= r[1] && y <= r[1] + r[3]; }
    function tap(px, py) {
      if (chipOn && inRect(chipRect, px, py)) { mapPref = !mapShown; mapShown = mapPref; schedule(); return; }
      if (mapA > 0.5 && inRect(mapRect, px, py)) {
        var sc = mapRect[2] / (2 * B);
        glideTo((px - mapRect[0]) / sc - B, (py - mapRect[1]) / sc - B, true);
        return;
      }
      if (py <= hy + 2) return;                          // above the horizon: not the floor
      var depth = EYE * focal / (py - hy);
      var right = (px - cx) / focal * depth;
      var sy = Math.sin(lis.yaw), cy = Math.cos(lis.yaw);
      glideTo(lis.x + sy * depth + cy * right, lis.z - cy * depth + sy * right, true);
    }
    function localXY(e) {
      var r = canvas.getBoundingClientRect ? canvas.getBoundingClientRect() : { left: 0, top: 0 };
      return [e.clientX - r.left, e.clientY - r.top];
    }

    function onPointerDown(e) {
      if (drag.id !== null) return;
      if (e.pointerType === 'mouse' && e.button !== 0) return;
      drag.id = e.pointerId; drag.type = e.pointerType || 'mouse';
      drag.x0 = drag.lx = e.clientX; drag.y0 = drag.ly = e.clientY;
      drag.t0 = drag.lt = now(); drag.moved = false; drag.active = false; drag.axis = 0; drag.v = 0; drag.w = 0;
      try { if (canvas.setPointerCapture) canvas.setPointerCapture(e.pointerId); } catch (_) { /* ignore */ }
      try { if (canvas.focus) canvas.focus({ preventScroll: true }); } catch (_) { /* ignore */ }
      if (e.preventDefault) e.preventDefault();
      schedule();
    }
    function onPointerMove(e) {
      if (e.pointerId !== drag.id) return;
      var x = e.clientX, y = e.clientY, t = now();
      if (!drag.moved) {
        var ddx = x - drag.x0, ddy = y - drag.y0, th = drag.type === 'mouse' ? 4 : 9;
        if (ddx * ddx + ddy * ddy < th * th) return;
        drag.moved = true; drag.active = true; gliding = false; vel = 0; angVel = 0;
        drag.axis = Math.abs(ddx) >= Math.abs(ddy) ? 1 : 2;   // 1 = turn, 2 = walk (locked per gesture)
        drag.lx = x; drag.ly = y; drag.lt = t;
        return;
      }
      var dx = x - drag.lx, dy = y - drag.ly, dts = Math.max(4, t - drag.lt) / 1000;
      drag.lx = x; drag.ly = y; drag.lt = t;
      if (drag.axis === 1) {
        var dyaw = -dx / focal;                                 // the room follows the finger
        lis.yaw = normYaw(lis.yaw + dyaw);
        drag.w = drag.w * 0.5 + (dyaw / dts) * 0.5;
      } else {
        var dist = dy * (22 / Math.max(240, H));                // pull the floor toward you = walk forward
        lis.x = clampL(lis.x + Math.sin(lis.yaw) * dist);
        lis.z = clampL(lis.z - Math.cos(lis.yaw) * dist);
        drag.v = drag.v * 0.5 + (dist / dts) * 0.5;
      }
      if (e.preventDefault) e.preventDefault();
      schedule();
    }
    function onPointerUp(e) {
      if (e.pointerId !== drag.id) return;
      try { if (canvas.releasePointerCapture) canvas.releasePointerCapture(e.pointerId); } catch (_) { /* ignore */ }
      var t = now();
      if (!drag.moved) {
        if (e.type === 'pointerup' && t - drag.t0 < 700) { var p = localXY(e); tap(p[0], p[1]); }
      } else {
        var recent = t - drag.lt < 90;
        if (drag.axis === 1) angVel = recent ? clamp(drag.w, -1.8, 1.8) : 0;   // gentle flick inertia
        else vel = recent ? clamp(drag.v, -6, 6) : 0;
      }
      drag.id = null; drag.active = false;
      schedule();
    }
    function onWheel(e) {
      if (e.preventDefault) e.preventDefault();
      var k = e.deltaMode === 1 ? 16 : e.deltaMode === 2 ? (H || 800) : 1;
      var dy = (e.deltaY || 0) * k, dx = (e.deltaX || 0) * k;
      gliding = false;
      if (Math.abs(dx) > Math.abs(dy)) angVel = clamp(angVel - dx * 0.0025, -2.5, 2.5);
      else vel = clamp(vel - dy * 0.02, -9, 9);
      schedule();
    }

    function editable(el) {
      if (!el) return false;
      if (el.isContentEditable) return true;
      var tg = el.tagName;
      return tg === 'INPUT' || tg === 'TEXTAREA' || tg === 'SELECT';
    }
    function keyAllowed(e) {
      if (e.ctrlKey || e.metaKey || e.altKey) return false;
      if (editable(e.target)) return false;
      if (doc && editable(doc.activeElement)) return false;
      return true;
    }
    function onKeyDown(e) {
      keys.shift = !!e.shiftKey;
      var k = KEYMAP[e.code] || KEYMAP[e.key];
      if (!k) {
        if ((e.code === 'KeyM' || e.key === 'm' || e.key === 'M') && !e.repeat && keyAllowed(e)) { mapPref = !mapShown; mapShown = mapPref; schedule(); }
        return;
      }
      if (!keyAllowed(e)) return;
      keys[k] = 1; latch[k] = 1; gliding = false;
      if (e.preventDefault) e.preventDefault();
      schedule();
    }
    function onKeyUp(e) {
      keys.shift = !!e.shiftKey;
      var k = KEYMAP[e.code] || KEYMAP[e.key];
      if (k) keys[k] = 0;
    }
    function clearKeys() { keys.f = keys.b = keys.l = keys.r = 0; latch.f = latch.b = latch.l = latch.r = 0; keys.shift = false; }
    function onVisibility() {
      if (hidden()) { if (raf) { cAF(raf); raf = 0; } clearKeys(); }
      else { lastTs = 0; schedule(); }
    }
    function onResize() { needMeasure = true; schedule(); }
    function onContextMenu(e) { if (drag.moved && e.preventDefault) e.preventDefault(); }

    // reduced motion
    var mql = null;
    function onMotionPref() { reduced = !!(mql && mql.matches); }
    if (typeof win.matchMedia === 'function') {
      try {
        mql = win.matchMedia('(prefers-reduced-motion: reduce)');
        onMotionPref();
        if (mql.addEventListener) mql.addEventListener('change', onMotionPref);
        else if (mql.addListener) mql.addListener(onMotionPref);
      } catch (_) { mql = null; }
    }

    // listeners
    var ro = null;
    function on(t, type, fn, o) { if (t && typeof t.addEventListener === 'function') t.addEventListener(type, fn, o); }
    function off(t, type, fn, o) { if (t && typeof t.removeEventListener === 'function') t.removeEventListener(type, fn, o); }
    on(canvas, 'pointerdown', onPointerDown);
    on(canvas, 'pointermove', onPointerMove);
    on(canvas, 'pointerup', onPointerUp);
    on(canvas, 'pointercancel', onPointerUp);
    on(canvas, 'wheel', onWheel, { passive: false });
    on(canvas, 'contextmenu', onContextMenu);
    on(win, 'keydown', onKeyDown);
    on(win, 'keyup', onKeyUp);
    on(win, 'blur', clearKeys);
    on(doc, 'visibilitychange', onVisibility);
    if (typeof win.ResizeObserver === 'function') {
      ro = new win.ResizeObserver(onResize);
      ro.observe(canvas);
    } else on(win, 'resize', onResize);

    // =========================================================================
    // Public API
    // =========================================================================
    function newSource(id) {
      return {
        id: id, x: 0, z: 0, radius: 0, rawLabel: null, label: [],
        pres: 0, leaving: false, gen: 0,
        tgt: 0, lvl: 0, peak: 0, fast: 0, slow: 0, stamp: -1e9, onset: -1e9, ring: -1,
        cd: 0, cr: 0, dist: 0, sx: 0, yT: 0, yB: 0, a: 0, vis: false
      };
    }

    var api = {
      setSources: function (list) {
        if (destroyed) return;
        gen++;
        if (list && list.length) {
          for (var i = 0; i < list.length; i++) {
            var it = list[i];
            if (!it || it.id == null) continue;
            var id = String(it.id), s = byId[id];
            if (!s) { s = newSource(id); byId[id] = s; sources.push(s); }
            s.gen = gen; s.leaving = false;
            s.x = isNum(it.x) ? clamp(it.x, -B, B) : 0;
            s.z = isNum(it.z) ? clamp(it.z, -B, B) : 0;
            s.radius = isNum(it.radius) && it.radius > 0 ? it.radius : 0;
            var lab = it.label == null ? '' : String(it.label);
            if (lab !== s.rawLabel) { s.rawLabel = lab; s.label = clusters(lab); }
          }
        }
        for (var j = 0; j < sources.length; j++) if (sources[j].gen !== gen) sources[j].leaving = true;
        schedule();
      },
      setLevels: function (levels) {
        if (destroyed || !levels) return;
        for (var id in levels) {
          var s = byId[id];
          if (!s) continue;
          var v = +levels[id];
          if (!(v >= 0)) v = 0; else if (v > 1) v = 1;
          s.tgt = v; s.stamp = clock;
        }
      },
      setTheme: function (theme) {
        if (destroyed) return;
        themeTarget = theme === 'white' ? 1 : 0;
        schedule();
      },
      setListener: function (l, o) {
        if (destroyed || !l) return;
        var x = isNum(l.x) ? clampL(l.x) : lis.x;
        var z = isNum(l.z) ? clampL(l.z) : lis.z;
        var yaw = isNum(l.yaw) ? normYaw(l.yaw) : lis.yaw;
        if (o && o.glide) startGlide(x, z, yaw, GLIDE_T);
        else { gliding = false; vel = 0; angVel = 0; lis.x = x; lis.z = z; lis.yaw = yaw; emitMove(false); }
        schedule();
      },
      setCaption: function (text) {
        if (destroyed) return;
        var raw = text == null ? '' : String(text);
        if (raw === capRaw) return;
        capRaw = raw; capPrev = capCur; capCur = clusters(raw); capT = capPrev.length ? 0 : 0.35;
        schedule();
      },
      getListener: function () { return { x: lis.x, z: lis.z, yaw: lis.yaw }; },
      destroy: function () {
        if (destroyed) return;
        destroyed = true;
        if (raf) { cAF(raf); raf = 0; }
        off(canvas, 'pointerdown', onPointerDown);
        off(canvas, 'pointermove', onPointerMove);
        off(canvas, 'pointerup', onPointerUp);
        off(canvas, 'pointercancel', onPointerUp);
        off(canvas, 'wheel', onWheel, { passive: false });
        off(canvas, 'contextmenu', onContextMenu);
        off(win, 'keydown', onKeyDown);
        off(win, 'keyup', onKeyUp);
        off(win, 'blur', clearKeys);
        off(doc, 'visibilitychange', onVisibility);
        off(win, 'resize', onResize);
        if (ro) { ro.disconnect(); ro = null; }
        if (mql) { if (mql.removeEventListener) mql.removeEventListener('change', onMotionPref); else if (mql.removeListener) mql.removeListener(onMotionPref); }
        sources.length = 0; byId = Object.create(null); onMove = null;
      }
    };

    // Internal hooks for tests / debugging (not part of the contract).
    Object.defineProperty(api, '_debug', {
      enumerable: false,
      value: {
        frame: function (dt) { if (destroyed) return; step(dt == null ? 1 / 60 : dt); render(); },
        project: function (x, y, z) {
          if (needMeasure) measure();
          setupCamera(false);
          var dx = x - camX, dz = z - camZ;
          var r = dx * cosY + dz * sinY, d = dx * sinY - dz * cosY;
          if (d < NEAR) return null;
          return { x: cx + focal * r / d, y: hy - focal * (y - eyeY) / d, depth: d };
        },
        tap: function (px, py) { tap(px, py); },
        key: function (k, down) { keys[k] = down ? 1 : 0; },
        stats: stats,
        sourceCount: function () { return sources.length; },
        size: function () { return { w: W, h: H, dpr: dpr, focal: focal, horizon: hy }; }
      }
    });

    measure();
    updateColors();
    schedule();
    return api;
  }

  root.RoomView = { create: create };
  if (typeof module !== 'undefined' && module.exports) module.exports = { create: create };
})(typeof window === 'undefined' ? globalThis : window);
