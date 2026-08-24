# Android app (TWA) — build & release

The Android app is a Trusted Web Activity wrapping https://saigon-soundscape.onrender.com.
It is NOT built by CI — built locally and published as a GitHub release asset.

**Download link (stable, referenced by /app page):**
https://github.com/Almusallah/saigon-sound-map/releases/latest/download/saigon-sound-map.apk

## Pieces

| What | Where |
|---|---|
| Android project (bubblewrap-generated) | `~/projects/soundmap-twa/` |
| TWA config | `~/projects/soundmap-twa/twa-manifest.json` |
| Signing keystore (RSA 2048, alias `soundmap`) | `~/Desktop/saigon-backups/soundmap-android.keystore` (+ `.password.txt`, chmod 600) |
| JDK 17 + Android SDK 34 toolchain | `~/.bubblewrap/` |
| Cert SHA-256 (must match Render env `APK_CERT_SHA256`) | `E8:5C:2E:58:94:4B:DE:82:AA:0C:BC:9B:C9:B6:C9:53:F4:F3:DB:F5:16:2C:12:C7:BA:3B:4B:D6:12:26:98:8B` |

The server serves `/.well-known/assetlinks.json` from `server/index.js`, reading the
fingerprint from the `APK_CERT_SHA256` env var on Render. If the fingerprint and the
APK signature ever diverge, the app opens with a browser URL bar instead of fullscreen.

## Rebuild (e.g. after bumping version)

`bubblewrap build` fails on this machine (it validates the SDK path against the
pre-2019 `tools/` layout). Use gradle directly:

```bash
cd ~/projects/soundmap-twa
# bump versionCode + versionName in app/build.gradle (and twa-manifest.json to stay honest)
export JAVA_HOME=$HOME/.bubblewrap/jdk/jdk-17.0.20.1+1/Contents/Home
export ANDROID_HOME=$HOME/.bubblewrap/android_sdk
export PATH=$JAVA_HOME/bin:$PATH
./gradlew assembleRelease --no-daemon
BT=$ANDROID_HOME/build-tools/34.0.0
PASS=$(cat ~/Desktop/saigon-backups/soundmap-android.keystore.password.txt)
"$BT/zipalign" -f -p 4 app/build/outputs/apk/release/app-release-unsigned.apk app/build/outputs/apk/release/app-release-aligned.apk
"$BT/apksigner" sign --ks ~/Desktop/saigon-backups/soundmap-android.keystore \
  --ks-key-alias soundmap --ks-pass "pass:$PASS" --key-pass "pass:$PASS" \
  --out saigon-sound-map.apk app/build/outputs/apk/release/app-release-aligned.apk
"$BT/apksigner" verify --print-certs saigon-sound-map.apk
gh release upload app-v1.0.0 saigon-sound-map.apk --clobber   # or create a new app-vX.Y.Z release
```

The `latest/download/` URL always points at the newest release, so /app never needs editing.
Note: after `--clobber`, GitHub's CDN can serve the previous bytes for a few minutes.

## iOS

No sideload path exists outside the App Store. iPhone users install the PWA:
Safari → Share → Add to Home Screen (instructions on /app). Same fullscreen
map, camera and mic capture all work in the installed PWA.
