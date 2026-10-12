# Phòng nghe — visual QA, 12 October 2026

Result: passed for the scoped desktop/mobile redesign. Base: 8b20cec.

## Brief and reference

Yuri clarified that OJAS is exclusively a mood board for site colours, material sensibility and exhibition layout. No generated image belongs in Phòng nghe. The generated room image was removed from the client before release; the implementation contains no photographic assets or simulated loudspeakers.

Reference: [OJAS at USM](https://us.usm.com/blogs/journal-home/devon-turnbull-ojas-listening-room). Grey wood, black components, muted metal and space between objects inform the palette and the separated live traces. This is an interpretation for an interface, not a pixel reproduction or an OJAS collaboration.

## Visual comparison

The reference and final captures were viewed together. Workspace evidence: `review/2026-10-12/ojas-usm-reference.png`, `room-desktop-no-image.png` (1280×720), `room-mobile-no-image.png` (390×844). Earlier captures containing a room image are superseded.

- Neutral grey and olive surfaces, charcoal type and subdued waveform colours replace neon/purple styling.
- Regular uppercase Helvetica-system headings, restrained dividers and generous space replace the promotional treatment. No external fonts or images.
- Four measured amplitude histories occupy separate positions; voice/place above, pulse/bass lower. The visualization does not invent activity.
- Persistent transport and collapsible balance, atlas and settings preserve access to existing functions. Six movement labels form two readable rows on phones.
- Mobile document width equals viewport width (390px); no horizontal overflow. Desktop capture shows the complete signal field above the transport. Lower controls and movement trail are reached by scrolling.

## Fixed during QA

P2: the SVG viewBox could retain pre-playback dimensions after the compact listening header changed its container height. ResizeObserver now follows actual dimensions.

P2: freezing the visual also prevented its status from reflecting pause/mute. Status updates now continue when the trace is frozen.

The image-based draft is superseded by explicit user direction; its asset and markup are absent from the release.

## Verification and limits

49 existing Sound Map regression tests pass; inline scripts parse and git diff whitespace checks pass. Browser loaded real recordings, entered the room, advanced to Encounter with a recalled detail, paused/resumed and froze/resumed visuals. Atlas opens with source links and recording metadata; no browser console errors observed. Solo and mix controls were checked earlier in this same redesign session.

The audio engine is unchanged by this visual revision. Prior engine release checks covered complete movement progression. These checks do not establish perceptual listening quality or real iPhone/Safari behaviour. No cross-agent review was performed. No known P0/P1/P2 defect remains in the inspected visual scope.

## Optional minimal menu study — later on 12 October 2026

Local preview only, based on Yuri's supplied location-menu screenshot. Open `/listen?ui=minimal`; ordinary `/listen` retains the released appearance. `listen-minimal.css` loads only for this variant, and links between Whole city and Thanh Đa retain it. The engine is shared and unchanged.

The user reference supersedes OJAS as the visual reference for this optional study: near-black olive panel, subtle rounded outline, plain system type, simple rows and blue selection. No generated image or background photo. Four actual amplitude traces become compact horizontal rows; optional balance/atlas/settings retain their existing interactions.

QA passed for the local study at 390×844 and 1280×900, with no horizontal overflow. Mobile screenshot and user reference were viewed together; desktop also reviewed. Files in the parent workspace: `review/2026-10-12/minimal-menu-reference.png`, `minimal-menu-mobile.png`, `minimal-menu-desktop.png`. Browser checks: start, source loading, Solo/return to mix, manual Arrival→Encounter, pause, collapsible balance. No console errors. All 49 tests and inline-script syntax checks pass. No new perceptual or real-iPhone validation. Not committed or deployed; production remains 33a6c7c.

## Approved minimal glass interface and spatial score — 12 October 2026

Yuri approved the menu direction, requested a final subtle transparency pass and shipping, and asked for Saigon-like lines while keeping the atlas a score rather than a representation. Minimal is now the default `/listen` appearance; `ui=minimal` links remain compatible. The conditional preview bootstrap was replaced with a normal stylesheet link, and waveform placement uses the approved compact rows.

Glass uses two static CSS surfaces: the panel background pseudo-element and the fixed transport. The panel itself has no filter, preserving the viewport positioning of its fixed playback control. Desktop blur is 18–20px, mobile 10–12px with a more opaque tint. Unsupported browsers retain a dark fallback; reduced-transparency preference disables blur and makes the surfaces opaque. No images, animated backgrounds, external fonts or geographic tiles.

The atlas has abstract river-like curves drawn in canvas, quiet archive points underneath brighter active sources and curved connections from the listening ring. Point positions and pointer/keyboard mapping still use the existing archive bounds. Curves are normalized graphic gestures and never influence audio or claim to trace actual riverbanks. Grid, compass, coordinate text and kilometre ring were removed. A legend and brief interaction copy explain the score. Audio engine unchanged.

Validation: 49 tests and script syntax pass; browser checks confirm default-route styling, real recordings, atlas opening and keyboard movement with temporary manual attention. Mobile and desktop screenshots checked for legibility, fixed transport and overflow. Evidence in the parent workspace: `review/2026-10-12/glass-room-mobile.png`, `glass-atlas-mobile.png`, `glass-room-desktop.png`. No human listening assessment or physical iPhone/Safari test. This section supersedes the local-only status of the preceding study; release verification is recorded in the workspace handoff.
