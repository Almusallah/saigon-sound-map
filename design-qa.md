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
