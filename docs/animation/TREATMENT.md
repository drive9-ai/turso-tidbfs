# SQL state that outlives an agent

The viewer should remember: agents can be replaced while their committed SQL
state stays available to readers and the next writer.

## Structure

Three candidates considered:

1. A terminal capture: direct evidence, but too much SQL for a first-time visitor.
2. One persistent system diagram: the same writer, storage and two reader cards
   show what changes and what stays fixed. **Chosen:** makes the mechanism visible.
3. A checklist of benefits: concise, but does not show transaction consistency.

The opening establishes local SQL execution and durable filesystem state. The
turn is writer failure: data and readers remain in place. The signature move is
the lease badge transferring to the replacement writer while the old reader
transaction stays pinned. The ending lets that transaction finish and catch up.

## Direction

- Medium: silent looping GIF for GitHub README, 1280 × 720, 24 seconds, 24 fps.
- Style: Siddon Visual Engineering v2 / Dark Tech Keynote. Fixed camera and stable
  object anchors prioritize diagram readability. No audio, voice or subtitles;
  event captions are part of the diagram. No unrelated camera motion or effects.
- Reference grammar: the restraint of Apple software presentations and the
  continuous causal diagrams of 3Blue1Brown; no borrowed UI, branding or shots.
- Primary accent: blue = committed data / active ownership. Amber = unavailable
  or refused. Neutral text = fixed snapshot. Text and shapes reinforce colors.
- Persistent IDs: writer A, writer D, TiDB FS, following reader B, transaction C.
- Data: illustrative row counts 128 → 129 → 130. Time is compressed. This is an
  explanatory animation, not a live recording or latency chart.
- The old writer must be refused only after the replacement has acquired the
  lease. Reader B advances only after publication; C advances only after its
  transaction ends. A missing response is never shown as a successful commit.

## Timeline

| Seconds | Trigger → state change → consequence | Caption |
| --- | --- | --- |
| 0–3 | Writer A owns the lease. All views show 128 rows. | One writer. Many readers. |
| 3–6.5 | A submits a commit; FS publishes 129; B follows asynchronously. | Commit. Readers follow. |
| 6.5–9.5 | B shows 129 while C's open transaction stays at 128. | Transactions stay put. |
| 9.5–14 | A becomes unavailable. D waits through lease expiry. Stored state remains 129. | Wait for lease expiry. |
| 14–17.5 | D acquires ownership, commits 130, then B follows. | A new agent takes over. |
| 17.5–20.5 | A returns and tries to publish. The old revision is rejected. D remains owner. | Old write refused. |
| 20.5–24 | C ends its transaction; its next query adopts 130. | Next query. Latest state. |

Hold the 129/128 split long enough to read. Keep all objects on stage across
failure and recovery. The loop is explicitly illustrative; the reset starts a
new demonstration, not a database rollback. Captions are fully visible for at
least three seconds. Request tokens exist only during corresponding operations.

## Evidence and delivery

Mechanisms are documented in [storage design](../storage.md); actual remote
checks are in [following](../../results/follow.md) and
[handoff](../../results/handoff.md). The GIF links readers to those results.

Reference guides consulted from `lemomo-ai/lemo-opuscar`, revision
`bebbbe1aa92a3cb4bb6f83500e61b23a17d1aa00`: AGENTS.md, DIRECTOR.md,
TECHNIQUE.md, styles/dark-keynote/STYLE.md and core/README.md. This README-sized
silent diagram uses its own deterministic Pillow renderer, with no demo code.

Deliver: embedded GIF, static poster, source, font license and one-command build.
Validate selected frames in shuffled time order, text bounds, key transition
strips, compressed GIF frames, total duration, phone-size readability and links.
