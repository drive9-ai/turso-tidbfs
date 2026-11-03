# README animation

The [overview GIF](../../assets/overview.gif) illustrates durable SQL state,
asynchronous readers, transaction snapshots and writer handoff. Counts and
timing are illustrative. Actual measurements are in the
[following report](../../results/follow.md) and [handoff report](../../results/handoff.md).

The [treatment](TREATMENT.md) records the story, semantic colors, causal order
and reference revision. All geometry and motion are original, drawn from
`render(t)` with no playback history, random inputs or remote assets at render
time. The Inter font and its OFL license are vendored under `fonts/`.

## Build

Use Python 3.9+, Pillow 11.3.0 and FFmpeg 7+ on PATH. This tooling is optional;
it adds no dependencies to the database or its smoke/benchmark scripts.

```bash
python3 -m venv /tmp/turso-animation-env
/tmp/turso-animation-env/bin/pip install -r docs/animation/requirements.txt
/tmp/turso-animation-env/bin/python docs/animation/render.py
```

With Pillow already available, the one-command build is:

```bash
python3 docs/animation/render.py
```

It writes `assets/overview.gif` (1280 × 720, 24 seconds, looping) and
`assets/overview.png` (the static transaction-snapshot view). FFmpeg uses a
shared palette and rectangle differencing to keep the GIF below 5 MiB.
Temporary palette files are removed automatically. The animation is silent
for direct embedding on GitHub.

## Review

```bash
python3 docs/animation/render.py --check-only --review-dir /tmp/turso-animation-review
```

This renders a contact sheet, a 390-pixel-wide preview and a poster. It checks
text bounds, caption reading time, monotonic row visibility and ownership order,
and compares frames rendered in different orders. The full build also decodes
the GIF to verify its size, loop flag and duration. Inspect the contact sheet,
the compressed GIF's transitions and the phone preview before publishing.
No database build, credentials or remote requests are needed.

See [credits](CREDITS.md) for the font source and reference license.
