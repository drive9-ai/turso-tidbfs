#!/usr/bin/env python3
"""Deterministic README diagram. Python 3.9+, Pillow 11.3, FFmpeg 7+."""
import argparse
from functools import lru_cache
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile

from PIL import Image, ImageDraw, ImageFont

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
W, H, SCALE, FPS, DURATION = 1280, 720, 2, 24, 24
BG = (11, 16, 25)
SURFACE = (21, 29, 42)
BORDER = (49, 63, 82)
TEXT = (239, 244, 252)
MUTED = (157, 172, 194)
DIM = (97, 115, 140)
BLUE = (110, 168, 255)
AMBER = (232, 182, 103)
BEATS = [
    (0, 3, "One writer. Many readers."),
    (3, 6.5, "Commit. Readers follow."),
    (6.5, 9.5, "Transactions stay put."),
    (9.5, 14, "Wait for lease expiry."),
    (14, 17.5, "A new agent takes over."),
    (17.5, 20.5, "Old write refused."),
    (20.5, 24, "Next query. Latest state."),
]


def clamp(value):
    return max(0, min(1, value))


def ease(value):
    value = clamp(value)
    return value * value * (3 - 2 * value)


def mix(a, b, amount):
    return tuple(round(x + (y - x) * amount) for x, y in zip(a, b))


@lru_cache(None)
def font(size, weight=500):
    face = ImageFont.truetype(str(HERE / "fonts/Inter.ttf"), round(size * SCALE))
    face.set_variation_by_axes([14, weight])
    return face


class Painter:
    def __init__(self):
        self.image = Image.new("RGB", (W * SCALE, H * SCALE), BG)
        self.draw = ImageDraw.Draw(self.image)

    def line(self, points, color=BORDER, width=2):
        self.draw.line([(round(x * SCALE), round(y * SCALE)) for x, y in points], fill=color, width=max(1, round(width * SCALE)))

    def box(self, xy, fill=SURFACE, outline=BORDER, radius=16, width=1.4):
        x, y, w, h = xy
        self.draw.rounded_rectangle((round(x * SCALE), round(y * SCALE), round((x + w) * SCALE), round((y + h) * SCALE)), radius=round(radius * SCALE), fill=fill, outline=outline, width=round(width * SCALE))

    def text(self, xy, value, size=24, color=TEXT, weight=500):
        x, y = xy
        face = font(size, weight)
        bounds = self.draw.textbbox((x * SCALE, y * SCALE), value, font=face, anchor="lt")
        assert bounds[0] >= 0 and bounds[1] >= 0 and bounds[2] <= W * SCALE and bounds[3] <= H * SCALE, (value, bounds)
        self.draw.text((x * SCALE, y * SCALE), value, fill=color, font=face, anchor="lt")

    def circle(self, x, y, radius, color):
        self.draw.ellipse(((x - radius) * SCALE, (y - radius) * SCALE, (x + radius) * SCALE, (y + radius) * SCALE), fill=color)


def curve(points, u):
    a, b, c, d = points
    v = 1 - u
    return (v**3 * a[0] + 3 * v*v*u*b[0] + 3*v*u*u*c[0] + u**3*d[0],
            v**3 * a[1] + 3 * v*v*u*b[1] + 3*v*u*u*c[1] + u**3*d[1])


CHANNELS = {
    "a": [(330, 324), (381, 324), (404, 359), (454, 359)],
    "d": [(330, 540), (382, 540), (401, 472), (454, 472)],
    "b": [(766, 359), (834, 359), (868, 311), (936, 311)],
    "c": [(766, 472), (834, 472), (868, 525), (936, 525)],
}


def channel(p, name, color):
    points = [curve(CHANNELS[name], i / 40) for i in range(41)]
    p.line(points, color, 2)
    x, y = points[-1]
    p.line([(x - 8, y - 5), (x, y), (x - 8, y + 5)], color, 2)


def packet(p, name, t, start, end, color=BLUE, reverse=False, stop=1):
    if not start <= t <= end:
        return
    u = ease((t - start) / (end - start)) * stop
    x, y = curve(CHANNELS[name], 1 - u if reverse else u)
    p.circle(x, y, 9, mix(BG, color, 0.2))
    p.circle(x, y, 5, color)


def key(p, x, y, color=BLUE):
    p.circle(x - 9, y, 7, color)
    p.circle(x - 9, y, 3, SURFACE)
    p.line([(x - 2, y), (x + 14, y), (x + 14, y + 5)], color, 3)
    p.line([(x + 6, y), (x + 6, y + 5)], color, 3)


def state(t):
    beat = next((i for i, (a, b, _) in enumerate(BEATS) if a <= t < b), len(BEATS) - 1)
    return {"beat": beat, "caption": BEATS[beat][2],
            "stored": 128 if t < 4.2 else 129 if t < 15.8 else 130,
            "following": 128 if t < 5.8 else 129 if t < 16.9 else 130,
            "transaction": 128 if t < 21.85 else 130,
            "owner": "A" if t < 14.5 else "D"}


def render(t):
    t = max(0, min(DURATION - 1 / FPS, t))
    s = state(t)
    p = Painter()
    # Static registration grid. Motion represents requests or state changes only.
    for x in range(0, W, 48):
        p.line([(x, 176), (x, 624)], (15, 22, 33), 0.5)
    for y in range(192, 625, 48):
        p.line([(40, y), (1240, y)], (15, 22, 33), 0.5)
    p.text((48, 24), "TURSO + TIDB FILESYSTEM", 18, BLUE, 600)
    p.text((48, 65), "SQL state that outlives your agent.", 48, TEXT, 650)
    p.text((48, 131), "SQL runs locally. Committed state stays in shared storage.", 24, MUTED)
    p.text((1056, 25), f"0{s['beat'] + 1} / 07", 20, MUTED)
    p.text((48, 192), "WRITE", 17, DIM, 600)
    p.text((454, 192), "PERSIST", 17, DIM, 600)
    p.text((936, 192), "READ", 17, DIM, 600)

    channel(p, "a", DIM if t < 9.5 else mix(BG, AMBER, 0.4))
    if t >= 9.5:
        channel(p, "d", DIM if t < 14.5 else mix(BG, BLUE, 0.75))
    channel(p, "b", mix(BG, BLUE, 0.6))
    channel(p, "c", BORDER if t < 20.5 else mix(BG, BLUE, 0.6))

    # Writer identities stay in place across loss and replacement.
    p.box((48, 244, 282, 160), outline=BORDER if t < 9.5 else mix(BORDER, AMBER, 0.65))
    p.text((72, 264), "Writer A", 32, TEXT, 650)
    if t < 3:
        action, color = "Runs SQL locally", MUTED
    elif t < 6.5:
        action, color = "COMMIT +1", BLUE
    elif t < 9.5:
        action, color = "Commit confirmed", BLUE
    elif t < 17.5:
        action, color = "Unavailable", AMBER
    elif t < 18.6:
        action, color = "COMMIT +1", AMBER
    else:
        action, color = "Write refused", AMBER
    p.text((72, 318), action, 25, color, 550)
    p.text((72, 370), "Lease owner" if t < 9.5 else "Lease expiring" if t < 14.5 else "Old session", 19, BLUE if t < 9.5 else MUTED)

    if t >= 9.5:
        p.box((48, 462, 282, 156), outline=BORDER if t < 14.5 else mix(BORDER, BLUE, 0.7))
        p.text((72, 482), "Writer D", 32, TEXT, 650)
        p.text((72, 532), "Waiting" if t < 14.5 else "COMMIT +1" if t < 17.5 else "Continues writing", 25, MUTED if t < 14.5 else BLUE, 550)
        p.text((72, 584), "After lease expiry" if t < 14.5 else "Lease owner", 19, MUTED if t < 14.5 else BLUE)
    key_y = 380 + (598 - 380) * ease((t - 14.5) / 0.5)
    key(p, 294, key_y, BLUE if t < 9.5 or t >= 14.5 else AMBER)

    # The database + WAL remain available when compute stops.
    p.box((454, 278, 312, 266), fill=(23, 34, 51), outline=mix(BORDER, BLUE, 0.6), radius=20)
    p.text((480, 300), "TiDB Filesystem", 28, TEXT, 600)
    p.text((480, 342), "Database + WAL", 22, MUTED)
    p.line([(480, 382), (740, 382)], BORDER, 1)
    p.text((480, 407), str(s["stored"]), 64, BLUE, 650)
    p.text((480, 490), "committed rows", 23, MUTED)

    for y, name, subtitle, rows, color in [
        (224, "Reader B", "Follows new commits", s["following"], BLUE),
        (438, "Reader C", "Open transaction" if t < 20.5 else "Transaction ended", s["transaction"], TEXT if t < 21.85 else BLUE),
    ]:
        p.box((936, y, 296, 174))
        p.text((960, y + 18), name, 30, TEXT, 650)
        p.text((960, y + 59), subtitle, 21, MUTED)
        p.text((960, y + 93), str(rows), 52, color, 650)
        p.text((1091, y + 119), "rows", 22, MUTED)
        # Same processes persist; ending a transaction is not a process restart.
        p.circle(1210, y + 31, 4, BLUE)

    packet(p, "a", t, 3.15, 4.2)
    packet(p, "a", t, 4.25, 4.85, reverse=True)
    packet(p, "b", t, 5.0, 5.8)
    packet(p, "d", t, 13.8, 14.5)
    packet(p, "d", t, 15.0, 15.8)
    packet(p, "b", t, 16.1, 16.9)
    packet(p, "a", t, 17.55, 18.6, AMBER, stop=0.9)
    if 18.6 <= t:
        p.line([(427, 349), (443, 365)], AMBER, 3)
        p.line([(427, 365), (443, 349)], AMBER, 3)
    packet(p, "c", t, 21.0, 21.85)

    p.box((48, 642, 1184, 54), fill=(20, 29, 43), outline=(38, 53, 73), radius=12)
    p.circle(75, 669, 4, BLUE if s["beat"] not in (3, 5) else AMBER)
    p.text((94, 654), s["caption"], 28, TEXT, 550)
    p.text((920, 655), "Illustrative loop", 18, MUTED)
    p.text((1092, 655), "Time compressed", 14, MUTED)
    return p.image.resize((W, H), Image.Resampling.LANCZOS)


def validate(out):
    # A frame must not depend on which frame was drawn before it.
    times = [0, 4.5, 7.5, 12, 14.75, 16.95, 19.3, 22.5]
    hashes = {t: hashlib.sha256(render(t).tobytes()).hexdigest() for t in times}
    for t in reversed(times):
        assert hashlib.sha256(render(t).tobytes()).hexdigest() == hashes[t]
    for i in range(FPS * DURATION):
        s = state(i / FPS)
        assert s["stored"] >= s["following"] >= s["transaction"]
        assert s["owner"] == ("D" if i / FPS >= 14.5 else "A")
    for a, b, caption in BEATS:
        required = len(caption.replace(" ", "")) / 15 + 1.5
        assert b - a >= required, (caption, b - a, required)
    out.mkdir(parents=True, exist_ok=True)
    sheet = Image.new("RGB", (W, 4 * (H // 2 + 28)), BG)
    draw = ImageDraw.Draw(sheet)
    for i, t in enumerate(times):
        thumb = render(t).resize((W // 2, H // 2), Image.Resampling.LANCZOS)
        x, y = (i % 2) * (W // 2), (i // 2) * (H // 2 + 28)
        sheet.paste(thumb, (x, y + 28))
        draw.text((x + 12, y + 6), f"t={t:.2f}s", fill=TEXT)
    sheet.save(out / "contact.png")
    render(7.5).resize((390, 219), Image.Resampling.LANCZOS).save(out / "phone.png")
    render(7.5).save(out / "poster.png")
    print(json.dumps({"deterministic_frames": len(times), "timeline_frames": FPS * DURATION,
                      "caption_reading_checks": len(BEATS)}), flush=True)


def stream(command, frames, phase):
    process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL)
    try:
        for i, t in enumerate(frames):
            process.stdin.write(render(t).tobytes())
            if i and i % (FPS * 4) == 0:
                print(f"{phase}: {t:.0f}s", flush=True)
        process.stdin.close()
        if process.wait() != 0:
            raise RuntimeError("FFmpeg failed: " + phase)
    except BaseException:
        process.kill()
        process.wait()
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check-only", action="store_true")
    parser.add_argument("--review-dir", type=Path, default=Path(tempfile.gettempdir()) / "turso-readme-review")
    args = parser.parse_args()
    validate(args.review_dir)
    if args.check_only:
        return
    assets = ROOT / "assets"
    assets.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="turso-readme-render-") as temp:
        palette = Path(temp) / "palette.png"
        source = ["ffmpeg", "-v", "error", "-y", "-f", "rawvideo", "-pixel_format", "rgb24", "-video_size", f"{W}x{H}", "-framerate", str(FPS), "-i", "pipe:0"]
        # Palette samples cover every state; no need to buffer an entire movie.
        samples = [i / 2 for i in range(DURATION * 2)]
        stream(source + ["-vf", "palettegen=reserve_transparent=0", "-frames:v", "1", str(palette)], samples, "palette")
        target = Path(temp) / "overview.gif"
        stream(source + ["-i", str(palette), "-filter_complex", "[0:v][1:v]paletteuse=dither=none:diff_mode=rectangle", "-loop", "0", str(target)], [i / FPS for i in range(FPS * DURATION)], "gif")
        with Image.open(target) as gif:
            total = 0
            for i in range(gif.n_frames):
                gif.seek(i)
                total += gif.info.get("duration", 0)
            assert abs(total / 1000 - DURATION) <= 0.1, total
            assert gif.size == (W, H) and gif.info.get("loop") == 0
        assert target.stat().st_size < 5 * 1024 * 1024, "README GIF exceeds 5 MiB budget"
        (assets / "overview.gif").write_bytes(target.read_bytes())
    render(7.5).save(assets / "overview.png", optimize=True)
    print(json.dumps({"gif": str(assets / "overview.gif"), "bytes": (assets / "overview.gif").stat().st_size, "duration_seconds": DURATION}), flush=True)


if __name__ == "__main__":
    main()
