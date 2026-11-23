# Pictures of real or 3D play from above, drawn the same way for both:
#
#   python3 draw.py runs  <nba|tape> <file>                    long half-court stretches
#   python3 draw.py paths <nba|tape> <file> <i0> <i1> <out.png> [title]
#   python3 draw.py video <nba|tape> <file> <i0> <i1> <out.mp4> [title]   (needs ffmpeg)
#
# i0, i1: frame numbers (10 a second) from `runs`. Play is turned to attack
# the left rim. Offense red, defense blue; paths start small and end big,
# rings where a man stood still; the man with the ball ringed gold.
import os
import subprocess
import sys

import numpy as np
from PIL import Image, ImageDraw

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from frames import from_sportvu, from_tape  # noqa: E402
from measures import half_court  # noqa: E402

S, PAD = 12, 20
OFF, DEF = (200, 40, 40), (30, 110, 190)


def px(p):
    return (PAD + p[0] * S, PAD + (50 - p[1]) * S)


def court():
    img = Image.new("RGB", (int(52 * S + 2 * PAD), int(50 * S + 2 * PAD)), (240, 222, 196))
    d = ImageDraw.Draw(img)
    line = (130, 110, 90)
    d.rectangle([px((0, 50)), px((94, 0))], outline=line, width=2)
    d.line([px((47, 0)), px((47, 50))], fill=line, width=2)
    d.rectangle([px((0, 33)), px((19, 17))], outline=line, width=2)
    for cx, r in ((19, 6), (47, 6)):
        d.ellipse([px((cx - r, 25 + r)), px((cx + r, 25 - r))], outline=line, width=2)
    a = np.degrees(np.arcsin(22 / 23.75))
    h = px((5.25, 25))
    d.arc([h[0] - 23.75 * S, h[1] - 23.75 * S, h[0] + 23.75 * S, h[1] + 23.75 * S], -a, a, fill=line, width=2)
    xc = 5.25 + 23.75 * np.cos(np.radians(a))
    for y in (3, 47):
        d.line([px((0, y)), px((xc, y))], fill=line, width=2)
    d.arc([h[0] - 4 * S, h[1] - 4 * S, h[0] + 4 * S, h[1] + 4 * S], -90, 90, fill=line, width=2)
    d.ellipse([h[0] - 9, h[1] - 9, h[0] + 9, h[1] + 9], outline=(230, 90, 40), width=2)
    return img


def load(kind, path):
    F = from_sportvu(path) if kind == "nba" else from_tape(path)
    # (Turned to attack the left rim.)
    right = F["hoop"][:, 0] > 47
    F["xy"] = np.where(right[:, None, None], [94, 50] - F["xy"], F["xy"])
    F["ball"][:, :2] = np.where(right[:, None], [94, 50] - F["ball"][:, :2], F["ball"][:, :2])
    F["hoop"] = np.where(right[:, None], [94, 50] - F["hoop"], F["hoop"])
    return F


def runs(F, least=12):
    half = F["live"] & half_court(F)
    d = np.diff(np.concatenate([[0], half.astype(int), [0]]))
    for a, b in zip(np.nonzero(d == 1)[0], np.nonzero(d == -1)[0]):
        if (b - a) * 0.1 >= least and len(set(F["seg"][a:b])) == 1:
            print(a, b, f"{(b - a) * 0.1:.1f} s")


def paths(F, i0, i1, title):
    img = court()
    d = ImageDraw.Draw(img)
    for j in range(10):
        col = OFF if F["off"][i0, j] else DEF
        pts = [px(F["xy"][k, j]) for k in range(i0, i1)]
        d.line(pts, fill=col, width=3, joint="curve")
        v = np.linalg.norm(np.diff(F["xy"][i0:i1, j], axis=0), axis=1) / 0.1
        for k in range(0, len(v), 5):
            if v[k] < 1.5:
                x, y = pts[k]
                d.ellipse([x - 5, y - 5, x + 5, y + 5], outline=col, width=1)
        for (x, y), r in ((pts[0], 6), (pts[-1], 13)):
            d.ellipse([x - r, y - r, x + r, y + r], fill=col)
    d.text((12, 8), title, fill=(30, 30, 30))
    return img


def frame(F, i, i0, title):
    img = court()
    d = ImageDraw.Draw(img)
    for j in range(10):
        col = OFF if F["off"][i, j] else DEF
        trail = [px(F["xy"][k, j]) for k in range(max(i0, i - 15), i + 1)]
        if len(trail) > 1:
            d.line(trail, fill=tuple(int(c * 0.5 + 255 * 0.5) for c in col), width=2)
        x, y = px(F["xy"][i, j])
        r = 1.3 * S
        d.ellipse([x - r, y - r, x + r, y + r], fill=col)
        if F["hold"][i] == j:
            d.ellipse([x - r - 4, y - r - 4, x + r + 4, y + r + 4], outline=(255, 190, 0), width=3)
    bx, by = px(F["ball"][i, :2])
    r = (0.6 + F["ball"][i, 2] / 25) * S
    d.ellipse([bx - r, by - r, bx + r, by + r], fill=(255, 140, 0), outline=(120, 60, 0))
    d.text((12, 8), f"{title}  {(i - i0) * 0.1:4.1f}s", fill=(30, 30, 30))
    return img


if __name__ == "__main__":
    what, kind, path = sys.argv[1:4]
    F = load(kind, path)
    if what == "runs":
        runs(F)
        sys.exit()
    i0, i1, out = int(sys.argv[4]), int(sys.argv[5]), sys.argv[6]
    title = sys.argv[7] if len(sys.argv) > 7 else ""
    if what == "paths":
        paths(F, i0, i1, title).save(out)
    else:
        w, h = court().size
        p = subprocess.Popen(
            ["ffmpeg", "-v", "error", "-y", "-f", "rawvideo", "-pix_fmt", "rgb24", "-s", f"{w}x{h}",
             "-r", "10", "-i", "-", "-c:v", "libx264", "-pix_fmt", "yuv420p", "-crf", "20", out],
            stdin=subprocess.PIPE,
        )
        for i in range(i0, i1):
            p.stdin.write(frame(F, i, i0, title).tobytes())
        p.stdin.close()
        p.wait()
