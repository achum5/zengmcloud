# One small zoomed panel per flag of one kind, four to a row, across tapes.
#
#   python3 sheet.py <kind> <out.png> <tape.json> [<tape.json> ...]
#
# Env: MAX (panels, default 12), SKIP (flags to skip first), R (half-width of
# a panel, feet, default 12), S (pixels a foot, default 18).
# Each panel: the 1.2 s before the flag and 0.6 s after, as fading trails;
# the flagged man green (the other man of a pair purple), facing ticks and
# animation names at the flagged moment, the ball orange.
import json, math, os, re, sys
from PIL import Image, ImageDraw

kind, out, tapes = sys.argv[1], sys.argv[2], sys.argv[3:]
MAX = int(os.environ.get("MAX", "12")); SKIP = int(os.environ.get("SKIP", "0"))
R = int(os.environ.get("R", "12")); S = int(os.environ.get("S", "18"))
items = []
for path in tapes:
    tape = json.load(open(path))
    team = {p["pid"]: p["team"] for p in tape["players"]}
    for P in tape["possessions"]:
        for fl in P["flags"]:
            if fl["kind"] == kind:
                items.append((tape["game"], team, P, fl))
items = items[SKIP:SKIP + MAX]
size = 2 * R * S
panels = []
for game, team, P, fl in items:
    pair = [int(x) for x in re.findall(r"(\d+) and (\d+)", fl["text"])[0]] if " and " in fl["text"] else []
    fr = [f for f in P["frames"] if fl["t"] - 1200 <= f["t"] <= fl["t"] + 600]
    if not fr:
        continue
    cen = (47, 25)
    for f in fr:
        if abs(f["t"] - fl["t"]) < 60:
            for pid, x, y, yaw, anim, sh in f["p"]:
                if pid == fl.get("pid"):
                    cen = (x, y)
    img = Image.new("RGB", (size, size + 30), "white"); d = ImageDraw.Draw(img)
    X = lambda x: (x - cen[0] + R) * S; Y = lambda y: (y - cen[1] + R) * S
    d.rectangle([X(0), Y(0), X(94), Y(50)], outline=(160, 160, 160))
    d.line([X(47), Y(0), X(47), Y(50)], fill=(200, 200, 200))
    for rx in (5.25, 88.75):
        d.ellipse([X(rx) - 5, Y(25) - 5, X(rx) + 5, Y(25) + 5], outline="orange", width=2)
        d.arc([X(rx) - 23.75 * S, Y(25) - 23.75 * S, X(rx) + 23.75 * S, Y(25) + 23.75 * S], 0, 360, fill=(225, 225, 225))
    for x0 in (0, 75):
        d.rectangle([X(x0), Y(17), X(x0 + 19), Y(33)], outline=(220, 220, 220))
    n = len(fr)
    for i, f in enumerate(fr):
        a = i / max(1, n - 1); last = abs(f["t"] - fl["t"]) < 60
        for pid, x, y, yaw, anim, sh in f["p"]:
            if not sh:
                continue
            base = (40, 90, 230) if team[pid] == 0 else (220, 50, 50)
            if pid == fl.get("pid"):
                base = (0, 160, 0)
            elif pid in pair:
                base = (150, 0, 200)
            col = tuple(int(255 - (255 - c) * (0.2 + 0.8 * a)) for c in base)
            r = 2.2 if not last else 6
            d.ellipse([X(x) - r, Y(y) - r, X(x) + r, Y(y) + r], fill=col)
            if last:
                d.line([X(x), Y(y), X(x + math.cos(yaw) * 1.8), Y(y + math.sin(yaw) * 1.8)], fill="black", width=2)
                d.text((X(x) + 7, Y(y) - 7), f"{pid} {anim}", fill="black")
        bx, by = f["b"][0], f["b"][1]
        d.ellipse([X(bx) - 2, Y(by) - 2, X(bx) + 2, Y(by) + 2], fill=(255, 140, 0))
    bt = [b["type"] for b in P["beats"] if fl["t"] - 3000 <= b["t"] <= fl["t"] + 3000]
    d.text((4, size + 2), f"{game} p{P['i']} t{fl['t']} {fl['text']}"[:72], fill=(170, 0, 0))
    d.text((4, size + 15), ",".join(bt)[:72], fill="black")
    panels.append(img)
if not panels:
    sys.exit(f"no {kind} flags")
cols = 4; rows = (len(panels) + cols - 1) // cols
tot = Image.new("RGB", (size * cols, (size + 30) * rows), "white")
for k, im in enumerate(panels):
    tot.paste(im, ((k % cols) * size, (k // cols) * (size + 30)))
tot.save(out); print(out, len(panels), "panels")
