# One small zoomed panel per flag of one kind, four to a row, across football
# tapes.
#
#   python3 sheet.py <kind> <out.png> <tape.json> [<tape.json> ...]
#
# Env: MAX (panels, default 12), SKIP (flags to skip first), R (half-width of
# a panel, feet, default 15), S (pixels a foot, default 14).
# Each panel: the 1.2 s before the flag and 0.6 s after, as fading trails;
# the flagged man green, facing ticks and moves at the flagged moment, the
# ball orange, the yard lines grey and the line of scrimmage blue.
import json, math, os, sys
from PIL import Image, ImageDraw

kind, out, tapes = sys.argv[1], sys.argv[2], sys.argv[3:]
MAX = int(os.environ.get("MAX", "12")); SKIP = int(os.environ.get("SKIP", "0"))
R = int(os.environ.get("R", "15")); S = int(os.environ.get("S", "14"))
items = []
for path in tapes:
    tape = json.load(open(path))
    team = {p["pid"]: p["team"] for p in tape["players"]}
    for P in tape["plays"]:
        for fl in P["flags"]:
            if fl["kind"] == kind:
                items.append((tape["game"], team, P, fl))
items = items[SKIP:SKIP + MAX]
size = 2 * R * S
panels = []
for game, team, P, fl in items:
    fr = [f for f in P["frames"] if fl["t"] - 1200 <= f["t"] <= fl["t"] + 600]
    if not fr:
        continue
    cen = (180, 80)
    for f in fr:
        if abs(f["t"] - fl["t"]) < 60:
            for pid, x, y, yaw, move in f["p"]:
                if pid == fl["pid"]:
                    cen = (x, y)
    img = Image.new("RGB", (size, size + 30), "white"); d = ImageDraw.Draw(img)
    X = lambda x: (x - cen[0] + R) * S; Y = lambda y: (y - cen[1] + R) * S
    d.rectangle([X(0), Y(0), X(360), Y(160)], outline=(160, 160, 160))
    for k in range(21):
        gx = 30 + 15 * k
        d.line([X(gx), Y(0), X(gx), Y(160)], fill=(215, 215, 215))
    d.line([X(P["losX"]), Y(0), X(P["losX"]), Y(160)], fill=(90, 140, 255), width=2)
    n = len(fr)
    for i, f in enumerate(fr):
        a = i / max(1, n - 1); last = abs(f["t"] - fl["t"]) < 60
        for pid, x, y, yaw, move in f["p"]:
            base = (40, 90, 230) if team.get(pid) == 0 else (220, 50, 50)
            if pid == fl["pid"]:
                base = (0, 160, 0)
            col = tuple(int(255 - (255 - c) * (0.2 + 0.8 * a)) for c in base)
            r = 2.2 if not last else 6
            d.ellipse([X(x) - r, Y(y) - r, X(x) + r, Y(y) + r], fill=col)
            if last:
                d.line([X(x), Y(y), X(x + math.cos(yaw) * 1.8), Y(y + math.sin(yaw) * 1.8)], fill="black", width=2)
                d.text((X(x) + 7, Y(y) - 7), f"{pid} {move}", fill="black")
        bx, by = f["b"][0], f["b"][1]
        if bx >= 0:
            d.ellipse([X(bx) - 2, Y(by) - 2, X(bx) + 2, Y(by) + 2], fill=(255, 140, 0))
    d.text((4, size + 2), f"{game} p{P['i']} t{fl['t']} {fl['text']}"[:72], fill=(170, 0, 0))
    d.text((4, size + 15), f"{P['kind']} {P['situation']}"[:72], fill="black")
    panels.append(img)
if not panels:
    sys.exit(f"no {kind} flags")
cols = 4; rows = (len(panels) + cols - 1) // cols
tot = Image.new("RGB", (size * cols, (size + 30) * rows), "white")
for k, im in enumerate(panels):
    tot.paste(im, ((k % cols) * size, (k // cols) * (size + 30)))
tot.save(out); print(out, len(panels), "panels")
