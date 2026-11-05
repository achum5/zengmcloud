# One play of a football game tape, as top-down panels of 1.5 s each (two to
# a row):
#
#   python3 view.py <tape.json> <play> <out.png> [<from ms> <to ms>] [<pid>]
#
# Times are from the snap. Each panel shows the part of the field the men are
# on. Trails fade in over each panel; facing ticks, positions and moves at its
# last moment; the ball orange; <pid> drawn green; the line of scrimmage blue;
# flags in each panel listed under it.
import json, math, sys
from PIL import Image, ImageDraw

tape = json.load(open(sys.argv[1])); pi = int(sys.argv[2]); out = sys.argv[3]
P = [p for p in tape["plays"] if p["i"] == pi][0]
fr = P["frames"]
ta = int(sys.argv[4]) if len(sys.argv) > 4 else fr[0]["t"]
tb = int(sys.argv[5]) if len(sys.argv) > 5 else fr[-1]["t"]
focus = int(sys.argv[6]) if len(sys.argv) > 6 else None
fr = [f for f in fr if ta <= f["t"] <= tb]
team = {p["pid"]: p["team"] for p in tape["players"]}
roles = P["roles"]
# The window: everybody over the whole stretch, and the ball, a little room
# round them, kept on the field (and its borders).
xs = [x for f in fr for _, x, _, _, _ in f["p"]] + [f["b"][0] for f in fr if f["b"][0] >= 0]
ys = [y for f in fr for _, _, y, _, _ in f["p"]] + [f["b"][1] for f in fr if f["b"][0] >= 0]
x0, x1 = max(-12, min(xs) - 8), min(372, max(xs) + 8)
y0, y1 = max(-12, min(ys) - 8), min(172, max(ys) + 8)
S = min(9, 900 / (x1 - x0), 640 / (y1 - y0)); pad = 12
W, H = int((x1 - x0) * S) + 2 * pad, int((y1 - y0) * S) + 2 * pad
chunks = []; cur = []
for f in fr:
    cur.append(f)
    if f["t"] - cur[0]["t"] >= 1500:
        chunks.append(cur); cur = [f]
if len(cur) > 1:
    chunks.append(cur)
flags = P["flags"]
imgs = []
for ch in chunks:
    img = Image.new("RGB", (W, H + 34), "white"); d = ImageDraw.Draw(img)
    X = lambda x: pad + (x - x0) * S; Y = lambda y: pad + (y - y0) * S
    d.rectangle([X(0), Y(0), X(360), Y(160)], outline=(150, 150, 150))
    for k in range(21):
        gx = 30 + 15 * k
        d.line([X(gx), Y(0), X(gx), Y(160)], fill=(150, 150, 150) if k in (0, 20) else (215, 215, 215), width=2 if k in (0, 20) else 1)
    d.line([X(P["losX"]), Y(0), X(P["losX"]), Y(160)], fill=(90, 140, 255), width=2)
    n = len(ch)
    for i, f in enumerate(ch):
        last = i == n - 1
        a = i / max(1, n - 1)
        for pid, x, y, yaw, move in f["p"]:
            base = (40, 90, 230) if team.get(pid) == 0 else (220, 50, 50)
            if pid == focus:
                base = (0, 150, 0)
            col = tuple(int(255 - (255 - c) * (0.25 + 0.75 * a)) for c in base)
            r = 1.6 if not last else 5
            d.ellipse([X(x) - r, Y(y) - r, X(x) + r, Y(y) + r], fill=col)
            if last:
                d.line([X(x), Y(y), X(x + math.cos(yaw) * 2), Y(y + math.sin(yaw) * 2)], fill="black", width=2)
                d.text((X(x) + 6, Y(y) - 6), f"{roles.get(str(pid), '')}{pid} {move}", fill="black")
        bx, by, bz, h = f["b"]
        if bx >= 0:
            d.ellipse([X(bx) - 1.8, Y(by) - 1.8, X(bx) + 1.8, Y(by) + 1.8], fill=(255, 140, 0))
    t0 = ch[0]["t"]; t1 = ch[-1]["t"]
    fl = [x for x in flags if t0 - 50 <= x["t"] <= t1 + 50]
    for x in fl:
        f = min(ch, key=lambda f: abs(f["t"] - x["t"]))
        for pid, px, py, _, _ in f["p"]:
            if pid == x["pid"]:
                d.ellipse([X(px) - 9, Y(py) - 9, X(px) + 9, Y(py) + 9], outline=(0, 0, 0), width=2)
    d.text((pad, H - 2), f"play {pi} {P['kind']} {P['situation']}  {t0}-{t1} ms (whistle {P['whistle']})", fill="black")
    d.text((pad, H + 14), ("FLAGS: " + "; ".join(f"{x['kind']} {x['text']}@{x['t']}" for x in fl))[:160], fill=(180, 0, 0))
    imgs.append(img)
cols = 2
rows = (len(imgs) + cols - 1) // cols
w = imgs[0].width; h = imgs[0].height
tot = Image.new("RGB", (w * cols, h * rows), "white")
for k, im in enumerate(imgs):
    tot.paste(im, ((k % cols) * w, (k // cols) * h))
tot.save(out); print(out, len(imgs), "panels")
