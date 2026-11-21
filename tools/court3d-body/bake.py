# A TOOL, not part of the game: the body built in Blender (classic.py's
# mesh, weights.py's weights, on the probe's skeleton) baked for the court -
# src/ui/views/LiveGame/court3d/bodyMesh.meta.ts (what the court needs to
# know of him from the start) and bodyMesh.data.ts (the mesh, fetched only
# when a man is drawn as one - see meshBody.ts).
#
#   python3 tools/court3d-body/bake.py masses.json out_dir \
#     src/ui/views/LiveGame/court3d/bodyMesh.data.ts
#
# Everything in heights (his height 1), his own frame: f forward, s his
# left, u up from the floor. Each point is held in up to four pieces' frames
# (meshBody.ts poses them), as much in each as its weight.
import base64
import json
import sys

import numpy as np

SRC, DIR, OUT = sys.argv[1], sys.argv[2], sys.argv[3]
D = json.load(open(SRC))
SK = D["sk"]
HGT = D["body"]["H"]
M = np.load(f"{DIR}/classic.npz")
Wz = np.load(f"{DIR}/weights.npz")
SEG = [str(s) for s in Wz["seg"]]
SI = {n: i for i, n in enumerate(SEG)}
P = M["P"] / HGT
T = M["T"]
W = Wz["W"].astype(float)
# (His hands are the court's own, sculpted over him: none of him goes with
# them - his wrist with his forearm.)
for s in "RL":
    W[:, SI["fore" + s]] += W[:, SI["hand" + s]]
    W[:, SI["hand" + s]] = 0
W /= np.maximum(W.sum(axis=1, keepdims=True), 1e-9)


def j(v):
    return np.array([v["f"], v["s"], v["u"]]) / HGT


J = {"pelvis": j(SK["pelvis"]), "chest": j(SK["chest"]), "head": j(SK["head"])}
for s in "RL":
    leg, arm = SK["leg" + s], SK["arm" + s]
    J["hip" + s], J["knee" + s], J["ankle" + s] = j(leg["root"]), j(leg["mid"]), j(leg["end"])
    J["toe" + s] = j(leg.get("tip") or leg["end"])
    J["shoulder" + s], J["elbow" + s], J["wrist" + s] = j(arm["root"]), j(arm["mid"]), j(arm["end"])
    J["handTip" + s] = j(arm.get("tip") or arm["end"])
    # (His palm toward his side, square to his hand.)
    ha = J["handTip" + s] - J["wrist" + s]
    ha /= np.linalg.norm(ha)
    inward = np.array([0.0, -1.0 if s == "L" else 1.0, 0.0])
    inward -= ha * inward.dot(ha)
    J["palm" + s] = J["wrist" + s] + 0.01 * inward / np.linalg.norm(inward)

# ---- what each face of him is ---------------------------------------------
# sculpt.ts kinds, as meshBody.ts names them.
TORSO, SHORTS, UPPER, FORE, THIGH, SHIN, SHOE, NECK, TONGUE = 0, 1, 2, 3, 5, 6, 7, 8, 14
# (His head: classic.py's own kind - the sculpt draws heads apart.)
HEAD_KIND = 99


def name_of(kind, side):
    sd = "R" if side == 0 else "L"
    return {
        TORSO: "torso",
        NECK: "neck",
        SHORTS: "shorts" + sd,
        UPPER: "upper" + sd,
        FORE: "fore" + sd,
        THIGH: "thigh" + sd,
        SHIN: "shin" + sd,
        SHOE: "shoe" + sd,
        TONGUE: "shoe" + sd,
        HEAD_KIND: "head",
    }.get(int(kind), "torso")


kind_p = M["kind"]
side_p = M["side"]
# (His shoe up to where the sculpt has its top - sculpt.ts's cut.shoeTop -
# his sock above it: one skin, so cut by height, not by mass.)
B = D["body"]
SHOE_TOP = (B["ankleH"] + B["ankleR"] * 1.4) / HGT
names = []
for t in T:
    # (A face is what most of its corners are - or the first's.)
    ks = [name_of(kind_p[v], side_p[v]) for v in t]
    n = max(set(ks), key=ks.count)
    if n.startswith(("shoe", "shin")):
        u = P[t, 2].mean()
        n = ("shoe" if u <= SHOE_TOP else "shin") + n[-1]
    names.append(n)
KINDS = sorted(set(names))
kind = np.array([KINDS.index(n) for n in names], np.uint8)
print(f"points {len(P)}, faces {len(T)}, kinds {KINDS}", file=sys.stderr)


# ---- each point in each of its pieces' frames ------------------------------
def frame(h, t, foot=False):
    a = t - h
    a = a / np.linalg.norm(a)
    f = np.array([0, 0, 1.0]) if foot else np.array([1.0, 0, 0])
    f = f - a * f.dot(a)
    f = f / np.linalg.norm(f)
    return h, a, f, np.cross(a, f)


FR = {}
pel, che = J["pelvis"], J["chest"]
for n in ("pelvis", "spine1", "spine2", "spine3", "spine4", "chest"):
    FR[n] = frame(pel, che)
for n in ("neck1", "neck2", "head"):
    FR[n] = frame(che, J["head"])
for s in "RL":
    FR["shoulder" + s] = frame(J["shoulder" + s], J["elbow" + s])
    FR["upper" + s] = frame(J["shoulder" + s], J["elbow" + s])
    FR["fore" + s] = frame(J["elbow" + s], J["wrist" + s])
    ha = J["handTip" + s] - J["wrist" + s]
    ha = ha / np.linalg.norm(ha)
    hf = J["palm" + s] - J["wrist" + s]
    hf = hf - ha * hf.dot(ha)
    hf /= np.linalg.norm(hf)
    FR["hand" + s] = (J["wrist" + s], ha, hf, np.cross(ha, hf))
    FR["thigh" + s] = frame(J["hip" + s], J["knee" + s])
    FR["shin" + s] = frame(J["knee" + s], J["ankle" + s])
    FR["foot" + s] = frame(J["ankle" + s], J["toe" + s], True)
infl = []
for i, p in enumerate(P):
    w = W[i]
    order = np.argsort(-w)[:4]
    ws = w[order]
    row = []
    for bi, wv in zip(order, ws):
        if wv / max(ws.sum(), 1e-9) < 0.02:
            continue
        h, a, f, s = FR[SEG[bi]]
        d = p - h
        row.append((bi, wv, d.dot(a), d.dot(f), d.dot(s)))
    if not row:
        h, a, f, s = FR["chest"]
        d = p - h
        row = [(SI["chest"], 1.0, d.dot(a), d.dot(f), d.dot(s))]
    tot = sum(r[1] for r in row)
    infl.append([(r[0], r[1] / tot, r[2], r[3], r[4]) for r in row])

# ---- out ----------------------------------------------------------------------
counts = np.array([len(r) for r in infl], np.uint8)
flat = [x for r in infl for x in r]
bone = np.array([x[0] for x in flat], np.uint8)
wt = np.array([x[1] for x in flat], np.float32)
loc = np.array([[x[2], x[3], x[4]] for x in flat], np.float32)
tri = np.array(T, np.uint16)
# How much each point goes with his arm (past his shoulder): his jersey cut
# where it does (see sculpt.ts), smoothly across its faces - his shoulders'
# points, the round of his deltoid, partly his arm's.
ARM = [SI[n] for n in SEG if n.startswith(("upper", "fore", "hand"))]
SHO = [SI[n] for n in SEG if n.startswith("shoulder")]
armW = W[:, ARM].sum(axis=1) + 0.6 * W[:, SHO].sum(axis=1)
arm = np.round(np.clip(armW, 0, 1) * 255).astype(np.uint8)
# ---- his face: where on a faces.js face each point of his head is ----------
# faces.js draws a face in a 400 x 600 box, his head round (200, 300), 400
# across crown to chin - and the court draws that head as a circle of his
# head's radius round where his head is (figure.ts). So a point of his head
# goes to the face as if seen straight from in front, at that scale. With
# it: how much it faces forward (from in front, the face is drawn on; side
# on, round the back, his hair is his haircut - see sculpt.ts), and how far
# round his head it is (1 in front, -1 behind).
headP = np.array(np.isin(kind_p, [HEAD_KIND]))
face = np.tile(np.array([200.0, 1000.0, 0.0, 0.0]), (len(P), 1))
if headP.any():
    R = B["headR"] / HGT
    C = J["head"]
    N = np.zeros_like(P)
    fn = np.cross(P[T[:, 1]] - P[T[:, 0]], P[T[:, 2]] - P[T[:, 0]])
    for k in range(3):
        np.add.at(N, T[:, k], fn)
    N /= np.maximum(np.linalg.norm(N, axis=1, keepdims=True), 1e-12)
    hp = P[headP]
    fc = (hp[:, 0].max() + hp[:, 0].min()) / 2
    d = np.stack([hp[:, 0] - fc, hp[:, 1]], axis=1)
    ct = d[:, 0] / np.maximum(np.linalg.norm(d, axis=1), 1e-9)
    face[headP] = np.stack(
        [200 + (hp[:, 1] - C[1]) * 200 / R, 300 - (hp[:, 2] - C[2]) * 200 / R, N[headP, 0], ct], axis=1
    )
# (faces.js units x 8; facing and round his head x 1000.)
face = np.round(face * np.array([8, 8, 1000, 1000])).astype(np.int16)
b64 = lambda a: base64.b64encode(np.ascontiguousarray(a).tobytes()).decode()
joints = {k: [round(float(x), 5) for x in v] for k, v in J.items()}
HEAD = (
    "// THE PLAYERS' BODY, BAKED by tools/court3d-body/bake.py - the court's own\n"
    "// Classic player built in Blender (classic.py) and rigged there\n"
    "// (weights.py). Do not edit: bake it again.\n"
)
LOCAL_STEP = float(np.abs(loc).max()) / 32000
wq = np.round(wt * 255).astype(np.uint8)
lq = np.round(loc / LOCAL_STEP).astype(np.int16)
META = OUT.replace(".data.ts", ".meta.ts")
with open(META, "w") as o:
    o.write(HEAD)
    o.write("export const BODY_META = {\n")
    o.write(f"\tpoints: {len(P)},\n\tbones: {json.dumps(SEG)},\n\tkinds: {json.dumps(KINDS)},\n")
    o.write(f"\tjoints: {json.dumps(joints)},\n\tlocalStep: {LOCAL_STEP!r},\n")
    o.write("} as const;\n")
with open(OUT, "w") as o:
    o.write(HEAD)
    o.write("export const BODY_DATA = {\n")
    o.write(f'\tcounts: "{b64(counts)}",\n\tbone: "{b64(bone)}",\n\tweight: "{b64(wq)}",\n')
    o.write(f'\tlocal: "{b64(lq)}",\n\ttris: "{b64(tri)}",\n\tkind: "{b64(kind)}",\n\tarm: "{b64(arm)}",\n\tface: "{b64(face)}",\n')
    o.write("};\n")
print(f"bytes {len(open(OUT).read())} + {len(open(META).read())}", file=sys.stderr)
