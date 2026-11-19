# THE PLAYERS' BODY, BAKED (see src/ui/views/LiveGame/court3d/meshBody.ts).
#
# All of it MakeHuman's (CC0 - released to the public domain, 2020): his
# body its own base mesh hm08, built by its targets into an athlete (young,
# male, lean and strong) and brought down to as many points as the court can
# draw quickly (LOD - see decimate.py), his head left whole for his face to go
# on; every point weighted by its default skeleton's weights. His kit is
# painted onto him (see sculpt.ts), but for his shorts: his hips and thighs
# over again, stood off him and draped, baggy to his knees. On his feet, its
# sneakers shoes05. (BODY=low CLOTHES=suit: its low-poly male in its casual
# suit instead - the first try.)
#
#   LOD=5500 python3 tools/court3d-body/bake.py <makehuman data> \
#     <makehuman assets> src/ui/views/LiveGame/court3d/bodyMesh.data.ts
#
# (Writes bodyMesh.meta.ts beside it.)
#
# <makehuman data>: as the app ships it (github.com/makehumancommunity/
# makehuman, makehuman/data/): 3dobjs/base.obj, rigs/default.mhskel,
# rigs/default_weights.mhw, and targets/macrodetails/ -
# {african,caucasian}-male-young, universal-male-young-maxmuscle-
# averageweight, universal-male-young-averagemuscle-minweight.
# <makehuman assets>: its CC0 system assets pack (files.makehumancommunity.org
# /asset_packs/makehuman_system_assets/makehuman_system_assets_cc0.zip),
# unzipped.
import base64, json, math, os, sys
import numpy as np

DATA, ASSETS, OUT = sys.argv[1], sys.argv[2], sys.argv[3]

# ---- the base mesh, his build --------------------------------------------------
V = []
for line in open(f"{DATA}/3dobjs/base.obj"):
    if line.startswith("v "):
        V.append([float(x) for x in line.split()[1:4]])
V = np.array(V)
def target(name, w):
    for line in open(f"{DATA}/targets/macrodetails/{name}.target"):
        if line[:1].isdigit():
            i, dx, dy, dz = line.split()
            V[int(i)] += w * np.array([float(dx), float(dy), float(dz)])
target("african-male-young", 0.6)
target("caucasian-male-young", 0.4)
target("universal-male-young-maxmuscle-averageweight", 0.55)
target("universal-male-young-averagemuscle-minweight", 0.45)

skel = json.load(open(f"{DATA}/rigs/default.mhskel"))
def joint(name): return V[skel["joints"][name]].mean(axis=0)
def bhead(b): return joint(skel["bones"][b]["head"])
def btail(b): return joint(skel["bones"][b]["tail"])
assert bhead("upperarm01.L")[0] > 0, "his left is +x"

# ---- the base mesh's weights, per point -------------------------------------------
MHW = json.load(open(f"{DATA}/rigs/default_weights.mhw"))["weights"]
BONES_MH = sorted(MHW)
BI_MH = {b: i for i, b in enumerate(BONES_MH)}
WB = np.zeros((len(V), len(BONES_MH)), np.float32)
for b, pairs in MHW.items():
    for vi, w in pairs:
        WB[vi, BI_MH[b]] += w
WB /= np.maximum(WB.sum(axis=1, keepdims=True), 1e-9)

# ---- fitted the MakeHuman way: each point held to three of the base mesh's --------
def fitted(path):
    refs = []; scale = {}; mode = None
    for line in open(path):
        p = line.split()
        if not p or p[0].startswith("#"): continue
        if p[0] in ("x_scale", "y_scale", "z_scale"):
            a, b, d = int(p[1]), int(p[2]), float(p[3])
            ax = "xyz".index(p[0][0])
            scale[ax] = abs(V[a][ax] - V[b][ax]) / d
            continue
        if p[0] == "verts": mode = "v"; continue
        if p[0] == "delete_verts": mode = None; continue
        # (Settings among the points - a material, a weights file - are not
        # points.)
        if not p[0].lstrip("-").replace(".", "", 1).isdigit(): continue
        if mode == "v":
            if len(p) == 9:
                refs.append(([int(x) for x in p[:3]], [float(x) for x in p[3:6]], [float(x) for x in p[6:9]]))
            elif len(p) == 1 or (len(p) == 2 and p[1].replace(".", "", 1).isdigit()):
                refs.append(([int(p[0])], [1.0], [0, 0, 0]))
            else:
                mode = None
    sc = np.array([scale.get(i, 1.0) for i in range(3)])
    P = np.array([sum(w * V[r] for r, w in zip(rs, ws)) + np.array(ds) * sc for rs, ws, ds in refs])
    W = np.array([sum(w * WB[r] for r, w in zip(rs, ws)) for rs, ws, _ in refs])
    return P, W
def obj_faces(path):
    F = []
    for line in open(path):
        if line.startswith("f "):
            f = [int(x.split("/")[0]) - 1 for x in line.split()[1:]]
            for k in range(1, len(f) - 1):
                F.append((f[0], f[k], f[k + 1]))
    return F

# MakeHuman: y up, the face toward +z, his left toward +x. Ours: f forward,
# s his left, u up - in units of his height, his feet on the floor.
# BODY=full (the default): MakeHuman's own body, every point of it - its
# chest and back, his knees, his hands. BODY=low: its low-poly male.
# CLOTHES=painted (the default): his kit painted onto him (see sculpt.ts);
# CLOTHES=suit: the suit's t-shirt and trousers over him.
BODY = os.environ.get("BODY", "full")
CLOTHES = os.environ.get("CLOTHES", "painted")
if BODY == "full":
    bodyF = []; grp = None
    for line in open(f"{DATA}/3dobjs/base.obj"):
        if line.startswith("g "): grp = line.split()[1]
        elif line.startswith("f ") and grp == "body":
            f = [int(x.split("/")[0]) - 1 for x in line.split()[1:]]
            for k in range(1, len(f) - 1): bodyF.append((f[0], f[k], f[k + 1]))
    used = sorted({v for t in bodyF for v in t})
    at = {v: i for i, v in enumerate(used)}
    bodyF = [tuple(at[v] for v in t) for t in bodyF]
    bodyP = V[used]; bodyW = WB[used]

else:
    bodyP, bodyW = fitted(f"{ASSETS}/proxymeshes/male1591/male1591.proxy")
    bodyF = obj_faces(f"{ASSETS}/proxymeshes/male1591/male1591.obj")
floor = bodyP[:, 1].min(); crown = bodyP[:, 1].max(); HGT = crown - floor
def ours(p): return np.array([p[..., 2], p[..., 0], p[..., 1] - floor]).T / HGT if p.ndim > 1 else np.array([p[2], p[0], p[1] - floor]) / HGT

# ---- the court's pieces of him ----------------------------------------------------
# Each MakeHuman bone moves with one of these (see meshBody.ts): his spine
# and neck a little at a time, his shoulders half with his chest and half
# with his arms.
SEG = ["pelvis", "spine1", "spine2", "spine3", "spine4", "chest", "neck1", "neck2", "head",
       "shoulderR", "shoulderL", "upperR", "upperL", "foreR", "foreL", "handR", "handL",
       "thighR", "thighL", "shinR", "shinL", "footR", "footL"]
SI = {n: i for i, n in enumerate(SEG)}
def seg_of(mh):
    side = "L" if mh.endswith(".L") else "R" if mh.endswith(".R") else ""
    base = mh.split(".")[0]
    table = {"root": "pelvis", "spine05": "pelvis", "pelvis": "pelvis", "spine04": "spine1",
             "spine03": "spine2", "spine02": "spine3", "spine01": "spine4", "breast": "chest",
             "clavicle": "chest", "neck01": "neck1", "neck02": "neck2", "neck03": "head"}
    if base in table: return table[base]
    if base == "shoulder01": return "shoulder" + side
    if base.startswith("upperarm"): return "upper" + side
    if base.startswith("lowerarm"): return "fore" + side
    if base == "wrist" or base.startswith("metacarpal") or base.startswith("finger"): return "hand" + side
    if base.startswith("upperleg"): return "thigh" + side
    if base.startswith("lowerleg"): return "shin" + side
    if base == "foot" or base.startswith("toe"): return "foot" + side
    return "head"
M = np.zeros((len(BONES_MH), len(SEG)), np.float32)
for b, i in BI_MH.items():
    M[i, SI[seg_of(b)]] = 1
def segw(W): return W @ M

# His joints, rest, as the court's skeleton has them.
J = {}
for s, S in (("R", ".R"), ("L", ".L")):
    J["hip" + s] = ours(bhead("upperleg01" + S)); J["knee" + s] = ours(bhead("lowerleg01" + S))
    J["ankle" + s] = ours(bhead("foot" + S)); J["toe" + s] = ours(btail("foot" + S))
    J["shoulder" + s] = ours(bhead("upperarm01" + S)); J["elbow" + s] = ours(bhead("lowerarm01" + S))
    J["wrist" + s] = ours(bhead("wrist" + S))
# His hands: wrist to the knuckle of his middle finger, and a point off
# his palm (the side his fingers curl to).
for s, S in (("R", ".R"), ("L", ".L")):
    J["handTip" + s] = ours(bhead("finger3-1" + S))
    ha = J["handTip" + s] - J["wrist" + s]; ha /= np.linalg.norm(ha)
    ks = ours(bhead("finger2-1" + S)) - ours(bhead("finger5-1" + S))
    pn = np.cross(ha, ks); pn /= np.linalg.norm(pn)
    curl = ours(btail("finger3-3" + S)) - J["handTip" + s]
    if pn.dot(curl - ha * curl.dot(ha)) < 0: pn = -pn
    J["palm" + s] = J["wrist" + s] + pn * 0.02
J["pelvis"] = (J["hipR"] + J["hipL"]) / 2
J["chest"] = ours(bhead("neck01")); J["head"] = ours(bhead("head"))

# ---- the pieces, out of their meshes ------------------------------------------------
def components(F, n):
    par = list(range(n))
    def find(a):
        while par[a] != a: par[a] = par[par[a]]; a = par[a]
        return a
    for t in F:
        for i in (1, 2): par[find(t[0])] = find(t[i])
    return [find(i) for i in range(n)]
parts = []  # (name, P (ours), segment weights, faces, face kinds)
def dom(W, i): return SEG[int(np.argmax(W[i]))]
def side(seg): return seg[-1] if seg[-1] in "RL" else ""
# His body: his hands off; his head kept (kind "head": not drawn yet).
P = ours(bodyP); W = segw(bodyW)
# LOD=<points>: fewer faces, the same man (see decimate.py) - his head, for
# his face to go on, left as it is.
if BODY == "full" and os.environ.get("LOD"):
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from decimate import decimate
    face = W[:, SI["head"]] + W[:, SI["neck2"]] > 0.5
    bodyF = decimate(P, bodyF, int(os.environ["LOD"]), lambda u, v: not face[u])
    print(f"decimated: faces {len(bodyF)}", file=sys.stderr)
def body_kind(t):
    ds = [dom(W, v) for v in t]
    # (His hand off where all of a face is his hand's: the faces round his
    # wrist are his forearm's - it is only three rings of points long.)
    if BODY != "full":
        if all(x.startswith("hand") for x in ds): return None
        ds = ["fore" + x[4:] if x.startswith("hand") else x for x in ds]
    d = max(sorted(set(ds)), key=ds.count)
    if d in ("pelvis", "spine1", "spine2", "spine3", "spine4", "chest"): return "torso"
    if d in ("neck1", "neck2"): return "neck"
    if d == "head": return "head"
    if d.startswith("shoulder"): return "upper" + side(d)
    return d
parts.append(("body", P, W, bodyF, body_kind))
# His shorts, baggy as a player's are: his hips and thighs over again,
# stood off him - close at the waistband, more and more down to a hem at his
# knees. (Held to him as the points of him they stand off are, so they swing
# with his legs.)
if BODY == "full" and CLOTHES == "painted":
    # (Up past the top of the waistband - cut there as painted: see
    # sculpt.ts.)
    waistTop = J["pelvis"][2] + 0.11
    knee = (J["kneeR"][2] + J["kneeL"][2]) / 2 - 0.06
    N = np.zeros_like(P)
    Fb = np.array(bodyF)
    fn = np.cross(P[Fb[:, 1]] - P[Fb[:, 0]], P[Fb[:, 2]] - P[Fb[:, 0]])
    for k in range(3): np.add.at(N, Fb[:, k], fn)
    N /= np.maximum(np.linalg.norm(N, axis=1, keepdims=True), 1e-12)
    inside = (P[:, 2] <= waistTop) & (P[:, 2] >= knee)
    sF = [t for t in bodyF if all(inside[v] for v in t)
          and not any(dom(W, v).startswith(("hand", "fore")) for v in t)]
    used = sorted({v for t in sF for v in t})
    at = {v: i for i, v in enumerate(used)}
    tt = np.clip((waistTop - P[used, 2]) / (waistTop - knee), 0, 1)
    off = 0.004 + 0.02 * tt ** 1.3
    # (Across between his legs, hanging: not round him like tights - the
    # inside of each leg stood off toward the other's, the crotch low.)
    Nu = N[used]; Pu_ = P[used]
    inner = np.maximum(0, -np.sign(Pu_[:, 1]) * Nu[:, 1])
    crotch = np.clip((J["pelvis"][2] - Pu_[:, 2]) / 0.06 + 1, 0, 1) * np.clip((Pu_[:, 2] - knee) / 0.08, 0, 1)
    off = off + 0.016 * inner * crotch + 0.014 * np.maximum(0, -Nu[:, 2]) * crotch
    sP = Pu_ + Nu * off[:, None]
    # (Cloth, not skin: draped over the creases of him - his groin, between
    # his legs - not dipping into them; never closer to him than it stands
    # off.)
    nbr = [set() for _ in used]
    for t in sF:
        for i in range(3):
            a0, b0 = at[t[i]], at[t[(i + 1) % 3]]
            nbr[a0].add(b0); nbr[b0].add(a0)
    for _ in range(8):
        avg = np.array([sP[list(nb)].mean(axis=0) if nb else sP[i] for i, nb in enumerate(nbr)])
        sP = sP + 0.6 * (avg - sP)
        d = np.einsum("ij,ij->i", sP - Pu_, Nu)
        sP = sP + Nu * np.maximum(0, off - d)[:, None]
    sW = W[used]
    sF = [tuple(at[v] for v in t) for t in sF]
    def shorts_kind(t, W=sW):
        ds = [dom(W, v) for v in t]
        d = max(sorted(set(ds)), key=ds.count)
        if d.startswith(("thigh", "shin", "foot")): return "shorts" + side(d)
        return "shortsTorso"
    parts.append(("shorts", sP, sW, sF, shorts_kind))
# His suit: the t-shirt and the trousers, apart.
groups = {}
if CLOTHES == "suit":
    cP, cW = fitted(f"{ASSETS}/clothes/male_casualsuit06/male_casualsuit06.mhclo")
    cF = obj_faces(f"{ASSETS}/clothes/male_casualsuit06/male_casualsuit06.obj")
    cPo = ours(cP); cWs = segw(cW)
    comp = components(cF, len(cPo))
    for t in cF: groups.setdefault(comp[t[0]], []).append(t)
for root, ts in groups.items():
    vs = sorted({v for t in ts for v in t})
    top = np.mean(cPo[vs, 2])
    if top > J["pelvis"][2] + 0.05:
        # (The shirt, its sleeves off: a jersey's armholes.)
        def jersey_kind(t, W=cWs):
            ds = [dom(W, v) for v in t]
            # (Its sleeves past the elbow off; the rest of each cut by the
            # jersey's own armhole, as it is painted - see meshBody.ts.)
            if any(d.startswith(("fore", "hand")) for d in ds): return None
            return "jersey"
        parts.append(("jersey", cPo, cWs, ts, jersey_kind))
    else:
        def shorts_kind(t, W=cWs):
            ds = [dom(W, v) for v in t]
            d = max(sorted(set(ds)), key=ds.count)
            if d.startswith(("shin", "foot")): return None
            if d.startswith("thigh"): return "shorts" + side(d)
            return "shortsTorso"
        parts.append(("shorts", cPo, cWs, ts, shorts_kind))
sP, sW = fitted(f"{ASSETS}/clothes/shoes05/shoes05.mhclo")
sF = obj_faces(f"{ASSETS}/clothes/shoes05/shoes05.obj")
sPo = ours(sP); sWs = segw(sW)
def shoe_kind(t, W=sWs):
    ds = [dom(W, v) for v in t]
    d = max(sorted(set(ds)), key=ds.count)
    s = side(d) or ("L" if np.mean(sPo[list(t), 1]) > 0 else "R")
    return "shoe" + s
parts.append(("shoes", sPo, sWs, sF, shoe_kind))

# ---- all in one: points, their pieces' weights, faces and what they are ------------
# ---- his face: where on a faces.js face each point of his head is --------------------
# faces.js draws a face in a 400 x 600 box, the same every time: his eyes at
# y 310 (120 apart), his mouth at 440, his chin at 500, the top of his head at
# 100. His own (MakeHuman's), measured off him: the middle of his eyes, the
# line of his lips, the bottom of his chin seen from in front, his crown. So
# a point of his head goes to the faces.js face as if seen straight from in
# front, put where its own eyes, mouth and chin are. With it: how much it
# faces forward (from in front, the face is drawn on; side on, round the
# back, his hair is his haircut - see sculpt.ts), and how far round his head
# it is (1 in front, -1 behind).
def face_of(Pp, Fp):
    eye = ours(bhead("eye.L")); sep = 2 * abs(eye[1])
    hd = Pp[:, 2] > J["chest"][2]
    mid = Pp[hd & (np.abs(Pp[:, 1]) < 0.0015)]
    # (His profile: the front-most point of him at each height.)
    def front(u0, u1):
        m = mid[(mid[:, 2] > u0) & (mid[:, 2] < u1)]
        return m[np.argmax(m[:, 0])]
    nose = front(eye[2] - 0.04, eye[2] - 0.01)
    # (His lips: the furthest forward under his nose; where they meet, the
    # furthest back between the two.)
    lips = mid[(mid[:, 2] < nose[2] - 0.006) & (mid[:, 2] > nose[2] - 0.02)]
    top = lips[np.argmax(lips[:, 0])]
    under = lips[lips[:, 2] < top[2] - 0.002]
    low = under[np.argmax(under[:, 0])]
    # (His chin, from in front: the lowest point of him still near as far
    # forward as his lips.)
    chinPts = mid[(mid[:, 0] > low[0] - 0.012) & (mid[:, 2] < low[2])]
    mouth = (top[2] + low[2]) / 2
    chin = chinPts[:, 2].min()
    crown = Pp[hd][:, 2].max()
    print(f"face: eyes {eye[2]:.4f} mouth {mouth:.4f} chin {chin:.4f} crown {crown:.4f}", file=sys.stderr)
    us = [crown, eye[2], mouth, chin]; ys = [100, 310, 440, 500]
    # (Past his chin and his crown: on at the same rate.)
    def fy(u):
        y = np.interp(-u, [-x for x in us], ys)
        y = np.where(u > crown, 100 - (u - crown) * 210 / (crown - eye[2]), y)
        return np.where(u < chin, 500 + (chin - u) * 60 / (mouth - chin), y)
    fx = 200 + Pp[:, 1] * 120 / sep
    # Normals, at rest.
    N = np.zeros_like(Pp)
    F = np.array(Fp)
    fn = np.cross(Pp[F[:, 1]] - Pp[F[:, 0]], Pp[F[:, 2]] - Pp[F[:, 0]])
    for k in range(3): np.add.at(N, F[:, k], fn)
    N /= np.maximum(np.linalg.norm(N, axis=1, keepdims=True), 1e-12)
    # (Round his head: from the middle of it, front to back.)
    fc = (Pp[hd][:, 0].max() + Pp[hd][:, 0].min()) / 2
    d = np.stack([Pp[:, 0] - fc, Pp[:, 1]], axis=1)
    ct = d[:, 0] / np.maximum(np.linalg.norm(d, axis=1), 1e-9)
    out = np.stack([fx, fy(Pp[:, 2]), N[:, 0], ct], axis=1)
    # (Below his head: far under his chin - no face, no hair.)
    out[~hd] = [200, 1000, 0, 0]
    return out
allP = []; allW = []; tris = []; kinds = []; allFace = []
for name, Pp, Wp, Fp, kind_of in parts:
    keep = {}
    fa = face_of(Pp, Fp) if name == "body" else np.tile([200, 1000, 0, 0], (len(Pp), 1))
    for t in Fp:
        k = kind_of(t)
        if k is None: continue
        for v in t:
            if v not in keep:
                keep[v] = len(allP); allP.append(Pp[v]); allW.append(Wp[v]); allFace.append(fa[v])
        tris.append([keep[v] for v in t]); kinds.append(k)
allP = np.array(allP); allW = np.array(allW); allFace = np.array(allFace)
KINDS = sorted(set(kinds))
print(f"points {len(allP)}, faces {len(tris)}, kinds {KINDS}", file=sys.stderr)

# ---- each point in each of its pieces' frames ----------------------------------------
# A piece's frame at rest: along it, head to tail; then square to that, his
# forward and his left. (His spine and neck pieces are his torso's and his
# neck's frames, at points along them.)
def frame(h, t, foot=False):
    a = t - h; a /= np.linalg.norm(a)
    f = np.array([0, 0, 1.0]) if foot else np.array([1.0, 0, 0])
    f = f - a * f.dot(a); f /= np.linalg.norm(f)
    return h, a, f, np.cross(a, f)
FR = {}
pel, che = J["pelvis"], J["chest"]
for n, t in (("pelvis", 0), ("spine1", 0.25), ("spine2", 0.5), ("spine3", 0.7), ("spine4", 0.85), ("chest", 0)):
    h, a, f, s = frame(pel, che)
    FR[n] = (pel, a, f, s)
for n in ("neck1", "neck2", "head"):
    FR[n] = frame(che, J["head"])
for s in "RL":
    FR["shoulder" + s] = frame(J["shoulder" + s], J["elbow" + s])
    FR["upper" + s] = frame(J["shoulder" + s], J["elbow" + s])
    FR["fore" + s] = frame(J["elbow" + s], J["wrist" + s])
    ha = J["handTip" + s] - J["wrist" + s]; ha = ha / np.linalg.norm(ha)
    hf = J["palm" + s] - J["wrist" + s]; hf = hf - ha * hf.dot(ha); hf /= np.linalg.norm(hf)
    FR["hand" + s] = (J["wrist" + s], ha, hf, np.cross(ha, hf))
    FR["thigh" + s] = frame(J["hip" + s], J["knee" + s])
    FR["shin" + s] = frame(J["knee" + s], J["ankle" + s])
    FR["foot" + s] = frame(J["ankle" + s], J["toe" + s], True)
infl = []
for i, p in enumerate(allP):
    w = allW[i].copy()
    order = np.argsort(-w)[:4]
    ws = w[order]
    row = []
    for bi, wv in zip(order, ws):
        if wv / max(ws.sum(), 1e-9) < 0.02: continue
        h, a, f, s = FR[SEG[bi]]; d = p - h
        row.append((bi, wv, d.dot(a), d.dot(f), d.dot(s)))
    if not row:
        h, a, f, s = FR["chest"]; d = p - h; row = [(SI["chest"], 1.0, d.dot(a), d.dot(f), d.dot(s))]
    tot = sum(r[1] for r in row)
    infl.append([(r[0], r[1] / tot, r[2], r[3], r[4]) for r in row])

# ---- out ----------------------------------------------------------------------------
counts = np.array([len(r) for r in infl], np.uint8)
flat = [x for r in infl for x in r]
bone = np.array([x[0] for x in flat], np.uint8)
wt = np.array([x[1] for x in flat], np.float32)
loc = np.array([[x[2], x[3], x[4]] for x in flat], np.float32)
tri = np.array(tris, np.uint16)
# How much each point goes with his arm (past his shoulder): his jersey cut
# where it does (see sculpt.ts), smoothly across its faces.
ARM = [SI[n] for n in SEG if n.startswith(("upper", "fore", "hand"))]
# (faces.js units x 8; facing and round his head x 1000.)
face = np.round(allFace * np.array([8, 8, 1000, 1000])).astype(np.int16)
# (His shoulders' points - the round of his deltoid - partly his arm's.)
SHO = [SI[n] for n in SEG if n.startswith("shoulder")]
armW = allW[:, ARM].sum(axis=1) + 0.6 * allW[:, SHO].sum(axis=1)
arm = np.round(np.clip(armW / np.maximum(allW.sum(axis=1), 1e-9), 0, 1) * 255).astype(np.uint8)
kind = np.array([KINDS.index(k) for k in kinds], np.uint8)
b64 = lambda a: base64.b64encode(np.ascontiguousarray(a).tobytes()).decode()
joints = {k: [round(float(x), 5) for x in v] for k, v in J.items()}
# Two files: what the court needs to know of him from the start (small),
# and the mesh itself - only fetched when a man is drawn as one (see
# meshBody.ts). Weights as bytes, offsets as 16-bit steps of LOCAL_STEP.
HEAD = ("// THE PLAYERS' BODY, BAKED by tools/court3d-body/bake.py from MakeHuman's\n"
        "// (CC0 1.0 - its base mesh hm08, its targets, skeleton, weights and clothes,\n"
        "// (C) 2020 Data Collection AB, Joel Palmius, Jonas Hauquier, released to\n"
        "// the public domain). Do not edit: bake it again.\n")
LOCAL_STEP = float(np.abs(loc).max()) / 32000
wq = np.round(wt * 255).astype(np.uint8)
lq = np.round(loc / LOCAL_STEP).astype(np.int16)
META = OUT.replace(".data.ts", ".meta.ts")
with open(META, "w") as o:
    o.write(HEAD)
    o.write("export const BODY_META = {\n")
    o.write(f"\tpoints: {len(allP)},\n\tbones: {json.dumps(SEG)},\n\tkinds: {json.dumps(KINDS)},\n")
    o.write(f"\tjoints: {json.dumps(joints)},\n\tlocalStep: {LOCAL_STEP!r},\n")
    o.write("} as const;\n")
with open(OUT, "w") as o:
    o.write(HEAD)
    o.write("export const BODY_DATA = {\n")
    o.write(f"\tcounts: \"{b64(counts)}\",\n\tbone: \"{b64(bone)}\",\n\tweight: \"{b64(wq)}\",\n")
    o.write(f"\tlocal: \"{b64(lq)}\",\n\ttris: \"{b64(tri)}\",\n\tkind: \"{b64(kind)}\",\n\tarm: \"{b64(arm)}\",\n\tface: \"{b64(face)}\",\n")
    o.write("};\n")
print(f"bytes {len(open(OUT).read())} + {len(open(META).read())}", file=sys.stderr)
