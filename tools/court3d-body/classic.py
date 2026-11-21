# A TOOL, not part of the game: the court's Classic player (sculpt.ts's
# masses, stood in a T/A-pose - dumped by a probe to masses.json) built in
# Blender as one mesh.
#
# Each mass is a solid - a tube as thick along it as it is, rounded at its
# ends (or cut square at a hem) - and they are melted together the way the
# sculpt melts them (see sculpt.ts's shade): those of one group into one
# another a long way (a smooth minimum as wide as most of the thinner one),
# two groups only a little where they meet. His skin is where that comes to
# nothing, taken off a grid (surface nets), smoothed a touch and brought
# down to so many faces. Saved as a .blend and an .npz - points in feet in
# his own frame (f forward, s his left, u up), triangles, and what each
# point is (the mass it is nearest the surface of) - for bake.py.
#
#   blender -b --factory-startup --python tools/court3d-body/classic.py -- \
#     masses.json out_dir [faces]
import json
import os
import sys

import bpy
import numpy as np

argv = sys.argv[sys.argv.index("--") + 1 :]
SRC, OUT = argv[0], argv[1]
FACES = int(argv[2]) if len(argv) > 2 else 6000
os.makedirs(OUT, exist_ok=True)
D = json.load(open(SRC))

# Sculpt kinds (sculpt.ts): what is left to the sculpt, not in the mesh -
# his hands and nails (posed by what they do), the ball, a camera, a towel.
HAND, BALL, CAM_BODY, CAM_LENS, TOWEL, NAIL = 4, 9, 10, 11, 12, 13
SKIP = {HAND, BALL, CAM_BODY, CAM_LENS, TOWEL, NAIL}
# The grid (feet), and how far masses of one group melt (of the thinner's
# radius: sculpt.ts's melt) and two groups do where they meet (feet).
H = float(os.environ.get("GRID", "0.016"))
MELT = float(os.environ.get("MELT", "0.3"))
ACROSS = float(os.environ.get("ACROSS", "0.035"))

masses = [m for m in D["masses"] if m["kind"] not in SKIP]
for m in masses:
    m["A"] = np.array(m["A"], float)
    m["B"] = np.array(m["B"], float)
    m["r"] = np.array(m["r"], float)


# ---- toward a real man's build ------------------------------------------------
# What the sculpt's masses leave out, where his skin shows: the deltoid's
# round over his shoulder, biceps and triceps, the swell of his forearm
# below the elbow, his calves (bigger on the inside, high on his shin), the
# teardrop above his knee - each melted into its limb a little, the way a
# muscle sits under the skin. Placed on his own skeleton (the probe's), so
# a longer or shorter limb has them where they go.
SK = D["sk"]
ANATOMY = os.environ.get("ANATOMY", "1") == "1"


def jt(v):
    return np.array([v["f"], v["s"], v["u"]], float)


def limb_frame(a, b, side_s):
    ax = b - a
    L = np.linalg.norm(ax)
    ax /= L
    fw = np.array([1.0, 0, 0]) - ax * ax[0]
    fw /= np.linalg.norm(fw)
    out = np.cross(ax, fw)
    # (Out: away from his middle.)
    if out[1] * side_s < 0:
        out = -out
    return ax, fw, out, L


def muscle(a, b, r0, r1, kind, group, side, k, at=None):
    n = 9
    t = np.linspace(0, 1, n)
    # (Full in the middle, tapering to its ends.)
    r = r0 + (r1 - r0) * np.sin(np.pi * t) ** 0.8
    return {"A": a, "B": b, "r": r, "kind": kind, "group": group, "side": side, "flat": False, "k": k}


UPPER, FORE, THIGH, SHIN = 2, 3, 5, 6
if ANATOMY:
    for side, sd in ((0, "R"), (1, "L")):
        arm, leg = SK["arm" + sd], SK["leg" + sd]
        s_sign = -1 if side == 0 else 1
        g_arm, g_leg = 2 + side, 6 + side
        sh, el, wr = jt(arm["root"]), jt(arm["mid"]), jt(arm["end"])
        ax, fw, out, L = limb_frame(sh, el, s_sign)
        p = lambda t, f=0.0, o=0.0: sh + ax * L * t + fw * f + out * o
        # Deltoid: over the top and outside of his shoulder, to a third down.
        masses.append(muscle(p(0.02, 0.0, 0.05), p(0.36, 0.0, 0.07), 0.07, 0.15, UPPER, g_arm, side, 0.12))
        masses.append(muscle(p(0.04, 0.07, 0.01), p(0.3, 0.06, 0.03), 0.06, 0.12, UPPER, g_arm, side, 0.1))
        masses.append(muscle(p(0.04, -0.07, 0.01), p(0.3, -0.06, 0.03), 0.06, 0.12, UPPER, g_arm, side, 0.1))
        # Biceps in front, triceps behind.
        masses.append(muscle(p(0.3, 0.05, 0.0), p(0.82, 0.04, 0.0), 0.05, 0.105, UPPER, g_arm, side, 0.09))
        masses.append(muscle(p(0.22, -0.06, 0.02), p(0.7, -0.05, 0.0), 0.05, 0.1, UPPER, g_arm, side, 0.09))
        ax, fw, out, L = limb_frame(el, wr, s_sign)
        p = lambda t, f=0.0, o=0.0: el + ax * L * t + fw * f + out * o
        # The forearm's swell below the elbow, on the thumb side, slimming
        # to the wrist.
        masses.append(muscle(p(0.05, 0.02, 0.04), p(0.55, 0.01, 0.02), 0.06, 0.115, FORE, g_arm, side, 0.09))
        masses.append(muscle(p(0.05, -0.03, -0.03), p(0.5, -0.02, -0.02), 0.05, 0.095, FORE, g_arm, side, 0.09))
        hip, kn, an = jt(leg["root"]), jt(leg["mid"]), jt(leg["end"])
        ax, fw, out, L = limb_frame(kn, an, s_sign)
        p = lambda t, f=0.0, o=0.0: kn + ax * L * t + fw * f + out * o
        # Calves: the two heads of it, high on the back of his shin - the
        # inside one bigger and lower.
        masses.append(muscle(p(0.05, -0.07, -0.04), p(0.55, -0.06, -0.03), 0.06, 0.135, SHIN, g_leg, side, 0.1))
        masses.append(muscle(p(0.05, -0.07, 0.04), p(0.45, -0.06, 0.035), 0.06, 0.12, SHIN, g_leg, side, 0.1))
        # (The shin's own front, flat down to his ankle.)
        masses.append(muscle(p(0.08, 0.06, 0.03), p(0.6, 0.05, 0.03), 0.04, 0.075, SHIN, g_leg, side, 0.08))
        ax, fw, out, L = limb_frame(hip, kn, s_sign)
        p = lambda t, f=0.0, o=0.0: hip + ax * L * t + fw * f + out * o
        # The teardrop above his knee, on the inside.
        masses.append(muscle(p(0.62, 0.05, -0.08), p(0.93, 0.04, -0.06), 0.06, 0.13, THIGH, g_leg, side, 0.1))
lo = np.min([np.minimum(m["A"], m["B"]) - m["r"].max() for m in masses], axis=0) - 4 * H
hi = np.max([np.maximum(m["A"], m["B"]) + m["r"].max() for m in masses], axis=0) + 4 * H
shape = np.ceil((hi - lo) / H).astype(int) + 1
print("grid", shape, shape.prod())


def smin(a, b, k):
    k = np.maximum(k, 1e-6)
    h = np.maximum(k - np.abs(a - b), 0) / k
    return np.minimum(a, b) - h * h * k / 4


def box_of(ms, pad):
    a = np.min([np.minimum(m["A"], m["B"]) - m["r"].max() for m in ms], axis=0) - pad
    b = np.max([np.maximum(m["A"], m["B"]) + m["r"].max() for m in ms], axis=0) + pad
    i0 = np.clip(np.floor((a - lo) / H).astype(int), 0, shape - 1)
    i1 = np.clip(np.ceil((b - lo) / H).astype(int) + 1, 1, shape)
    return i0, i1


def points(i0, i1):
    ax = [lo[d] + H * np.arange(i0[d], i1[d]) for d in range(3)]
    return np.meshgrid(*ax, indexing="ij")


def mass_field(m, X, Y, Z):
    # How far out of the mass each point is (feet; inside, less than 0),
    # and its radius there.
    A, B, r = m["A"], m["B"], m["r"]
    ab = B - A
    l2 = ab.dot(ab)
    px, py, pz = X - A[0], Y - A[1], Z - A[2]
    if l2 < 1e-10:
        t = np.zeros_like(X)
        raw = t
    else:
        raw = (px * ab[0] + py * ab[1] + pz * ab[2]) / l2
        t = np.clip(raw, 0, 1)
    dx, dy, dz = px - t * ab[0], py - t * ab[1], pz - t * ab[2]
    rr = np.interp(t, np.linspace(0, 1, len(r)), r)
    d = np.sqrt(dx * dx + dy * dy + dz * dz) - rr
    if m["flat"] and l2 > 1e-10:
        # (Cut square at B: a hem.)
        L = np.sqrt(l2)
        d = np.maximum(d, (raw - 1) * L)
    return d, rr


# Each group melted into one, then the groups into one another a little.
F = np.full(shape, 1.0, np.float32)
groups = sorted({m["group"] for m in masses})
for g in groups:
    ms = [m for m in masses if m["group"] == g]
    i0, i1 = box_of(ms, 0.4)
    X, Y, Z = points(i0, i1)
    G = None
    Rg = None
    for m in ms:
        d, rr = mass_field(m, X, Y, Z)
        if G is None:
            G, Rg = d, rr
        else:
            G = smin(G, d, m["k"] if "k" in m else MELT * np.minimum(rr, Rg))
            Rg = np.where(d < G + 1e-9, rr, Rg)
    sl = tuple(slice(i0[k], i1[k]) for k in range(3))
    F[sl] = smin(F[sl], G.astype(np.float32), ACROSS)

# His skin, where that comes to nothing: surface nets - a point in every
# cell it passes through (where it crosses the cell's edges, on average),
# and a quad round every grid edge it crosses.
inside = F < 0
nx, ny, nz = shape
corner = [(i, j, k) for i in (0, 1) for j in (0, 1) for k in (0, 1)]
cs = np.zeros((nx - 1, ny - 1, nz - 1), np.int8)
for i, j, k in corner:
    cs += inside[i : nx - 1 + i, j : ny - 1 + j, k : nz - 1 + k]
active = (cs > 0) & (cs < 8)
cells = np.argwhere(active)
vid = np.full(active.shape, -1, np.int64)
vid[active] = np.arange(len(cells))
EDGES = [(a, b) for a in range(8) for b in range(a + 1, 8) if sum(x != y for x, y in zip(corner[a], corner[b])) == 1]
vals = np.stack([F[cells[:, 0] + i, cells[:, 1] + j, cells[:, 2] + k] for i, j, k in corner], axis=1)
cpos = np.array(corner, float)
acc = np.zeros((len(cells), 3))
cnt = np.zeros(len(cells))
for a, b in EDGES:
    va, vb = vals[:, a], vals[:, b]
    cross = (va < 0) != (vb < 0)
    t = np.where(cross, va / np.where(cross, va - vb, 1), 0)
    p = cpos[a][None, :] + t[:, None] * (cpos[b] - cpos[a])[None, :]
    acc += p * cross[:, None]
    cnt += cross
V = lo + H * (cells + acc / cnt[:, None])
quads = []
for axis in range(3):
    o = [0, 0, 0]
    o[axis] = 1
    a_in = inside[: nx - o[0], : ny - o[1], : nz - o[2]]
    b_in = inside[o[0] :, o[1] :, o[2] :]
    flip = a_in & ~b_in
    edge = flip | (~a_in & b_in)
    e = np.argwhere(edge)
    # (The four cells round it: the edge's other two axes back one or not.)
    u_ax, w_ax = [d for d in range(3) if d != axis]
    keep = (e[:, u_ax] > 0) & (e[:, w_ax] > 0) & (e[:, u_ax] < shape[u_ax] - 1) & (e[:, w_ax] < shape[w_ax] - 1)
    e = e[keep]
    fl = flip[tuple(e.T)]

    def cell(du, dw):
        c = e.copy()
        c[:, u_ax] -= du
        c[:, w_ax] -= dw
        return vid[tuple(c.T)]

    q = np.stack([cell(1, 1), cell(0, 1), cell(0, 0), cell(1, 0)], axis=1)
    # (Wound so its face looks out of him.)
    q = np.where(fl[:, None] ^ (axis == 1), q[:, ::-1], q)
    quads.append(q)
Q = np.concatenate(quads)
Q = Q[(Q >= 0).all(axis=1)]
print("points", len(V), "quads", len(Q))


# His frame (f forward, s his left, u up) in Blender's: facing -y, his left
# toward +x - so a camera in front of him (at -y) sees him as you would.
def to_b(P):
    return np.stack([P[:, 1], -P[:, 0], P[:, 2]], axis=1)


bpy.ops.wm.read_factory_settings(use_empty=True)
me = bpy.data.meshes.new("classic")
me.from_pydata(to_b(V).tolist(), [], Q.tolist())
me.update()
ob = bpy.data.objects.new("classic", me)
bpy.context.collection.objects.link(ob)
bpy.context.view_layer.objects.active = ob
ob.select_set(True)
bpy.ops.object.mode_set(mode="EDIT")
bpy.ops.mesh.select_all(action="SELECT")
bpy.ops.mesh.normals_make_consistent(inside=False)
bpy.ops.object.mode_set(mode="OBJECT")
lap = ob.modifiers.new("lap", "LAPLACIANSMOOTH")
lap.iterations = 2
lap.lambda_factor = 0.5
lap.use_volume_preserve = True
bpy.ops.object.modifier_apply(modifier="lap")
before = len(ob.data.polygons)
dec = ob.modifiers.new("dec", "DECIMATE")
dec.ratio = min(1.0, FACES / 2 / max(1, before))
bpy.ops.object.modifier_apply(modifier="dec")
bpy.ops.object.mode_set(mode="EDIT")
bpy.ops.mesh.select_all(action="SELECT")
bpy.ops.mesh.quads_convert_to_tris()
bpy.ops.object.mode_set(mode="OBJECT")
for p in ob.data.polygons:
    p.use_smooth = True
print(f"faces {before} quads -> {len(ob.data.polygons)} triangles, points {len(ob.data.vertices)}")

# What each point is: the mass it is nearest the surface of.
Pb = np.array([v.co[:] for v in ob.data.vertices])
P = np.stack([-Pb[:, 1], Pb[:, 0], Pb[:, 2]], axis=1)
best = np.full(len(P), np.inf)
kind = np.zeros(len(P), np.int32)
side = np.zeros(len(P), np.int32)
group = np.zeros(len(P), np.int32)
for m in masses:
    d, _ = mass_field(m, P[:, 0], P[:, 1], P[:, 2])
    better = d < best
    best[better] = d[better]
    kind[better] = m["kind"]
    side[better] = m["side"]
    group[better] = m["group"]
T = np.array([p.vertices[:] for p in ob.data.polygons], np.int32)
np.savez(os.path.join(OUT, "classic.npz"), P=P, T=T, kind=kind, side=side, group=group)
bpy.ops.wm.save_as_mainfile(filepath=os.path.join(OUT, "classic.blend"))
print("saved", OUT)
