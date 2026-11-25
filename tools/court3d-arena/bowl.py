# A TOOL, not part of the game (a pilot): the whole bowl of an NBA arena
# built in Blender - the lower bowl's rows with their aisles and tunnels,
# two levels of suites with a ribbon board along the front of each, the
# upper deck, the catwalk ring of court lights and the roof trusses - lit
# the way a building is on a game night, the fans in their seats, and
# rendered from the game's own cameras.
#
#   blender -b --factory-startup --python tools/court3d-arena/bowl.py -- \
#     <assets dir> <out dir> [cams,...] [scale] [samples]
#
# (The assets: the game's cameras, the stands' boards, the court and an
# atlas of the fans, pulled from a game in the browser. Lights from the
# environment: COURT_POWER, HOUSE_POWER, EXPOSURE, CONCOURSE, TUNNEL,
# STEP, TWALL.)
#
# Feet. Built in the game's frame (x along the court from the left
# baseline, y across it from the far sideline, z up) and written into
# Blender's with y turned round (the game's frame is left-handed).
import json
import math
import os
import random
import sys

import bpy
import numpy as np
from mathutils import Matrix, Vector

argv = sys.argv[sys.argv.index("--") + 1 :]
ASSETS = argv[0]
OUT = argv[1]
CAMS = argv[2].split(",") if len(argv) > 2 and argv[2] else []
SCALE = float(argv[3]) if len(argv) > 3 else 2
SAMPLES = int(argv[4]) if len(argv) > 4 else 64
os.makedirs(OUT, exist_ok=True)
INFO = json.load(open(os.path.join(ASSETS, "info.json")))
rng = random.Random(7)

bpy.ops.wm.read_factory_settings(use_empty=True)
scene = bpy.context.scene

COURT_W = 94.0
# The front of the stands all the way round: the LED wall behind the far
# sideline (y -12), the near one (62), and behind each baseline (13 back).
X0, X1, Y0, Y1 = -13.0, COURT_W + 13.0, -12.0, 62.0
RC = 8.0


def hexrgb(h, a=1.0):
    h = h.lstrip("#")
    c = [int(h[i : i + 2], 16) / 255 for i in (0, 2, 4)]
    # sRGB to linear.
    c = [x / 12.92 if x <= 0.04045 else ((x + 0.055) / 1.055) ** 2.4 for x in c]
    return (*c, a)


# THE PATH round the front of the stands: straight along each side and
# end, round each corner. u is the distance along it (from the far left
# corner's end, along the far side); every point has its way out from the
# floor.
SEGS = []  # (kind, length, data)
SEGS.append(("line", X1 - X0 - 2 * RC, ((X0 + RC, Y0), (1, 0))))
SEGS.append(("arc", RC * math.pi / 2, ((X1 - RC, Y0 + RC), -math.pi / 2)))
SEGS.append(("line", Y1 - Y0 - 2 * RC, ((X1, Y0 + RC), (0, 1))))
SEGS.append(("arc", RC * math.pi / 2, ((X1 - RC, Y1 - RC), 0.0)))
SEGS.append(("line", X1 - X0 - 2 * RC, ((X1 - RC, Y1), (-1, 0))))
SEGS.append(("arc", RC * math.pi / 2, ((X0 + RC, Y1 - RC), math.pi / 2)))
SEGS.append(("line", Y1 - Y0 - 2 * RC, ((X0, Y1 - RC), (0, -1))))
SEGS.append(("arc", RC * math.pi / 2, ((X0 + RC, Y0 + RC), math.pi)))
SEG_U = []
_u = 0.0
for s in SEGS:
    SEG_U.append(_u)
    _u += s[1]
L = _u


def base(u):
    u = u % L
    for i, s in enumerate(SEGS):
        u0 = SEG_U[i]
        if u < u0 + s[1] or i == len(SEGS) - 1:
            t = u - u0
            if s[0] == "line":
                (px, py), (dx, dy) = s[2]
                return (px + dx * t, py + dy * t), (dy, -dx), False
            (cx, cy), a0 = s[2]
            a = a0 + t / RC
            n = (math.cos(a), math.sin(a))
            return (cx + RC * n[0], cy + RC * n[1]), n, True


def P(u, d, z):
    (px, py), (nx, ny), _ = base(u)
    return (px + nx * d, py + ny * d, z)


def corner_us():
    out = []
    for i, s in enumerate(SEGS):
        if s[0] == "arc":
            for k in range(1, 12):
                out.append(SEG_U[i] + s[1] * k / 12)
    return out


def u_of_x_far(x):
    return x - (X0 + RC)


def u_of_x_near(x):
    return SEG_U[4] + (X1 - RC - x)


def u_of_y_left(y):
    return SEG_U[6] + (Y1 - RC - y)


def u_of_y_right(y):
    return SEG_U[2] + (y - (Y0 + RC))


# Aisles: round the floor every section (26 ft or so along the sides,
# either side of the middle), and one down the middle of each corner.
AISLES = []
for x in (8, 34, 60, 86):
    AISLES += [u_of_x_far(x), u_of_x_near(x)]
for y in (0, 25, 50):
    AISLES += [u_of_y_left(y), u_of_y_right(y)]
for i, s in enumerate(SEGS):
    if s[0] == "arc":
        AISLES.append(SEG_U[i] + s[1] / 2)
AISLES.sort()
AW = 3.4
# Tunnels into the lower bowl, each on an aisle.
TUNNELS = [u_of_x_far(8), u_of_x_far(86), u_of_x_near(8), u_of_x_near(86), u_of_y_left(25), u_of_y_right(25)]
TW = 9.0

# THE LOWER BOWL: the LED wall, then the rows.
WALL_H = 3.6
CAP = 0.9
LR = 20
RD = 2.85
D = [CAP + r * RD for r in range(LR + 1)]
Z = [1.9]
for r in range(1, LR):
    Z.append(Z[-1] + 1.12 + 0.034 * r)
ZW = Z[-1] + 1.4  # the walkway round the top of it
T_ROW0, T_ROW1 = 8, 13  # a tunnel comes up through these rows
# THE SUITES: two levels, each a ribbon board along the slab over it.
DB = D[LR] + 5.0  # the front of the balconies
SUITE_DEPTH = 14.0
SUITE_W = 15.5


def suite_level(zb):
    za = zb + 1.0
    zb2 = za + 1.4
    return {"zb": zb, "za": za, "zb2": zb2, "dg": DB + 6.5, "zc": zb2 + 9.5}


SL1 = suite_level(ZW)
SL2 = suite_level(SL1["zc"] + 4.2)
ZU = SL2["zc"] + 4.2
# THE UPPER DECK.
UR = 18
URD = 2.75
UD = [DB + 0.8 + r * URD for r in range(UR + 1)]
UZ = [ZU + 1.0]
for r in range(1, UR):
    UZ.append(UZ[-1] + 1.75 + 0.02 * r)
ZROOF = UZ[-1] + 36

print("lower top", round(Z[-1], 1), "suites", round(SL1["zb"], 1), round(SL2["zb"], 1), "upper", round(ZU, 1), round(UZ[-1], 1), "path", round(L, 1))


# MESHES, gathered by material.
class Acc:
    def __init__(self):
        self.v = []
        self.f = []
        self.uv = []

    def poly(self, pts, uvs=None):
        i = len(self.v)
        self.v += pts
        self.f.append(list(range(i, i + len(pts))))
        self.uv += uvs or [(0, 0)] * len(pts)


ACC = {}


def acc(name):
    if name not in ACC:
        ACC[name] = Acc()
    return ACC[name]


def to_blender(name, a, mat):
    me = bpy.data.meshes.new(name)
    verts = [(x, -y, z) for (x, y, z) in a.v]
    faces = [list(reversed(f)) for f in a.f]
    me.from_pydata(verts, [], faces)
    uvl = me.uv_layers.new(name="uv")
    # Corners in the reversed order.
    flat = []
    for f in a.f:
        for vi in reversed(f):
            flat += a.uv[vi]
    uvl.data.foreach_set("uv", flat)
    me.materials.append(mat)
    ob = bpy.data.objects.new(name, me)
    scene.collection.objects.link(ob)
    return ob


# MATERIALS.
def mat_diffuse(name, color, rough=1.0):
    m = bpy.data.materials.new(name)
    m.use_nodes = True
    b = m.node_tree.nodes["Principled BSDF"]
    b.inputs["Base Color"].default_value = color
    b.inputs["Roughness"].default_value = rough
    b.inputs["Specular IOR Level"].default_value = 0.0
    return m


def mat_emit(name, color, strength):
    m = bpy.data.materials.new(name)
    m.use_nodes = True
    nt = m.node_tree
    nt.nodes.clear()
    o = nt.nodes.new("ShaderNodeOutputMaterial")
    e = nt.nodes.new("ShaderNodeEmission")
    e.inputs["Color"].default_value = color
    e.inputs["Strength"].default_value = strength
    nt.links.new(e.outputs[0], o.inputs[0])
    return m


def mat_image_emit(name, path, strength, closest=True):
    m = bpy.data.materials.new(name)
    m.use_nodes = True
    nt = m.node_tree
    nt.nodes.clear()
    o = nt.nodes.new("ShaderNodeOutputMaterial")
    e = nt.nodes.new("ShaderNodeEmission")
    t = nt.nodes.new("ShaderNodeTexImage")
    t.image = bpy.data.images.load(path)
    t.interpolation = "Closest" if closest else "Linear"
    t.extension = "EXTEND"
    e.inputs["Strength"].default_value = strength
    nt.links.new(t.outputs["Color"], e.inputs["Color"])
    nt.links.new(e.outputs[0], o.inputs[0])
    return m


def mat_image_diffuse(name, path, closest=True, alpha=False):
    m = bpy.data.materials.new(name)
    m.use_nodes = True
    nt = m.node_tree
    nt.nodes.clear()
    o = nt.nodes.new("ShaderNodeOutputMaterial")
    d = nt.nodes.new("ShaderNodeBsdfDiffuse")
    t = nt.nodes.new("ShaderNodeTexImage")
    t.image = bpy.data.images.load(path)
    t.interpolation = "Closest" if closest else "Linear"
    t.extension = "EXTEND"
    nt.links.new(t.outputs["Color"], d.inputs["Color"])
    if alpha:
        tr = nt.nodes.new("ShaderNodeBsdfTransparent")
        mx = nt.nodes.new("ShaderNodeMixShader")
        nt.links.new(t.outputs["Alpha"], mx.inputs["Fac"])
        nt.links.new(tr.outputs[0], mx.inputs[1])
        nt.links.new(d.outputs[0], mx.inputs[2])
        nt.links.new(mx.outputs[0], o.inputs[0])
    else:
        nt.links.new(d.outputs[0], o.inputs[0])
    return m


def mat_glass(name, color=(0.6, 0.7, 0.75, 1), strength=0.12):
    # A pane: mostly see-through, a little of the room's light in it.
    m = bpy.data.materials.new(name)
    m.use_nodes = True
    nt = m.node_tree
    nt.nodes.clear()
    o = nt.nodes.new("ShaderNodeOutputMaterial")
    tr = nt.nodes.new("ShaderNodeBsdfTransparent")
    tr.inputs["Color"].default_value = (0.82, 0.86, 0.88, 1)
    d = nt.nodes.new("ShaderNodeBsdfDiffuse")
    d.inputs["Color"].default_value = color
    mx = nt.nodes.new("ShaderNodeMixShader")
    mx.inputs["Fac"].default_value = strength
    nt.links.new(tr.outputs[0], mx.inputs[1])
    nt.links.new(d.outputs[0], mx.inputs[2])
    nt.links.new(mx.outputs[0], o.inputs[0])
    return m


APRON = INFO["apron"]
MATS = {
    "concrete": mat_diffuse("concrete", hexrgb("#3a3b40")),
    "tread": mat_diffuse("tread", hexrgb("#2b2c31")),
    "riser": mat_diffuse("riser", hexrgb("#1c1d21")),
    "black": mat_diffuse("black", hexrgb("#0d0e11")),
    "fascia": mat_diffuse("fascia", hexrgb("#15161a")),
    "seat": mat_diffuse("seat", hexrgb("#24262c"), 0.7),
    "suiteWall": mat_diffuse("suiteWall", hexrgb("#b89a74")),
    "suiteFloor": mat_diffuse("suiteFloor", hexrgb("#4a3a2e")),
    "suiteCeil": mat_diffuse("suiteCeil", hexrgb("#d8d0c4")),
    "suiteLight": mat_emit("suiteLight", (1.0, 0.72, 0.45, 1), 9.0),
    "mullion": mat_diffuse("mullion", hexrgb("#1a1b1f")),
    "glass": mat_glass("glass"),
    "rail": mat_glass("rail", strength=0.2),
    "floor": mat_diffuse("floor", hexrgb("#241e18")),
    "apron": mat_diffuse("apron", hexrgb(APRON)),
    "stepLight": mat_emit("stepLight", (1.0, 0.78, 0.5, 1), float(os.environ.get("STEP", "1.0"))),
    "tunnelLight": mat_emit("tunnelLight", (1.0, 0.8, 0.6, 1), float(os.environ.get("TUNNEL", "0.6"))),
    "concourse": mat_emit("concourse", (1.0, 0.8, 0.58, 1), float(os.environ.get("CONCOURSE", "0.35"))),
    "tunnelWall": mat_diffuse("tunnelWall", hexrgb(os.environ.get("TWALL", "#24221f"))),
    "roof": mat_diffuse("roof", hexrgb("#08080a")),
    "steel": mat_diffuse("steel", hexrgb("#2c2e33")),
    "fixture": mat_emit("fixture", (1.0, 0.97, 0.92, 1), 60.0),
    "portal": mat_emit("portal", (1.0, 0.82, 0.62, 1), float(os.environ.get("PORTAL", "0.8"))),
    "banner0": mat_diffuse("banner0", hexrgb("#1f2a44")),
    "banner1": mat_diffuse("banner1", hexrgb("#1aa39a")),
}
for k in ("ribbon", "wall", "end", "court"):
    p = os.path.join(ASSETS, k + ".png")
    if os.path.exists(p):
        if k == "court":
            MATS[k] = mat_image_diffuse(k, p, closest=True)
        else:
            MATS[k] = mat_image_emit(k, p, 2.2)
MATS["fans"] = mat_image_diffuse("fans", os.path.join(ASSETS, "atlas.png"), closest=True, alpha=True)


def band(name, us, d0, z0, d1, z1, period=None):
    # A strip along the path: from (d0, z0) to (d1, z1) in section, at
    # each of the us - a picture laid along it once every `period` feet.
    a = acc(name)
    for i in range(len(us) - 1):
        ua, ub = us[i], us[i + 1]
        pts = [P(ua, d0, z0), P(ub, d0, z0), P(ub, d1, z1), P(ua, d1, z1)]
        uvs = None
        if period:
            k = math.floor(ua / period + 1e-9)
            s0, s1 = ua / period - k, ub / period - k
            uvs = [(s0, 0), (s1, 0), (s1, 1), (s0, 1)]
        a.poly(pts, uvs)


def samples(u0, u1, step=4.0, extra=()):
    pts = {u0, u1}
    n = max(1, int(math.ceil((u1 - u0) / step)))
    for k in range(n + 1):
        pts.add(u0 + (u1 - u0) * k / n)
    for c in corner_us() + [s for s in SEG_U] + list(extra):
        if u0 < c < u1:
            pts.add(c)
    return sorted(pts)


ALL = samples(0, L, 4.0)
WALL_ALL = samples(0, L, 4.0, extra=[k * 174.0 for k in range(1, 3)])
RIB_ALL = samples(0, L, 4.0, extra=[k * L / 3 for k in range(1, 3)])


def cut(us, holes):
    # The us split round holes (u, half width): the runs between them.
    pts = set(us)
    for c, hw in holes:
        pts.add(c - hw)
        pts.add(c + hw)
    pts = sorted(p for p in pts if us[0] <= p <= us[-1])
    inside = lambda m: any(c - hw < m < c + hw for c, hw in holes)
    runs, run = [], [pts[0]]
    for a, b in zip(pts, pts[1:]):
        if inside((a + b) / 2):
            if len(run) > 1:
                runs.append(run)
            run = [b]
        else:
            run.append(b)
    if len(run) > 1:
        runs.append(run)
    return runs


# ---- The lower bowl ----
# The LED wall and its cap.
band("wall", WALL_ALL, 0, 0.0, 0, WALL_H, period=174.0)
band("black", ALL, 0, WALL_H, CAP, WALL_H)
band("riser", ALL, CAP, WALL_H, CAP, Z[0])
tunnel_holes = [(t, TW / 2) for t in TUNNELS]
for r in range(LR):
    holes = tunnel_holes if T_ROW0 <= r < T_ROW1 else []
    runs = cut(ALL, holes) if holes else [ALL]
    for run in runs:
        band("tread", run, D[r], Z[r], D[r + 1], Z[r])
        if r + 1 < LR:
            band("riser", run, D[r + 1], Z[r], D[r + 1], Z[r + 1])
# The riser up to the walkway, its glass rail, the walkway.
band("riser", ALL, D[LR], Z[-1], D[LR], ZW)
band("rail", ALL, D[LR], ZW, D[LR], ZW + 3.0)
band("concrete", ALL, D[LR], ZW, DB, ZW)


# The tunnels: a trench up through rows 8-12, under the rows over it to
# the lit concourse.
def side_poly(name, u, pts_dz):
    acc(name).poly([P(u, d, z) for d, z in pts_dz])


ZT = Z[T_ROW0 - 1]
DT0 = D[T_ROW0]
DT1 = D[T_ROW1]
DBACK = DT1 + 14
ZCEIL = Z[T_ROW1] - 0.5
for t in TUNNELS:
    us = [t - TW / 2, t + TW / 2]
    band("tunnelFloor", us, DT0, ZT, DBACK, ZT)
    band("tunnelWall", us, DT1, ZCEIL, DBACK, ZCEIL)
    band("concourse", us, DBACK, ZT, DBACK, ZCEIL)
    for s in us:
        for r in range(T_ROW0, T_ROW1):
            side_poly("tunnelWall", s, [(D[r], ZT), (D[r + 1], ZT), (D[r + 1], Z[r]), (D[r], Z[r])])
            # (A dark handrail along its side - glass here, stacked row on
            # row and lit side-on, went white.)
            side_poly("riser", s, [(D[r], Z[r] + 2.9), (D[r + 1], Z[r] + 2.9), (D[r + 1], Z[r] + 3.2), (D[r], Z[r] + 3.2)])
        side_poly("tunnelWall", s, [(DT1, ZT), (DBACK, ZT), (DBACK, ZCEIL), (DT1, ZCEIL)])
    # Its face over the mouth: the riser of row 13 down to the ceiling.
    band("riser", us, DT1, ZCEIL, DT1, Z[T_ROW1])
    # A light strip down its ceiling.
    band("tunnelLight", [t - 0.6, t + 0.6], DT1 + 4, ZCEIL - 0.05, DBACK - 1, ZCEIL - 0.05)
acc("tunnelFloor")
MATS["tunnelFloor"] = MATS["concrete"]


# Aisles: a step halfway up each row, and a light on each step.
def arc_us(u, hw):
    return [u - hw, u + hw]


for a in AISLES:
    for r in range(LR - 1):
        if T_ROW0 <= r < T_ROW1 and any(abs(a - t) < TW for t in TUNNELS):
            continue
        rise = Z[r + 1] - Z[r]
        dm = D[r] + RD * 0.5 + 0.2
        us = arc_us(a, AW / 2)
        band("concrete", us, dm, Z[r] + rise / 2, D[r + 1], Z[r] + rise / 2)
        band("riser", us, dm, Z[r], dm, Z[r] + rise / 2)
        band("stepLight", [a - 0.2, a + 0.2], dm - 0.02, Z[r] + 0.05, dm - 0.02, Z[r] + 0.25)
        band("stepLight", [a - 0.2, a + 0.2], D[r + 1] - 0.02, Z[r] + rise / 2 + 0.05, D[r + 1] - 0.02, Z[r] + rise / 2 + 0.25)


# ---- The suites ----
def suites(sl, ribbon_mat):
    zb, za, zb2, dg, zc = sl["zb"], sl["za"], sl["zb2"], sl["dg"], sl["zc"]
    # The balcony's front, its two rows of seats, the glass of the rooms.
    band("fascia", ALL, DB, zb, DB, za + 1.3)
    band("rail", ALL, DB, za + 1.3, DB, za + 3.6)
    band("tread", ALL, DB, za, DB + 3.0, za)
    band("riser", ALL, DB + 3.0, za, DB + 3.0, zb2)
    band("tread", ALL, DB + 3.0, zb2, dg, zb2)
    band("glass", ALL, dg, zb2, dg, zc)
    # Inside: the floor, the back wall, the ceiling and its lights.
    band("suiteFloor", ALL, dg, zb2, dg + SUITE_DEPTH, zb2)
    band("suiteWall", ALL, dg + SUITE_DEPTH, zb2, dg + SUITE_DEPTH, zc)
    band("suiteCeil", ALL, dg, zc, dg + SUITE_DEPTH, zc)
    # The slab over them, its soffit out over the balcony, and its face -
    # the ribbon board.
    band("fascia", ALL, DB, zc, dg, zc)
    band(ribbon_mat, RIB_ALL, DB, zc + 0.4, DB, zc + 3.8, period=L / 3)
    band("fascia", ALL, DB, zc, DB, zc + 0.4)
    band("fascia", ALL, DB, zc + 3.8, DB, zc + 4.2)
    # Each room: walls between, frames up the glass, a light in the middle.
    u = 0.0
    while u < L - SUITE_W * 0.5:
        side_poly("suiteWall", u, [(dg, zb2), (dg + SUITE_DEPTH, zb2), (dg + SUITE_DEPTH, zc), (dg, zc)])
        band("mullion", [u - 0.25, u + 0.25], dg - 0.05, zb2, dg - 0.05, zc)
        band("mullion", [u + SUITE_W / 2 - 0.12, u + SUITE_W / 2 + 0.12], dg - 0.05, zb2, dg - 0.05, zc)
        for k in (0.3, 0.7):
            uc = u + SUITE_W * k
            band("suiteLight", [uc - 1.2, uc + 1.2], dg + 4, zc - 0.05, dg + 6.5, zc - 0.05)
        u += SUITE_W
    # Seats for both balcony rows (fans placed with the rest).
    return [(DB + 3.0, za, "balA"), (dg, zb2, "balB")]


bal1 = suites(SL1, "ribbon")
bal2 = suites(SL2, "ribbon")

# ---- The upper deck ----
band("fascia", ALL, DB, ZU, DB, ZU + 1.6)
band("rail", ALL, DB, ZU + 1.6, DB, ZU + 4.2)
band("tread", ALL, DB, ZU, UD[0], ZU)
band("riser", ALL, UD[0], ZU, UD[0], UZ[0])
for r in range(UR):
    band("tread", ALL, UD[r], UZ[r], UD[r + 1], UZ[r])
    if r + 1 < UR:
        band("riser", ALL, UD[r + 1], UZ[r], UD[r + 1], UZ[r + 1])
# The back wall, lit doorways at the top of each aisle out to the concourse.
DTOP = UD[UR]
band("black", ALL, DTOP, UZ[-1], DTOP, UZ[-1] + 14)
band("black", ALL, DTOP, UZ[-1] + 14, DTOP + 30, ZROOF)
for a in AISLES:
    band("portal", [a - 2.5, a + 2.5], DTOP - 0.05, UZ[-1], DTOP - 0.05, UZ[-1] + 7.5)
for a in AISLES:
    for r in range(UR - 1):
        rise = UZ[r + 1] - UZ[r]
        dm = UD[r] + URD * 0.5 + 0.2
        us = arc_us(a, AW / 2)
        band("concrete", us, dm, UZ[r] + rise / 2, UD[r + 1], UZ[r] + rise / 2)
        band("riser", us, dm, UZ[r], dm, UZ[r] + rise / 2)
        band("stepLight", [a - 0.2, a + 0.2], dm - 0.02, UZ[r] + 0.05, dm - 0.02, UZ[r] + 0.25)

# ---- The floor ----
F = acc("floor")
F.poly([(-300, -300, -0.02), (400, -300, -0.02), (400, 400, -0.02), (-300, 400, -0.02)])
# The apron: the court's color out to the stands, round at the corners.
AP = acc("apron")
ring = [P(u, 0, -0.005) for u in samples(0, L, 2.0)[:-1]]
cxy = ((X0 + X1) / 2, (Y0 + Y1) / 2, -0.005)
for i in range(len(ring)):
    AP.poly([cxy, ring[i], ring[(i + 1) % len(ring)]])
# The court picture, where the game lays it (it draws it over this too).
C = acc("court")
C.poly(
    [(-5, -2.5, 0.0), (99, -2.5, 0.0), (99, 52.5, 0.0), (-5, 52.5, 0.0)],
    [(0, 1), (1, 1), (1, 0), (0, 0)],
)

# ---- The roof: trusses across the building, the catwalk ring of court
# lights, banners ----
BX0, BX1 = X0 - DTOP - 30, X1 + DTOP + 30
BY0, BY1 = Y0 - DTOP - 30, Y1 + DTOP + 30
acc("roof").poly([(BX0, BY0, ZROOF), (BX1, BY0, ZROOF), (BX1, BY1, ZROOF), (BX0, BY1, ZROOF)])


def beam(name, a, b, w, h):
    a, b = Vector(a), Vector(b)
    ax = (b - a).normalized()
    up = Vector((0, 0, 1)) if abs(ax.z) < 0.9 else Vector((1, 0, 0))
    s = ax.cross(up).normalized() * (w / 2)
    t = s.cross(ax).normalized() * (h / 2)
    c = [a - s - t, a + s - t, a + s + t, a - s + t]
    e = [b - s - t, b + s - t, b + s + t, b - s + t]
    A = acc(name)
    for i in range(4):
        j = (i + 1) % 4
        A.poly([tuple(c[i]), tuple(c[j]), tuple(e[j]), tuple(e[i])])


for x in np.arange(-110, 210, 34):
    top, bot = ZROOF - 2, ZROOF - 14
    beam("steel", (x, BY0 + 10, top), (x, BY1 - 10, top), 1.4, 1.4)
    beam("steel", (x, BY0 + 10, bot), (x, BY1 - 10, bot), 1.4, 1.4)
    ys = np.arange(BY0 + 10, BY1 - 10, 12)
    for i, y in enumerate(ys[:-1]):
        z0, z1 = (top, bot) if i % 2 else (bot, top)
        beam("steel", (x, y, z0), (x, ys[i + 1], z1), 0.6, 0.6)
# The catwalk: a ring of grating over the floor, the court lights under it.
CZ = 98.0
CW_X0, CW_X1, CW_Y0, CW_Y1 = -22.0, COURT_W + 22.0, -26.0, 76.0
for (xa, ya), (xb, yb) in [
    ((CW_X0, CW_Y0), (CW_X1, CW_Y0)),
    ((CW_X1, CW_Y0), (CW_X1, CW_Y1)),
    ((CW_X1, CW_Y1), (CW_X0, CW_Y1)),
    ((CW_X0, CW_Y1), (CW_X0, CW_Y0)),
]:
    beam("steel", (xa, ya, CZ), (xb, yb, CZ), 4.0, 0.5)
    beam("steel", (xa, ya, CZ + 3.5), (xb, yb, CZ + 3.5), 0.3, 0.3)
LIGHTS = []
for (xa, ya), (xb, yb) in [
    ((CW_X0, CW_Y0), (CW_X1, CW_Y0)),
    ((CW_X1, CW_Y0), (CW_X1, CW_Y1)),
    ((CW_X1, CW_Y1), (CW_X0, CW_Y1)),
    ((CW_X0, CW_Y1), (CW_X0, CW_Y0)),
]:
    n = int(math.hypot(xb - xa, yb - ya) / 9)
    for k in range(n):
        t = (k + 0.5) / n
        x, y = xa + (xb - xa) * t, ya + (yb - ya) * t
        LIGHTS.append((x, y, CZ - 1.2))
        A = acc("fixture")
        A.poly([(x - 0.7, y - 0.7, CZ - 1.6), (x + 0.7, y - 0.7, CZ - 1.6), (x + 0.7, y + 0.7, CZ - 1.6), (x - 0.7, y + 0.7, CZ - 1.6)])
        beam("steel", (x, y, CZ - 1.4), (x, y, CZ), 1.6, 1.6)
# Hanging from the trusses up the other end of the court from the board:
# the banners.
for i, y in enumerate(np.arange(-4, 56, 7.5)):
    x = -46.0
    A = acc("banner0" if i % 2 == 0 else "banner1")
    A.poly([(x, y, ZROOF - 16), (x, y + 5.5, ZROOF - 16), (x, y + 5.5, ZROOF - 30), (x, y, ZROOF - 30)])

# ---- Seats and fans ----
CELL = INFO["cell"]
ATW = CELL["CW"] * CELL["COLS"]
ATH = CELL["CH"] * math.ceil(CELL["n"] / CELL["COLS"])
CELL_W = CELL["CW"] / CELL["P"]
CELL_H = CELL["CH"] / CELL["P"]
SEAT_W = 1.75
SEATS = []  # (x, y, z of the seat pan, facing (nx, ny))


def row_seats(d_back, z_tread, holes, kind):
    # Seats along the path at this offset, every SEAT_W along the row
    # (measured on the row itself, round the corners too), none in the
    # aisles or over a tunnel.
    step = 0.5
    us = np.arange(0, L, step)
    pts = [P(u, d_back, 0) for u in us]
    s = 0.0
    last = None
    acc_len = []
    for p in pts:
        if last is not None:
            s += math.hypot(p[0] - last[0], p[1] - last[1])
        acc_len.append(s)
        last = p
    total = acc_len[-1]
    n = int(total / SEAT_W)
    j = 0
    for k in range(n):
        target = (k + 0.5) * total / n
        while j < len(acc_len) - 1 and acc_len[j + 1] < target:
            j += 1
        u = us[j]
        if any(abs(((u - c + L / 2) % L) - L / 2) < hw for c, hw in holes):
            continue
        (px, py), (nx, ny), _ = base(u)
        SEATS.append((px + nx * d_back, py + ny * d_back, z_tread + 1.45, -nx, -ny, kind))


aisle_holes = [(a, AW / 2 + 0.6) for a in AISLES]
for r in range(LR):
    holes = list(aisle_holes)
    if T_ROW0 <= r < T_ROW1:
        holes += [(t, TW / 2 + 0.8) for t in TUNNELS]
    row_seats(D[r + 1] - 1.1, Z[r], holes, "lower")
for sl in (SL1, SL2):
    for d, z, kind in ((DB + 3.0 - 1.1, sl["za"], "bal"), (sl["dg"] - 1.1, sl["zb2"], "bal")):
        row_seats(d, z, aisle_holes, kind)
for r in range(UR):
    row_seats(UD[r + 1] - 1.1, UZ[r], aisle_holes, "upper")
print("seats", len(SEATS))

# Seat: a pan and a back, each a thin box (as quads).
S = acc("seat")
for x, y, z, fx, fy, kind in SEATS:
    # (fx, fy): the way he faces, toward the floor.
    rx, ry = -fy, fx  # along the row
    hw = 0.75
    bx, by = x - fx * 0.55, y - fy * 0.55  # the back, behind him
    S.poly([(bx - rx * hw, by - ry * hw, z), (bx + rx * hw, by + ry * hw, z), (bx + rx * hw, by + ry * hw, z + 1.5), (bx - rx * hw, by - ry * hw, z + 1.5)])
    S.poly([(bx - rx * hw, by - ry * hw, z), (bx + rx * hw, by + ry * hw, z), (x + fx * 0.6 + rx * hw, y + fy * 0.6 + ry * hw, z), (x + fx * 0.6 - rx * hw, y + fy * 0.6 - ry * hw, z)])

FILL = {"lower": 0.94, "bal": 0.7, "upper": 0.88}
FANS = []
for i, (x, y, z, fx, fy, kind) in enumerate(SEATS):
    if rng.random() < FILL[kind]:
        FANS.append((x + fx * 0.15 + (rng.random() - 0.5) * 0.3, y + fy * 0.15, z, rng.randrange(CELL["n"])))
# A few up in the suites, stood about behind the glass.
for sl in (SL1, SL2):
    u = 0.0
    while u < L:
        if rng.random() < 0.6:
            for _ in range(rng.randrange(1, 4)):
                uu = u + rng.random() * SUITE_W
                d = sl["dg"] + 2 + rng.random() * 8
                x, y, _ = P(uu, d, 0)
                FANS.append((x, y, sl["zb2"] + 2.3, rng.randrange(CELL["n"])))
        u += SUITE_W
print("fans", len(FANS))

# ---- Build it ----
OBJ = {}
for name, a in ACC.items():
    m = MATS.get(name) or MATS.get("concrete")
    OBJ[name] = to_blender(name, a, m)

# The fans: one quad each, turned to face the camera (see aim_fans).
fan_me = bpy.data.meshes.new("fans")
nF = len(FANS)
fan_me.vertices.add(nF * 4)
fan_me.loops.add(nF * 4)
fan_me.polygons.add(nF)
fan_me.polygons.foreach_set("loop_start", np.arange(0, nF * 4, 4, dtype=np.int32))
fan_me.polygons.foreach_set("loop_total", np.full(nF, 4, dtype=np.int32))
fan_me.loops.foreach_set("vertex_index", np.arange(nF * 4, dtype=np.int32))
uv = np.zeros((nF, 4, 2), dtype=np.float32)
for i, (_, _, _, c) in enumerate(FANS):
    col, row = c % CELL["COLS"], c // CELL["COLS"]
    u0, u1 = col * CELL["CW"] / ATW, (col + 1) * CELL["CW"] / ATW
    v1 = 1 - row * CELL["CH"] / ATH
    v0 = 1 - (row + 1) * CELL["CH"] / ATH
    uv[i] = [(u0, v0), (u1, v0), (u1, v1), (u0, v1)]
fan_me.uv_layers.new(name="uv").data.foreach_set("uv", uv.ravel())
fan_me.materials.append(MATS["fans"])
fan_ob = bpy.data.objects.new("fans", fan_me)
scene.collection.objects.link(fan_ob)
FAN_ARR = np.array([(x, y, z) for x, y, z, _ in FANS], dtype=np.float64)


def aim_fans(cam_pos):
    # Each a picture stood up in his seat, turned to the camera.
    dx = cam_pos[0] - FAN_ARR[:, 0]
    dy = cam_pos[1] - FAN_ARR[:, 1]
    ln = np.maximum(1e-6, np.hypot(dx, dy))
    fx, fy = dx / ln, dy / ln
    rx, ry = fy, -fx  # his right, as the camera sees it (game frame)
    hw = CELL_W / 2
    zb = FAN_ARR[:, 2] - CELL["ybFromBottom"]
    zt = zb + CELL_H
    v = np.zeros((nF, 4, 3))
    for k, (s, zz) in enumerate(((-1, zb), (1, zb), (1, zt), (-1, zt))):
        v[:, k, 0] = FAN_ARR[:, 0] + rx * hw * s
        v[:, k, 1] = -(FAN_ARR[:, 1] + ry * hw * s)
        v[:, k, 2] = zz
    fan_me.vertices.foreach_set("co", v.ravel())
    fan_me.update()


# ---- Lights ----
def add_spot(name, at, aim, power, size_deg, blend, color=(1, 0.97, 0.93), radius=0.6):
    ld = bpy.data.lights.new(name, "SPOT")
    ld.energy = power
    ld.spot_size = math.radians(size_deg)
    ld.spot_blend = blend
    ld.color = color
    ld.shadow_soft_size = radius
    ob = bpy.data.objects.new(name, ld)
    loc = Vector((at[0], -at[1], at[2]))
    ob.location = loc
    d = Vector((aim[0], -aim[1], aim[2])) - loc
    ob.rotation_euler = d.to_track_quat("-Z", "Y").to_euler()
    scene.collection.objects.link(ob)


def add_area(name, at, size, power, color=(1, 0.95, 0.88), aim=None):
    ld = bpy.data.lights.new(name, "AREA")
    ld.energy = power
    ld.shape = "RECTANGLE"
    ld.size, ld.size_y = size
    ld.color = color
    ob = bpy.data.objects.new(name, ld)
    ob.location = Vector((at[0], -at[1], at[2]))
    if aim:
        d = Vector((aim[0], -aim[1], aim[2])) - ob.location
        ob.rotation_euler = d.to_track_quat("-Z", "Y").to_euler()
    scene.collection.objects.link(ob)


COURT_POWER = float(os.environ.get("COURT_POWER", "90000"))
for i, (x, y, z) in enumerate(LIGHTS):
    # Each at the court, a little past the middle of it from where it hangs.
    ax = min(COURT_W - 6, max(6, x)) * 0.6 + COURT_W / 2 * 0.4
    ay = min(44, max(6, y)) * 0.6 + 25 * 0.4
    add_spot(f"court{i}", (x, y, z), (ax, ay, 0), COURT_POWER, 62, 0.55)
HOUSE = float(os.environ.get("HOUSE_POWER", "0.3"))
# House lights over the seats, turned down for the game: brighter over the
# lower bowl, dim up top.
for (cx, cy, sx, sy) in [
    ((X0 + X1) / 2, Y0 - 32, X1 - X0 + 40, 40),
    ((X0 + X1) / 2, Y1 + 32, X1 - X0 + 40, 40),
    (X0 - 32, (Y0 + Y1) / 2, 40, Y1 - Y0 + 40),
    (X1 + 32, (Y0 + Y1) / 2, 40, Y1 - Y0 + 40),
]:
    add_area("house", (cx, cy, CZ + 10), (sx, sy), 260000 * HOUSE)
for (cx, cy, sx, sy) in [
    ((X0 + X1) / 2, Y0 - 95, X1 - X0 + 150, 40),
    ((X0 + X1) / 2, Y1 + 95, X1 - X0 + 150, 40),
    (X0 - 95, (Y0 + Y1) / 2, 40, Y1 - Y0 + 150),
    (X1 + 95, (Y0 + Y1) / 2, 40, Y1 - Y0 + 150),
]:
    add_area("houseUp", (cx, cy, ZROOF - 8), (sx, sy), 160000 * HOUSE)

world = bpy.data.worlds.new("world")
world.use_nodes = True
world.node_tree.nodes["Background"].inputs["Color"].default_value = (0.004, 0.004, 0.005, 1)
scene.world = world

# ---- Render settings ----
scene.render.engine = "CYCLES"
scene.cycles.device = "CPU"
scene.cycles.samples = SAMPLES
scene.cycles.use_denoising = True
scene.cycles.max_bounces = 4
scene.cycles.transparent_max_bounces = 16
scene.view_settings.view_transform = "AgX"
scene.view_settings.look = os.environ.get("LOOK", "AgX - Medium High Contrast")
scene.view_settings.exposure = float(os.environ.get("EXPOSURE", "-0.3"))
scene.render.image_settings.file_format = "PNG"
scene.render.film_transparent = False

bpy.ops.wm.save_as_mainfile(filepath=os.path.join(OUT, "bowl.blend"))


CLIP = {"arena": 40.0}


def game_camera(c, name):
    cd = bpy.data.cameras.new("cam")
    ob = bpy.data.objects.new("cam", cd)
    scene.collection.objects.link(ob)
    W, H = c["viewW"], c["viewH"]
    cd.sensor_fit = "HORIZONTAL"
    cd.sensor_width = 36
    cd.lens = c["f"] / W * 36
    cd.shift_x = -(c["cx"] - W / 2) / W
    cd.shift_y = (c["cy"] - H / 2) / W
    # (A camera stood out in the stands where the game has none - its
    # cameras look at the far side - sees past the rows round it.)
    cd.clip_start = CLIP.get(name, 0.5)
    cd.clip_end = 5000
    m = lambda v: Vector((v["x"], -v["y"], v["z"]))
    R = Matrix((m(c["right"]), m(c["up"]), -m(c["fwd"]))).transposed()
    ob.matrix_world = Matrix.Translation(m(c["pos"])) @ R.to_4x4()
    scene.camera = ob
    scene.render.resolution_x = int(W * SCALE)
    scene.render.resolution_y = int(H * SCALE)
    scene.render.resolution_percentage = 100
    return ob


for name in CAMS:
    c = INFO["cams"][name]
    ob = game_camera(c, name)
    aim_fans((c["pos"]["x"], c["pos"]["y"]))
    scene.render.filepath = os.path.join(OUT, f"bowl-{name}.png")
    bpy.ops.render.render(write_still=True)
    print("rendered", name)
