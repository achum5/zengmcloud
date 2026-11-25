# A TOOL, not part of the game: the basket's stanchion built in Blender -
# the padded base behind the baseline, the padded column up the front of
# the post (wide at its foot, leaning back, two ears up either side of the
# boom), the boom out over the baseline padded where it meets the board,
# the lit box along the top of it, and the mount the backboard hangs on -
# written out as parts of a few faces each for the court to draw (see
# arena.ts drawHoop), and a .blend to look at. Laid out off an NBA
# stanchion as 2K draws it (TD Garden).
#
#   blender -b --factory-startup --python tools/court3d-arena/stanchion.py -- \
#     out.json [out.blend]
#
# Feet, the left basket's: d from the baseline in toward the court, y
# across the court (25 its middle), z up. The other basket is this one the
# other way round. Each part: its points, its faces (each a loop of points,
# wound so it faces out), what it is (`pad` - the team's color - or
# `metal`), and how much each point moves with the board when the basket
# rings after a dunk (0 at the post, 1 at the board). With them, the
# corners of the lit panels on the base, the column and the box on the boom.
import json
import math
import sys

import bmesh
import bpy
from mathutils import Matrix, Vector

argv = sys.argv[sys.argv.index("--") + 1 :]
OUT = argv[0]
BLEND = argv[1] if len(argv) > 1 else None

bpy.ops.wm.read_factory_settings(use_empty=True)
parts = []


def finish(name, bm, kind, flex=None):
    bm.normal_update()
    bmesh.ops.recalc_face_normals(bm, faces=bm.faces)
    me = bpy.data.meshes.new(name)
    bm.to_mesh(me)
    ob = bpy.data.objects.new(name, me)
    bpy.context.collection.objects.link(ob)
    pts = [list(v.co) for v in bm.verts]
    faces = [[v.index for v in f.verts] for f in bm.faces]
    fl = [round(flex(Vector(p)) if flex else 0.0, 3) for p in pts]
    parts.append(
        {
            "name": name,
            "kind": kind,
            "pts": [[round(c, 3) for c in p] for p in pts],
            "faces": faces,
            "flex": fl,
        }
    )
    bm.free()


def box(name, lo, hi, kind, bevel=0.0, segs=2, flex=None):
    bm = bmesh.new()
    bmesh.ops.create_cube(bm, size=1.0)
    c = (Vector(lo) + Vector(hi)) / 2
    s = Vector(hi) - Vector(lo)
    for v in bm.verts:
        v.co = Vector((v.co.x * s.x, v.co.y * s.y, v.co.z * s.z)) + c
    if bevel > 0:
        bmesh.ops.bevel(bm, geom=list(bm.edges), offset=bevel, segments=segs, affect="EDGES", profile=0.5)
    bm.verts.index_update()
    finish(name, bm, kind, flex)


def beam(name, a, b, wa, ha, wb, hb, kind, flex=None, up=Vector((0, 0, 1))):
    # A box from a to b, wa x ha across at a, wb x hb at b.
    a, b = Vector(a), Vector(b)
    ax = (b - a).normalized()
    side = ax.cross(up).normalized()
    top = side.cross(ax).normalized()
    bm = bmesh.new()
    vs = []
    for p, w, h in ((a, wa, ha), (b, wb, hb)):
        for sx, sz in ((-1, -1), (1, -1), (1, 1), (-1, 1)):
            vs.append(bm.verts.new(p + side * (sx * w / 2) + top * (sz * h / 2)))
    for i in range(4):
        j = (i + 1) % 4
        bm.faces.new((vs[i], vs[j], vs[4 + j], vs[4 + i]))
    bm.faces.new(vs[3::-1])
    bm.faces.new(vs[4:8])
    bm.verts.index_update()
    finish(name, bm, kind, flex)


def frustum(name, foot, top, kind, bevel=0.0, segs=2, flex=None):
    # A box narrowing from its foot to its top: each (d0, d1, y0, y1, z).
    bm = bmesh.new()
    vs = []
    for d0, d1, y0, y1, z in (foot, top):
        for d, y in ((d0, y0), (d1, y0), (d1, y1), (d0, y1)):
            vs.append(bm.verts.new((d, y, z)))
    for i in range(4):
        j = (i + 1) % 4
        bm.faces.new((vs[i], vs[j], vs[4 + j], vs[4 + i]))
    bm.faces.new(vs[3::-1])
    bm.faces.new(vs[4:8])
    bm.normal_update()
    bmesh.ops.recalc_face_normals(bm, faces=bm.faces)
    if bevel > 0:
        bmesh.ops.bevel(bm, geom=list(bm.edges), offset=bevel, segments=segs, affect="EDGES", profile=0.5)
    bm.verts.index_update()
    finish(name, bm, kind, flex)


# How far along the boom a point is: none of it at the post, all of it at
# the board (as arena.ts's along).
along = lambda p: max(0.0, min(1.0, (p.x + 6.3) / 10))

# THE BASE: padded in the team's colors, its edges rounded - the column
# rising off the front of it, the rest of it (the weight) running back
# into the seats behind the baseline.
BASE = (-11.6, -5.0, 22.3, 27.7, 3.0)
box("base", (BASE[0], BASE[2], 0), (BASE[1], BASE[3], BASE[4]), "pad", bevel=0.28, segs=2)
# The post up behind the column, and the column: padding up the front of
# it, wide at its foot and leaning back to its top - and its two ears up
# either side of the boom.
box("post", (-8.75, 24.55, 2.9), (-7.75, 25.45, 12.1), "metal", bevel=0.06, segs=1)
COL_FOOT = (-7.85, -5.3, 23.2, 26.8, 2.95)
COL_TOP = (-7.85, -6.45, 23.8, 26.2, 10.7)
frustum("column", COL_FOOT, COL_TOP, "pad", bevel=0.22, segs=2)
for name, y0, y1 in (("earNear", 25.42, 26.12), ("earFar", 23.88, 24.58)):
    box(name, (-7.75, y0, 10.5), (-6.6, y1, 11.95), "pad", bevel=0.16, segs=1)
# The boom out to the board, deep at the post and slimmer at the board, and
# the padding round it where it meets the board.
BOOM_Z = 11.55
BOOM = ((-8.25, 0.72, 0.96), (3.62, 0.56, 0.64))
beam("boom", (BOOM[0][0], 25, BOOM_Z), (BOOM[1][0], 25, BOOM_Z), BOOM[0][1], BOOM[0][2], BOOM[1][1], BOOM[1][2], "metal", flex=along)
box("boomPad", (0.5, 24.56, BOOM_Z - 0.48), (3.5, 25.44, BOOM_Z + 0.46), "pad", bevel=0.14, segs=1, flex=along)
# The lit box along the top of the boom by the column.
BOX = (-6.15, -2.45, 24.5, 25.5, BOOM_Z + 0.44, BOOM_Z + 1.78)
box("adBox", (BOX[0], BOX[2], BOX[4]), (BOX[1], BOX[3], BOX[5]), "pad", bevel=0.1, segs=1, flex=along)
# The mount behind the glass the board hangs on.
box("mount", (3.62, 23.6, 9.75), (3.96, 26.4, 12.3), "metal", bevel=0.04, segs=1, flex=lambda p: 1.0)

# The lit panels - each three corners: top-left, top-right, bottom-left as
# read from in front. On the base, inset on its face toward the court and
# its two sides; up the front of the column (leaning back with it); and
# either side of the box on the boom.
z0, z1 = 0.45, BASE[4] - 0.4
anchors = {
    "panelCourt": [[BASE[1] + 0.01, BASE[2] + 0.4, z1], [BASE[1] + 0.01, BASE[3] - 0.4, z1], [BASE[1] + 0.01, BASE[2] + 0.4, z0]],
    "panelNear": [[BASE[0] + 0.35, BASE[3] + 0.01, z1], [BASE[1] - 0.35, BASE[3] + 0.01, z1], [BASE[0] + 0.35, BASE[3] + 0.01, z0]],
    "panelFar": [[BASE[1] - 0.35, BASE[2] - 0.01, z1], [BASE[0] + 0.35, BASE[2] - 0.01, z1], [BASE[1] - 0.35, BASE[2] - 0.01, z0]],
}
# (The column's front at a height, a hair out from it.)
col_d = lambda z: COL_FOOT[1] + (COL_TOP[1] - COL_FOOT[1]) * (z - COL_FOOT[4]) / (COL_TOP[4] - COL_FOOT[4]) + 0.02
ct, cb = 8.9, 7.6
anchors["colCourt"] = [[col_d(ct), 23.85, ct], [col_d(ct), 26.15, ct], [col_d(cb), 23.85, cb]]
bt, bb = BOX[5] - 0.14, BOX[4] + 0.14
anchors["adNear"] = [[BOX[0] + 0.15, BOX[3] + 0.01, bt], [BOX[1] - 0.15, BOX[3] + 0.01, bt], [BOX[0] + 0.15, BOX[3] + 0.01, bb]]
anchors["adFar"] = [[BOX[1] - 0.15, BOX[2] - 0.01, bt], [BOX[0] + 0.15, BOX[2] - 0.01, bt], [BOX[1] - 0.15, BOX[2] - 0.01, bb]]

with open(OUT, "w") as f:
    json.dump({"parts": parts, "anchors": anchors}, f)
if BLEND:
    bpy.ops.wm.save_as_mainfile(filepath=BLEND)
print("parts", len(parts), "faces", sum(len(p["faces"]) for p in parts), "points", sum(len(p["pts"]) for p in parts))
