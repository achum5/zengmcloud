# A TOOL, not part of the game: the basket's stanchion built in Blender -
# the padded base behind the baseline, the post up out of it padded round
# its foot, the boom out over the baseline with a brace under it and bars
# between, and the mount the backboard hangs on - written out as parts of
# a few faces each for the court to draw (see arena.ts drawHoop), and a
# .blend to look at.
#
#   blender -b --factory-startup --python tools/court3d-arena/stanchion.py -- \
#     out.json [out.blend]
#
# Feet, the left basket's: d from the baseline in toward the court, y
# across the court (25 its middle), z up. The other basket is this one the
# other way round. Each part: its points, its faces (each a loop of points,
# wound so it faces out), what it is (`base`, `pad` - the team's color -
# `metal`), and how much each point moves with the board when the basket
# rings after a dunk (0 at the post, 1 at the board). With them, the
# corners of the lit panels on the base and of the sign along the boom.
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


# How far along the boom a point is: none of it at the post, all of it at
# the board (as arena.ts's along).
along = lambda p: max(0.0, min(1.0, (p.x + 6.3) / 10))

# The base: a dark padded block behind the baseline, its edges rounded.
box("base", (-9.5, 22.4, 0), (-5.0, 27.6, 3.4), "base", bevel=0.28, segs=2)
# The post, and the padding round its foot in the team's color.
box("post", (-6.85, 24.62, 3.3), (-6.05, 25.38, 12.75), "metal", bevel=0.06, segs=1)
box("postPad", (-7.25, 24.25, 3.3), (-5.65, 25.75, 7.4), "pad", bevel=0.3, segs=2)
# The boom out to the board, deep at the post and slimmer at the board;
# the brace under it, and bars between the two.
beam("boom", (-6.45, 25, 12.25), (3.62, 25, 11.75), 0.62, 0.86, 0.5, 0.56, "metal", flex=along)
beam("brace", (-6.4, 25, 8.6), (3.62, 25, 10.2), 0.4, 0.36, 0.34, 0.3, "metal", flex=along)
for d in (-3.6, -0.9, 1.8):
    lo_z = 8.6 + (10.2 - 8.6) * (d + 6.4) / 10.02
    hi_z = 12.25 + (11.75 - 12.25) * (d + 6.45) / 10.07
    beam(f"bar{d}", (d, 25, lo_z + 0.1), (d + 0.9, 25, hi_z - 0.25), 0.22, 0.22, 0.22, 0.22, "metal", flex=along)
# The mount behind the glass the board hangs on.
box("mount", (3.62, 23.6, 9.75), (3.96, 26.4, 12.3), "metal", bevel=0.04, segs=1, flex=lambda p: 1.0)

# The lit panels on the base (inset on its face toward the court and its two
# sides), and the sign along the boom (either side of it) - each four
# corners: top-left, top-right, bottom-left as read from in front.
anchors = {
    "panelCourt": [[-4.99, 22.75, 2.95], [-4.99, 27.25, 2.95], [-4.99, 22.75, 0.5]],
    "panelNear": [[-9.2, 27.61, 2.95], [-5.3, 27.61, 2.95], [-9.2, 27.61, 0.5]],
    "panelFar": [[-5.3, 22.39, 2.95], [-9.2, 22.39, 2.95], [-5.3, 22.39, 0.5]],
}
for name, y in (("signNear", 25.33), ("signFar", 24.67)):
    d0, d1 = -4.3, 2.1
    top = lambda d: 12.25 + (11.75 - 12.25) * (d + 6.45) / 10.07 + 0.3
    low = lambda d: 12.25 + (11.75 - 12.25) * (d + 6.45) / 10.07 - 0.3
    first, last = (d0, d1) if name == "signNear" else (d1, d0)
    anchors[name] = [[first, y, top(first)], [last, y, top(last)], [first, y, low(first)]]

with open(OUT, "w") as f:
    json.dump({"parts": parts, "anchors": anchors}, f)
if BLEND:
    bpy.ops.wm.save_as_mainfile(filepath=BLEND)
print("parts", len(parts), "faces", sum(len(p["faces"]) for p in parts), "points", sum(len(p["pts"]) for p in parts))
