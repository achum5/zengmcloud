# A TOOL, not part of the game: the body classic.py built, rigged in
# Blender - an armature on the court's own skeleton as it stood (the
# probe's masses.json), one bone for each piece meshBody.ts poses, and the
# mesh weighted to it by Blender's automatic weights (bone heat), so it bends
# smoothly at every joint. Saved as an .npz of each point's weight for each
# piece, for bake.py.
#
#   blender -b --factory-startup --python tools/court3d-body/weights.py -- \
#     masses.json out_dir/classic.blend out_dir/weights.npz
import json
import sys

import bpy
import numpy as np
from mathutils import Vector

argv = sys.argv[sys.argv.index("--") + 1 :]
SRC, BLEND, OUT = argv[0], argv[1], argv[2]
D = json.load(open(SRC))
SK = D["sk"]
# The pieces (meshBody.ts's, in its order).
SEG = [
    "pelvis", "spine1", "spine2", "spine3", "spine4", "chest", "neck1", "neck2", "head",
    "shoulderR", "shoulderL", "upperR", "upperL", "foreR", "foreL", "handR", "handL",
    "thighR", "thighL", "shinR", "shinL", "footR", "footL",
]


def P(v):
    # His frame (f, s, u) in Blender's (see classic.py).
    return Vector((v["s"], -v["f"], v["u"]))


bpy.ops.wm.open_mainfile(filepath=BLEND)
body = bpy.data.objects["classic"]
arm_data = bpy.data.armatures.new("rig")
rig = bpy.data.objects.new("rig", arm_data)
bpy.context.collection.objects.link(rig)
bpy.context.view_layer.objects.active = rig
bpy.ops.object.mode_set(mode="EDIT")
eb = arm_data.edit_bones
pel, che, hed = P(SK["pelvis"]), P(SK["chest"]), P(SK["head"])


def bone(name, a, b, parent=None):
    x = eb.new(name)
    x.head, x.tail = a, b
    if parent:
        x.parent = eb[parent]
    return x


# His spine a little at a time from his hips to the base of his neck (the
# twist of each piece, posed, is that far between his hips' and his
# shoulders'); his neck up into his head.
sp = lambda t: pel + (che - pel) * t
bone("pelvis", sp(-0.08), sp(0.18))
bone("spine1", sp(0.18), sp(0.38), "pelvis")
bone("spine2", sp(0.38), sp(0.6), "spine1")
bone("spine3", sp(0.6), sp(0.78), "spine2")
bone("spine4", sp(0.78), sp(0.9), "spine3")
bone("chest", sp(0.9), che, "spine4")
nk = lambda t: che + (hed - che) * t
bone("neck1", che, nk(0.4), "chest")
bone("neck2", nk(0.4), nk(0.75), "neck1")
bone("head", nk(0.75), hed + (hed - che) * 0.3, "neck2")
for s in "RL":
    leg, arm = SK["leg" + s], SK["arm" + s]
    sh = P(arm["root"])
    # (His collarbone: from the top of his chest out to his shoulder joint.)
    bone("shoulder" + s, sp(0.92) + (sh - sp(0.92)) * 0.25, sh, "chest")
    bone("upper" + s, sh, P(arm["mid"]), "shoulder" + s)
    bone("fore" + s, P(arm["mid"]), P(arm["end"]), "upper" + s)
    bone("hand" + s, P(arm["end"]), P(arm.get("tip") or arm["end"]), "fore" + s)
    bone("thigh" + s, P(leg["root"]), P(leg["mid"]), "pelvis")
    bone("shin" + s, P(leg["mid"]), P(leg["end"]), "thigh" + s)
    bone("foot" + s, P(leg["end"]), P(leg.get("tip") or leg["end"]), "shin" + s)
bpy.ops.object.mode_set(mode="OBJECT")

bpy.ops.object.select_all(action="DESELECT")
body.select_set(True)
rig.select_set(True)
bpy.context.view_layer.objects.active = rig
bpy.ops.object.parent_set(type="ARMATURE_AUTO")

n = len(body.data.vertices)
W = np.zeros((n, len(SEG)), np.float32)
gi = {g.index: g.name for g in body.vertex_groups}
for v in body.data.vertices:
    for g in v.groups:
        name = gi[g.group]
        if name in SEG:
            W[v.index, SEG.index(name)] = g.weight
empty = (W.sum(axis=1) <= 1e-6).sum()
print("weighted", n, "points;", empty, "with none")
np.savez(OUT, W=W, seg=np.array(SEG))
bpy.ops.wm.save_as_mainfile(filepath=BLEND.replace(".blend", "_rig.blend"))
