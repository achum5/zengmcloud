# A TOOL, not part of the game: a look at the body classic.py built - from
# in front, three-quarters, the side and behind, his kit's colors on him
# by what each point is (sculpt.ts's kinds), lit plainly.
#
#   blender -b --factory-startup --python tools/court3d-body/render.py -- \
#     out_dir/classic.blend out_dir/classic.npz shots.png
import math
import sys

import bpy
import numpy as np
from mathutils import Vector

argv = sys.argv[sys.argv.index("--") + 1 :]
BLEND, NPZ, PNG = argv[0], argv[1], argv[2]
bpy.ops.wm.open_mainfile(filepath=BLEND)
ob = bpy.data.objects["classic"]
d = np.load(NPZ)
kind = d["kind"]
# torso, shorts, upper, fore, hand, thigh, shin, shoe, neck, ..., tongue
JERSEY = (0.85, 0.12, 0.16)
SHORTS = (0.85, 0.12, 0.16)
SKIN = (0.42, 0.25, 0.14)
SHOE = (0.95, 0.95, 0.95)
COLORS = {0: JERSEY, 1: SHORTS, 2: SKIN, 3: SKIN, 4: SKIN, 5: SKIN, 6: SKIN, 7: SHOE, 8: SKIN, 14: SHOE}
me = ob.data
attr = me.color_attributes.new("kit", "FLOAT_COLOR", "POINT")
for i, k in enumerate(kind):
    c = COLORS.get(int(k), (0.5, 0.5, 0.5))
    attr.data[i].color = (c[0], c[1], c[2], 1.0)
for p in me.polygons:
    p.use_smooth = True

scene = bpy.context.scene
scene.render.engine = "BLENDER_WORKBENCH"
shading = scene.display.shading
shading.light = "STUDIO"
shading.color_type = "VERTEX"
shading.show_specular_highlight = False
scene.display.shading.show_cavity = False
scene.render.resolution_x = 360
scene.render.resolution_y = 720
scene.render.film_transparent = False
bpy.context.scene.world = bpy.data.worlds.new("w") if not bpy.context.scene.world else bpy.context.scene.world
bpy.context.scene.world.color = (0.93, 0.93, 0.91)

cam_data = bpy.data.cameras.new("cam")
cam_data.type = "ORTHO"
cam_data.ortho_scale = 7.6
cam = bpy.data.objects.new("cam", cam_data)
scene.collection.objects.link(cam)
scene.camera = cam
shots = []
for i, deg in enumerate((0, 35, 90, 180)):
    a = math.radians(deg)
    # (He faces -y: in front is -y, his left +x.)
    pos = Vector((math.sin(a) * 12, -math.cos(a) * 12, 3.4 + 1.2))
    cam.location = pos
    look = Vector((0, 0, 3.4)) - pos
    cam.rotation_euler = look.to_track_quat("-Z", "Y").to_euler()
    path = PNG.replace(".png", f"_{i}.png")
    scene.render.filepath = path
    bpy.ops.render.render(write_still=True)
    shots.append(path)
print("SHOTS", " ".join(shots))
