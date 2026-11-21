# A TOOL, not part of the game: motion capture (the CMU library's ASF/AMC
# files) read in Blender, and every joint's place worked out frame by frame
# - written out as JSON for fit.py, which turns it into the court's own
# joint angles (see poses.ts).
#
#   blender -b --factory-startup --python tools/court3d-mocap/amc_to_joints.py \
#     -- subject.asf trial.amc out.json [every]
#
# `every` keeps one frame in so many (the library's are 120 a second; 4
# keeps 30). Places are in feet, y up, as the capture had them; each frame
# also gives the root's facing (its x axis on the floor).
import json
import math
import sys

from mathutils import Euler, Matrix, Vector


def read_asf(path):
    bones = {}
    hierarchy = {}
    root = {"order": [], "axis": "XYZ", "position": (0, 0, 0), "orientation": (0, 0, 0)}
    length_unit = 1.0
    section = None
    cur = None
    with open(path) as f:
        lines = [l.rstrip("\n") for l in f]
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        i += 1
        if not line or line.startswith("#"):
            continue
        if line.startswith(":"):
            parts = line.split()
            section = parts[0][1:]
            if section == "units":
                while i < len(lines) and not lines[i].strip().startswith(":"):
                    p = lines[i].split()
                    if p and p[0] == "length":
                        length_unit = float(p[1])
                    i += 1
            continue
        if section == "root":
            p = line.split()
            if p[0] == "order":
                root["order"] = [x.lower() for x in p[1:]]
            elif p[0] == "axis":
                root["axis"] = p[1]
            elif p[0] == "position":
                root["position"] = tuple(float(x) for x in p[1:4])
            elif p[0] == "orientation":
                root["orientation"] = tuple(float(x) for x in p[1:4])
        elif section == "bonedata":
            p = line.split()
            if p[0] == "begin":
                cur = {"dof": [], "axis": (0, 0, 0), "axisOrder": "XYZ"}
            elif p[0] == "end":
                bones[cur["name"]] = cur
                cur = None
            elif cur is not None:
                if p[0] == "name":
                    cur["name"] = p[1]
                elif p[0] == "direction":
                    cur["direction"] = Vector(tuple(float(x) for x in p[1:4]))
                elif p[0] == "length":
                    cur["length"] = float(p[1])
                elif p[0] == "axis":
                    cur["axis"] = tuple(float(x) for x in p[1:4])
                    cur["axisOrder"] = p[4] if len(p) > 4 else "XYZ"
                elif p[0] == "dof":
                    cur["dof"] = [x.lower() for x in p[1:]]
        elif section == "hierarchy":
            p = line.split()
            if p[0] in ("begin", "end"):
                continue
            hierarchy[p[0]] = p[1:]
    return bones, hierarchy, root, length_unit


def euler_matrix(angles_deg, order):
    # ASF/AMC rotations: applied in the order given (an "XYZ" axis is
    # Rx then Ry then Rz about fixed axes: M = Rz * Ry * Rx).
    m = Matrix.Identity(3)
    for ax, a in zip(order, angles_deg):
        r = Matrix.Rotation(math.radians(a), 3, ax.upper())
        m = r @ m
    return m


def read_amc(path):
    frames = []
    cur = None
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or line.startswith(":"):
                continue
            p = line.split()
            if len(p) == 1 and p[0].isdigit():
                cur = {}
                frames.append(cur)
                continue
            if cur is not None:
                cur[p[0]] = [float(x) for x in p[1:]]
    return frames


def main():
    argv = sys.argv[sys.argv.index("--") + 1 :]
    asf, amc, out = argv[0], argv[1], argv[2]
    every = int(argv[3]) if len(argv) > 3 else 4
    bones, hierarchy, root, unit = read_asf(asf)
    # (Inches: the library's length unit is 1/unit of an inch - in feet.)
    to_ft = (1.0 / unit) / 12.0
    # Each bone's own frame (C) and its inverse, from its axis.
    for b in bones.values():
        c = euler_matrix(b["axis"], b["axisOrder"])
        b["C"] = c
        b["Cinv"] = c.inverted()
    frames = read_amc(amc)
    order_root = root["order"]
    out_frames = []
    # (First, his rest pose - every joint at nothing - the capture's own
    # skeleton stood as it was made: what fit.py measures angles from.)
    rest = {"root": [0.0] * 6}
    for k in [-1] + list(range(0, len(frames), every)):
        fr = rest if k < 0 else frames[k]
        rv = fr.get("root", [0] * 6)
        vals = dict(zip(order_root, rv))
        pos = Vector((vals.get("tx", 0), vals.get("ty", 0), vals.get("tz", 0)))
        rot_order = [a[1] for a in order_root if a.startswith("r")]
        rot = [vals.get("r" + a, 0) for a in rot_order]
        root_m = euler_matrix(rot, "".join(a.upper() for a in rot_order))
        world = {"root": (root_m, pos)}
        places = {"root": pos.copy()}

        def walk(parent):
            pm, pp = world[parent]
            for child in hierarchy.get(parent, []):
                b = bones[child]
                d = fr.get(child, [])
                angles = {"rx": 0.0, "ry": 0.0, "rz": 0.0}
                for name, v in zip(b["dof"], d):
                    angles[name] = v
                local = euler_matrix(
                    [angles["rx"], angles["ry"], angles["rz"]], "XYZ"
                )
                m = pm @ b["C"] @ local @ b["Cinv"]
                end = pp + m @ (b["direction"] * b["length"])
                world[child] = (m, end)
                places[child] = end.copy()
                walk(child)

        walk("root")
        # The root's facing on the floor: its frame's z axis (the subject
        # faces +z in the ASF's rest pose).
        fwd = root_m @ Vector((0, 0, 1))
        out_frames.append(
            {
                "j": {n: [round(v * to_ft, 4) for v in p] for n, p in places.items()},
                "f": [round(fwd.x, 4), round(fwd.y, 4), round(fwd.z, 4)],
            }
        )
    with open(out, "w") as f:
        json.dump(
            {"fps": 120 / every, "rest": out_frames[0], "frames": out_frames[1:]}, f
        )
    print("WROTE", out, len(out_frames), "frames")


main()
