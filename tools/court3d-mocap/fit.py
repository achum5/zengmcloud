# A TOOL, not part of the game: motion capture's joints (from
# amc_to_joints.py) as the court's own joint angles (see poses.ts's Pose),
# frame by frame - and, for a run, each stride's: how much of it each foot
# is down, how far a stride goes, his legs through their swing, his arms,
# his lean and his shoulders' turn - what poses.ts's GAITS are made of.
#
#   python3 tools/court3d-mocap/fit.py joints/78_12.json [more.json ...]
#
# Angles in degrees, as poses.ts has them: a thigh's swing forward from
# straight down (hip), a knee's bend, an upper arm's swing forward from his
# side (sh) and out from it (ab), an elbow's bend, his body's lean forward,
# his shoulders turned on his hips (left positive). N is his right side, F
# his left.
import json
import math
import sys

import numpy as np

UP = np.array([0.0, 1.0, 0.0])


def unit(v):
    n = np.linalg.norm(v)
    return v / n if n > 1e-9 else v


def frame_axes(j, fwd_hint):
    # Forward on the floor: square to his hips, the way the capture's root
    # faces.
    hip_line = np.array(j["lhipjoint"]) - np.array(j["rhipjoint"])
    hl = hip_line - UP * hip_line.dot(UP)
    f = unit(np.cross(hl, UP))
    if f.dot(fwd_hint) < 0:
        f = -f
    left = unit(np.cross(UP, f))
    return f, left


def deg(a):
    return math.degrees(a)


def angles(j, f, left):
    p = {k: np.array(v) for k, v in j.items()}
    out = {}
    for side, s in (("N", "r"), ("F", "l")):
        hip, knee, ankle = p[s + "hipjoint"], p[s + "femur"], p[s + "tibia"]
        t = knee - hip
        sh = ankle - knee
        h = deg(math.atan2(t.dot(f), -t.dot(UP)))
        shin = deg(math.atan2(sh.dot(f), -sh.dot(UP)))
        out["hip" + side] = h
        out["knee" + side] = h - shin
        out["foot" + side] = float(p[s + "foot"][1])
        out["ankle" + side] = float(ankle[1])
    spine = p["thorax"] - p["root"]
    out["lean"] = deg(math.atan2(spine.dot(f), spine.dot(UP)))
    ut = unit(spine)
    ft = unit(f - ut * f.dot(ut))
    lt = unit(np.cross(ut, ft))
    for side, s, out_sign in (("N", "r", -1), ("F", "l", 1)):
        shoulder, elbow, wrist = p[s + "clavicle"], p[s + "humerus"], p[s + "radius"]
        ua = elbow - shoulder
        fa = wrist - elbow
        out["sh" + side] = deg(math.atan2(ua.dot(ft), -ua.dot(ut)))
        out["ab" + side] = deg(math.atan2(out_sign * ua.dot(lt), -ua.dot(ut)))
        c = ua.dot(fa) / (np.linalg.norm(ua) * np.linalg.norm(fa))
        out["el" + side] = deg(math.acos(max(-1.0, min(1.0, c))))
    sl = p["lclavicle"] - p["rclavicle"]
    hl = p["lhipjoint"] - p["rhipjoint"]
    a1 = math.atan2(sl.dot(-f), sl.dot(left))
    a2 = math.atan2(hl.dot(-f), hl.dot(left))
    tw = deg(a1 - a2)
    out["twist"] = (tw + 180) % 360 - 180
    out["hips"] = float(p["root"][1])
    return out


def analyse(path):
    d = json.load(open(path))
    fps = d["fps"]
    frames = d["frames"]
    roots = np.array([fr["j"]["root"] for fr in frames])
    # His way across the floor overall (for which way is forward).
    travel = roots[-1] - roots[0]
    travel[1] = 0
    rows = []
    for fr in frames:
        hint = np.array(fr["f"])
        hint[1] = 0
        if np.linalg.norm(hint) < 1e-6:
            hint = travel
        f, left = frame_axes(fr["j"], unit(hint))
        rows.append(angles(fr["j"], f, left))
    keys = rows[0].keys()
    series = {k: np.array([r[k] for r in rows]) for k in keys}
    # Measured from his own skeleton's rest pose (legs straight, stood
    # upright): what that has is the skeleton's, not his.
    if "rest" in d:
        rj = d["rest"]["j"]
        f0, l0 = frame_axes(rj, np.array([0.0, 0.0, 1.0]))
        r0 = angles(rj, f0, l0)
        for k in ("hipN", "hipF", "kneeN", "kneeF", "lean", "twist"):
            series[k] = series[k] - r0[k]
        res_rest = {k: round(r0[k], 1) for k in ("hipN", "kneeN", "lean", "twist", "shN", "abN", "elN")}
    else:
        res_rest = None
    # Floor: the lowest a foot gets; a foot is down when within 0.15 ft of it
    # and hardly moving.
    feet = np.concatenate([series["footN"], series["footF"]])
    floor = np.percentile(feet, 2)
    res = {"file": path.split("/")[-1], "fps": fps, "n": len(rows), "floor": floor, "rest": res_rest}
    speed = np.linalg.norm(np.diff(roots[:, [0, 2]], axis=0), axis=1) * fps
    res["speed"] = float(np.median(speed))
    down = {}
    for side in ("N", "F"):
        h = series["foot" + side] - floor
        down[side] = h < 0.18
    # Right foot touchdowns: up then down.
    td = [i for i in range(1, len(rows)) if down["N"][i] and not down["N"][i - 1]]
    res["touchdowns"] = td
    strides = []
    for a, b in zip(td, td[1:]):
        n = b - a
        if n < 6:
            continue
        duty = float(np.mean(down["N"][a:b]))
        st = {
            "frames": n,
            "secs": n / fps,
            "length": float(np.linalg.norm(roots[b, [0, 2]] - roots[a, [0, 2]])),
            "duty": duty,
        }
        ph = np.linspace(0, 1, 21)
        idx = a + ph * n
        for k in ("hipN", "kneeN", "hipF", "kneeF", "shN", "shF", "elN", "elF", "abN", "abF", "lean", "twist", "hips"):
            v = np.interp(idx, np.arange(len(rows)), series[k])
            st[k] = (v.round(3) if k == "hips" else v.round(1)).tolist()
        strides.append(st)
    res["strides"] = strides
    return res


def main():
    for path in sys.argv[1:]:
        r = analyse(path)
        print(f"== {r['file']}: {r['n']} frames, speed {r['speed']:.1f} ft/s, strides {len(r['strides'])}, rest {r['rest']}")
        for st in r["strides"]:
            print(
                f"  stride {st['secs']:.2f}s {st['length']:.1f}ft duty {st['duty']:.2f} "
                f"lean {np.mean(st['lean']):.0f} twist {min(st['twist']):.0f}..{max(st['twist']):.0f} "
                f"shN {min(st['shN']):.0f}..{max(st['shN']):.0f} elN {min(st['elN']):.0f}..{max(st['elN']):.0f} "
                f"abN {np.mean(st['abN']):.0f} hips bob {max(st['hips']) - min(st['hips']):.2f}"
            )
            print("    hipN ", " ".join(f"{v:4.0f}" for v in st["hipN"][::2]))
            print("    kneeN", " ".join(f"{v:4.0f}" for v in st["kneeN"][::2]))


if __name__ == "__main__":
    main()
