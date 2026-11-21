# A TOOL, not part of the game: motion capture's strides (fit.py's) at a
# pace, averaged - each joint angle 21 times through a stride, 0 as his
# right foot lands - what poses.ts's GAITS are fit to.
#
#   python3 tools/court3d-mocap/curves.py LOW HIGH both|right|left joints/*.json
#
# Only strides LOW to HIGH ft/s, each foot down 0.24 to 0.4 of it (a run),
# at least 6 ft long. `both` folds his left side in as if his right (half a
# stride on); `left` takes his arms from his left side - the free arm of a
# player dribbling with his right hand.
import sys

import numpy as np

from fit import analyse


def half_on(v):
    # A curve over a stride (21 points, the last the first) half a stride on.
    v = np.roll(np.array(v[:-1]), -10)
    return np.append(v, v[0])


def main():
    lo, hi, side = float(sys.argv[1]), float(sys.argv[2]), sys.argv[3]
    acc = {}
    duty = []
    for path in sys.argv[4:]:
        for st in analyse(path)["strides"]:
            v = st["length"] / st["secs"]
            if not (lo <= v <= hi) or not (0.24 <= st["duty"] <= 0.4) or st["length"] < 6:
                continue
            duty.append(st["duty"])
            for k in ("hip", "knee", "sh", "el", "ab"):
                n, f = np.array(st[k + "N"]), half_on(st[k + "F"])
                arm = k in ("sh", "el", "ab")
                if side == "both":
                    acc.setdefault(k, []).extend([n, f])
                elif side == "left" and arm:
                    acc.setdefault(k, []).append(f)
                elif side == "left":
                    acc.setdefault(k, []).extend([n, f])
                else:
                    acc.setdefault(k, []).append(n)
            tw = np.array(st["twist"])
            acc.setdefault("twist", []).extend([tw, -half_on(st["twist"])] if side == "both" else [tw])
            acc.setdefault("lean", []).append(np.array(st["lean"]))
            h = np.array(st["hips"])
            acc.setdefault("hips", []).append(h - h.mean())
    if not duty:
        print("no strides")
        return
    print(f"strides {len(duty)}, duty {np.mean(duty):.3f}")
    print("phase ", " ".join(f"{x:5.2f}" for x in np.linspace(0, 1, 21)))
    for k in ("hip", "knee", "sh", "el", "ab", "twist", "lean", "hips"):
        m = np.mean(acc[k], axis=0)
        fmt = "{:5.2f}" if k == "hips" else "{:5.0f}"
        print(f"{k:6}", " ".join(fmt.format(x) for x in m))


if __name__ == "__main__":
    main()
