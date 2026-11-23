# Getting back: after the ball changes hands with it in the new defense's
# front court, where its five are 1-8 s on - measured the same way on real
# NBA tracking and on the 3D game tape (see README.md):
#
#   python3 transition.py nba <game.json> ...
#   python3 transition.py tape <tape_x.json> ...
#
# Each second: the share of the five nearer their rim than the ball, how
# much nearer (median, feet along the floor), each one's distance to the
# nearest attacker (median, feet), attackers past all five, and how far the
# ball is from the rim.
import os
import sys

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from frames import DT, HOOPS, from_sportvu, from_tape  # noqa: E402

SECONDS = (1, 2, 3, 4, 5, 6, 8)


def trips(F, kind):
    seg, off, ball, hoop, t = F["seg"], F["off"], F["ball"], F["hoop"], F["t"]
    n = len(t)
    if kind == "tape":
        # (Each possession of the tape its own run, from the ball changing
        # hands.)
        for i in range(n):
            if i == 0 or seg[i] != seg[i - 1]:
                yield i, hoop[i]
        return
    for i in range(1, n):
        if seg[i] != seg[i - 1] or not (off[i] != off[i - 1]).any() or off[i].sum() != 5:
            continue
        # (The rim they are going at: where the ball is as their trip ends -
        # one at least 8 s long.)
        k = i
        while (
            k + 1 < n
            and seg[k + 1] == seg[i]
            and (off[k + 1] == off[i]).all()
            and t[k + 1] - t[i] < 24
        ):
            k += 1
        if t[k] - t[i] >= 8:
            yield i, HOOPS[int(ball[k, 0] > 47)]


def measure(Fs, kind):
    rows = {s: [] for s in SECONDS}
    count = 0
    for F in Fs:
        seg, off, xy, ball = F["seg"], F["off"], F["xy"], F["ball"]
        for i, H in trips(F, kind):
            # (Only with the ball still in their front court.)
            if abs(ball[i, 0] - H[0]) < 47:
                continue
            count += 1
            for s in SECONDS:
                j = i + int(round(s / DT))
                if j >= len(seg) or seg[j] != seg[i] or (off[j] != off[i]).any():
                    continue
                D = xy[j][~off[j]]
                A = xy[j][off[j]]
                d = np.abs(D[:, 0] - H[0])
                b = abs(ball[j, 0] - H[0])
                near = np.linalg.norm(D[:, None, :] - A[None, :, :], axis=2).min(1)
                past = int((np.abs(A[:, 0] - H[0]) < d.min()).sum())
                rows[s].append((np.mean(d < b), np.median(b - d), np.median(near), past, b))
    return count, rows


if __name__ == "__main__":
    kind, files = sys.argv[1], sys.argv[2:]
    Fs = [from_sportvu(p) if kind == "nba" else from_tape(p) for p in files]
    count, rows = measure(Fs, kind)
    print(f"{kind}: {len(files)} games, {count} trips up the floor")
    print("s  back of ball  nearer (ft)  to nearest (ft)  past all  ball (ft)    n")
    for s in SECONDS:
        R = np.array(rows[s])
        if len(R) == 0:
            continue
        print(
            f"{s}  {R[:, 0].mean():12.2f}  {np.median(R[:, 1]):11.1f}"
            f"  {np.median(R[:, 2]):15.1f}  {R[:, 3].mean():8.2f}"
            f"  {np.median(R[:, 4]):9.1f}  {len(R):4d}"
        )
