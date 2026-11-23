# The realism measures, the same on real NBA tracking and on the 3D game tape
# (frames.py): how men move (speed, starting, stopping, turning, standing,
# lockstep), how the defense sits (on the ball, sag to the rim, shading to the
# ball), how the offense spaces, and how the ball moves. Only live play counts.
import itertools
import numpy as np

from frames import DT

PERMS = np.array(list(itertools.permutations(range(5))))
SPEED_BANDS = [0, 2, 5, 8, 12, 16, 30]


def pct(x, ps=(10, 25, 50, 75, 90, 95, 99)):
    x = np.asarray(x, float)
    x = x[np.isfinite(x)]
    if len(x) == 0:
        return {}
    return {f"p{p}": round(float(np.percentile(x, p)), 2) for p in ps} | {"n": int(len(x))}


def kinematics(F, win=3):
    """Velocity and acceleration (feet a second, and a second) per frame and
    slot, off positions smoothed over win frames, within each run."""
    xy, seg = F["xy"], F["seg"]
    n = len(seg)
    v = np.full_like(xy, np.nan)
    a = np.full_like(xy, np.nan)
    b = np.nonzero(np.diff(seg) != 0)[0] + 1
    k = np.ones(win) / win
    h = win // 2
    for s, e in zip([0] + b.tolist(), b.tolist() + [n]):
        if e - s < win + 4:
            continue
        sm = np.empty_like(xy[s:e])
        for j in range(10):
            for c in range(2):
                sm[:, j, c] = np.convolve(xy[s:e, j, c], k, mode="same")
        vv = np.gradient(sm[h : e - s - h], DT, axis=0)
        v[s + h : e - h] = vv
        aa = np.gradient(vv, DT, axis=0)
        a[s + h + 1 : e - h - 1] = aa[1:-1]
    return v, a


def half_court(F):
    right = F["hoop"][:, 0] > 47
    x = F["xy"][..., 0]
    return np.where(right[:, None], x > 47, x < 47).all(1)


def _motion(F):
    v, a = kinematics(F)
    spd = np.linalg.norm(v, axis=2)
    with np.errstate(invalid="ignore", divide="ignore"):
        u = v / spd[..., None]
        fwd = (a * u).sum(2)
        lat = np.abs(a[..., 0] * u[..., 1] - a[..., 1] * u[..., 0])
        turn = np.degrees(lat / spd)
    return spd, fwd, lat, turn


def envelope(F):
    """What bodies do, by speed: how hard men speed up, brake and turn
    (sideways acceleration), 95th and 99th percentiles."""
    spd, fwd, lat, _ = _motion(F)
    L = F["live"][:, None] & np.isfinite(spd) & np.isfinite(fwd)
    out = {}
    for lo, hi in zip(SPEED_BANDS[:-1], SPEED_BANDS[1:]):
        m = L & (spd >= lo) & (spd < hi)
        f, l = fwd[m], lat[m]
        if (f > 0).sum() < 50 or (f < 0).sum() < 50:
            continue
        q = lambda x, p: round(float(np.percentile(x, p)), 1)
        out[f"{lo}-{hi}"] = {
            "share": round(float(m.sum() / L.sum()), 3),
            "up_p95": q(f[f > 0], 95),
            "up_p99": q(f[f > 0], 99),
            "brake_p95": q(-f[f < 0], 95),
            "brake_p99": q(-f[f < 0], 99),
            "side_p95": q(l, 95),
            "side_p99": q(l, 99),
        }
    return out


def player_tops(F, least_min=10):
    """Each man's top speed in live play (99.5th percentile, ft/s), his
    average (mph) and his quickest starts (seconds from rest to 12 ft/s, the
    20th percentile of his, with 25 or more), over at least least_min live
    minutes."""
    spd, *_ = _motion(F)
    starts = starts_stops(F, spd, kinematics(F)[0])["by_pid"]
    by = {}
    for j in range(10):
        ok = F["live"] & np.isfinite(spd[:, j])
        for pid in np.unique(F["pid"][:, j]):
            m = ok & (F["pid"][:, j] == pid)
            by.setdefault(int(pid), []).append(spd[m, j])
    rows = []
    for pid, arrs in by.items():
        x = np.concatenate(arrs)
        if len(x) * DT < least_min * 60:
            continue
        name = F["names"].get(pid, (str(pid), "", ""))
        rows.append(
            {
                "top": round(float(np.percentile(x, 99.5)), 1),
                "mph": round(float(x.mean() * 0.6818), 2),
                "start12": (
                    round(float(np.percentile(starts[pid], 20)), 2)
                    if len(starts.get(pid, [])) >= 25
                    else None
                ),
                "name": name[0],
                "pos": name[1],
            }
        )
    return sorted(rows, key=lambda r: r["top"])


def starts_stops(F, spd, v):
    """Getting going (seconds from rest to 8, 12 and 16 ft/s), pulling up
    (from 12 ft/s to rest) and cutting (the slowest a man goes turning more
    than 90 degrees between running at 9 ft/s or more before and after)."""
    live, seg = F["live"], F["seg"]
    n = len(live)
    res = {"to8": [], "to12": [], "to16": [], "stop_from12": [], "cut_slowest": [], "by_pid": {}}
    for j in range(10):
        sj = spd[:, j]
        for i in range(5, n - 30):
            if not live[i] or seg[i + 29] != seg[i]:
                continue
            w = sj[i : i + 30]
            if sj[i] < 1.5 <= sj[i + 1]:
                for thr in (8, 12, 16):
                    hit = np.nonzero(w >= thr)[0]
                    if len(hit) and np.all(np.isfinite(w[: hit[0] + 1])):
                        res[f"to{thr}"].append(hit[0] * DT)
                        if thr == 12:
                            res["by_pid"].setdefault(int(F["pid"][i, j]), []).append(hit[0] * DT)
            if sj[i] >= 12 > sj[i + 1]:
                hit = np.nonzero(w < 1.5)[0]
                if len(hit) and np.all(np.diff(w[: hit[0] + 1]) < 1.0):
                    res["stop_from12"].append(hit[0] * DT)
        vj = v[:, j]
        i = 10
        while i < n - 15:
            if live[i] and sj[i - 5] > 9 and seg[i - 5] == seg[i + 14]:
                for k in range(1, 15):
                    if not np.isfinite(sj[i + k]):
                        break
                    if sj[i + k] > 9 and vj[i - 5] @ vj[i + k] < 0:
                        res["cut_slowest"].append(float(np.nanmin(sj[i - 5 : i + k + 1])))
                        i += k
                        break
            i += 1
    return res


def _bouts(on):
    d = np.diff(np.concatenate([[0], on.astype(int), [0]]))
    return ((np.nonzero(d == -1)[0] - np.nonzero(d == 1)[0]) * DT).tolist()


def measures(F):
    xy, ball, off, hold, hoop, live = F["xy"], F["ball"], F["off"], F["hold"], F["hoop"], F["live"]
    seg, pid, team = F["seg"], F["pid"], F["team"]
    n = len(live)
    spd, fwd, lat, turn = _motion(F)
    hc = half_court(F)
    half = live & hc
    trans = live & ~hc
    R = {"live_min": round(float(live.sum() * DT / 60), 1)}
    s_live = spd[live]
    s_live = s_live[np.isfinite(s_live)]
    R["mph_live"] = round(float(s_live.mean() * 0.6818), 2)
    R["standing_live"] = round(float((s_live < 1.5).mean()), 3)
    R["over15_live"] = round(float((s_live > 15).mean()), 3)
    R["speed_top"] = round(float(np.percentile(s_live, 99.9)), 2)
    for nm, m in (("half", half), ("trans", trans)):
        R[f"speed_off_{nm}"] = pct(spd[m][off[m]])
        R[f"speed_def_{nm}"] = pct(spd[m][~off[m]])
    # By role, half court: the man with the ball, the four without, the man
    # guarding the ball (the nearest defender), the four helping.
    role = np.full(xy.shape[:2], -1)
    for i in np.nonzero(half & (hold >= 0))[0]:
        h = hold[i]
        role[i][off[i]] = 1
        role[i][~off[i]] = 3
        role[i, h] = 0
        di = np.nonzero(~off[i])[0]
        role[i, di[np.linalg.norm(xy[i, di] - xy[i, h], axis=1).argmin()]] = 2
    for r, nm in enumerate(("handler", "off_ball_O", "on_ball_D", "help_D")):
        x = spd[role == r]
        x = x[np.isfinite(x)]
        if len(x):
            R[f"role_{nm}"] = {
                "mph": round(float(x.mean() * 0.6818), 2),
                "still": round(float((x < 1.5).mean()), 3),
            }
    L = live[:, None] & np.ones((1, 10), bool)
    R["accel_up"] = pct(fwd[L & (fwd > 0)])
    R["accel_down"] = pct(-fwd[L & (fwd < 0)])
    for lo, hi in ((4, 8), (8, 13), (13, 18), (18, 30)):
        R[f"turn_{lo}_{hi}"] = pct(turn[L & (spd >= lo) & (spd < hi)])
    # How long a man keeps going once going (over 4 ft/s), and stays still.
    moving, still = [], []
    for j in range(10):
        moving += _bouts((spd[:, j] > 4) & live)
        still += _bouts((spd[:, j] < 1.5) & live)
    R["moving_bout_s"] = pct(moving)
    R["still_bout_s"] = pct(still)
    ss = starts_stops(F, spd, kinematics(F)[0])
    for k in ("to8", "to12", "to16", "stop_from12", "cut_slowest"):
        R[f"start_{k}" if k.startswith("to") else k] = pct(ss[k], (10, 25, 50, 75, 90))
    # Lockstep: set-offs (from under 1.5 ft/s to over 4 within half a second)
    # within 0.1 s of another man's.
    offs = []
    for j in range(10):
        s = spd[:, j]
        for i in range(5, n):
            if live[i] and s[i] > 4 >= s[i - 1] and np.nanmin(s[i - 5 : i]) < 1.5:
                offs.append((i, j))
    at = {}
    for i, j in offs:
        at.setdefault(i, []).append(j)
    lock = sum(
        1 for i, j in offs if any(k != j for ii in (i - 1, i, i + 1) for k in at.get(ii, []))
    )
    R["lockstep"] = round(lock / max(1, len(offs)), 3)
    R["setoffs_per_min"] = round(len(offs) / max(1e-9, live.sum() * DT / 60), 1)
    # Defense: each defender matched to a man for each second (least total
    # distance), then where he stands against that man, the ball and the rim.
    hf = np.nonzero(half)[0]
    dman, dball, sag, shade, onball = [], [], [], [], []
    for w0 in range(0, len(hf) - 10, 10):
        idx = hf[w0 : w0 + 10]
        if idx[-1] - idx[0] != 9 or seg[idx[0]] != seg[idx[-1]]:
            continue
        i = idx[5]
        oi = np.nonzero(off[i])[0]
        di = np.nonzero(~off[i])[0]
        if len(oi) != 5 or len(di) != 5:
            continue
        D = np.linalg.norm(xy[idx][:, di, None, :] - xy[idx][:, None, oi, :], axis=3).mean(0)
        p = PERMS[D[np.arange(5)[None, :], PERMS].sum(1).argmin()]
        for k in range(5):
            dd, oo = di[k], oi[p[k]]
            for t in idx[::2]:
                O, Dp, B = xy[t, oo], xy[t, dd], ball[t, :2]
                ax = hoop[t] - O
                Lh = np.linalg.norm(ax)
                if Lh < 1:
                    continue
                ax = ax / Lh
                rel = Dp - O
                perp = np.array([-ax[1], ax[0]])
                side = np.sign((B - O) @ perp) or 1.0
                dman.append(np.linalg.norm(rel))
                dball.append(np.linalg.norm(O - B))
                sag.append(float(rel @ ax) / Lh)  # 0 on his man, 1 at the rim
                shade.append(float(rel @ perp) * side)  # feet toward the ball's side
                onball.append(hold[t] == oo)
    dman, dball, sag, shade, onball = map(np.array, (dman, dball, sag, shade, onball))
    if len(dman):
        R["def_on_ball"] = pct(dman[onball])
        for lo, hi in ((0, 15), (15, 25), (25, 35), (35, 99)):
            m = (dball >= lo) & (dball < hi) & ~onball
            R[f"def_off_ball_{lo}_{hi}"] = {
                "dist": pct(dman[m], (25, 50, 75)),
                "sag": pct(sag[m], (25, 50, 75)),
                "ballside": pct(shade[m], (25, 50, 75)),
            }
    # Offense spacing.
    near, beyond, paint = [], [], []
    for t in hf[::3]:
        oi = np.nonzero(off[t])[0]
        if len(oi) != 5:
            continue
        O = xy[t, oi]
        D = np.linalg.norm(O[:, None] - O[None], axis=2) + np.eye(5) * 999
        near += D.min(1).tolist()
        beyond.append(int((np.linalg.norm(O - hoop[t], axis=1) > 22.5).sum()))
        base = 0 if hoop[t, 0] < 47 else 94
        paint.append(int(((np.abs(O[:, 1] - 25) < 8) & (np.abs(O[:, 0] - base) < 19)).sum()))
    R["off_nearest_mate"] = pct(near)
    R["off_beyond_arc"] = {str(k): round(beyond.count(k) / max(1, len(beyond)), 3) for k in range(6)}
    R["off_in_paint"] = {str(k): round(paint.count(k) / max(1, len(paint)), 3) for k in range(6)}
    # The ball: how long a man holds it, and passes (from one man to a
    # teammate, in the air or on a bounce in between).
    holds, ptime, pdist, pspd = [], [], [], []
    cur, start, lastend, lastteam, lastpid = None, 0, None, None, None
    for i in range(n):
        h = hold[i]
        hp = pid[i, h] if h >= 0 else None
        if hp == cur:
            continue
        if cur is not None and live[start:i].all() and seg[start] == seg[i - 1]:
            holds.append((i - start) * DT)
        if cur is not None:
            lastend, lastpid = i - 1, cur
            lastteam = team[i - 1, hold[i - 1]] if hold[i - 1] >= 0 else None
        if hp is not None and lastend is not None and hp != lastpid and team[i, h] == lastteam:
            gap = (i - lastend) * DT
            if (
                0.05 < gap < 2.5
                and live[lastend : i + 1].all()
                and seg[lastend] == seg[i]
                and ball[lastend : i + 1, 2].max() < 14
            ):
                dist = np.linalg.norm(ball[i, :2] - ball[lastend, :2])
                ptime.append(gap)
                pdist.append(dist)
                pspd.append(dist / gap)
        cur, start = hp, i
    R["hold_s"] = pct(holds)
    R["pass_s"] = pct(ptime)
    R["pass_ft"] = pct(pdist)
    R["pass_ft_s"] = pct(pspd)
    R["passes_per_live_min"] = round(len(ptime) / max(1e-9, live.sum() * DT / 60), 2)
    R["envelope"] = envelope(F)
    return R
