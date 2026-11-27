# Real NBA tracking (SportVU, 2015-16) and the 3D game tape in one frame format,
# 10 frames a second, so the same measures run on both (see measures.py).
#
# A frame set F is a dict of numpy arrays, n frames long:
#   t     seconds                      seg   id of a continuous run (no gap, same ten)
#   live  ball in play, at real speed  ball  (n, 3) feet
#   xy    (n, 10, 2) feet              team  (n, 10) team of each slot
#   pid   (n, 10) player ids           hold  slot with the ball, or -1
#   off   (n, 10) on offense           hoop  (n, 2) the basket the offense attacks
# Both use the 2D/3D court's own frame: feet, x along the 94 ft length, y
# across the 50 ft width, origin in a corner.
import json
import warnings

import numpy as np

DT = 0.1
HOOPS = np.array([[5.25, 25.0], [88.75, 25.0]])


def _sportvu_moments(path):
    d = json.load(open(path))
    seen = {}
    for e in d["events"]:
        for m in e["moments"]:
            if len(m[5]) == 11 and m[5][0][0] == -1:
                seen.setdefault((m[0], m[1]), m)
    keys = sorted(seen)
    n = len(keys)
    q = np.zeros(n, int)
    ms = np.zeros(n, np.int64)
    gc = np.zeros(n)
    ball = np.zeros((n, 3))
    team = np.zeros((n, 10), np.int64)
    pid = np.zeros((n, 10), np.int64)
    xy = np.zeros((n, 10, 2))
    for i, k in enumerate(keys):
        m = seen[k]
        q[i], ms[i], gc[i] = m[0], m[1], m[2]
        ball[i] = m[5][0][2:5]
        for j, r in enumerate(sorted(m[5][1:], key=lambda r: (r[0], r[1]))):
            team[i, j], pid[i, j] = r[0], r[1]
            xy[i, j] = r[2], r[3]
    names = {}
    for e in d["events"][:1]:
        for side in ("home", "visitor"):
            for p in e[side]["players"]:
                names[p["playerid"]] = (
                    f"{p['firstname']} {p['lastname']}",
                    p["position"],
                    e[side]["abbreviation"],
                )
    e = d["events"][0]
    label = f"{d['gamedate']} {e['visitor']['abbreviation']} at {e['home']['abbreviation']}"
    return q, ms, gc, ball, team, pid, xy, names, label


def _holder(F, reach=3.0, keep=3):
    # The nearest man within reach of the ball, below 9 ft, for keep frames
    # running (a flicker as it passes a man is not a catch).
    d = np.linalg.norm(F["xy"] - F["ball"][:, None, :2], axis=2)
    j = d.argmin(1)
    near = (d[np.arange(len(j)), j] < reach) & (F["ball"][:, 2] < 9)
    raw = np.where(near, j, -1)
    hold = np.full(len(raw), -1)
    cand, cnt, cur = -1, 0, -1
    for i in range(len(raw)):
        if i and F["seg"][i] != F["seg"][i - 1]:
            cand, cnt, cur = -1, 0, -1
        if raw[i] == cand:
            cnt += 1
        else:
            cand, cnt = raw[i], 1
        if cnt >= keep:
            cur = cand
        if raw[i] == -1 and cnt >= 2:
            cur = -1
        hold[i] = cur
    return hold


def from_sportvu(path):
    q, ms, gc, ball, team, pid, xy, names, label = _sportvu_moments(path)
    n = len(ms)
    # (Moments come 39-41 ms apart.)
    cont = np.zeros(n, bool)
    cont[1:] = (
        (np.abs(np.diff(ms) - 40) <= 3)
        & (q[1:] == q[:-1])
        & np.all(pid[1:] == pid[:-1], 1)
    )
    run = np.cumsum(~cont)
    out = {k: [] for k in ("t", "seg", "live", "ball", "xy", "team", "pid")}
    for r in np.unique(run):
        idx = np.nonzero(run == r)[0]
        if len(idx) < 25:
            continue
        t = (ms[idx] - ms[idx[0]]) / 1000.0
        grid = np.arange(0, t[-1], DT)

        def at(arr):
            flat = arr[idx].reshape(len(idx), -1)
            cols = [np.interp(grid, t, flat[:, c]) for c in range(flat.shape[1])]
            return np.stack(cols, 1).reshape((len(grid),) + arr.shape[1:])

        g = np.interp(grid, t, gc[idx])
        running = np.zeros(len(grid), bool)
        running[1:] = np.diff(g) < -0.01
        if len(grid) > 1:
            running[0] = running[1]
        out["t"].append(grid + ms[idx[0]] / 1000.0)
        out["seg"].append(np.full(len(grid), r))
        out["live"].append(running)
        out["ball"].append(at(ball))
        out["xy"].append(at(xy))
        out["team"].append(np.repeat(team[idx[:1]], len(grid), 0))
        out["pid"].append(np.repeat(pid[idx[:1]], len(grid), 0))
    F = {k: np.concatenate(v) for k, v in out.items()}
    F["hold"] = _holder(F)
    offteam = np.zeros(len(F["hold"]), np.int64)
    last = 0
    for i, h in enumerate(F["hold"]):
        if h >= 0:
            last = F["team"][i, h]
        offteam[i] = last
    F["off"] = F["team"] == offteam[:, None]
    ox = np.where(F["off"], F["xy"][..., 0], np.nan)
    with warnings.catch_warnings():
        # (Nobody on offense yet: the left rim, as good as any.)
        warnings.simplefilter("ignore", RuntimeWarning)
        F["hoop"] = HOOPS[(np.nanmean(ox, 1) > 47).astype(int)]
    F["live"] &= offteam > 0
    F["names"] = names
    F["game"] = label
    return F


# The play-by-play lines a possession is live through (see gameTape.test.ts).
SHOT_PLAYS = ("fg", "miss", "tp", "ast", "tov", "stl", "pf", "blk", "orb", "drb")


def from_tape(path):
    T = json.load(open(path))
    team_of = {p["pid"]: p["team"] for p in T["players"]}
    keys = ("t", "seg", "live", "ball", "xy", "team", "pid", "hold", "off", "hoop")
    out = {k: [] for k in keys}
    # (The clock as the frame before had it - the possession before's last,
    # at the first of one.)
    was = None
    for s, P in enumerate(T["possessions"]):
        beats = P["beats"]
        side = P["side"]
        for fr in P["frames"]:
            # (The game clock running, where the tape has it: stopped - a
            # whistle, the ball out, a free throw - is not live, as in the
            # real games' tracking.)
            running = "c" not in fr or (was is not None and fr["c"] < was - 0.01)
            was = fr.get("c")
            ps = [p for p in fr["p"] if p[5]]
            if len(ps) != 10:
                continue
            ps = sorted(ps, key=lambda p: (team_of[p[0]], p[0]))
            t = fr["t"]
            before = [b for b in beats if b["t"] <= t]
            beat = before[-1]["type"].lower() if before else ""
            # (Live: shown at real speed, the ball in play.)
            live = (
                running
                and fr["f"] < 1.2
                and beat.startswith(SHOT_PLAYS)
                and not beat.endswith("ft")
            )
            slots = [p[0] for p in ps]
            h = fr["b"][3]
            out["t"].append(t / 1000.0)
            out["seg"].append(s)
            out["live"].append(live)
            out["ball"].append(fr["b"][:3])
            out["xy"].append([[p[1], p[2]] for p in ps])
            out["team"].append([team_of[p] for p in slots])
            out["pid"].append(slots)
            out["hold"].append(slots.index(h) if h in slots else -1)
            out["off"].append([team_of[p] == side for p in slots])
            out["hoop"].append(HOOPS[1] if side == 1 else HOOPS[0])
    F = {k: np.array(v) for k, v in out.items()}
    # (A frame missing inside a possession starts a new run.)
    gap = np.zeros(len(F["t"]), bool)
    gap[1:] = (np.abs(np.diff(F["t"]) - DT) > 0.01) | (np.diff(F["seg"]) != 0)
    F["seg"] = np.cumsum(gap)
    F["names"] = {}
    F["game"] = T["game"]
    return F


def pooled(Fs):
    """Many games' frames as one set (runs kept apart)."""
    off = 0
    for F in Fs:
        F["seg"] = F["seg"] + off
        off = F["seg"].max() + 1
    names = {}
    for F in Fs:
        names.update(F.get("names", {}))
    keys = [k for k in Fs[0] if k not in ("names", "game")]
    P = {k: np.concatenate([F[k] for F in Fs]) for k in keys}
    P["names"] = names
    return P
