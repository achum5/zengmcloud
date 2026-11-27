"""Does margin so far predict the rest of the season beyond the outlook's read?

Reads forwardAudit FA_STRENGTH dumps (one per league) and, for every in-season
outlook and team, sets the margin so far against what the reads expected of
those games (the team's and its opponents', the home edge), and the rest of
its season against what the read expects of it (docs/smart-cpu.md, "How teams
have played, in the read").

  python3 -I tools/resultsProbe.py <league.json> ...        by games played
  python3 -I tools/resultsProbe.py --fit <league.json> ...  the weight's fit
  python3 -I tools/resultsProbe.py --checks <league.json> ...
      the playoffs and hurt or rebuilt teams, read by the reads then
"""
import json
import math
import sys
from collections import defaultdict

GAME_SIGMA = 13.1


def load(path):
    d = json.load(open(path))
    return d["reads"], d["games"]


def ols(xs, ys):
    n = len(xs)
    mx = sum(xs) / n
    my = sum(ys) / n
    sxx = sum((x - mx) ** 2 for x in xs)
    sxy = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    b = sxy / sxx if sxx else 0
    res = [y - my - b * (x - mx) for x, y in zip(xs, ys)]
    s2 = sum(r * r for r in res) / max(1, n - 2)
    se = math.sqrt(s2 / sxx) if sxx else 0
    return b, se, my - b * mx


def ols0(xs, ys):
    """Through the origin."""
    sxx = sum(x * x for x in xs)
    sxy = sum(x * y for x, y in zip(xs, ys))
    b = sxy / sxx if sxx else 0
    res = [y - b * x for x, y in zip(xs, ys)]
    s2 = sum(r * r for r in res) / max(1, len(xs) - 1)
    return b, math.sqrt(s2 / sxx) if sxx else 0


def rows_for(path, league):
    reads, seasons = load(path)
    out = []
    season_rows = []
    for sg in seasons:
        season = sg["season"]
        games = sorted(sg["games"], key=lambda x: x[0])
        games = [x for x in games if x[2] >= 0 and x[5] >= 0]
        reg = [x for x in games if x[1] == 0]
        po = [x for x in games if x[1] == 1]
        # Each team's regular-season games in order: (day, opp, home, margin,
        # own ovr, opp ovr).
        by_team = defaultdict(list)
        for day, _, t0, o0, p0, t1, o1, p1 in reg:
            by_team[t0].append((day, t1, 1, p0 - p1, o0, o1))
            by_team[t1].append((day, t0, -1, p1 - p0, o1, o0))
        po_team = defaultdict(list)
        for day, _, t0, o0, p0, t1, o1, p1 in po:
            po_team[t0].append((day, t1, 1, p0 - p1))
            po_team[t1].append((day, t0, -1, p1 - p0))
        rounds = {t[0]: t[3] for t in sg["teams"]}
        ins = [
            r
            for r in reads
            if r["first"] == season and r["phase"] in (1, 2) and "soFar" in r
        ]
        pre = [r for r in reads if r["first"] == season and r["phase"] == 0]
        pre_read = pre[-1] if pre else (ins[0] if ins else None)
        if not ins:
            continue
        # Home edge: mean home margin less the reads' gap (first read).
        hs = []
        r0 = ins[0]
        for day, _, t0, o0, p0, t1, o1, p1 in reg:
            hs.append(
                p0 - p1 - (r0["teams"][str(t0)]["s"][0] - r0["teams"][str(t1)]["s"][0])
            )
        hca = sum(hs) / len(hs)
        tids = sorted(int(t) for t in ins[0]["teams"])
        # The read in effect before each team's game j: the latest outlook
        # made with fewer than j+1 games played by it.
        for k, r in enumerate(ins):
            for tid in tids:
                gp, pts, opp = r["soFar"][str(tid)]
                tg = by_team[tid]
                if gp < 5 or gp >= len(tg) - 5:
                    continue
                read_now = r["teams"][str(tid)]["s"][0]
                played = tg[:gp]
                rest = tg[gp:]
                mov = (pts - opp) / gp
                # Expected so far, read now (team and opponents), with home.
                exp_now = sum(
                    read_now - r["teams"][str(o)]["s"][0] + h * hca
                    for _, o, h, _, _, _ in played
                ) / gp
                # Expected so far by the reads in effect at each game.
                exp_then = 0
                for j, (_, o, h, _, _, _) in enumerate(played):
                    then = None
                    for rr in ins[: k + 1]:
                        if rr["soFar"][str(tid)][0] <= j:
                            then = rr
                    if then is None:
                        then = pre_read or ins[0]
                    exp_then += (
                        then["teams"][str(tid)]["s"][0]
                        - then["teams"][str(o)]["s"][0]
                        + h * hca
                    )
                exp_then /= gp
                # Game-time ovr against ovr now (the next game's): how much the
                # lineups that played differ from today's, BBGM ovr points to
                # margin (15 points of margin per 50 of ovr).
                own_now = rest[0][4]
                lineup = sum(
                    (go - own_now) * 0.3 for _, _, _, _, go, _ in played
                ) / gp
                rest_mov = sum(x[3] for x in rest) / len(rest)
                exp_rest = sum(
                    read_now - r["teams"][str(o)]["s"][0] + h * hca
                    for _, o, h, _, _, _ in rest
                ) / len(rest)
                out.append(
                    dict(
                        league=league,
                        season=season,
                        k=k,
                        tid=tid,
                        progress=r["progress"],
                        n=gp,
                        m=len(rest),
                        mov=mov,
                        read=read_now,
                        exp_now=exp_now,
                        exp_then=exp_then,
                        lineup=lineup,
                        rest=rest_mov,
                        exp_rest=exp_rest,
                        title=r["teams"][str(tid)]["title"],
                        edge=r["teams"][str(tid)]["edge"],
                        results=r["teams"][str(tid)].get("results", 0),
                        rounds=rounds.get(tid, -1),
                        po=po_team.get(tid, []),
                        last_reads={
                            int(t): v["s"][0] for t, v in r["teams"].items()
                        },
                        hca=hca,
                    )
                )
        season_rows.append(
            dict(league=league, season=season, by_team=by_team, pre=pre_read, ins=ins, hca=hca, rounds=rounds, po=po_team)
        )
    return out, season_rows


def main():
    rows = []
    seasons = []
    for i, p in enumerate(sys.argv[1:]):
        r, s = rows_for(p, i)
        rows += r
        seasons += s
    print(f"{len(rows)} team-moments, {len(seasons)} league-seasons")
    print(
        "home edge (home margin less reads' gap):",
        " ".join(f"{s['hca']:.2f}" for s in seasons),
    )

    # The read's own error on the rest: is it unbiased with slope 1?
    xs = [r["exp_rest"] for r in rows]
    ys = [r["rest"] for r in rows]
    b, se, a = ols(xs, ys)
    print(f"rest margin on read's expectation: slope {b:.3f} ± {se:.3f}, intercept {a:.2f}")

    bins = [(5, 15), (15, 25), (25, 35), (35, 45), (45, 55), (55, 65), (65, 78)]
    for label, key in [
        ("so far against read now", "exp_now"),
        ("so far against reads then", "exp_then"),
        ("so far against read now + lineup", "lineup"),
    ]:
        print(f"\n== residual {label} ==")
        print("games   moments  beta     se    theory(1.5)  sd(x)  sd(y)")
        for lo, hi in bins:
            sub = [r for r in rows if lo <= r["n"] < hi]
            if len(sub) < 30:
                continue
            if key == "lineup":
                xs = [r["mov"] - r["exp_now"] - r["lineup"] for r in sub]
            else:
                xs = [r["mov"] - r[key] for r in sub]
            ys = [r["rest"] - r["exp_rest"] for r in sub]
            b, se = ols0(xs, ys)
            n = sum(r["n"] for r in sub) / len(sub)
            th = 1.5**2 / (1.5**2 + GAME_SIGMA**2 / n)
            sdx = math.sqrt(sum(x * x for x in xs) / len(xs))
            sdy = math.sqrt(sum(y * y for y in ys) / len(ys))
            print(
                f"{lo:2d}-{hi:2d}  {len(sub):6d}  {b:6.3f}  {se:5.3f}   {th:6.3f}     {sdx:5.2f}  {sdy:5.2f}"
            )

    # Fit sigma (the read's persistent miss) for the weight
    # w(n) = sigma^2 / (sigma^2 + s^2 / n), by least squares on the rest.
    for key in ["exp_now", "exp_then", "lineup"]:
        best = None
        for sig10 in range(0, 61):
            sig = sig10 / 10
            err = 0
            for r in rows:
                x = r["mov"] - (
                    r["exp_now"] + r["lineup"] if key == "lineup" else r[key]
                )
                w = sig**2 / (sig**2 + GAME_SIGMA**2 / r["n"]) if sig else 0
                pred = r["exp_rest"] + w * x
                err += (r["rest"] - pred) ** 2 * r["m"]
            if best is None or err < best[1]:
                best = (sig, err)
        base = sum((r["rest"] - r["exp_rest"]) ** 2 * r["m"] for r in rows)
        tot_m = sum(r["m"] for r in rows)
        print(
            f"\nbest sigma against {key}: {best[0]:.1f}; rest-of-season miss (games-weighted rms) "
            f"{math.sqrt(base / tot_m):.3f} -> {math.sqrt(best[1] / tot_m):.3f}"
        )
        for sig in [1.0, 1.25, 1.5, 2.0]:
            err = 0
            for r in rows:
                x = r["mov"] - (
                    r["exp_now"] + r["lineup"] if key == "lineup" else r[key]
                )
                w = sig**2 / (sig**2 + GAME_SIGMA**2 / r["n"])
                err += (r["rest"] - r["exp_rest"] - w * x) ** 2 * r["m"]
            print(f"   sigma {sig}: {math.sqrt(err / tot_m):.3f}")

    # Without opponents: margin so far against the read alone (what a
    # simple blend would use).
    print("\n== simple: margin so far against the read now, no opponents ==")
    for lo, hi in bins:
        sub = [r for r in rows if lo <= r["n"] < hi]
        if len(sub) < 30:
            continue
        xs = [r["mov"] - r["read"] for r in sub]
        ys = [r["rest"] - r["exp_rest"] for r in sub]
        b, se = ols0(xs, ys)
        print(f"{lo:2d}-{hi:2d}  {len(sub):6d}  {b:6.3f}  {se:5.3f}")

    # Playoffs: each playoff game's margin less the last read's gap, against
    # the regular season's residual at the last read.
    print("\n== playoffs ==")
    xs, ys, ws = [], [], []
    for s in seasons:
        last = s["ins"][-1]
        reads = {int(t): v["s"][0] for t, v in last["teams"].items()}
        for tid, games in s["po"].items():
            tg = s["by_team"][tid]
            gp, pts, opp = last["soFar"][str(tid)]
            if gp < 20:
                continue
            exp = sum(
                reads[tid] - reads[o] + h * s["hca"] for _, o, h, _, _, _ in tg[:gp]
            ) / gp
            x = (pts - opp) / gp - exp
            # Each game's residual.
            for _, o, h, mg in games:
                xs.append(x)
                ys.append(mg - (reads[tid] - reads[o] + h * s["hca"]))
    if xs:
        b, se = ols0(xs, ys)
        print(f"{len(xs)} playoff team-games: beta {b:.3f} ± {se:.3f}")

    # Next season: this season's final residual (full season against the
    # reads then) against next season's (full season against its preseason
    # read).
    print("\n== next season ==")
    by = {(s["league"], s["season"]): s for s in seasons}
    xs, ys = [], []
    for s in seasons:
        nxt = by.get((s["league"], s["season"] + 1))
        if not nxt or not nxt["pre"]:
            continue
        last = s["ins"][-1]
        for tid, tg in s["by_team"].items():
            r = {int(t): v["s"][0] for t, v in last["teams"].items()}
            x = sum(m for _, _, _, m, _, _ in tg) / len(tg) - sum(
                r[tid] - r[o] + h * s["hca"] for _, o, h, _, _, _ in tg
            ) / len(tg)
            pre = {int(t): v["s"][0] for t, v in nxt["pre"]["teams"].items()}
            ng = nxt["by_team"][tid]
            y = sum(m for _, _, _, m, _, _ in ng) / len(ng) - sum(
                pre[tid] - pre[o] + h * nxt["hca"] for _, o, h, _, _, _ in ng
            ) / len(ng)
            xs.append(x)
            ys.append(y)
    if xs:
        b, se = ols0(xs, ys)
        print(f"{len(xs)} team-seasons: beta {b:.3f} ± {se:.3f}")




def fit2(paths):
    """w(n) = p / (p + t + s^2 / n): p the read's miss that lasts, t the
    part of the margin so far over the read that does not."""
    rows = []
    for i, p in enumerate(paths):
        rows += rows_for(p, i)[0]
    s2 = GAME_SIGMA**2
    tot_m = sum(r["m"] for r in rows)
    def err(p, t, key="exp_now"):
        e = 0
        for r in rows:
            x = r["mov"] - r[key]
            w = p / (p + t + s2 / r["n"]) if p > 0 else 0
            e += (r["rest"] - r["exp_rest"] - w * x) ** 2 * r["m"]
        return math.sqrt(e / tot_m)
    for key in ["exp_now", "exp_then"]:
        best = min(
            ((err(p / 10, t / 10, key), p / 10, t / 10) for p in range(0, 31) for t in range(0, 61, 2)),
        )
        print(f"two-part fit against {key}: miss {best[0]:.4f} at lasting {best[1]:.1f} (sd {math.sqrt(best[1]):.2f}), passing {best[2]:.1f} (sd {math.sqrt(best[2]):.2f}); none {err(0, 0, key):.4f}")
        for n in (10, 20, 30, 41, 52, 60, 70, 80):
            print(f"   w({n}) = {best[1] / (best[1] + best[2] + s2 / n):.3f}")
    # The read's miss of the rest that is not the games' luck, by games
    # played, before and after the blend (lasting 1.0, passing 2.5).
    print("\nrest-of-season miss beyond luck (sd), before and after the blend:")
    for lo, hi in [(5, 15), (15, 25), (25, 35), (35, 45), (45, 55), (55, 65), (65, 78)]:
        sub = [r for r in rows if lo <= r["n"] < hi]
        luck = sum(s2 / r["m"] for r in sub) / len(sub)
        before = sum((r["rest"] - r["exp_rest"]) ** 2 for r in sub) / len(sub)
        after = sum(
            (r["rest"] - r["exp_rest"] - 1.0 / (3.5 + s2 / r["n"]) * (r["mov"] - r["exp_now"])) ** 2
            for r in sub
        ) / len(sub)
        prog = sum(r["progress"] for r in sub) / len(sub)
        print(
            f"   {lo:2d}-{hi:2d} progress {prog:.2f}: {math.sqrt(max(0, before - luck)):.2f} -> {math.sqrt(max(0, after - luck)):.2f}"
            f"  (measuredStrengthUncertainty {1.5 * (1 - 0.25 * prog):.2f})"
        )


def checks(paths):
    """Two things the read of teams as they stand gets wrong."""
    # The playoffs: a team's margin over its reads in the regular season
    # against its playoff games, both by the reads at the season's end and by
    # the reads in effect at each game - a man hurt at the end and back for
    # the playoffs makes the first look like more than it is.
    out = defaultdict(lambda: ([], []))
    for path in paths:
        d = json.load(open(path))
        for sg in d["games"]:
            season = sg["season"]
            ins = [
                r
                for r in d["reads"]
                if r["first"] == season and r["phase"] in (1, 2) and "soFar" in r
            ]
            if not ins:
                continue
            games = sorted(
                [x for x in sg["games"] if x[2] >= 0 and x[5] >= 0], key=lambda x: x[0]
            )
            by = defaultdict(list)
            for day, po, t0, o0, p0, t1, o1, p1 in games:
                if po == 0:
                    by[t0].append((t1, 1, p0 - p1))
                    by[t1].append((t0, -1, p1 - p0))
            home = 3.0

            def read_at(tid, j):
                then = ins[0]
                for r in ins:
                    if r["soFar"][str(tid)][0] <= j:
                        then = r
                return then["teams"]

            last = ins[-1]["teams"]
            x_then, x_now = {}, {}
            for tid, tg in by.items():
                e = 0
                for j, (o, h, m) in enumerate(tg):
                    rt = read_at(tid, j)
                    e += rt[str(tid)]["s"][0] - rt[str(o)]["s"][0] + h * home
                mov = sum(m for _, _, m in tg) / len(tg)
                x_then[tid] = mov - e / len(tg)
                x_now[tid] = mov - sum(
                    last[str(tid)]["s"][0] - last[str(o)]["s"][0] + h * home
                    for o, h, _ in tg
                ) / len(tg)
            for day, po, t0, o0, p0, t1, o1, p1 in games:
                if po != 1:
                    continue
                for t, o, h, m in ((t0, t1, 1, p0 - p1), (t1, t0, -1, p1 - p0)):
                    ref = last[str(t)]["s"][0] - last[str(o)]["s"][0] + h * home
                    for key, x in (("by the reads at the end", x_now[t]), ("by the reads then", x_then[t])):
                        out[key][0].append(x)
                        out[key][1].append(m - ref)
    print("playoff games against the regular season's margin over its reads:")
    for key, (xs, ys) in out.items():
        b, se = ols0(xs, ys)
        print(f"   {key:25s} {len(xs)} team-games: {b:.3f} ± {se:.3f}")
    # Teams read now well below or above the reads over the games they have
    # played (hurt now, or traded for better): what the edge gives them, and
    # what of the rest of their season is still left over after it.
    rows = []
    for i, p in enumerate(paths):
        rows += rows_for(p, i)[0]
    s2 = GAME_SIGMA**2
    w = lambda n: 0.64 / (0.64 + s2 / n)  # noqa: E731
    print("\nteams by their read now against the reads over their games (20+ games):")
    for label, f in [
        ("read now 1+ below (hurt now)", lambda r: r["exp_now"] - r["exp_then"] < -1),
        ("read now 1+ above (rebuilt)", lambda r: r["exp_now"] - r["exp_then"] > 1),
        ("the rest", lambda r: abs(r["exp_now"] - r["exp_then"]) <= 1),
    ]:
        sub = [r for r in rows if f(r) and r["n"] >= 20]
        for key in ("exp_now", "exp_then"):
            left = sum(
                (r["rest"] - r["exp_rest"] - w(r["n"]) * (r["mov"] - r[key])) * r["m"]
                for r in sub
            ) / sum(r["m"] for r in sub)
            edge = sum(w(r["n"]) * (r["mov"] - r[key]) for r in sub) / len(sub)
            print(f"   {label:30s} {len(sub):5d} by {key}: edge {edge:+.2f}, left over {left:+.2f}")


if __name__ == "__main__":
    if sys.argv[1] == "--fit":
        fit2(sys.argv[2:])
    elif sys.argv[1] == "--checks":
        checks(sys.argv[2:])
    else:
        main()
