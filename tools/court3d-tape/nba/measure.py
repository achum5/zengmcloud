# Measure games, pooled, into one json (see README.md):
#
#   python3 measure.py nba <out.json> <game.json> ...      real NBA tracking
#   python3 measure.py tape <out.json> <tape_x.json> ...   the 3D game tape
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from frames import from_sportvu, from_tape, pooled  # noqa: E402
from measures import measures, player_tops  # noqa: E402

kind, out, files = sys.argv[1], sys.argv[2], sys.argv[3:]
Fs = []
for p in files:
    Fs.append(from_sportvu(p) if kind == "nba" else from_tape(p))
    print(os.path.basename(p), round(Fs[-1]["live"].sum() / 600, 1), "live min", file=sys.stderr)
F = pooled(Fs)
R = measures(F)
R["games"] = [f"{F_['game']} ({os.path.basename(p)})" for F_, p in zip(Fs, files)]
if kind == "nba":
    tops = player_tops(F)
    R["player_tops"] = {
        "players": len(tops),
        "spread_ft_s": {
            p: round(float(sorted(r["top"] for r in tops)[int(len(tops) * p / 100)]), 1)
            for p in (5, 50, 95)
        },
        "slowest": tops[:5],
        "fastest": tops[-5:],
    }
json.dump(R, open(out, "w"), indent=1)
print(json.dumps({k: R[k] for k in ("live_min", "mph_live", "standing_live", "lockstep")}))
