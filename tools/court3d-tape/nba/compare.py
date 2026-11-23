# Two measure files side by side, as a markdown table:
#
#   python3 compare.py targets.json now.json ["Real NBA" "3D today"]
import json
import sys

A = json.load(open(sys.argv[1]))
B = json.load(open(sys.argv[2]))
na = sys.argv[3] if len(sys.argv) > 3 else "Real NBA"
nb = sys.argv[4] if len(sys.argv) > 4 else "3D"


def get(R, path):
    v = R
    for k in path.split("."):
        v = v.get(k) if isinstance(v, dict) else None
    return v


ROWS = [
    ("Live minutes measured", "live_min", ""),
    ("Average speed, live play (mph)", "mph_live", ""),
    ("Standing still (under 1.5 ft/s)", "standing_live", "%"),
    ("... the man with the ball", "role_handler.still", "%"),
    ("... offense off the ball", "role_off_ball_O.still", "%"),
    ("... the man guarding the ball", "role_on_ball_D.still", "%"),
    ("... help defense", "role_help_D.still", "%"),
    ("Over 15 ft/s", "over15_live", "%"),
    ("Top speed (ft/s, 99.9th pct)", "speed_top", ""),
    ("Speeding up, 90th pct (ft/s2)", "accel_up.p90", ""),
    ("Slowing down, 90th pct (ft/s2)", "accel_down.p90", ""),
    ("Turning at 8-13 ft/s, median (deg/s)", "turn_8_13.p50", ""),
    ("Turning at 8-13 ft/s, 90th pct (deg/s)", "turn_8_13.p90", ""),
    ("Keeps moving once going, median (s)", "moving_bout_s.p50", ""),
    ("Set off within 0.1 s of another man", "lockstep", "%"),
    ("On-ball defender distance, median (ft)", "def_on_ball.p50", ""),
    ("Man 15-25 ft from ball: defender off him (ft)", "def_off_ball_15_25.dist.p50", ""),
    ("... sagged toward the rim (0 on him, 1 at rim)", "def_off_ball_15_25.sag.p50", ""),
    ("Man 35+ ft from ball: defender off him (ft)", "def_off_ball_35_99.dist.p50", ""),
    ("... sagged toward the rim", "def_off_ball_35_99.sag.p50", ""),
    ("... shaded to the ball side (ft)", "def_off_ball_35_99.ballside.p50", ""),
    ("Nearest offensive teammate, median (ft)", "off_nearest_mate.p50", ""),
    ("Nobody in the paint", "off_in_paint.0", "%"),
    ("Ball held, median (s)", "hold_s.p50", ""),
    ("Passes a live minute", "passes_per_live_min", ""),
    ("Pass length, median (ft)", "pass_ft.p50", ""),
]


def fmt(v, unit):
    if v is None or v == {}:
        return "-"
    return f"{v * 100:.0f}%" if unit == "%" else f"{v:g}"


print(f"| Measure | {na} | {nb} |")
print("|---|---|---|")
for name, path, unit in ROWS:
    print(f"| {name} | {fmt(get(A, path), unit)} | {fmt(get(B, path), unit)} |")
arc = lambda R: sum(R["off_beyond_arc"].get(k, 0) for k in "345")
print(f"| Three or more beyond the arc | {arc(A) * 100:.0f}% | {arc(B) * 100:.0f}% |")
