# The 3D game against real NBA tracking

How far the 3D court's movement is from the real thing, measured the same way
on both: real NBA player tracking (SportVU, 2015-16: every player and the ball,
25 times a second) and the 3D game tape (`../gameTape.test.ts`). The real
numbers, pooled over 17 games (775 live minutes), are kept in `targets.json`;
they are the targets the possession sim is built to.

Only numbers fitted from the tracking go into the game, never the data.

## Real games

The tracking logs are public, one `.7z` a game (about 5 MB, 95 MB unpacked), in
`data/2016.NBA.Raw.SportVU.Game.Logs/` of github.com/linouk23/NBA-Player-Movements.
Fetch only the games wanted (the whole repository is gigabytes):

```
git clone --filter=blob:none --no-checkout https://github.com/linouk23/NBA-Player-Movements nba-data
cd nba-data && git checkout HEAD -- data/2016.NBA.Raw.SportVU.Game.Logs/01.01.2016.DAL.at.MIA.7z
7z x -o../games data/2016.NBA.Raw.SportVU.Game.Logs/01.01.2016.DAL.at.MIA.7z
```

(`targets.json` lists the 17 games it was measured on, by game id.)

## Measuring

Python 3 with numpy (and Pillow for the pictures):

```
python3 measure.py nba targets.json games/*.json             # the real targets
python3 measure.py tape now.json /tmp/tape/tape_real_*.json  # the 3D, from the game tape (see ../README.md)
python3 compare.py targets.json now.json "Real NBA" "3D"     # side by side, as a table
```

Only live play counts: the clock running (real), or shown at real speed with
the ball in play (3D). Both are taken to 10 frames a second and smoothed over
three frames before speeds are worked out.

What is measured:

- **Moving.** Speed (average, standing still, over 15 ft/s, top), by role (the
  man with the ball, the offense off it, the man guarding it, the help); how
  hard men speed up and slow down; how fast they turn at each speed; how long
  they keep moving once going; lockstep (setting off within 0.1 s of another man).
- **The envelope** (`envelope` in the json): by speed, the 95th and 99th
  percentile of speeding up, braking and sideways acceleration - what a body
  does, the limits the movement layer works within.
- **Defense.** Each defender matched to a man each second; how far off him,
  how far toward the rim (0 on him, 1 at the rim) and how far to the ball side,
  by how far the man is from the ball; the man guarding the ball.
- **Offense.** Nearest teammate, men beyond the arc, men in the paint.
- **The ball.** How long a man holds it; passes a live minute, their length and speed.
- **Players (real only).** Each man's top speed in games: from about 17.8 ft/s
  (5th percentile; slow bigs) to 21.2 (95th; quick guards and wings) - the
  spread ratings should give.

## Pictures

```
python3 draw.py runs  tape /tmp/tape/tape_real_11.json                       # long half-court stretches
python3 draw.py paths tape /tmp/tape/tape_real_11.json 18356 18456 out.png "3D"  # ten men's paths
python3 draw.py video nba games/0021500491.json 814 1000 out.mp4 "Real"          # from above, played (ffmpeg)
```
