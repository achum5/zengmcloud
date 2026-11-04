# 3D game tape

Look through whole games the way the 3D court plays them, instead of watching
them. Three steps, all from the repo root:

1. **Games.** Play some with the real sim (real ratings):

   ```
   GEN_DIR=/tmp/games GEN_SEEDS=11,22,33 npx vitest run --project basketball \
     tools/court3d-tape/realGames.test.ts
   ```

2. **Tape.** Every player's spot, facing and animation and the ball, every
   0.1 s, possession by possession, plus a list of what looks unnatural at
   real speed (running backward, sliding without stepping, jumping from spot
   to spot, two men inside each other, standing with the ball, an open lane
   to the rim passed up):

   ```
   TAPE_GAMES=/tmp/games/real_11.json,/tmp/games/real_22.json TAPE_DIR=/tmp/tape \
     npx vitest run --project basketball tools/court3d-tape/gameTape.test.ts
   ```

   `/tmp/tape/tape_real_11.txt` has the counts and every flag (possession,
   time, what); `tape_real_11.json` is the whole tape. A test-game seed
   (`lane-a`) works as a game too.

3. **Look.** Needs Python with Pillow.

   ```
   python3 tools/court3d-tape/sheet.py moonwalk /tmp/moon.png /tmp/tape/tape_real_*.json
   python3 tools/court3d-tape/view.py /tmp/tape/tape_real_11.json 40 /tmp/p40.png
   ```

   `sheet.py` draws a small panel for every flag of one kind; `view.py` draws a
   whole possession (or a stretch of it) in 1.5 s panels.
