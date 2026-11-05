# Football 3D game tape

Football's version of `tools/court3d-tape`: look through whole games the way
the 3D field plays them, instead of watching them. Three steps, all from the
repo root:

1. **Games.** Play some with the sim (the tests' own games, see
   `src/ui/views/LiveGame/field3d/testGames.ts`):

   ```
   GEN_DIR=/tmp/games GEN_SEEDS=7,23,24 npx vitest run --project football \
     tools/field3d-tape/realGames.football.test.ts
   ```

2. **Tape.** Every man's spot, facing and move and the ball, every 0.1 s,
   play by play, plus a list of what looks unnatural at real speed (jumping
   from spot to spot, a pace changing in one step, cornering at speed,
   spinning on the spot, running backward, sliding without stepping, walking
   or jogging too fast, standing stock still):

   ```
   TAPE_GAMES=/tmp/games/real_7.json,/tmp/games/real_23.json TAPE_DIR=/tmp/tape \
     npx vitest run --project football tools/field3d-tape/gameTape.football.test.ts
   ```

   `/tmp/tape/tape_real_7.txt` has the counts and every flag (play, time from
   the snap, what); `tape_real_7.json` is the whole tape. A seed (`23`) works
   as a game too. `TAPE_SPAN=pre|after|all` looks at the rest of each play
   instead of snap to whistle.

3. **Look.** Needs Python with Pillow.

   ```
   python3 tools/field3d-tape/sheet.py lurch /tmp/lurch.png /tmp/tape/tape_real_*.json
   python3 tools/field3d-tape/view.py /tmp/tape/tape_real_7.json 40 /tmp/p40.png
   ```

   `sheet.py` draws a small panel for every flag of one kind; `view.py` draws a
   whole play (or a stretch of it, times from the snap, and a man to follow)
   in 1.5 s panels.

4. **Variety.** How many different ways one kind of play comes out on the
   field, from the parts it is put together from - a run: the formation,
   the exchange, the hole, a cutback or not, a move to make a man miss,
   how it ended and how many brought him down; a catch: the drop, where
   the throw went and how high, how it was caught and how open he was,
   and the same ending - overall, every combination, and for each player
   how often he did it a way he had before:

   ```
   VARIETY_GAMES=/tmp/games/real_7.json,/tmp/games/real_23.json \
   VARIETY_LINE=run VARIETY_OUT=/tmp/variety_run.txt \
     npx vitest run --project football tools/field3d-tape/variety.football.test.ts
   ```

   `VARIETY_LINE=catch` for completed passes; `VARIETY_BY=throw,catch` also
   counts just how those parts go together.

`PLAYS.md` takes the lines one at a time: their parts, how many ways each
comes out today, and what is next.
