# The new possession sim

The 3D game rebuilt so the ten on the floor look like NBA players trying to
win: each going about it with his own body and, step by step, his own head.
The game sim engine does not change. Its play-by-play is the truth, and the
picture has to look like it decided it.

Built behind a switch: Global Settings, Games, "3D Game Sim" (`setting.ts`,
kept per device). Off, today's 3D plays exactly as it did. Work here can be
pushed as it is finished, since nobody sees it until the switch is on.

## How a possession runs

One possession runs forward in time through six layers. Each owns one kind
of decision, so a fix has exactly one place to go.

1. **Facts.** The engine's lines for the possession, as checkpoints the rest
   must hit: who has the ball when, the shot (who, zone, when, result), a
   turnover, a foul, the rebound. Nothing later changes them.
2. **The planner.** Each side's intentions, once a possession and again when
   it breaks: tempo, the action that leads to the engine's result, roles,
   matchups and coverages. From personnel, never from coach settings.
3. **The ten.** Every step each man sees (a beat late, only what he faces),
   decides (his role gives a goal: a spot, a pace, a way to face) and, as a
   fact of his comes due, commits to being where it needs him.
4. **Movement and contact** (`movement.ts`, `body.ts`). A goal becomes motion
   inside what a body can do. Nobody passes through anybody.
5. **The ball.** Dribbles, passes by type and speed, shots, then today's
   flight, rim and rebound.
6. **Output.** The same timeline the court reads today. Renderer, camera,
   replays, key plays and multiplayer do not change.

Every layer reads the man's own ratings. The floor is an NBA player for
everyone: ratings say how far above it he is, so stars look like stars and a
35 still looks like a pro.

## Files

- `body.ts`: what his body can do (top speed, burst, brakes, cutting grip,
  how quickly a push builds, how quickly he turns), from his ratings and how
  worn down he is. Ranges measured off real NBA tracking.
- `movement.ts`: one step (`STEP_S`, 50 ms) of getting about the floor. A
  goal (where, how that spot is moving, how hard, which way to face) becomes
  a push that builds, braking in time to stop on the spot, and a cut taken at
  a cutting pace. Decides nothing.
- `follow.ts`: step 3's stand-in for the ten. Each man goes where today's
  stepping wants him, moved by his own body. Today's play starts and stops
  men quicker than any body can, so he takes its runs the way a man who
  knows where he is going does (into a run a little early, out of it a
  little late), and where it still asks too much he is pulled along (at
  most 5 ft/s on top of his own going) rather than left feet behind. On the
  play itself whenever he catches, shoots, passes or screens. Swapped in for
  `stepper.ts`'s `follow`, one man at a time (`Director.ownMovers`).
- `packages.ts`: the library of moves. Each kind of move (walking, running,
  down in his stance, backpedaling for now) comes in versions, packages,
  each with its timing (read by the staging), its look (read only by the
  picture) and who it fits (ratings that lean a man toward it, and floors).
  Ids are for good: a saved signature names them.
- `signature.ts`: which package each man has for each kind, picked from his
  ratings with a draw of his own until one is saved for him.
- `gait.ts`: how quickly his steps go round at each pace, from real runners
  and walkers, scaled to his height and his package: each step as long as
  that makes it. The staged timeline carries each man's signature and
  height (`CourtTimeline.gaits`), and the picture steps him by them: a jog
  lengthening on into a run, a run into a sprint, as real runners' strides
  do (`../poses.ts` lenMost), rather than one gait's stride at a quicker
  beat.
- `setting.ts`: the switch.

## Built in this order

Each step ends with the rules green, the scorecard before and after, and film
for Alex. A step is done when the numbers move the right way and Alex says it
looks better.

1. Fast-forward to the action: the last seconds before each shot, turnover
   or foul at real speed, or whole possessions (`../pacing.ts`).
2. The before: real NBA tracking as the target (`tools/court3d-tape/nba`).
3. **Movement, done properly**. Today's plan, today's actions,
   each man moved by his own body. Judged on its own before anything decides
   differently.
4. **How they move**: the first packages (walk, run, stance,
   backpedal) and each man's signature; his steps at his own beat and
   length. Then stops, plant-and-cut and slides (built: real footsteps,
   `footsteps.ts` - each step a spot on the floor, planned off the path
   he takes, and his legs reach for it), judged in the movement lab
   (`tools/court3d-tape/lab.test.ts`) beside real footage. His steps go on
   through whatever he does on the floor (a screen, a jab, setting his feet
   for a shot), stepping to where that move has his feet; his legs are the
   move's own only up in a jump (handed over in the air, both ways) and in
   moves that take them their own way (sat down, knocked over). A foot down
   slides on 1.6% of its time over a game (LAB_MEASURE footSliding), from
   28% on the old step cycles (5.6% before shots, jumps and screens were
   stepped through).
5. Defense (built), the first of the ten deciding for themselves. The facts, the
   planner's first pieces and the per-possession loop come in with it, in
   this folder, and today's defensive patch passes are off under the switch.
6. Offense off the ball.
7. The ball handler.
8. The planner's full vocabulary: transition, the playbook, team styles.
9. Contact: screens hit, bumps, box-outs.
10. The rest of the packages (shots, finishes, dribble moves, passes).
11. Switch on, when Alex says so on film. The old possession code goes.

## Step 5: the defense deciding

What pins a defender down (the facts, as the play is staged from the
engine's lines): his acts (the contest, the block, the steal, the foul, the
charge, the box-out, the rebound, a bump or a fight for position), where
and when the play has each; the picture cutting; the ball changing hands
between the sides; a shot in the air until it is rebounded. He is on the
spot as each starts, getting there by his own body, and between them he
goes where his own head takes him.

The planner, for now, is today's director: it says who guards whom (its
switches included) and the runs that lead to the engine's result (the help
that stops a drive, a closeout late enough to leave the shot as open as it
was drawn, a break the engine has beat them). A defender follows those runs
as laid, and guards his man himself in between - helping on a drive and
recovering himself too.

Getting back is his own too: from a beat after the ball changes hands (not
on a break - there the play has them beaten, as the engine says), he heads
back and onto the man the planner gives him as the ball comes up the floor,
by the same rules as in the half court (his man far from the ball, so well
off him toward the rim). Real games, a few seconds after the ball changes
hands, have each defender about 11 ft from the nearest attacker and 30 ft
nearer his rim than the ball; the play alone had them 16 ft off and 40-47 ft
back, all in the paint.

Each step (`defense.ts`), each of the five:

- sees the five on offense and the ball where they were a moment ago (his
  own reaction time, quicker the better his defensive IQ), carried on by how
  they were going;
- picks his spot: on the ball, between his man and the rim at a cushion
  (real: about 5.6 ft), tighter on a shooter and a quick defender, looser
  on a man who can't shoot or a long way out; off the ball, a share of the
  way from his man to the rim and over toward the ball's side, both more
  the farther his man is from the ball (real: a quarter of the way and half
  a foot over within 15 ft of it, almost half the way and two feet over past
  35), more off a poor shooter and less off a great one;
- the five read a drive together (a beat late): the man nearest the ball's
  way in, off a man away from the ball, steps up in front of it, and those
  near it show; nobody steps up if somebody is in front already;
- goes there with his own body (`movement.ts`), facing the ball: sliding,
  turning to run when he has to, closing out at a set man with it; never
  through anybody - round a man in his way, out of the way of one coming
  through;
- as one of his facts comes due, makes for its spot when his own legs say
  he must (stepped on ahead to see how long getting there and set takes,
  going the other way included), there by the time the play has him back.

Time runs forward a possession at a time (`stepper.ts` stepTimeline): the
offense first (it does not answer the defense until step 6), then the five
on defense together, each carrying on from where the last stretch left him.

Handing over, both ways, without a jump: the play takes him back from
where he is, its way shifted over by how far off it he is and the shift let
go before his next fact. A contest, a reach or a bump he does wherever he
is, feet set - unless something that pins him follows close behind (a
contest, then the box-out): then the play has him through both. Nor does he
take over just before one of those: the play has him till it is done.

Today's passes that move defenders around after the plan (following his man
a beat behind, the help share, stopping the ball, biting on fakes, giving
ground to a jab, holding his ground on a jumper) no longer move a defender
guarding his man under the switch: what they add as acts stays, as facts.

Built as: 5a the facts, the loop, guarding his man on and off the ball;
5b help and recover, closeouts; 5c transition; 5d what the play keeps.

What the play keeps (real_11, live defense after the first 4 s of a trip):
the five decide 84% of it. The rest is the shot in the air till the
rebound (4%), a break the engine scored, the end of a quarter, and stretches
the planner gives nobody a man because the engine's result needs him
somewhere: rotated off a shooter the engine left open, a closeout that
arrives late, a man backing him down in the post, a foul. Those stay laid
by the play till the planner can say them as aims (be this far off the
shooter at the release) rather than runs; so does `planned`, the help that
stops a drive (0.1%). Nothing after the stepper moves the five to speak of:
the court's own passes shift a man the five laid more than 1 ft on under 1%
of their steps; turning the steps into runs, 0.2 ft at the median.

## How we know

- Rules that always hold, in the tests here: nobody faster, pushing harder
  or jolting quicker than his body lets him; stops on his spot, not past it;
  the sharper the cut the slower through it; the same step always goes the
  same way (every device agrees).
- The scorecard against real NBA games, from the same measures run on both:

  ```
  TAPE_POSSESSION=1 TAPE_GAMES=<games.json> TAPE_DIR=<dir> \
    vitest run --project basketball tools/court3d-tape/gameTape.test.ts
  python3 tools/court3d-tape/nba/measure.py tape <out.json> <dir>/tape_*.json
  python3 tools/court3d-tape/nba/compare.py tools/court3d-tape/nba/targets.json <out.json>
  ```

- Getting back, the same way on both (`nba/transition.py nba|tape <files>`):
  where the five on defense are 1-8 s after the ball changes hands with it
  in their front court.

- Film beside film, for Alex's eye.
