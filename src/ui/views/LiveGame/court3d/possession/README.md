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
6. Offense off the ball (built). Coming into a catch on the move waits
   for steps 7 and 8, where the catches are laid.
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

## Step 6: offense off the ball

Where it starts (real_11 against 17 real games, half court, the ball in an
attacker's hands, the four without it):

| | 3D now | Real |
|---|---|---|
| Stood still (under 1.5 ft/s) | 52% | 19% |
| Walking, jogging, running | 31%, 13%, 5% | 47%, 27%, 7% |
| Feet covered a minute | 181 | 313 |
| The passer, 2 s after his pass: stays, relocates, cuts | 48%, 43%, 9% | 10%, 60%, 30% |
| Catching it on the move (median, ft/s) | 3.3 | 5.8 |
| A drive: of the four, how many move 3 ft in its first second | 1.3 (none, 28%) | 2.3 (none, 4%) |
| Nearest teammate (median, ft) | 14.7 | 14.1 |
| Cuts a minute, each man | 0.88 | 0.75 |

Spacing and cutting are about right already; what is wrong is that a man
off the ball stands on his spot until the play next moves him. Most of
that standing is the plain stood pose (`ready`); screens are 4% of it.

What pins an attacker off the ball (the facts, as the play is staged from
the engine's lines): his catch (the ball reaching him at a spot, at a
time); a screen he sets; his other acts (a handoff, sealing in the post, a
box-out, going to the glass); the ball in his hands (step 7); a shot in the
air till it is rebounded; the picture cutting; a free throw; a break (the
planner's, step 8). He is on the spot as each starts, getting there by his
own body.

The planner, for now, is today's director. It lays the set (where each man
is as it moves) and its actions: the screens, the cuts and runs a play
has a man make, the roll, the handoff, the post-up. Those stay laid as it
lays them. Its spacing runs (the set's spots, spreading out off a board,
moving on after a pass, the drift on a drive, the walking about added on
top) say where it wants him, not how he gets there.

Between those, each attacker decides for himself, each step (`offense.ts`):

- sees the floor a beat late (his own reaction time, quicker the higher
  his offensive IQ), carried on by how it was going;
- has a spot: where the set wants him, never on top of a teammate (about
  10 ft at the least out on the floor), out of the way the ball is going
  in, behind the line if he shoots it there, out of the lane before the
  count of three;
- does not stand on it like a cone: he walks it, a few steps along the
  line and back, toward where the passer can see him, set and ready when
  the ball might come to him;
- reads the ball: a drive, and a beat later a man in its way gets out of
  it (a big to the dunker spot on the far side, anybody else to the short
  corner or the wing), a man in close slides along the baseline away from
  it, the corner stays (lifting to the wing on one down his baseline at
  him), a wing or the top with it gone past him cuts in behind it or
  drifts on round the arc, and the far side sinks a few steps or stays;
- passes and moves: as he lets it go, cuts to the rim (with the lane
  empty; more often the better he reads it, and when his man turns to
  watch the ball), otherwise relocates to an open spot a good few feet
  away, now and then stays;
- reads his man: one who turns his back on him to help, he cuts behind;
  one up in the passing lane close in, he goes backdoor;
- keeps his next job in mind: nothing of his own (a cut, a clear-out, a
  relocation) that takes him so far off he could not get to his next catch
  or screen at a run with a second to spare, and as it comes due he makes
  for its spot when his own legs say he must (up to 6 s ahead, out of a
  corner across the floor);
- stays on the floor: never off it to get round anybody.

The play's own runs into its spots (bringing it into a set, spreading out
off a board, moving on after a pass, out of the way of a drive) are his:
the run's end is his spot from the moment it starts, and he gets there
his own way.

The ten step together, a step at a time: each side sees the other as it
was a beat before (`loop.ts`), so a cut can read the man guarding him and
the man guarding him the cut.

Ratings: offensive IQ for how quick he reads it and how often he cuts on a
man who looks away; his three-point shooting for staying out on the arc;
size and position for where a big spaces (dunker spot, short corner,
elbow); his body (`body.ts`) for how he gets there.

Built as: 6a the ten stepped together, the facts, spacing (spot and
walking it); 6b drive, pass and move, reading his man.

Alex's call (2026-11-24): between their parts in a play, the men off the
ball work around the spots the set gives them (not spots of their own).
Real teams run sets and read off them, and the catches the engine decides
are laid on those spots.

Where it is now (real_11, the same measures as above; the defense's own
measures are where step 5 left them):

| | Before | Now | Real |
|---|---|---|---|
| Stood still (under 1.5 ft/s) | 52% | 32% | 19% |
| Walking, jogging, running | 31%, 13%, 5% | 47%, 16%, 5% | 47%, 27%, 7% |
| Feet covered a minute | 181 | 233 | 313 |
| The passer, 2 s after his pass: stays, relocates, cuts | 48%, 43%, 9% | 33%, 53%, 14% | 10%, 60%, 30% |
| Catching it on the move (median, ft/s) | 3.3 | 3.3 | 5.8 |
| A drive: of the four, how many move 3 ft in its first second | 1.3 (none, 28%) | 1.7 (none, 13%) | 2.3 (none, 4%) |
| Nearest teammate (median, ft) | 14.7 | 15.0 | 14.1 |
| Three or more out beyond the arc | 53% | 42% | 48% |
| Help defense stood still | 39% | 33% | 18% |

Still short:

- The rest of the standing is the play's own time: on a spot waiting for
  a catch it lays there, set for a screen, between its runs. Catches come
  stood because the play lays them that way; a man coming into the ball
  on the move belongs to the passer leading him (step 7) and the planner
  laying it (step 8).
- After a pass he cuts half as often as real players, and moves about half
  as far: the play's next part for him (a screen away, a catch back) is
  often only a second or two off.
- Jolts: a few more quick changes of pace per game than before (lurch
  flags 137 against 116, teleports the same 8), mostly where a man hands
  back to the play's run partway through it.

Built across 6a and 6b, then: the ten stepped as one; the play's spacing
runs (into a set, spreading out off a board, moving on after a pass, out
of the way of a drive) tagged as the set's so each man makes them his own
way; and, found on the way, the step 5 defense had quietly stopped
deciding once the offense was stepped in the same loop (a defender only
guards his man himself where his man has been stepped already) - fixed,
with a test that both ends decide for themselves (`staged.test.ts`).

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
