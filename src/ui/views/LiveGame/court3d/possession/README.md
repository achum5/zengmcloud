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
- `through.ts`: step 8a. The play's runs into a catch, a throw, a pull-up,
  a contest or a rebound laid on through it, rather than ended stood on its
  spot before the ball gets there (see Step 8a, Closeouts, Rebounds).
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
7. The ball handler (built).
   - Then, before 8, natural and smooth (Alex, 2026-11-24; built): men
     stand and walk like it away from the play (upright, a plain step or
     none for a small shift; the deep stance looks wrong), legs that move
     like a real player's rather than splayed, and nothing that hitches a
     frame (60 fps for everyone).
8. The planner (Alex's pick, 2026-11-24, before the rest of the
   packages): the play becomes something the ten run, not something they
   are dragged through.
   - 8a, checkpoints as windows: catches, passes, contests, rebounds,
     reaches and shots met at a pace a body allows (catches, passes,
     pull-ups, closeouts, rebounds, fouls, steals, blocks and loose balls
     built: see below).
   - 8b, calm decisions: a choice holds a moment; a target slides.
   - 8c, plays run live: the action, roles and coverage from the players
     on the floor.
   - 8d, transition: push or walk it up, lanes, a rim runner, a trailer.
   - 8e, team styles from the lineup's ratings.

   Alex's calls (2026-11-24): a catch may come a few feet from the play's
   spot along the catcher's own way in, and a catch or a pass 0.3 s either
   side of the play's moment (the shot exact on the clock). The shooter's
   move picks his spot inside the engine's zone, but a two is plainly a
   two in the picture (feet inside the arc) and a three plainly a three.
   Nothing seen gives the result away: a late closeout leans toward a
   make, but some contested shots go in and some open ones miss - the
   same for every mechanic.

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

## Step 7: the man with the ball

Where it starts (real_11 watched in whole possessions, against 17 real
games; a touch is from when he has it till it leaves his hands, in the half
court): stood still with it 62% of the time (real, 19%), and on the dribble
at 0.8 ft/s (real, 5.5). Of his time with it, the play lays its runs half
(mostly a dribble somewhere), something it pins him to 37% (facing up 8.5%,
passing about 10%), and nothing at all the rest: stood on its spot.

What pins the man with the ball: the play's actions with it (a drive,
coming off a screen, backing in: its runs), his shot (on the play's spot
exactly: the zone is the engine's), a handoff, and anything of theirs at
him the play pins (a swipe at the ball, a foul: on the play's spot for it).
Facing up, a jab or a fake, a move put through where he stands, calling
the play and his pass he does wherever he is.

Between those, he works his man himself, a step at a time
(`handler.ts`), within a few steps of the play's spot (Alex's call,
2026-11-24: the play's passes and shots leave from about where it lays
them):

- on the dribble he is never stood on it for long: across the floor along
  the arc to the side with room (on the way he is going, more often than
  not), a few hard dribbles in at his man by his weak side and back out of
  it, now and then sizing him up a beat where he is;
- reads his man: crowded, he gives ground first; given room, he attacks
  it;
- stays out of the paint on his own (in there is a drive, the play's) and
  off his teammates;
- with it in his hands (picked up, a move put through where he stands,
  passing it) his feet are where they are: a stride or two to gather,
  then his pivot, never walking with it; they go round him;
- is back on the play's spot by the time he stops dribbling when
  something of the play's comes before he is on it again (his shot, the
  play taking him on), since he can go nowhere till then;
- coming into a catch of his own in the front court (one nothing of the
  play's follows close behind), meets it: a few steps to the ball as it
  comes, rather than stood where the play has him.

The ball goes with him: going across one way and then the other, he
takes it over into the hand on the side he heads for (`stepper.ts`
handsWithHim).

Ratings: ball handling and speed for how often and how hard he attacks his
man and how quick across; a big works it less and sizes his man up more.

Where it is now (the same measures):

| | Before | Now | Real |
| --- | --- | --- | --- |
| Stood still with it (under 1.5 ft/s) | 62% | 45% | 19% |
| A long touch on the dribble, stood still | 59% | 34% | 18% |
| On the dribble, pace (median, ft/s) | 0.8 | 2.3 | 5.5 |
| How far about his spot he works it (median, ft) | 0.7 | 1.4 | 2.4 |
| Ground covered in a touch (median, ft) | 1.1 | 4.4 | 4.3 |
| Moving: across, in, back out | 56%, 34%, 10% | 56%, 30%, 14% | 55%, 33%, 12% |
| Changes of direction a second | 0.05 | 0.20 | 0.16 |
| First second after the catch (3 in 4 under, ft) | 0.6 | 1.8 | 5.2 |
| The man guarding the ball stood still | 35% | 23% | 16% |

Still short:

- Most catches are still laid stood by the play: he meets only those that
  are his own (16% of live catches). Coming into the ball on the move
  belongs to the planner laying it (step 8).
- The rest of his standing is the play's: a run it lays slow, and facing
  up where it has him.
- Jolts about where they were (teleport flags 14 against 13, lurches 185
  against 194): now and then a defender lunging the last few feet into a
  swipe, a foul or a contest the play pins him to.

## Step 8a: on the move through the ball

Real men catch a pass on the move and throw one on the move: 5.6 ft/s at
the catch and 3.9 at the throw, median, over the 17 games - a catch-and-
shoot about 4 as it comes into his hands, still 2 or 3 a few tenths later
as he goes up; a catch passed on or dribbled off hardly slowing at all.
The play laid every run into a catch or a throw as one that ended on its
spot, stood, before the ball got there: braked hard, then stood. That was
the commonest jolt, and the staged look.

`through.ts`, on the play's runs before they are stepped: each run into a
catch, and each run into a throw, is laid on through it.

- Off on another run straight after (an outlet dribbled up the floor, a
  drive off the catch, a cut after the pass), he goes on into it, handed on
  at the pace both runs allow - and the turn between them (a change of no
  more than 4 ft/s in the way he is going: turning back, he plants).
- Otherwise he is still going as the ball comes or goes (no faster than
  he can stop from: 11 ft/s a second), and stopped on the play's spot by
  the time he has to be stood: up into his shot (a moment before he leaves
  the floor), a move with his feet set, his next run. A pass on from him,
  or anything else his legs run on under, he does on the way.
- He catches it a step short of the play's spot, along his own way in; the
  ball flies to his hands wherever they are. Whatever he does after is
  from the play's spot, as before.

The man with the ball makes for the spot of his pick-up going as the play
has him going then (`handler.ts` home): a pass off the dribble leaves on
the move, the stride or two of his gather under it.

A jumper off the dribble, a pull-up, is gathered into the same way
(`throughPullUps`): still going as he picks it up, stopped on his spot as
he goes up. A hop into it the way he was going is one run with the run in.
Before, he stood from 0.7 s before it left his hands; now 2.9 ft/s 0.6 s
before, 1.6 at 0.5, stopped by 0.4 (real: 5.7 at 0.6 s, 4.8 at 0.4, 2.7 as
it leaves his hands).

| Measure (real_11, whole possessions) | Before | Now | Real |
| --- | --- | --- | --- |
| Pace at the catch (median, ft/s) | 0.5 | 3.6 | 5.6 |
| ... a catch-and-shoot, as it comes into his hands | 0.2 | 3.4 | 3.9 |
| Stood for the catch (under 1.5 ft/s; passes in play, as staged) | 69% | 17% | about 10% |
| The passer as it leaves his hands (median, ft/s) | 0.2 | 1.4 | 3.9 |
| ... off the dribble (as staged) | 0.5 | 4.0 | |
| Sudden starts and stops (lurch flags) | 185 | 162 | old sim 16 |
| Jumps from one spot to another (teleport flags) | 14 | 10 | |

Still short:

- A man stood a long while for a pass (most of a second and more) still
  takes it stood; a step to meet it is a choice of his (8b/8c).
- Passes held a second and more are thrown stood, as the man with the ball
  holds it now (how long he holds it is 8c's).
- A catch-and-shoot comes to a stop a few tenths sooner than a real one
  (stopped 0.2 s after the catch, real 0.6).
- Real pull-up shooters go up still going and drift in the air; the play's
  jump goes up from a stand, so he is stopped 0.4 s before it leaves his
  hands. That is the shot's own (how it is laid), later.

## Closeouts (8a)

Real defenders are still coming as a jumper leaves the shooter's hands:
the nearest one going 6.5 ft/s, median (7.9 off a catch he shot within a
second, 5.6 off one he held), on 92 shots in 100 faster than 2 ft/s,
about 25 to 35 degrees off straight at him, whether he is 2 ft off him or
6 (how near he is hardly changes it) - still about 5.8 four tenths later,
and coming no nearer him than about 2 ft (the 17 games, 1,504 jumpers
from 10 to 30 ft). The play laid every closeout as a run that ended on
the spot giving the shooter the room it means him to have, stood there -
often half a second and more before the shot - and the contest's jump
straight up from it; or had him on that spot all along; or still on his
way to it as he went up, stopping in the air.

`through.ts` (`throughContests`), on the play's runs before they are
stepped: the run in to a contest (his choppy steps and all, back as far as
it goes the one way, 1.6 s at the most) is laid on through it.

- On the play's spot as it leaves the shooter's hands, as before: the room
  the play gives him (`ROOM_FT`, leaning made or missed as the league's
  tracking has it) is the same.
- Still coming, at a pace drawn for each man from the real spread (off a
  quick catch or one held; never leaning made or missed), as fast as his
  way in allows: from stood, he sets off as late as gets him there that
  fast; drifting in too near the spot for it, he pulls up first and goes
  from there. Up into the air at that pace and on at it in the air,
  braking once he is down (15 ft/s a second) - or on into his next run.
- Stood on the spot long before it (a man who holds it a while): stood a
  step or two further off it (as far as he needs to get going that fast,
  1.5 to 4.5 ft), at an angle off straight at the shooter drawn from the
  real spread - or back along the way he came, if he came onto it on the
  move - and in through it.
- Still on his way to it as he goes up: in from the run he is on, on it
  where the play had him as it leaves the shooter's hands.
- Never nearer the shooter than 1.5 ft: coming straight at him, he bends
  by him as he goes up (no more of a change than 4 ft/s), on the side he
  is off to. He comes down inside the lines, stopped by anything of his
  that wants him stood (a box-out). The play's next run of his waits till
  he is down and slowed for it (all of it later, if he is only stood after
  it), and is up to 3 ft/s harder than it was (a real man flies on by
  and has further to come back); stood once he is down, he may set off on
  it from there.
- Only a closeout the play has him make: not a hand up after it is long
  gone (going up more than 0.4 s after it leaves the shooter's hands).

The new sim's defender, guarding his man, commits to the play's spot for
the contest ahead of it (see `loop.ts`) and, the box-out or the boards
coming straight after, is the play's through it.

| Measure (real_11, whole possessions, jumpers 10-30 ft) | Before | First pass | Now | Real |
| --- | --- | --- | --- | --- |
| Nearest defender's pace as it leaves his hands (median, ft/s) | 2.6 | 3.4 | 4.6 | 6.5 |
| ... the slowest quarter of them, at most | 0.1 | 1.7 | 2.9 | 4.0 |
| ... off a catch he shot within a second | | 4.2 | 6.4 | 7.9 |
| ... on a man who had it 2 s and more | 1.6 | 2.7 | 3.9 | 5.6 |
| ... four tenths of a second later | | 2.3 | 4.7 | 5.8 |
| How far off him then (median, ft) | 4.2 | 4.3 | 4.4 | 4.7 |
| Sudden starts and stops (lurch flags) | 163 | 162 | 144 | |
| Jumps from one spot to another (teleport flags) | 10 | 10 | 8 | |

(First pass: the run in laid on through it at as fast as it allowed, 8
ft/s at the most. The lurch and teleport flags since include the
rebounds, below: 147 and 8 before this.)

How open a three is, in the new sim: the play's odds for its room (the
league's tracking, `ROOM_MADE_3` / `ROOM_MISSED_3`: nobody within 6 ft
of about half of them) are measured from before it is away, his man
still on his way out. As it leaves his hands, the 17 games have 6 ft and
more off 32 threes in 100, and 8.6 ft and more off 10. The new sim draws
a three's room from `ROOM_MADE_3_LIVE` / `ROOM_MISSED_3_LIVE` instead
(`director.ts`): a step late far more often than left all alone, still
leaning made or missed. Two whole games (real_11 and real_22):

| Threes, nearest defender as it leaves his hands | Before | Now | Real |
| --- | --- | --- | --- |
| 6 ft and more off him (both games) | 38% | 32% | 32% |
| How far off him (median, ft: real_11, real_22) | 5.5, 3.8 | 4.9, 4.7 | 5.1 |
| Lurch flags (real_11, real_22) | 144, 192 | 186, 154 | |
| Teleport flags (real_11, real_22) | 8, 24 | 13, 14 | |

(The lurch flags move with the whole game's timeline: a change to how a
shot is staged shifts everything after it, and real_11's count is the
one each fix so far was checked on. Taken over both games, even.)

Still short:

- A man who holds it a while: the play has his defender on his spot long
  before the shot, and he comes in from a step or two off it; real
  defenders guard a man holding it live, on the move with him - 8c.
- Too many threes have a man right on the shooter: under 3 ft off a
  quarter of ours, a tenth of the real ones under 3.4. A man already
  tighter than the room the play gives the shot stays there.

## Rebounds (8a)

A real man who comes down with a rebound is on the move through it: at
his own end 5.8 ft/s as it comes into his hands, at the other 4.5
(median, the 17 games), and stood half a second or more in the second
and a half before it only one time in twelve. The play laid his run to
the spot he takes it at as one that ended there, and his jump straight up
from it: stood for it more than half the time.

`through.ts` (`throughBoards`), on the play's runs before they are
stepped, lays it on through, the same way as a closeout (`upThrough`):

- On the play's spot as the ball comes to his hands (the top of his jump),
  so the ball still meets him there; still going, into the air at that
  pace and on at it, braking once he is down, or on into what he does
  next.
- His way in is the play's run there, the shuffle onto the spot at the end
  of it, and any stop on the way, back as far as 2.5 s before he goes up;
  not across a box-out, a contest or his own shot (straight up off those,
  as before).
- How fast he goes up is each man's own draw from the real spread (its
  tenth to nine tenths 1.5 to 8.9 ft/s at the other end, 1.9 to 12.8 at
  his own), as fast as his way in and the floor allow. From stood he sets
  off as late as gets him there that fast, not at a walk all the way.

A man stepped by the new sim's loop lost the first beat of a stretch of
his when it started on a rounding (at a change of possession, mostly): he
stood a beat and jumped. That beat is his again (`ticksOf`, `stepInto`).

| Measure (real_11, whole possessions) | Before | Now | Real |
| --- | --- | --- | --- |
| Pace as it comes into his hands, at his own end (median, ft/s) | 0.0 | 3.0 | 5.8 |
| ... at the other end | 0.0 | 0.0 | 4.5 |
| Stood half a second or more in the 1.5 s before | 57% | 36% | 8% |
| Sudden starts and stops (lurch flags) | 162 | 147 | |
| Jumps from one spot to another (teleport flags) | 10 | 8 | |

Still short: a miss comes down right on the man the line says gets it
(the play aims it at him), so a defensive rebounder, often stood right
there, has no way in to lay. Real misses come down in nearer the rim than
he was as the shot went up, and off to a side of him; aiming them that
way changes the whole game after (the rebound comes later), so it waits
on a tape that shows what that does to everything else.

## Fouls, steals, blocks and loose balls (8a)

The play sends a man at the ball, or at the man with it (a foul, a reach
in for a steal, a block, a loose ball he grabs), as late as the old sim's
legs get him there: from stood at more of a run than a body has, or
turned straight back the way he was going. Moved by his own body he falls
a few feet behind and is yanked onto the spot in the last half second.
And a loose ball he ran in for and stopped dead to grab.

`through.ts` (`throughReaches`), on the play's runs before they are
stepped:

- His run in is set off as soon as getting there takes a man going at a
  good run (pushing up to about 16 ft/s, not flat out), from where the
  play had him then, up to 1.5 s sooner; pulling up first if he was going
  the other way. Not across anything else of his that has him stood.
- A loose ball is taken on the move, as a catch is (`carryThrough`).

| Measure (real_11 + real_22, whole possessions) | Before | Now |
| --- | --- | --- |
| Jolts going for a foul, steal, block or loose ball (moments) | 27 | 14 |
| Sudden starts and stops (lurch flags) | 340 | 314 |
| Jumps from one spot to another (teleport flags) | 27 | 18 |

Still short: a man the play has guarding somebody else, a long way off,
when the shot he blocks goes up (a three, say), or getting back flat out
and asked to stop dead and come straight back out: no body gets there in
the time. Where the ten are as it comes is for the plays run live (8c).

## Natural and smooth (before step 8)

Alex, on film of step 7: slightly choppy; men who should be easy about it
sliding their feet hard; stood in their stance it looks unrealistic, and
their legs have a spider look - they don't look natural moving about.

What it was, measured over a whole game (real_11):

- Every defender drawn in his stance whatever he was doing: off the ball,
  81% of the time stood, 89% moving slowly, 43% moving - the deep crouch,
  feet 2.5 ft apart, arms out. Real defenders off the ball are up on their
  feet, watching.
- Steps too long for the figure's legs. The figure's legs are 45% of his
  height, a real man's 53%: stepped as a real man his height steps, his
  feet were planted out past where his legs reach, his hips sank to get to
  them, his thighs split wide - running, as far apart as 119 degrees (a
  real runner's 70-80), sprinting 141 (90-100).
- A swinging foot carried on a line under him, lifted: the thigh flung
  forward.
- His steps planned 2.5 s at a time, all at once when they ran out: now
  and then a frame that took that much longer.

What changed:

1. Up on his feet unless it is on him (`defense.ts`): in his stance only on
   the ball, helping, planted for a charge, committed to a pin, or within
   12 ft of a drive. Otherwise walking, stood up ("help": weight shifting,
   hands loose), walking backward upright (`backWalk`), side-stepping. His
   stance, when he is in it, 62% of the full crouch (one held through a
   possession, not a drill's), feet a little nearer.
2. Steps that fit his legs (`evaluate.ts` stepInchesOf): planned as a real
   man's whose legs are as long as the figure's - a little quicker, each a
   little shorter at the pace.
3. Up over his toes (`poses.ts` reachFeet): his heel up from about halfway
   through a walking step, his ankle up and forward over his toes, the toes
   staying put; a foot behind him past his reach rolls further up onto
   them rather than his hips sinking.
4. A swinging leg as a runner's (`evaluate.ts` ownSwing): mid-swing, his
   stride's own (fit to motion capture: heel folded up behind him, knee
   driven through), from where it left the floor and back to where it
   comes down.
5. Walking, a foot down half a step out in front of him, as running.
6. His steps kept 2.5 s ahead a step at a time, as the game plays.

| Measure (whole game, new sim) | Before | Now | Old sim | Real |
| --- | --- | --- | --- | --- |
| Defender off the ball in his stance: stood | 81% | 10% | - | up on his feet |
| ... moving slowly | 89% | 18% | - | |
| ... moving | 43% | 12% | - | |
| Thighs apart at most (p90, deg): walking 4-7 ft/s | 93 | 67 | 53 | 40-45 |
| ... jogging 7-11 ft/s | 97 | 60 | 61 | 55-65 |
| ... running 11-16 ft/s | 119 | 77 | 71 | 70-80 |
| ... sprinting | 141 | 94 | 99 | 90-100 |
| Hips at their lowest running (p10, of stood) | 0.56 | 0.65 | 0.74 | about 0.9 |

A frame's work with the new sim on is the old one's: as many new player
pictures a frame (0.80 a man, against 0.84), and its own part (his steps,
his legs to them) about 1% of the frame. Most of any frame, on or off, is
making the players' pictures.

Still short:

- Sudden starts and stops: 185 a game (a change of 8 ft/s or more in a
  tenth of a second) against the old sim's 16. They come where the play
  pins a man to a spot at a moment he can't reach smoothly: into a catch
  (38), a contest (38), a hack, a box-out or a screen (34), and 44 with
  nothing pinned close. The planner laying those (step 8) is where they go.
- Walking, the back leg still trails further than a real man's at starts
  and stops.

(The legs measured off his skeleton as drawn, every tenth of a second,
by pace; the stances off the tape's anims; a frame's work with Chrome's
profiler on the live page.)

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
