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
     on the floor (the ball moved round before the set built: see Moving
     it round; runs eased in and out: see Easing in and out; a shot up
     played by all ten: see A shot up).
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

## Calm decisions (8b)

Jolts with nothing pinned - a man not at the ball, not catching, not set
for anything - were about 23 a game, and two thirds of them were men
running into one another: a defender at 16-19 ft/s comes within a step of
a man (his own man, say, ahead of him) and the keep-apart (`moveOn`)
stops him dead in one step. The rest were men shoved aside in a blink, a
step's worth of floor in a tenth of a second with no step taken.

`defense.ts` (`moveOn`):

- A man in his way within 12 ft is braked for in time: no faster at him
  than he could still pull up from before he is on him, slowed no harder
  than he brakes - only for his own going at him, less the man's going on
  away (a man coming at him gets out of the way himself, see `giveWay`).
- Kept off a man by how fast they close, not how fast he goes: on the
  heels of a man running the same way he keeps up, not stopped dead.
- Stepped aside no quicker than he gets going, from however quick he was
  going off the man already, his pace going with it.

| Measure (real_11 + real_22, whole possessions) | Before | Now |
| --- | --- | --- |
| Jolts with nothing pinned (moments) | 46 | 10 |
| Sudden starts and stops (lurch flags) | 314 | 278 |
| Jumps from one spot to another (teleport flags) | 18 | 16 |
| Stood still at the wrong time (statue, frozen) | 42 | 42 |

A choice holding a moment. Counted over the first ten minutes of real_11
(every man, every 50 ms - `zz_body`, not kept), a man's legs and body went
from one way of going to another and straight back inside 0.15 s 760
times: standing, a step, standing; walking, a backpedal, walking; up out
of his stance and down again. Most of it was an inch here and an inch
there - a man creeping after a spot that moves a hair, each inch laid as a
step - and a backpedal decided afresh on every few inches of run.

- Creeping laid as stood (`toMoves`, `stepper.ts`, the new sim's men
  only): no quicker than 1 ft/s for 0.4 s or more, getting no further than
  0.4 ft/s over it and a foot at most, is one run from still to still - the
  way the court reads a man stood.
- A backpedal only when he is turned for a good moment (`shownSteps`,
  `evaluate.ts`): turned from the way he goes for 0.2 s either side,
  against the way he goes at each of those moments - not a corner rounded
  from one run to the next. A run shorter than 0.2 s goes the way the
  longer of the runs either side of it does; one decided going backward is
  judged on the way he goes over 0.3 s either side of it.
- Walking backward, he walks backward up on his feet, as a run laid going
  backward does - not down in a backpedal for a frame.
- Stood, all through a crept run (`stoodSpan`, `evaluate.ts`): what a man
  does stood - calling for it, pointing out his man, a swipe at the ball,
  hands on his hips at a stoppage - goes on through it, not held off till
  it ends. (Without it, a man laid creeping for ten seconds stood those ten
  seconds stock still: real_22's stock-still count went 19 to 26.)

| Measure (real_11, first 10 minutes) | Before | Now |
| --- | --- | --- |
| Flicks back inside 0.15 s | 760 | 438 |
| ... a step, a stance or a backpedal between two of the same kind | 389 | 123 |

What is left of them is mostly the moment going into or out of something
of his - a catch, a screen, a box-out - a beat early or late (8c). The
full-game flags on real_11 and real_22 are the same as before it (lurch
278, teleport 16, statue and frozen 42), and so is the time men stand: 22%
of the time going slower than 1.5 ft/s over half a second.

Tried and dropped: a man stood within half a foot of a spot that is not
going anywhere staying stood (a hold in `moveOn`). It took nothing more
off the flicks (432 with it, 438 without) and had men off their spots
when a screen or a fight for the ball pinned them there - four more
teleports on real_11.

## Bodies that skip (after 8b)

Alex, on the 8b film (clip 2, a man jogging off the ball): it still feels
skippy and glitchy - weird movements.

What it was, frame by frame: at every footfall his hips dropped 6 to 8
inches in one frame (0.57-0.68 ft in 33 ms), his knees down near the
floor, then climbed back over the next four. Three things:

- His foot came down about 2 ft out in front of his hips, past where the
  figure's legs reach at a jog - his hips let down all at once, the moment
  it touched, to get to it. A step taken speeding up was planned as a
  walking one (the pace as the last foot came down: under 6 ft/s), so a
  walk's long time down, out in front as far as a walk's; and a braking
  step went out as far as 2.3 ft.
- In the air between steps his hips went down, not up: his stride
  picture's lower foot put on the floor, and only 40% of its height given
  back. And as one picture leg or the other was the lower, his hips jumped.
- Walking, a foot that would have stepped across the other stayed down
  while the other stepped again - and again: left planted 3-4 ft behind
  him, his hips sunk as far as they go to reach it.

What changed:

1. Walking or running by the pace he goes as the step comes down, when he
   is speeding up (`footsteps.ts` RUN_FROM).
2. No foot down further out from under him than about half his leg
   (REACH_MOST 2.3 -> 1.7 ft for a man 6 ft 6 in), however it came to be
   put there - and sliding, his feet at least half a foot apart.
3. His hips let down toward a foot over the last half of its swing as it
   comes down, and back up over the first half after one leaves the floor
   (`poses.ts` reachFeet), not all at once.
4. Stepping on his own feet, his hips ride as his stride has them - its
   own rise and fall, lowest over the foot down running, highest walking
   (`poses.ts` strideHips, `evaluate.ts` ridingOf) - not wherever its
   picture's lower foot puts them.
5. A foot kept down while the other steps is down only till he is past it.

| Hips (first 10 min, every man, every frame) | real_11 before | now | real_22 before | now |
| --- | --- | --- | --- | --- |
| Up or down over 1 in. in a frame (a man-minute) | 158 | 51 | 150 | 52 |
| ... over 2.4 in. | 64 | 13 | 62 | 13 |
| ... over 4.2 in. | 24 | 2.4 | 24 | 2.5 |
| Running with a knee down under 14 in. | 8.6% | 0.3% | 8.8% | 0.3% |
| Running, hips at their lowest (p10, of stood) | 0.86 | 0.91 | 0.86 | 0.90 |

(A real runner's hips go up and down 3 inches or so a step - well under an
inch a frame. Switch off unchanged: the same hash.)

Still short: what is left is mostly a jump or a quick move taking his legs
over from his steps, and a stop with both feet down far apart.

## Moving it round (8c, first piece)

Real teams pass it 2.6 times a trip and a man has it 1.4 s at the median
(17 games of tracking). Ours brought it up and stood dribbling at the top
while the set waited: 7.6 passes a live minute against a real 11.3, held
2.5 s at the median.

`director.ts` (`moveIt`, the new sim only - switched off, `sizeUp` as
before):

- Brought up with time on the clock, the ball goes round before the set:
  out to a wing, on round the arc, up to a big at the elbow, and back to
  the man the set starts with. How many passes goes by the clock the trip
  took (`MOVE_PASSES`), so with the set's own and the shot's a trip has
  about as many as a real one.
- Who it goes to (`nextFor`): a man out of the paint a pass away (8 to 30
  ft), the nearer the likelier, not straight back to the man who gave it
  him, nor one the play has busy.
- Each man has it a moment: straight off the catch (0.25-0.6 s) or a
  dribble or two first (0.7-1.5 s). Now and then he takes it a few
  dribbles before it goes on (`carryIt`): in at his man, or on round the
  arc toward the man it goes to next, his man going with him - a better
  handler more often.
- Now and then the man with it calls the set first, a hand up.

`possession/handler.ts`: a catch he holds on to carries him on a step or
two, not stopped dead on it (real: caught at 4.2 ft/s, still going 2.5
half a second on, 2 ft gone in 0.7 s; ours had gone half a foot). A move
of his own he could not be back from in time for his pick-up is cut down
to one he could, rather than dropped for standing on the spot.

`possession/loop.ts` (`pickUpFor`): a blink of dribbling between two moves
of his with the ball - a hesitation, then the ball in his hands to throw
it - is one spell, not a chance to go somewhere. (Taking it as the end of
one, a man dribbled 7 ft off where the play has him throw it, and was
pulled back the 7 ft in a fifth of a second to throw it.)

| Measure (real_11 + real_22, whole games) | Real | Before | Now |
| --- | --- | --- | --- |
| Passes a live minute | 11.3 | 7.6 | 9.5 |
| Ball held, median (s) | 1.4 | 2.5 | 2.2 |
| The man with it stood | 15% | 34% | 32% |
| Flags (lurch, teleport, corner, statue, frozen and the rest) | | 362 | 324 |

Still short: a man has it 3 s on a set's step with a quarter of a pass
between (the set's own moments, 8c-2); he stands twice as much as a real
one, most of all the first second after a catch (46% of it, real 18%) and
the last half second before it goes (45%, real 17%); and everybody sets off
from a standstill twice as often as real men do (81 a live minute, real
41), mostly short hops of 4 to 15 ft from stood to stood, which is most of
why they set off together (44% within 0.1 s of another man, real 26%).

## Easing in and out (stop-and-go, first piece)

Real men get going and pull up gently: nine times in ten under 8.9 ft/s²
speeding up and 8.3 slowing down (17 games of tracking). Ours went 12.0 and
12.5, and the tape flagged a lurch (a stride's worth of jolt) about 120
times a game.

- `stepper.ts` (`toMoves`), `motion.ts` (`evenShape`), `evaluate.ts`,
  `director.ts` (`shareAlong`): a run cut from a new-sim man's own steps is
  marked `steady` and shown the way his steps went, his pace going from
  what it was at one end to what it is at the other. Before, every one was
  replayed the court's way (`runShape`: going as hard as a man can, on at a
  pace, pulled up hard), so two or three feet over a second was a burst and
  a stop; and a stood-to-stood step was laid as one such run (`wholeRun`,
  not done for them now). The old sim is as it was.
- `follow.ts`: a man the play pulls along builds the pull over most of a
  second, not a sixth (`PULL_BUILD` 15 to 7); before something of his he
  starts closing on the play 1.3 s ahead, not 1, and eases the last of it
  in over 0.8 s, not 0.5 - each step taking its share of what is left, not
  most of it in the last few steps.
- `defense.ts`: off the ball a defender goes about it at 0.6 of what he
  has, not 0.8 (`OFF_EFFORT`): shifting with the ball, not bursting from
  spot to spot.

| Measure (real_11 + real_22, whole games) | Real | Before | Now |
| --- | --- | --- | --- |
| Speeding up, 90th percentile (ft/s²) | 8.9 | 12.0 | 11.1 |
| Slowing down, 90th percentile (ft/s²) | 8.3 | 12.5 | 11.3 |
| Lurch flags | | 242 | 133 |
| All flags (lurch, teleport, corner, statue, frozen and the rest) | | 324 | 210 |
| Set off from a standstill, a live minute | 41.5 | 84 | 95 |

Set-offs went up because the old way of showing runs hid some of the
stopping: counted on the sim's own steps, they come out the same as now.
The stopping and starting again is the sim's own, twice as often as real
men - short stood-to-stood hops by men following the play (a third of
them) and off-ball defenders (another third). Closing onto the play just
before something of his (a pinned act) is still the sharpest moment (90th
percentile over 30 ft/s²): the play's own runs, laid the court's way,
come through it. New: 4 moonwalk flags in the two games (a man pulled
along after the play, eyes on the ball, going away from it at a jog).

## No flinching into the stance (fix list, first item)

Alex's clip: a man off the ball stood up straight, then all at once down in
a defender's slide, arms thrown out wide, feet spread and trunk tipped,
and a moment later snapped back upright. Measured over the first ten
minutes of real_11 (100 man-minutes), a man went down into his stance
(slide, shuffle, backpedal) for less than 0.7 s between spells up on his
feet 1.2 times a man-minute. Three causes, all in how the court shows the
new sim's men (`evaluate.ts`, gated on their own gaits; the old sim as it
was):

- A man eased a step aside off another (keep-apart) was drawn in a
  defender's slide, whoever he was. Now he steps over up on his feet
  (`sideStep`); only the man up on the ball slides.
- A run below a slide's pace near the ball, or a defender's run slowing
  for a moment between two quicker ones, was a slide for that run alone
  (`heldPace`): down for a moment between runs up on his feet, he stays up
  through it, at his pace (`STANCE_LEAST`, 700 ms), turned the way he goes
  as far as running lets him, eyes on the ball.
- Going backward at no more than a walk's pace off a jog (`BACK_WALK_FTPS`,
  8.5 ft/s), he walks backward up on his feet, not down in a backpedal.

And into his stance off a run, or up out of it, he goes over a third of a
second (`STANCE_MS`, 320 ms), not a sixth. `stepper.ts` (`heldStance`)
does the same for the slides the sim itself asks for: a moment's slide
between runs is run.

| Measure (real_11, first 10 minutes) | Before | Now |
| --- | --- | --- |
| Down in his stance under 0.7 s between spells up, a man-minute | 1.22 | 0.71 |
| Frames a hand flies past 10 ft/s against his chest, a man-minute | 26.8 | 21.6 |
| His hips dropping or rising 0.35 ft in 0.1 s, a man-minute | 9.6 | 6.5 |

Whole-game tape flags (real_11 + real_22) are the same as before (210):
this changes how men look, not where they go. Still there: the slide's own
arms, both out wide of a man up on the ball, read as a plane's wings from
in front; the brief drops left are mostly a defender slowing near the ball
between two runs.

## Hands in the slide (fix list, first item, the rest)

What was left of Alex's clip: sliding with his man, stepping up or giving
ground in his stance, a defender held both arms out to his sides at about
level - from in front, a plane's wings. Counted over the first ten minutes
of real_11 from in front (an arm reaching out past 0.6 ft to his side, no
more than 30 degrees below level, both at once): 78% of the slide's frames,
60% of stepping up, 51% of giving ground; 7% of every frame of every man.
Real defenders on the ball carry their hands low and working, bent at the
elbow: one down at the ball about the hip, the other up in front in the
way of a pass, or both down and out at the hips.

For the new sim's men (`poses.ts` `slideHandsOwn`, those with a stance
depth of their own; the old sim's look as it was), the slide, stepping up
and giving ground carry the hands that way: his lead hand down and out at
his hip, the other up in front of him at about his shoulder - or, for the
men who carry both hands out on the ball (`GUARD_HANDS`), both down and out
at the hips - with a little of the slide's working of them kept. Those
frames now: none (the wide arms left are boxing out and bumping, as real).

## The man with the ball on the move (holder standing)

Where it started (real_11 and real_22 watched in whole possessions, against
17 real games): in his hands after a catch he had stopped by 0.4 s (real
men are still going 2.7 ft/s then, 2.1 at 0.6 s); on the dribble 4.1 ft/s,
median, and stood 18% of it (real: 6.6, 8%); stood with the ball 32% of
the time he had it (real: 15%). Causes:

- A pass caught and held on to was gathered as a stride or two to stop,
  450 ms at 11 ft/s a second (`through.ts`), and the handler's own gather
  settled onto its spot rather than easing into it.
- The play's own dribble runs - about 3 ft over half a second each, a stop
  between - pinned him, chopping his own stretches to under a second.
- His own moves ended stood on their spot before the next, and the
  director walked him through little dribble steps of its own in the front
  court.

Now:

- A pass he catches and holds on to is eased to a stop over up to 1.2 s
  (`HELD_GATHER_MOST`, `GATHER_EASE`); in his hands his gather is paced by
  how far he has left to go (`GATHER`, `GATHER_LAG`).
- The play's dribble runs shorter than 6 ft are his own to work
  (`loop.ts` `WORKED_MOST`); its drives, screens and post-ups still pin
  him. For the new sim the director no longer walks him about in the front
  court (`keepDribbling`).
- His moves flow: still going as he gets to one (`FLOW`), done with it
  2.5 ft out (`THERE_GOING`), across and in a little farther and quicker,
  sizing his man up less often.
- Putting a catch on the floor, his first move goes somewhere: at his man
  far more often (`OFF_CATCH_*`), never stood sizing him up.
- The play taking him back with nothing of his own first: he is about on
  its spot by then (`END_LEAD`, `END_SPARE`), handed back 0.6 ft off it,
  median, not 2.3 ft and snatched onto it (the first try, without this,
  had 202 jolt flags).

| Measure (both games) | Before | Now | Real |
| --- | --- | --- | --- |
| On the dribble, pace (median, ft/s) | 4.1 | 4.6 | 6.6 |
| On the dribble, stood | 18% | 12% | 8% |
| Stood with the ball, all the time he has it | 32% | 29% | 15% |
| ... the middle of a touch (not its first 1 s or last 1.5 s) | 24% | 18% | |
| In his hands after a catch, 0.2 / 0.4 / 0.6 s on (ft/s) | 2.3 / 0.7 / 0.8 | 2.5 / 1.4 / 0.6 | 3.5 / 2.7 / 2.1 |
| A catch put on the floor: its first second (ft) | 3.2 | 3.5 | 7.1 |
| Tape flags: lurches, teleports | 133, 16 | 138, 14 | |

Still short: in his hands after a catch he still stops too soon - most
catches go straight into something the play lays where it has him (facing
up, a pass), stood for it; off the catch he goes in at his man half as far
as real men; and before each thing of the play's he makes back for its
spot (a few steps' leash, Alex's call), about half his time on the
dribble.

## A shot up (stop-and-go, second piece)

From a shot going up till 1.5 s after it comes down, real men are on the
move: the offense stood 11% of that time, the defense 13% (17 games of
tracking; a sixth of the live half court). Ours stood 32% and 41% (both
games taped whole). Two causes:

- `stepper.ts` took every ball out of a man's hands to a spot as a shot,
  and through a shot the new sim's men play the play's plan, none working
  his own way: a bounce pass too (down to the floor, up off it to the
  man). On each, everybody off the ball stood on the play's spot for two
  seconds - about two bounce passes a minute.
- The play's plan for a shot (`director.ts`, `crashTheGlass`) had the men
  going to the glass go well before it came down, but the rest only from
  1.5 s before it did - a dash back a few steps and stood watching it -
  and the men with nothing in it stood where they were.

Now, the new sim only:

- A bounce pass is a pass: played by every man as he plays one
  (`stepper.ts`), the ball on its way to the man it is bounced to from the
  throw (`loop.ts`, `goingTo`), not lost on the floor between. (The first
  try without that had a man meeting a bounce pass snatched onto the
  play's spot for the catch: jumps of 4 ft in a tenth of a second.)
- Everybody plays the shot as it goes up. A man getting back goes back
  the whole time it is up and a little after (`SHOT_BACK_ON`) at an easy
  pace, his own draw of 4 to 7 ft/s (`SHOT_BACK_PACE`), walking it or
  backing off facing it, no further than 20 ft - not a dash and a stand.
- A man with nothing yet to do about it - the shooter come down from it,
  one left out of the crash - is not stood watching it either: on offense
  he heads back the same way (a big in close follows it in a step or two,
  `SHOT_FOLLOW_IN`); on defense he goes in to where a long one comes down,
  there as it does. A defender with nobody to box out goes in at an easy
  pace if he has the time, not a dash in and stood.

| Measure (real_11 + real_22, whole games) | Real | Before | Now |
| --- | --- | --- | --- |
| A shot up: offense stood | 11% | 32% | 22% |
| A shot up: defense stood | 13% | 41% | 32% |
| ... its first half second, offense / defense | 16% / 13% | 36% / 39% | 34% / 33% |
| ... after it comes down, offense / defense | 8% / 12% | 34% / 43% | 17% / 32% |
| Half court, off the ball on offense stood | 17.0% | 28.8% | 24.5% |
| Half court, the man with the ball stood | 15.2% | 31.9% | 29.7% |
| Half court, defense stood | 16.6% | 28.6% | 26.4% |
| Set off from standing, a live minute | 41.5 | 93.7 | 95.4 |
| Tape flags: lurch, teleport | | 138, 14 | 126, 20 |

The teleport flags up from 14 to 20 are plays the director lays faster
than a body goes (a loose ball picked up, a heave at the buzzer, a swipe
at the ball on the break), the same kinds as before, in other trips: the
game goes another way after the first shot, the plan drawing its own luck
for each man's way back.

Still short: the first half second (men stood till their way in or back
starts, a beat after the shot leaves his hand), and the defense after it
comes down - stood boxed out once it is down, and round the man who has
it till the trip the other way starts, once he is down with it. Starting
the rest off as soon as the board is had was tried (`develop`): it moved
them a tenth or two of a second, nothing in the numbers. And men set off
from standing no less often: most of that is the play's own runs, a dash
and a stand each (the players timing the play's actions, next).

## Kept off each other (sharp turns)

The tapes flag a man turning more than 40 degrees in a tenth of a second
at a run - more than any body turns. Of the 12 over two whole games, 11
were the last thing the director does to everybody's runs (`keepOff`,
`director.ts`): two whose runs go through each other kept apart by
pushing one aside, at up to 14 ft/s, over a third of a second. Two coming
together running the same way - a trailer on the break merging onto the
line of the man bringing it up - were pushed 5 ft to one side of him, then
as his run came out the other side of the dribbler's, swung 8 ft over to
that side, through him.

Now, the new sim only:

- The further over a man has to go, the sooner he starts over and the
  longer he takes about it: never pushed aside harder than 12 ft/s each
  second (`SEP_ACCEL`) - he bends his run round the man, not knocked off
  it. (No wider than up to anything he does, either side: a screen, a
  charge taken, is where it was.)
- Two going the same way stay on the sides of each other they came
  together on (`SIDE_SAME`): the one on the move goes on behind the
  other, not over through him. Not when his way goes right across the
  other's (more than `SIDE_PAST`, 3 ft, through him): kept on his side
  that long, he would go 12 ft out of his way.

| Measure (real_11 + real_22, whole games) | Before | Now |
| --- | --- | --- |
| Tape flags: sharp turns at a run | 12 | 6 |
| Tape flags: lurch, teleport | 126, 20 | 124, 20 |
| Kept off a man at over 10 ft/s, times | 28 | 24 |

Five of the 6 left are runs laid right across each other - a lane runner
on the break through the man bringing it up, two lanes crossing, five
coming on for five off the scorer's table together - which no push aside
fixes: those runs are the play's to lay apart (next). The sixth is a man
backing off turning to contest a shot.

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
