# 3D plays, one play-by-play line at a time

Every line the basketball live sim can show, and how the 3D court plays it.
One line is worked at a time: list every way it can really happen, make the
3D court able to show each of them, check that it looks right every time
(the game tape's checks, tests, frames), and check that over a game it
doesn't feel like the same clip again.

Status: `todo`, `working`, `done` (with the commits). Per game: how often a
line comes up in a real-sim game (three games, seeds 11/22/33).

## The goal

Thousands of different ways every line can play out - each one real - and
which of them happens decided by the ratings of the men on the floor; and
for any one player, or group of players with the same ratings, still
thousands, so somebody who watches one team all season never sees the same
thing twice and it never feels scripted.

Not thousands of clips: each line is put together from parts that vary on
their own (how he got the ball, where, his footwork, the defense, how it
ends, what everybody else does), and the ways multiply. Each part is:

1. **Real every time** - checked on every game the tape plays (see
   gameTape.test.ts), not just on the ones looked at.
2. **Weighted by ratings** - a shooter gets his catch-and-shoots, a handler
   his pull-ups, a sharp defender is there on time.
3. **Remembered** - a man doesn't get the same combination again while
   another is there for him.

Each line's section lists its parts, how many ways each has today and
should have, how they are measured (distinct combinations a game, per
player; repeats), and its checks.

**The target is hundreds of ways for each part (on the order of 500), for
every line.** No part gets there as a list. Each is itself put together from
smaller pieces that vary on their own and multiply - "how he got the shot"
is the pass (kind, from where, early or on time) times his way to the spot
(the cut, the relocation, the screen and how he used it) times his catch
(the hop, the 1-2, turning into it), each piece weighted by the ratings of
the men in it. The variety tool counts the distinct combinations inside
each part, so the table below can say how far along each part is.

## Order

| #   | Line                                                                                          | Per game | Status  |
| --- | --------------------------------------------------------------------------------------------- | -------: | ------- |
| 1   | Three pointer, no good (`fgaTp` → `missTp`)                                                   |       39 | working |
| 2   | Defensive rebound (`drb`)                                                                     |       74 | working |
| 3   | Three pointer, made (`tp`)                                                                    |       25 | working |
| 4   | Post shot, made or missed (`fgaLowPost` → `fgLowPost` / `missLowPost`)                        |       49 | todo    |
| 5   | Shot at the rim, made or missed (`fgaAtRim` → `fgAtRim` / `missAtRim`)                        |       47 | todo    |
| 6   | Mid-range jumper, made or missed (`fgaMidRange` → `fgMidRange` / `missMidRange`)              |       34 | todo    |
| 7   | Substitution (`sub`)                                                                          |       55 | todo    |
| 8   | Free throws (`ft` / `missFt`)                                                                 |       38 | todo    |
| 9   | Offensive rebound (`orb`), putback and tip-in (`fgaPutBack`, `fgaTipIn` and results)          |       25 | todo    |
| 10  | Non-shooting foul, in the bonus (`pfNonShooting`, `pfBonus`)                                  |       19 | todo    |
| 11  | Shooting foul (`pfFG`, `pfTP`)                                                                |       13 | todo    |
| 12  | Turnover (`tov`: bad pass, lost ball, travel, offensive foul, shot clock, 5 seconds)          |       15 | todo    |
| 13  | Steal (`stl`)                                                                                 |       13 | todo    |
| 14  | Out of bounds (`outOfBounds`, `tov`/`stl` out of bounds)                                      |       20 | todo    |
| 15  | Blocks (`blkTp`, `blkMidRange`, `blkLowPost`, `blkAtRim`, `blkTipIn`, `blkPutBack`)           |        9 | todo    |
| 16  | And-ones (`fgAtRimAndOne`, `fgLowPostAndOne`, `fgMidRangeAndOne`, `tpAndOne`, putback/tip-in) |        4 | todo    |
| 17  | Timeouts (`timeout`, `timeouts`)                                                              |        5 | todo    |
| 18  | Start and end of a period, overtime (`period`, `endOfPeriod`, `overtime`)                     |        6 | todo    |
| 19  | Jump ball (`jumpBall`)                                                                        |        1 | todo    |
| 20  | Game over (`gameOver`)                                                                        |        1 | todo    |
| 21  | Injury, fouling out (`injury`, `foulOut`)                                                     |     rare | todo    |
| 22  | Heaves and the clock running out (`noShot`, `runOutClock`, `fgaTpFake`)                       |     rare | todo    |
| 23  | Intentional foul (`intentionalFoul`)                                                          |     rare | todo    |
| 24  | Elam ending, shootout (`elamActive`, `shootout*`)                                             |  setting | todo    |

## Side notes (behind the list, not ahead of it)

- **A break that stops dead** (an OKC clip, 2026-11-04): off the rebound the
  man with it has the floor in front of him, then stops and throws it cross
  court. The pass can stay - the way into it should look like a man reading
  the floor: easing off as the defense gets back, picking his dribble up, a
  look, then the pass.
- **Injured men on the bench in street clothes**, not in uniform.
- **The opening tip** (a clip, 2026-11-04): the two bigs should step up into
  the circle and go up for it like men jumping, not stand there; and off the
  tip, men of the team that won it ran the wrong way. (See #19.)

## 1. Three pointer, no good

What can be different about it, and each needs to look right on its own:

- **How he gets it**: a catch-and-shoot spot-up (kick-out, swing, skip,
  extra pass), off a screen (pin-down, flare, stagger, dribble handoff), a
  pull-up off the dribble (in the half court, in transition), a step-back,
  a side-step, off a pump fake, late in the clock, a heave.
- **Where from**: either corner, either wing, the top, a step or several
  behind the line.
- **His form**: his own jumper (quick release, set shot, push, lean, kick),
  the same for him every time, as a real shooter's is.
- **The defense**: wide open, a late closeout, a hand up, a hard
  closeout he leans away from.
- **How it misses**: front rim, back rim, off the side, in and out, off the
  glass, an airball - and the bounce off the rim agreeing with the rebound
  line that comes next (long, short, to the side, out of bounds).
- **Everyone else**: crashing or getting back, boxing out, the shooter's
  reaction.

Measured with `variety.test.ts` over three real-sim games (116 missed
threes at real speed):

| Part                                     |             Ways now | Notes                                                                                                                                                                                                                                                          |
| ---------------------------------------- | -------------------: | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| How he got it                            |                   18 | swing 31%, skip, kick-out, handoff, pull-up, step-back, hesitation; + screen, + fake, + transition                                                                                                                                                             |
| Where from                               |                    8 | corners, wings, top; on the line / a step off / deep                                                                                                                                                                                                           |
| His form                                 |                   10 | his own jumper: shoot, quick, push, split, kick, high kick, lean, high                                                                                                                                                                                         |
| His footwork into it                     |              10 → 11 | measured 2026-11-08: on the move into the catch 53% (lifting out to the line, mostly) → 58%, set 47% → 42%; arriving squared to the rim 19 → 26 of 116                                                                                                         |
| The defense                              |                    6 | wide open, open, contested, late closeout, hand in his face, late hand                                                                                                                                                                                         |
| His man closing out                      |              23 → 21 | measured 2026-11-08: from on him, a few steps or flat out from the help, running or chopping in, the hand he gets up; still coming in at the shooter 0.7 s and more after the shot 25 → 0 of 116                                                               |
| His man after it                         |                6 → 4 | drifts in 67 → 61, stays watching 23 → 28, to the glass 21 → 24, runs on by 3                                                                                                                                                                                  |
| His way to the spot                      |                   11 | measured 2026-11-08, off a pass (77): drifting down to the corner 31, lifting out 26, along the arc 17, coming in 3; on the move as it came 51, set a while 16, just there 10 - nobody stood still the 2.5 s before                                            |
| The shooter after it                     |                1 → 3 | stood where he landed 108 of 116 → backs off toward his own end 51, stands watching 41, a big in after it 24                                                                                                                                                   |
| Everyone else after it                   |                      | measured 2026-11-08, from it going up to it hitting the rim: men stood stock still 2.9 a miss → 1.1 (out on the floor on his side 97 → 14, their help in the middle 91 → 36); going in 1.4 → 1.8, their men to the glass 1.5 → 2.0; boxing out still few (0.3) |
| How it misses                            |                6 → 9 | was 70% front rim, nothing long, no air balls; now front 58%, back 26% (half in and out), side 13%, air balls and the glass now and then                                                                                                                       |
| After it                                 |                   11 | rebound by whom, short / mid / long, off the floor                                                                                                                                                                                                             |
| Whole combination                        | 116 of 116 different |                                                                                                                                                                                                                                                                |
| Same man, same setup + spot + form again |              8 of 72 |                                                                                                                                                                                                                                                                |

How it misses (aimed short / long / off the side / air / glass by his
shooting, distance and where the rebound goes) is in, 2026-11-06, with the
ball off a dunk going down away from the dunker - once the glitches the
games it reshuffles turned up were fixed (see "Blocking" below).

His footwork into it (measured from 2026-11-08: how he came into the
catch, which way his steps took him round it, how far he turned). Most
already came into it on the move - lifting out to the line on his run
there, the ball meeting him - and most of the rest stood set. Now a man
just got to his spot as a pass he shoots straight off comes (unless he is
one to wait set: a third of the time, more for a big or a slow man -
`footworkIn`) opens up to the rim on his last steps, a 1-2 shuffle turned
to it, his feet to the rim as for a man stood waiting on it
(`OPEN_UP_FT`, `stepIntoShots`; evaluate's catchToShoot) - not swung right
round on it off the catch; and one come in across the floor or at the rim
carries on into the catch, planted a step after it, rather than stopping to
wait on it (few: 5 in three games). A man stood there a while steps off his
spot and back into it (rarer still) - and one of those who wait set, stood
there a beat at least, now and then steps into it as it comes: his own hop
in at the rim from a step behind his spot, or a 1-2 in off the passer's
side, along the line in the corner (`stepIntoSet`, `SET_STEP`; once a game
or so - of the 26 set for a missed three in three games nearly all only
pulled up on the spot lifting out to it as the ball came). (The tool's
footwork now counts only a catch off a pass: 39 of the 116 came off the
dribble.)

How he got it - the pass (measured from 2026-11-08: the kind he threw,
how far, whether it led a man on the move): 17 ways, the chest pass 61%,
over the top 20% (the skips), bounce 9%, pushed one-handed 9% (off a
live dribble - and, now and then, a quick swing straight off a catch,
the better a passer the more often: `SWING_PUSH`); only 7% lead a man on
the move. (A bounce pass was counted as a pull-up until then: the tool
followed only its last flight, up off the floor.)

The defense - his man closing out (measured from 2026-11-08: how far he
came, running or chopping in, the hand he got up, when he got there; and
what he did the next second or so). A man still coming in at the shooter
long after the shot went - chopping in on him a second and more after it,
a quarter of them - pulls up short of him instead, stopped by 0.65 s after
it (`CLOSE_QUIT_BY`), and plays the shot from there. And a man still flat
out at him as it goes up, a stride off him, now and then flies at it - up
off his feet a stride off, a hand high, and on by past his shoulder
(`flyAtShot`; a few a game, the less disciplined the more often). The hand
most get up is still a little late one: "a hand up late" on 64 of 116.

The games those reshuffled turned up, each fixed for itself: a man
following his man into a run of his own a moment later (crashing the
glass) stopped dead from a run and off again from standing, his feet
flung - on into it now, as one laid from a mark already was; a defender
caught on a screen shoved back through the screener by the room kept in
front of the man with the ball (`SCREEN_PINCH`); the help stepping up to
stop the ball being the man on the one a bounce pass went to (read as a
shot), held off him as he backed down in the post (`STOP_ON_HIM`); a
contest kept in sight read off where the director thought a man was, not
where the picture had him; a step aside into a man going up with a shot
in close; and coaches easing out of one thing into the next losing what
they were still easing out of - a frame's snap.

His way to the spot (measured from 2026-11-08: how far he went in the 2.5 s
before the pass came, which way, and how long he had been there) is more
varied than the footwork alone says - down into the corner, out to the
line, along the arc - but nobody stands on his spot the whole of that
while: the offense keeps every man moving to the end. Left for now: more
men waiting set on a spot means retiming the offense, which reshuffles
every game after it.

The shooter after it: come down off a jumper from out on the floor that
is off the rim, he stood stock still where he landed till somebody had
it (108 of 116). Now most back off a few steps toward their own end,
still holding the follow-through, eyes on it; a big now and then goes in
after it a couple of steps; the rest stand and watch - each man his own
way, the same every time (`afterTheMiss`, laid into the finished picture,
so nothing else about the game moves).

Everyone else after it (measured from 2026-11-08: who went in, got back,
boxed out or went to the glass between it going up and it hitting the
rim, and who just stood). Going in, getting back, boxing out were laid
to start about as it comes off the rim (crashTheGlass's `go`), so for the
second it was in the air nearly three men a miss stood stock still. Now
each sets off on his run as he reads it - there when he would have been,
the easier for it - and a man with nothing laid for him plays it anyway:
a defender turned to it and a step in toward the glass, a man in close a
step in for position, now and then one out on the floor backing off
toward his own end (`allPlayIt`; laid into the finished picture, so
nothing else about the game moves). And a man of theirs in close with one
of ours right on his back as it comes off, both stood there, turns and
sits on him - our man fighting him for it (`boxIn`; a sharp rebounder all
but always): box-outs 0.37 a miss → 0.85. (Few still: only so many of ours
are in there - 1.8 a miss.)

Next, in order: his catch; then item 2's parts.

Blocking how it misses - glitches the reshuffled test games turn up, each
to be fixed for itself (they would turn up in real games too):

- a man jumps 4.9 ft in a tenth of a second (a run set to start where a
  drift off the ball took him, with 28 ms to get back)
- a loose ball bouncing through a man stood there (4 where 1 is allowed)
- the help on a kick-out not closing back out often enough (79%, 85% wanted)
- sliding, the foot he is down on turning (0.39, under 0.3 wanted)
- a man sat on his chair too near the coach in a huddle
- with air balls on: the ball jumping 3.5 ft between frames off one

Fixes made for those, held back - each turns up a few more in the games it
reshuffles in turn (any change to how a play is laid out moves everything
after it), so each waits until its own fallout is fixed:

- a rebounder tipping it up to himself and taking it on the way down (more
  in a crowd, for a poor rebounder - about 6% of boards) - lost, to be built
  again
- a jumper's miss coming off as far as the shot was from, not only as far
  out as the rebounder already is (`towardOf`): off a three, under the rim
  18 → 10 of 95, out a way 10 → 14 - small, for a change that reshuffles
  every game; it waits to go in with the next one that does

Still turned up by the reshuffled games, not yet fixed: a man running his
lane to his team's huddle through the other team's chairs; a man sat in the
huddle far from his chair as the picture gets there (late).

Fixed at the root (2026-11-06) - each a glitch a reshuffled game turned up,
fixed where it comes from, narrowly enough that the games it doesn't happen
in play the same:

- a rebound's ball flying the last five feet to the rebounder in a frame:
  set off from where following his man had left him, his run to the ball
  was cut to nothing - `reachIn` said a run that has to end flat out goes
  nowhere in the half second there was, when he can't get up to flat out in
  it. He goes as far as he gets, as fast as he does get up to.
- the camera after the final buzzer on men walking off to both ends: a fifth
  of a second of the main camera between the interview and the last look
  round the building. Straight on from the one to the other now.
- a fight for position with nobody there: `nobodyToBox` read where men's
  runs had them, not their steps aside - the man to fight was a help
  defender stepped over 16 ft to the shooter. It reads where they really
  are.
- a contested jumper hidden from the camera: a man going by a shooter as he
  lets it go - on his way across - put his hand up square in front of him.
  He puts it up a step to the side (`WALL_SIGHT`).
- a rebound's ball flying the last nine feet into a man's hands in a frame:
  sent in after a ball coming down off the rim, he was set off from where
  following his man had him and cut short of it. He gets there (`mark`'s
  fetch), and where he still can't, the ball carries on to him no faster
  than a ball carries (`noSnatch`).
- a reverse layup's run under the rim to the far side done in the last
  tenth of a second before he left the floor - ten feet at 30 ft/s, the
  tape's sharpest lurches, 18 of them in three games: the far side came
  down in 1.2 ft of room, so in the air he could carry on at 3 ft/s at the
  most. Room to come down (`REVERSE_ROOM`), his pace through the air
  (`REVERSE_AIR`), and a reverse only from where the far side is in his
  last strides' reach (`REVERSE_REACH`): at 12-15 ft/s at the most now, 11
  reverses in 167 layups. Its reshuffled games turned up: a ball up off the
  floor into the hands of a man nine feet from it - set off after it from
  where following his man had him, his run was cut short for what he did
  next (what he does next waits on him now, and the ball waits on the floor
  for him: `pickUpWhereItIs`); and a hand up at a jumper square in front of
  him from the camera's side, from a contest wallUp hadn't put in itself
  (`contestsInSight` puts every one a step to the side).
- his man giving ground step for step with a man backing him down, from
  when the back-down really starts - and whoever of theirs is on him, if his
  own man isn't (`backDown`). Its reshuffled games turned up two more, fixed
  too: a man boxing out a trailer ran back out through the shooter on his
  way in (`runsThrough`), and a shot blocker met a finish right on the
  finisher's way in, round the front of the rim (`clearOfPath`). And then
  two more: the clock after a ball out of bounds started on the ball handed
  to the man taking it out, not on the inbound - with three seconds left,
  it ran out before the heave went up (a hand-over is `dead` now, never the
  inbound); and the man who had the one who ran into a charge, right on
  him, came in by him to help his teammate up (somebody else comes, or
  nobody). And two more again: a man who had just been on defense, the
  ball his own after the board and brought up the floor in his own hands,
  ran back after the man he had been guarding with it - into the
  backcourt, sliding like a defender (`mark`'s fill picks a man up again
  only with the ball the other team's); and a spot-up shooter on the wing,
  the pass from the top, caught it still 60-75 degrees off the rim and
  swung round on it - his feet turned no more than 50 degrees from the
  passer toward the rim (`SET_TURN` 75 now).
- a high flier's dunks plain eight in ten in one game, and a great shooter
  never once pulling up from way out in another: each was a coin flip, and
  a reshuffle could run them all one way. Each man's goes at them come in
  turn now (`turnOf`), as often in a game as his ratings have him do it.
- a man stepped aside off his run - into the way of the ball, off another
  man, a hand up to the side of a shot - thrown two or three feet over and
  back in a fifth of a second: the nudges' ramps were all 200 ms whatever
  the distance. Each step aside (3 ft or less) is eased over no faster than
  a step takes him (`easeNudges`, 7 ft/s on top of his run), over that much
  sooner and back that much later; a bigger one is a run, shown as one. The
  tape's lurches 51/52/47 to 33/36/33, its sharp corners 18/16/7 to 12/6/4.
- a highlight played on its own (`clipOfPlay`: a box score's highlight
  opened under its row) cut off on a turnover never staged: a defender's
  last stretch of following his man ran on to forever, nothing after it
  (`mark` follows no further than the end).
- a cutter caught a pass on his way to the rim and stopped dead to take it:
  the gather into the dunk or layup (`carry`, on from the cut) was a slow
  step that had to leave the floor quick, so it started him from nothing,
  and the cut braked from 14 ft/s to a stand in a tenth of a second. There
  with time to spare, the cut runs on easier into the gather at the pace he
  has (`CARRY_DIP`): 8.8 ft/s steady through the catch into the dunk.
- a man trailing up the floor (`liven`) timed at distance over a flat pace
  - 30 ft at 21 ft/s from standing, pulled up dead at the end: timed as a
    run (`runMs`), getting going and pulling up in it, at the easiest pace that
    gets him there - and not at all if even a sprint can't. Both: the tape's
    lurches 33/36/33 to 25/27/26.
- a batch of hard stops and starts, each from its own source: a defender
  following his man down into the corner ran into the sideline and stopped
  dead on it (`mark` brakes ahead of a line now); his stretch of following
  forced to end at a stand when its own path still had him a stride from
  one, which left the court to speed him up before slamming on the brakes
  (it ends as slow as slowing evenly over it lets him be); the short burst
  between the end of his following and his next run (that run sets off from
  where following left him instead, when it goes the same way and still
  gets him there in time); a run bent round a man from all but standing
  timed as if already at speed (`ALL_BUT_STILL`), or with a hard turn and a
  leg too short to take it in (`BEND_SHARP`, kept off him instead); the long
  step through to the rim off a post move, at 12.6 ft/s from a standstill
  (from standing now, `STEP_THROUGH_FTPS`); and the hard dribble attack
  where he stands, 4.4 ft in 0.3 s stand to stand (timed as a burst). The
  tape's lurches 77 to 51 over the three games, sharp corners 27 to 17.
  Its reshuffled games turned up four more, fixed too: a man with no time
  to trail all the way up left standing at the other end (he trails as far
  as he gets, `TRAIL_LEAST`); a man carried back against his run by a long
  step aside unwinding jogged backward (he faces it from a jog, not only
  faster than a step - `OFFSET_BACK_FTPS`); a teammate's step aside
  (`passBy`) into a shooter going up at the rim (clear of him all the while
  it lasts); and a pass tipped off the man it was meant for and out, so
  little turned by his hands it read as off the man who tipped it (a third
  of a right angle at the least).
- a travel's stride or two too many with the ball, along the baseline on a
  drive down it, taken still facing the end he attacks - stepped sideways
  and back, the tape's every moonwalk (three) - faces the way the strides
  carry him. And a run bent round a man with a leg either side too short to
  take even a mild turn in (`BEND_SHARP` 0.95): kept off him instead. The
  tape's lurches 51 to 49, sharp corners 17 to 16, no moonwalks.

Checks: the existing tests (feet behind the line, a contested jumper stays
in sight, the ball never jumps, real speed with no darts) on every change.

Done: (commits)

## 2. Defensive rebound

Measured over the same three games (223 at real speed):

| Part                   |             Ways now | Notes                                                                                                                                                                                                                       |
| ---------------------- | -------------------: | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Who                    |                    9 | by position: F, FC, GF, PG, SG, SF, PF, C, G                                                                                                                                                                                |
| How he came up with it |                5 → 6 | up high two hands 65%, chest height 30%; now and then tipped up to himself (more in a crowd, more for a poor rebounder) - in testing                                                                                        |
| Where                  |                   11 | under the rim in the middle 56%; by what it came off (`off`, 2026-11-06): at the rim 67 under it of 71, a jumper 42 of 57, a three 18 under, 62 in the lane, 15 out a way or long of 95 - long ones off threes a little few |
| Traffic                |                    9 | alone / one man near / in a crowd; box-outs by him or his mates                                                                                                                                                             |
| What he did with it    |                   13 | outlet (long or not), handed off, brought it up himself, held it; pushed or walked up                                                                                                                                       |
| Whole combination      | 195 of 223 different |                                                                                                                                                                                                                             |

The grab (2026-11-08): up high with two hands two boards in three, the rest
at chest height - every man alike. Built and held back: a man who goes up
and gets it ripping it down one-handed now and then - taken in the one
hand up over his shoulder, out to that side, either hand (`mirror`), the
other hand coming in to it on the way down, then chinned (`boardOne`, a
`palm` grip; decided in `reboundOff` before he goes, so he stands off it
to the side of that hand and the ball comes off the rim into it with no
swerve; 19 of 223 in three games). It moved where every rebounder stood
by a step and a half, enough to tip three of the suite's tests that sit
on their line (the shooter's man off him and back, a post-up's man, a
second device's catch-up) - in again once those are looked at.

The pile round him a moment after he has it (measured 2026-11-08: men
within 5 ft of him 0.9 s on, and those of them stood still): 1.3 a board,
0.3 stood - a scrum that stays put only now and then (6 of 223 with four
or five round him).

The outlet (measured 2026-11-08, the pass kind of every outlet over 12
ft): a chest pass every time but the long skips - overhead 4 of 120.
Built and held back (see HELD below): the man who came down with it
himself, still with it in his hands, mostly throwing it up over his head - a big 70%, a ball handler 35% (`OUTLET_OVER`,
within 2.5 s of the board, not under 8 ft; `passTo`'s `overTop` - the
kind's draw still made, so the rest of the game runs the same): overhead
87 of 147, chest 60. Up out of the chin, the windup starts that much
sooner (`throwAt`): the ball away when a chest pass would have been - with
the overhead's extra windup on top, every outlet off a board went 140 ms
later, the defense back more often, and kick-outs found the help left on
the shooter less (over 16 test games, 0.67 of them → 0.59; with it, 0.69).

The pivot (built and held back): a man of theirs within 3 ft of him as he
comes down with it (`OUTLET_PRESS`), he turns a shoulder into him first - round 1.6 rad off
the man coming for it, to the side away from him, 450 ms - then round to
throw (only with 700 ms or more to throw it in): 58 of the 155 outlets
off a board, the rest come round to the man at once.

Two men up for it (measured 2026-11-08, a man of theirs up with him or a
hand at it as he takes it): 153 of 223 - most of them a hand from every
man of theirs within 8 ft of it as it came down (`scramble`), where in
the league most defensive boards nobody of theirs is near. Built and held
back: only the nearest of theirs swiping at it, and only from within 3.5
ft (`SCRAMBLE_SWIPE`), and the man who goes up with him doing so 65% of
the time from within 4 ft, 35% from further (`RIVAL_GOES`, more for a
good man on the glass): 93 of 223 - up with him 26, a hand at it 67 (40
of those a loose one off the floor). It moves men about after every
board, so the games after play out otherwise, and the suite's tests that
sit on their line tip (a referee's walk round a corner at a quarter
break, a man along the half-court line, a footwork share) - in again
with those seen to.

Long off a three (measured 2026-11-08, where boards off threes were
taken): under the rim 26 of 96, in the lane 57, out a way 7, long 6 - a
miss is sent off the rim toward the man who gets it but never further out
than 11 ft (`towardOf`), so a man out there comes in to it. Tried: as far
as 6 ft and 0.4 of the shot's length toward a man out there - under 16 of
98, the lane 60, out a way 18, long 4 - but with it more men were left
stood out past 31 ft as the ball was played in close (the half-court test:
5 in its six games, master 2; 9 to 5 over twelve), the man who pitched it
ahead from out there trailing late. Backed out till the trailers come on.

HELD (2026-11-09): the one-handed board, the overhead outlet and the
pivot, the one swipe at a board and the and-one fouler's own run are in
`held/drb.patch` (made against 9f0c44580). Each changes where men are or
when, so every game after plays out otherwise - and each time, some few
of the suite's tests that sit right on their line tipped over, different
ones each run (a kick-out's help 0.59 for 0.6, a footwork share 0.304 for
0.3, a referee's walk round a corner at a quarter break, a man on the
half-court line, a post-up's man, a heave at the buzzer). In again once
those tests stand clear of their lines: the edge cases they catch fixed
where they are real, more games where they are only a count.

Next: a baseball outlet up the floor wants a man out ahead to throw it to,
the hit-ahead on a break (long outlets now: 1 in three games). A bobble is
the tip to himself, there already.

Seen along the way (2026-11-08), held back with the above: the man who
fouls a jump shooter (an and-one) is planned off from where shadowing his
own man has him, then that is reworked to follow the man (see mark) and
the run at the shooter can be left 5.5 ft short - `ownRun` in foulHim
fixes it (in the held-back patch).

## 3. Three pointer, made

The way into it is item 1's (the setup, the spot, his form, the footwork,
the closeout). What is its own: how it goes in, the shooter after it, his
teammates, and the other five taking it out.

Measured over the same three games (76 at real speed, 2026-11-08; the
variety tool's make parts - `jumperParts(..., made)`):

| Part           | Ways now | Notes                                                                                                                                          |
| -------------- | -------: | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| How it went in |        4 | swish 38, back rim and in 34, front rim and in 2, rattled in 2                                                                                 |
| Shooter after  |       12 | back down the floor 71 of 76; holds his follow-through most; a finger at the bench 19, three fingers up 25; a shimmy 1, thanks to the passer 2 |
| Taken out      |       12 | under the rim 63, off the floor 12; in again 4.5-5.5 s after the release nearly every time                                                     |

Next: the taking out - in again sooner, and not the same pace every time
(a team that pushes has it out and in at once); then his teammates on the
make.
