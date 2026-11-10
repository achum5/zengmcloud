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
| 4   | Post shot, made or missed (`fgaLowPost` → `fgLowPost` / `missLowPost`)                        |       49 | working |
| 5   | Shot at the rim, made or missed (`fgaAtRim` → `fgAtRim` / `missAtRim`)                        |       47 | working |
| 6   | Mid-range jumper, made or missed (`fgaMidRange` → `fgMidRange` / `missMidRange`)              |       34 | working |
| 7   | Substitution (`sub`)                                                                          |       55 | working |
| 8   | Free throws (`ft` / `missFt`)                                                                 |       38 | working |
| 9   | Offensive rebound (`orb`), putback and tip-in (`fgaPutBack`, `fgaTipIn` and results)          |       25 | working |
| 10  | Non-shooting foul, in the bonus (`pfNonShooting`, `pfBonus`)                                  |       19 | working |
| 11  | Shooting foul (`pfFG`, `pfTP`)                                                                |       13 | working |
| 12  | Turnover (`tov`: bad pass, lost ball, travel, offensive foul, shot clock, 5 seconds)          |       15 | working |
| 13  | Steal (`stl`)                                                                                 |       13 | working |
| 14  | Out of bounds (`outOfBounds`, `tov`/`stl` out of bounds)                                      |       20 | working |
| 15  | Blocks (`blkTp`, `blkMidRange`, `blkLowPost`, `blkAtRim`, `blkTipIn`, `blkPutBack`)           |        9 | working |
| 16  | And-ones (`fgAtRimAndOne`, `fgLowPostAndOne`, `fgMidRangeAndOne`, `tpAndOne`, putback/tip-in) |        4 | working |
| 17  | Timeouts (`timeout`, `timeouts`)                                                              |        5 | working |
| 18  | Start and end of a period, overtime (`period`, `endOfPeriod`, `overtime`)                     |        6 | todo    |
| 19  | Jump ball (`jumpBall`)                                                                        |        1 | working |
| 20  | Game over (`gameOver`)                                                                        |        1 | todo    |
| 21  | Injury, fouling out (`injury`, `foulOut`)                                                     |     rare | working |
| 22  | Heaves and the clock running out (`noShot`, `runOutClock`, `fgaTpFake`)                       |     rare | working |
| 23  | Intentional foul (`intentionalFoul`)                                                          |     rare | todo    |
| 24  | Elam ending, shootout (`elamActive`, `shootout*`)                                             |  setting | todo    |

## Side notes (behind the list, not ahead of it)

- **A break that stops dead** (an OKC clip, 2026-11-04): off the rebound the
  man with it has the floor in front of him, then stops and throws it cross
  court. The pass can stay - the way into it should look like a man reading
  the floor: easing off as the defense gets back, picking his dribble up, a
  look, then the pass.
- **Injured men on the bench in street clothes**, not in uniform. Done
  2026-11-09: a man out hurt from before the game (not hurt in it, not
  playing through it) sits with his team in a suit, a shirt open at the
  neck, his own every game - no number, no name - and never warms up.
- **The opening tip** (a clip, 2026-11-04): the two bigs should step up into
  the circle and go up for it like men jumping, not stand there; and off the
  tip, men of the team that won it ran the wrong way. (See #19.)
  Checked 2026-11-09 over nine real games: both jumpers set and go up for
  it (about a second off the floor), and all of the winners' men go the
  right way off it, every tip.

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

His catch (2026-11-09): every pass was taken the one way, hands out at
his chest - a skip thrown up over the top and a bounce pass alike. Now
one thrown over the top is taken up high, both hands over his head, and
one bounced to him down low, knees bent and hands out in front
(`catchHigh`, `catchLow`): the picture only - the catch act marked
(`Act.catchAt`), the pass flying to his hands wherever they are - so
nothing else in the game moves. (Of the passes into a missed three, over
the top 20%, bounced 9%.)

Next: item 2's parts.

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
at chest height - every man alike. Now (2026-11-09) a man who goes up and
gets it rips it down one-handed now and then, either hand (the better a
rebounder the more often - `ONE_HAND_BOARD`): the picture only - the
board act marked (`Act.oneHand`) and drawn as `boardOne`, its near arm as
for two and the ball just where two hands would have it, held in the one
(the `rip` grip), the other hand out at his chest till it comes in to it
on the way down - so nothing else in the game moves (first built moving
him over to the side of that hand, which moved where every rebounder
stood and tipped tests on their line; see HELD).

The pile round him a moment after he has it (measured 2026-11-08: men
within 5 ft of him 0.9 s on, and those of them stood still): 1.3 a board,
0.3 stood - a scrum that stays put only now and then (6 of 223 with four
or five round him).

The outlet (measured 2026-11-08, the pass kind of every outlet over 12
ft): a chest pass every time but the long skips - overhead 4 of 120.
Now (2026-11-09) the man who came down with it himself, still with it in
his hands, mostly throws it up over his head - only the picture of it:
the director lays the same chest pass at the same moments and marks it
(`Act.outlet`), and the picture draws it as `passOutlet`, up out of the
chin and away when the chest pass would have gone - so nothing else in
the game moves (65 of 121 overhead). As first built, mostly throwing it
up over his head - a big 70%, a ball handler 35% (`OUTLET_OVER`,
within 2.5 s of the board, not under 8 ft; `passTo`'s `overTop` - the
kind's draw still made, so the rest of the game runs the same): overhead
87 of 147, chest 60. Up out of the chin, the windup starts that much
sooner (`throwAt`): the ball away when a chest pass would have been - with
the overhead's extra windup on top, every outlet off a board went 140 ms
later, the defense back more often, and kick-outs found the help left on
the shooter less (over 16 test games, 0.67 of them → 0.59; with it, 0.69).

The pivot (2026-11-09, the picture only - `Act.turnFrom` on the one hold
act, read where his yaw is): a man of theirs within 3 ft of him as he
comes down with it (`OUTLET_PRESS`), he turns a shoulder into him first - round 1.6 rad off
the man coming for it, to the side away from him, 450 ms - then round to
throw (only with 700 ms or more to throw it in): 58 of the 155 outlets
off a board, the rest come round to the man at once.

Two men up for it (measured 2026-11-08, a man of theirs up with him or a
hand at it as he takes it): 153 of 223 - most of them a hand from every
man of theirs within 8 ft of it as it came down (`scramble`), where in
the league most defensive boards nobody of theirs is near. Now
(2026-11-09) only the nearest of theirs swipes at it, and only from
within 3.5 ft (`SCRAMBLE_SWIPE`), and the man who goes up with him does
so 65% of the time from within 4 ft, 35% from further (`RIVAL_GOES`, more
for a good man on the glass): 92 of 223 - up with him 16, a hand at it 76
(many a loose one off the floor). The picture only: the rest still step
in, their swipe or jump laid as before and left out of the picture
(`Act.quiet`, as `flash` is) - so nothing else in the game moves. (First
built taking them out of the game, which moved men about after every
board and tipped tests that sit on their line - see HELD.)

Long off a three (measured 2026-11-08, where boards off threes were
taken): under the rim 26 of 96, in the lane 57, out a way 7, long 6 - a
miss is sent off the rim toward the man who gets it but never further out
than 11 ft (`towardOf`), so a man out there comes in to it. Tried: as far
as 6 ft and 0.4 of the shot's length toward a man out there - under 16 of
98, the lane 60, out a way 18, long 4 - but with it more men were left
stood out past 31 ft as the ball was played in close (the half-court test:
5 in its six games, master 2; 9 to 5 over twelve), the man who pitched it
ahead from out there trailing late. Backed out till the trailers come on.

HELD (2026-11-09) and now all in: each piece of this item first built
changed where men are or when, so the games after played out otherwise
and some few of the suite's tests that sit right on their line tipped
over, different ones each run. Done over as the picture only - the
one-handed board, the outlet and pivot, the one swipe - nothing else in
the game moves; and the and-one fouler's own run, a fix where the game
does move, went in on its own once the suite stood clean with it.

Next: a baseball outlet up the floor wants a man out ahead to throw it to,
the hit-ahead on a break (long outlets now: 1 in three games). A bobble is
the tip to himself, there already.

Seen along the way (2026-11-08), in 2026-11-09: the man who fouls a jump
shooter (an and-one) was planned off from where shadowing his own man had
him, then that was reworked to follow the man (see mark) and the run at
the shooter could be left 5.5 ft short - now his own run (`ownRun` in
foulHim).

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

The taking out after a make runs fast in the picture unless the team
pushes it (`inboundAfterMake`'s `hurry`), so its slow even pace is little
seen - left.

The rest of them on the make (measured 2026-11-09, what the other nine
did with an arm or more in the 3.5 s after it): his mates mostly pointing
out who they have on the way back, theirs fetching it out of the net -
nobody about the shot itself. Now, off a pass, the man who found him now
and then points back at him on his own way back (`PASSER_POINTS`, 40%);
and the man who had him, now and then, hands up on his head on his way
(`BEATEN_HEAD`; 5 of 76) - arms only, nothing else in the game moves.

## 4. Post shot, made or missed

Measured 2026-11-09 over the same three games (the variety tool's `post`
part: his back-down, his moves, his man): 46 made, 69 missed at real
speed. Most come straight off a drive - a floater (58 of 115) or a hook
(46), a fade now and then (6) - not a back-down: 4 of 115 backed his man
down first. His man rim side of him at the release 35 of 46 makes. After
a miss in there he stood where he landed 23 of 69 - the paint crowded,
afterTheMiss's step back or in too often into somebody (left).

## 5. Shot at the rim, made or missed

Measured 2026-11-09 (61 made, 56 missed at real speed). Made: a dunk 35
(two hands 13, one 16, tomahawk 6), a power layup 8, a layup 7, a finger
roll 7, a reverse 3, a scoop 1; in clean 46, off the glass 9, off the
front of the rim 6. Missed: a power layup or a layup 32, a finger roll
11, a scoop 10, a reverse 3 - and off the front of the rim 45 of 56, the
rest rattled round (7), off the side or the glass (4). Now (2026-11-09) a
miss in close is aimed the way it misses as a jumper's is: off the front,
long, off a side, or too hard off the glass from an angle to it
(`closeMissStyleFor`, `CLOSE_MISS`): off the front 44 of 56, off the side
6, off the glass 6 (long off the back from that close mostly drops in, so
few). It moves the ball, so the games after play out otherwise; what the
reshuffled games turned up, fixed where it comes from:

- a post man's man giving ground ahead of him - at a jog, there before
  him, a gap opening - and nudged off him by keepApart for a teammate's
  sake: now he gets up on him quickly if he is not, then gives ground at
  his pace (`BACK_DOWN_CLOSE`), and holds his ground there as a man set in
  a screen does (keepApart's `backing`).
- (The loose-ball test now leaves out the man at the ball himself - the
  one who knocked it loose, a dunker coming down under his own dunk: the
  ball does come down on them.)
- a man over half court with it, sent to a spot back over it, dribbled
  along the line across the floor to it (kept from going back over, but
  not from going across): now he holds where he is across the floor, up
  off the line a step if he is not already (playDribble).
- (The half-court test now leaves out the walk over to shake hands after
  the game - no run for a spot.)

## 6. Mid-range jumper, made or missed

Measured 2026-11-09 (43 made, 50 missed at real speed; the variety tool's
spot inside the arc: along the baseline, the elbow or wing, straight on -
and short, mid or a long two): off the elbows and wings 67 of 93, straight
on 17, along the baseline 2 - the sets' mid-range reads all go to the
elbows, the high post and the mid wings (a pull-up to the short corner in
three of them). Now (2026-11-09) a corner man drifts up to the short
corner for a kick-out jumper in the high pick-and-roll, Horns and
drive-and-kick, and the ball handler takes it baseline to pull up there
off a side pick-and-roll or an isolation (the playbook's new
`R_short_corner` reads): along the baseline 7 of 93.

## 7. Substitution

Measured 2026-11-09 (185 men coming on at real speed, 58 more run through
fast; the variety tool's `sub` line): what stopped play (a free throw 61,
a foul 55, the ball out 36), how long he waited at the table (4-8 s, all of
them), his way out to his spot, the man going off to his chair (at a run,
nearly all), greeted there (134 of 185, by one or two), a slap of hands
on the way by (112), how long it held the game up (1.5-7 s), and how many
at once (1 to 5). Never: the man going off telling the man coming on who
he had, or an official waving the man at the table on.

Now (2026-11-09): with his side to defend when play goes on, the man going
off points out the man he had as the two come together (and the man
coming on, now and then, points at him too: got him) - 86 of 185; and the
official nearest the table, turned to it, waves the men waiting there on
(173 of 185; not one still signaling a call, or on his way back from one).

## 8. Free throws

Measured 2026-11-09 (92 made, 21 missed at real speed; the variety tool's
`ft` and `missFt` lines): which of his trip it is, his routine from the
official's bounce pass to the shot, how long that takes (1.5-2.5 s, then
the stroke, about a second), how it went (made: off the rim 40, clean 39;
missed: rimmed out, off, the odd brick or airball), the men on the lane
with it in the air (at ease - hands on hips, on their knees - with another
to come, set and in to box out on the last), the low fives after (on 32 of
the 45 with another to come), and who gets the last one missed (the
defense 10 of 13). Lane spots as the NBA has them: three of the defense,
two of the offense. The routine was one of four, every man's the same as a
quarter of the league's.

Now (2026-11-09): every man's routine is put together from pieces, his
own every trip - one to four bounces at his own beat (slow or quick), a
flip with backspin first or after them, a breath down in his legs, a beat
with it eyes on the rim - in the time his used to take: 15 ways over the
three games (4 before).

## 9. Offensive rebound, putback, tip-in

Measured 2026-11-09 (74 offensive boards at real speed; the variety tool's
`orb` line, off the shooter's own rim): how he got it (up high, two hands
36, ripped down one-handed 6; at chest height 22; off the floor 10), where
(under it 40, in the lane 27, further out 7), what it came off (a jumper or
one at the rim mostly under it, a three in the lane), the traffic (in a
crowd 41, one of theirs up with him or a hand at it 34), and what he did
with it - back up with it 24 (at once, after a gather, off a dribble), out
of it 50 (kicked out to the arc 37). 73 different of 74. Putbacks and
tip-ins as lines of their own are rare in the sim (one in three games).
Not yet: a tap out to a mate, the ball kept alive without being caught.

Box-outs, measured 2026-11-09 over missed jumpers in the nine games (468
at real speed): 0.81 of theirs boxing out a miss on average, none at all
on 40% of misses, and two in three of the box-outs begun only after it was
off the rim. Of the men really crashing (in 2 feet and more since it went
up, inside 12 feet as it comes off; 253), 36% had a body put on them;
the nearest of theirs to most of the rest was already boxing out somebody
else. Now (meetTheCrash) the nearest of theirs still free steps into a
crasher with nobody on him and boxes out before it is off the rim: 40%,
the box-out about 2.4 feet off him.

## 10. Non-shooting foul, in the bonus

Measured 2026-11-09 (46 away from a shot at real speed; the variety
tool's `pfNonShooting` and `pfBonus` lines): the call (held 25, hit on the
arm 11, a help man blocking the drive 10), on the ball or off it (a cutter
held up), where (the half court 28, at the rim 18), the man fouled (looks
for the call, jolted, knocked off balance - or nothing), the man who
fouled him (a reach or a hack, protesting it now and then), the officials'
signals, and the others with the whistle gone. Never: the official who
called it reporting it to the table.

Now (2026-11-09): through with the signal, the official who called it runs
over in front of the table and reports it, turned to it - who (fingers up,
one hand and the other) and what - then back to his place: all 46. Not yet
on a foul with free throws to come (the official at the line has the
ball), nor the ball handed to an official for the inbound.

## 11. Shooting foul

Measured 2026-11-09 (34 on a two at real speed, 4 on a three; the variety
tool's `pfFG` and `pfTP` lines): the shot (at the rim 13, in the post 20,
mid-range 2), the contact (a hack at his arms, every time), the man fouled
after it (nothing 26, picked up the ball 8), the man who fouled him (a
word about it 11), the official's signals (the fist, then the hit), and
the walk to the line (9-11 s to the first free throw).

Now (2026-11-09): down from it, the man fouled now and then claps it - he
drew it - more often off a three, and at the rim the odd word with the
official instead; the man who fouled him, with nothing to say about it,
now and then his hands on his hips or on his head. Only in a moment each
has to himself, stood there: 11 and 14 of 34 now. Not yet: other contact
than the hack (a body at the rim, a hand on the wrist), the number of shots
signaled, the call reported to the table.

Now (2026-11-09): fouled going up in close, now and then he comes down on
the floor with it (at the rim 30%, a putback 25%, a tip-in 20%, in the
post 12%): sat there till a teammate comes over and pulls him up by the
hands, or up on his own, and only then off to the line - 14 of 117 over
the nine games, 5 of them pulled up. The same on an and-one (not a dunk):
his flex or the chest bump once he is up. And out on the floor, a closeout
landing under or into a shooter (a three 12%, a jumper 8%) - with the man
who fouled him stepping back off him, not stood over him (he was, and the
spacing passes slid the man on the floor out from under him instead): 23
of 123 now, 18 of them pulled up.

## 12. Turnover, steal

Measured 2026-11-09 (57 turnovers and 47 steals at real speed; the variety
tool's `tov` and `stl` lines): how it was lost - a whistle for an
offensive foul 34 (a moving screen 11 of them), out of bounds 13, a
travel 10; a pass picked off 26, poked loose 14, stripped 7 - where, the
man who lost it after it, the man who took it (off up the floor 24, held
22), the officials' signals, and what came next. Offensive fouls were more
of the whistles than the NBA's (about half) - a moving screen nearly four
times a game. Rebalanced (2026-11-09, over nine games): a charge and a
travel about as often as each other, a moving screen about once a game;
offensive-foul whistles from about 11 a game to 7. The man who lost it
did nothing about it after 39 of the 47 steals.

Now (2026-11-09): the ball taken off him, the man who lost it now and then
has an arm up at the official as it goes - he was hit - or, on his way
back, his hands on his head (and, thrown away out of bounds, his hands on
his head now and then): 17 of the 47 steals. Arms only, so nothing else
changes.

(13. Steal: measured with the turnovers, above.)

## 15. Blocks

Measured 2026-11-09 (26 at real speed over the three games; the variety
tool's `blk*` lines): the shot (at the rim 6, in the post 8, mid-range 5,
a three 7), the block (from in front 22, run down from behind 3; off the
hand on the ball's side - for most shooters, right-handed, the blocker's
left), where the ball went (the defense's 19, back to the offense 6, out
of bounds 1), the blocker and the shooter after it (on with the play,
mostly), and the building - sat for all but one.

Now (2026-11-09): a block brings the building up - the home crowd on its
feet for a home one (nearly as much as for a dunk or a three), the road
fans for theirs less so - and the blocker's bench up off its chairs.

## 14. Out of bounds

Measured 2026-11-09 (39 at real speed; the variety tool's `outOfBounds`
line): what it came off (a miss knocked out going for it, mostly: off a
three 15, in the post 9), where it went out (the sideline at the corner
end 25, the baseline 11, the sideline at mid-court 3), the ball after it
(to an official 24, picked up by a player 15), the official's signal (the
way it goes, every time), and the ball back in. Never: anybody having a
say about whose ball it is.

Now (2026-11-09): as the whistle goes, the man of each side nearest where
it went out now and then points the way he says it goes - his own way:
19 of the 39, both sides at once in 4. Arms only, so nothing else
changes.

## 16. And-ones

Measured 2026-11-09 over nine real games (27 at the rim, 15 in the post at
real speed; the variety tool's `*AndOne` lines): the man who scored it
lets it out - a chest bump with a mate, a flex, a roar, his chest
pounded, both arms up - and points at the man who found him; theirs
protest, put their hands on their hips, box out for the free throw.
Nothing to add for now.

## 17. Timeouts

Measured 2026-11-09 (16 over nine real games): the whistle and the
signal, the walk to the benches run through fast, the huddle round the
chairs, a look round the building at real speed (the crowd, the dance
team, the moppers), and the walk back out. Nothing to add for now.

## 18. Start and end of a period

Not yet: quarters after the first start with the inbound at half court;
the NBA puts it in from the end line in the backcourt of the side that
has it (Rule 6-I). Tried 2026-11-09 - the spot is easy, but the ball has
to get there: whoever carried it into the huddle (often subbed off by
then, or on the other side) threw it 40-65 feet from the bench, and the
officials (crew.ts) only hold it at the table or the free-throw line. It
wants an official who walks it to the end line and hands it in.

## 19. Jump ball

Measured 2026-11-09 (10 tips over nine real games): the two jumpers set
in the circle and go up for it, the winner a touch higher; the rest round
the circle, the smallest man back; off the tip, the winners' men all go
the right way, and the losers' get back.

## 21. Injury, fouling out

Measured 2026-11-09: neither came up in nine real games. Hurt in a game, a
man goes down (or grabs at it), a teammate or two come over and crouch
by him, and his trainer comes out; the man out hurt from before the game
sits on the bench in his street clothes (see the side notes).

## 22. Heaves and the clock running out

Measured 2026-11-09 (16 period endings with no shot over nine real
games, `VARIETY_LINE=endOfPeriod`): half of them (8) had the horn go the
moment the line before ended - with a second or so left after a basket,
a free throw or the ball out of bounds, the horn went with the ball still
on the floor, nobody taking it in. Now, with half a second or more left
and the ball dead, it comes in to the man who brings it up, the clock
from his catch: a dribble or two up the floor if he is back in his own
half, turned to the basket if not, as the horn goes - and more often than
not a heave or a jumper a beat too late, waved off.

Running out the clock at the end of a game they have won, the man on the
ball stayed up on the dribbler (inside 4 feet in 3 of 5), and the
dribbler jabbed and hesitated at him - one jab after the horn. Now his
man backs off a step or two (7 feet) and stands, the dribbler just
dribbles it out, and nobody fakes at anybody once the horn has gone.
