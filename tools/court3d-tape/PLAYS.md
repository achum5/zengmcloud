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
| 3   | Three pointer, made (`tp`)                                                                    |       25 | todo    |
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

| Part                                     |             Ways now | Notes                                                                                                                                    |
| ---------------------------------------- | -------------------: | ---------------------------------------------------------------------------------------------------------------------------------------- |
| How he got it                            |                   18 | swing 31%, skip, kick-out, handoff, pull-up, step-back, hesitation; + screen, + fake, + transition                                       |
| Where from                               |                    8 | corners, wings, top; on the line / a step off / deep                                                                                     |
| His form                                 |                   10 | his own jumper: shoot, quick, push, split, kick, high kick, lean, high                                                                   |
| His footwork into it                     |                    - | not measured yet                                                                                                                         |
| The defense                              |                    6 | wide open, open, contested, late closeout, hand in his face, late hand                                                                   |
| How it misses                            |                6 → 9 | was 70% front rim, nothing long, no air balls; now front 58%, back 26% (half in and out), side 13%, air balls and the glass now and then |
| After it                                 |                   11 | rebound by whom, short / mid / long, off the floor                                                                                       |
| Whole combination                        | 116 of 116 different |                                                                                                                                          |
| Same man, same setup + spot + form again |              8 of 72 |                                                                                                                                          |

How it misses (aimed short / long / off the side / air / glass by his
shooting, distance and where the rebound goes) is in, 2026-11-06, with the
ball off a dunk going down away from the dunker - once the glitches the
games it reshuffles turned up were fixed (see "Blocking" below).

Next, in order: his footwork into it (measure first); how he got it (the pass, his way to
the spot, his catch); the defense (closeout kinds, the man's reaction
after); everyone after it.

Blocking how it misses - glitches the reshuffled test games turn up, each
to be fixed for itself (they would turn up in real games too):

- a man jumps 4.9 ft in a tenth of a second (a run set to start where a
  drift off the ball took him, with 28 ms to get back)
- a loose ball bouncing through a man stood there (4 where 1 is allowed)
- a man backing down in the post with nobody to back into
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

Next: rebounds coming off long (the ball off the rim decides where it
goes, and the man goes and gets it), the grab (a one-handed snatch, two
men up for it, a bobble), the landing (chinned, a pivot away from
pressure), the outlet (overhead, baseball, a dribble out of traffic first).
