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

Next, in order: how it misses (aimed short / long / off the side / air /
glass by his shooting, distance and where the rebound goes - built and
measured, not pushed: every rebound landing somewhere new reshuffles the
test games, and seven older glitches show up in the reshuffled ones - see
"Blocking" below);
his footwork into it (measure first); how he got it (the pass, his way to
the spot, his catch); the defense (closeout kinds, the man's reaction
after); everyone after it.

Blocking how it misses - glitches the reshuffled test games turn up, each
to be fixed for itself (they would turn up in real games too):

- a man jumps 4.9 ft in a tenth of a second (a run set to start where a
  drift off the ball took him, with 28 ms to get back)
- a loose ball bouncing through a man stood there (4 where 1 is allowed)
- a contested jumper hidden behind his defender from the camera
- a man backing down in the post with nobody to back into
- the help on a kick-out not closing back out often enough (79%, 85% wanted)
- sliding, the foot he is down on turning (0.39, under 0.3 wanted)
- a man sat on his chair too near the coach in a huddle
- with air balls on: the ball jumping 3.5 ft between frames off one

Fixes made for those, held back - each turns up a few more in the games it
reshuffles in turn (any change to how a play is laid out moves everything
after it), so each waits until its own fallout is fixed:

- the ball off a dunk goes down through the net away from where the dunker
  comes down, not bouncing up through his legs (the slam's push off the
  rim, away from him)
- his man gives ground step for step with a man backing him down, from when
  the back-down really starts - and whoever of theirs is on him, if his own
  man isn't (backDown)
- a drift off the ball (keeping his spacing) skipped where his next run, set
  off from the end of it, would be too quick for his legs
- a man right on a jumper square in front of him from the camera: the hand
  up has to stay (a test), so he has to step aside for it - a nudge in
  wallUp only for that man, not a pass over every contest (that broke the
  and-one's foul, the poster, the close contests)
- a rebounder tipping it up to himself and taking it on the way down (more
  in a crowd, for a poor rebounder - about 6% of boards)

Still turned up by the reshuffled games, not yet fixed: a man running his
lane to his team's huddle through the other team's chairs; a man sat in the
huddle far from his chair as the picture gets there (late); a rebound's
ball flying to a man the marking pass has moved away from it; a heave at
the buzzer whose ball goes out of bounds late.

Checks: the existing tests (feet behind the line, a contested jumper stays
in sight, the ball never jumps, real speed with no darts) on every change.

Done: (commits)

## 2. Defensive rebound

Measured over the same three games (223 at real speed):

| Part                   |             Ways now | Notes                                                                                                                                                                     |
| ---------------------- | -------------------: | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Who                    |                    9 | by position: F, FC, GF, PG, SG, SF, PF, C, G                                                                                                                              |
| How he came up with it |                5 → 6 | up high two hands 65%, chest height 30%; now and then tipped up to himself (more in a crowd, more for a poor rebounder) - in testing                                      |
| Where                  |                   11 | under the rim in the middle 56% - too many: the ball comes off the rim to where the rebounder already is (see `reads` in crashTheGlass, `towardOf`) and few come off long |
| Traffic                |                    9 | alone / one man near / in a crowd; box-outs by him or his mates                                                                                                           |
| What he did with it    |                   13 | outlet (long or not), handed off, brought it up himself, held it; pushed or walked up                                                                                     |
| Whole combination      | 195 of 223 different |                                                                                                                                                                           |

Next: rebounds coming off long (the ball off the rim decides where it
goes, and the man goes and gets it), the grab (a one-handed snatch, two
men up for it, a bobble), the landing (chinned, a pivot away from
pressure), the outlet (overhead, baseball, a dribble out of traffic first).
