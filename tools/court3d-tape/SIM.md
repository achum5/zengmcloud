# The ten on the floor, stepped: plan

The goal: basketball in the 3D picture that does not look scripted - though
every line of it is the sim's play-by-play, acted out. This is the plan for
the overhaul that gets there (phase 4 of the dribbling plan in PLAYS.md),
written before it starts so every session after this one builds the same
thing.

## Why it looks scripted now

The director stages each line by laying out runs: "be at B by time t". A
run is a straight leg, eased in and out. Then some forty passes go over the
laid-out runs and patch what is wrong with them - men standing on each
other (keepApart, keepOff), defenders not following their men (mark), men
standing too long (liven, keepDribbling, workHisMan, allPlayIt), nobody
stopping the ball (stopTheBall) - each a fix for what the runs got wrong,
each blind to the others, each with its own tests. Nobody on the floor
decides anything moment to moment: the man with the ball does not read his
man, the man off the ball does not read the ball, and the patches make it
worse where they meet (a step aside that a later pass undoes, a defender
pushed off a man another pass put him on).

Measured 2026-11-10 (see PLAYS.md, Dribbling): the man with the ball
stands 27-30% of the time he dribbles in the half court and changes
direction on the move under once a minute; the men off the ball stand 49%
of the time.

## What it becomes

One short-step simulation of all ten, between the script's moments.

- **The script stays.** The director still works out, from each line,
  what has to happen and when: who has the ball, every pass (who, when,
  from roughly where), every shot (who, when, from where - the shot chart's
  spot), screens, fouls, the whistle. Those are _checkpoints_: hard ones
  (the ball's moments: a catch, a release, a shot spot) that must be hit
  exactly, and soft ones (a set's spots, a screen's place) that shape where
  men go but give way.
- **Between checkpoints, everyone decides.** Every 100 ms each man picks
  where he wants to be going from what he is doing (his role at that
  moment) and what he sees (the ball, his man, the men near him), and moves
  that way - no faster, no quicker to turn or stop, than a man can (the
  motion model in motion.ts already says how quick that is).
  - The man with it: attack a shoulder, probe, retreat, change pace and
    direction off his man, keep his dribble alive - steered so he is where
    the next checkpoint needs him, when it needs him.
  - His man: between him and the rim, a beat behind him (a reaction lag
    from his ratings), cut off the drive, give ground, close out.
  - Off the ball, offense: keep the floor spaced, move with the ball (drift
    and lift on a drive, relocate on a kick, cut when his man turns his
    head), get to the set's spots in time for its actions.
  - Off the ball, defense: help and recover, rotate, sink to the ball side,
    box out on a shot.
  - Everyone: never through anybody (separation is part of the step, not a
    patch after it).
- **Deadlines win.** As a hard checkpoint comes up, the man it needs moves
  from deciding to committing: what he wants is blended toward "be there
  then" by how little time is left, so he always makes it, the way the
  runs make it now.
- **Same output.** The stepped paths come out as the tracks the court
  already reads (moves with their paces at each end, acts, the ball), so
  playback, the camera, the renderer and the shared engine (see
  docs/3d-engine.md) do not change. Acts (shots, passes, the reactions)
  stay as the director lays them; only where men are, and how they get
  there, comes from the simulation.

## How it gets built

Behind a switch (`compileCourt({ ..., sim: true })`), off until it is
better than what it replaces - by the measures below and by eye.

1. **The stepper.** Agents, a 100 ms tick, the motion model's limits,
   separation, deadline steering to checkpoints; tracks out. First run it
   with every man simply steering to the director's own runs as soft
   checkpoints: the picture should look like today's, smoother - and the
   suite should mostly still pass.
2. **The defense.** Man to man from the stepper instead of mark,
   keepApart, keepOff, stopTheBall, wallUp and the rest. The passes they
   replace are switched off under the switch.
3. **Off the ball.** Spacing and reading the ball instead of liven,
   flowOn, stagger, allPlayIt.
4. **The man with the ball.** Probing and reading his man instead of
   keepDribbling, workHisMan, dribbleThrough.
5. **Switch it on.** Tests written against the old passes' particulars
   are rewritten to what they were really for; the old passes go.

## Measures

The dribbling measures in PLAYS.md (standing, steady pace, changes of
direction, men off the ball standing), plus: how many men set off within
a tenth of a second of each other (lockstep), how long after the man with
the ball changes direction his man does (reaction), how much paths bend,
and every existing "nobody through anybody" check. A step is in when it
moves these the right way and the suite (old or rewritten) is clean.

## Where it stands

**Step 1, the stepper** (src/ui/views/LiveGame/court3d/stepper.ts,
`stepTimeline` / `stageGame`; not switched on). Each man steps every
50 ms after where the play has him, steering at where it will have him a
moment from now (as far ahead as it takes him to pull up), never faster
than a little quicker than the play, getting going, turning and pulling
up no harder than a man can; easing up into anything of his coming up so
he is there for it without a dead stop; a beat to react (his own, 60-320
ms) setting off from standing, unless something of his is close; on the
play exactly through his acts and at the picture's cuts. Laid back down as
runs (with his pace at each end) where the way bends or his pace turns.

Measured on test game "a" and real games 11 and 22, against the director's
own runs: hard starts, turns and stops (over 40 feet a second, a second)
0.57% -> 0.04% of moments; men setting off within a tenth of a second of
another 58% -> 35%; acts where the play has them (within 0.05 feet); off
the play under 2 feet 99% of the time. It costs about 2.7 s on a game that
takes 12 s to stage, and lays down 1.7 times the runs.

**Steps 2-4 begun: what each man does of his own.** (2026-11-10)

- _The man with the ball_ (probesOf): on his dribble 1.2 s or more within
  40 feet of the rim with nothing of his on, he works his man. Stood, he
  sways across him, jabs a step in and back, gives a step and comes again
  (0.7-1.1 s a move). Going somewhere, he gets there his own way: crosses
  from one side of his line to the other (1-2.2 feet, switching sides two
  times in three), hesitates a beat and bursts (0.55-0.9 s a move). Eased
  back to where the play has him for whatever comes next.
- _Off the ball_ (openingsOf): stood 3 s or more within 30 feet with
  nothing of his coming, a man relocates a step - along the arc, in, out -
  then sets his feet 2.4-4 s (so the court's off-ball life, spotting up and
  calling for it, still plays); behind the line if he was, never off the
  floor.
- _His man_: the defender nearest a man goes with whatever that man does
  of his own here, all of it within 7 feet, none past 12, his own 90-200
  ms late; none of it into anything of his own (his acts, the ball
  changing hands). Otherwise he goes where the play has him _now_, seeing
  none of it ahead: the play has him reacting to his man already (mark,
  160-240 ms), and looking ahead undid it.
- _Cuts_: a man coming to a corner in his way (turning more than 60
  degrees) runs to it, plants and goes, instead of rounding it off.

Measured, real games 11 and 22 (as staged -> stepped): the man with the
ball standing 29% / 28% -> 15% / 12% of his dribbling; changing direction
on the move 1.1 / 1.7 -> 3.0 / 3.4 a minute; men off the ball standing
50% / 46% -> 44% / 40%; men setting off within a tenth of a second of
another 58% -> 35%; defenders matching their man's changes of direction
best 250-300 ms after him as staged, 200-300 stepped.

**The switch** (stageCourt.ts): `court3dStep` = "1" in a device's local
storage stages its games stepped (stageGame). COURT3D_STEP=1 steps every
test game (testGame.ts compile) - the suite run that way is the list of
what stepping still breaks.

Next: what the suite run stepped says (the strides on stepped runs first:
footwork, sprint and short-run checks), the rest of the defense (help,
rotations), then the cost (stepping adds about a third to staging).
