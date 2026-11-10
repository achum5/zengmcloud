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
