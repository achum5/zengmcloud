# The 3D game as NBA basketball: plan

The goal: a game in the 3D picture that a basketball fan would take, at a
glance, for NBA basketball being played - not a simulation acting out a box
score. Four things stand between here and there, in the owner's order
(2026-11-17):

1. **Movement quality.** Weight shifts, planted feet, momentum. Today a
   path becomes a body by key poses; a real player's body carries how he
   is speeding up, slowing down and turning.
2. **Readable basketball.** A pick-and-roll, a handoff, a pin-down and the
   defense's answer to each (drop, hedge, switch, ice, blitz) that look like
   themselves - so someone watching can say what the play was.
3. **Variety and tempo.** Possessions that take as long as NBA possessions,
   men who move at NBA speeds and are rarely still, nobody moving in
   lockstep, no two trips that look stamped out.
4. **Contact.** Bodies that touch and move each other: screens hit, drives
   bump, posts and box-outs lean, rebounds jostle - and each hit shows.

This sits on top of the stepper plan (SIM.md) and the playbook (PLAYS.md,
playbook.ts): the sets are already real NBA sets weighted by how often they
are run, and ten men are already stepped between the script's moments. What
follows is mostly making those look the way they are meant to.

## Where it stands (2026-11-17)

- **The body.** Runs get up to speed and ease off as hard as a man can
  (motion.ts), carry speed round a corner, and plant and push off where a
  cut turns sharply; a foot down stays where it came down (footLock); a
  slide steps out foot by foot. Missing: the body leaning into how he is
  speeding up or slowing down, banking into a curve, the first step and the
  last steps of a stop as steps, arms and head that move with the run.
- **The plays.** 136 sets (102 half court) cast onto the five on the floor
  so the sim's shooter and passer land in the right roles; screens,
  handoffs, posts and five coverages are in the director. Not known: how
  much of it a viewer can read. Nobody has measured whether a screen is set
  where it should be, whether the ball is used off it, or whether a drop
  looks like a drop.
- **Tempo.** Real game 11 as taped 2026-11-14, at real speed only: men on
  the floor average 4.2 feet a second (2.9 mph) and stand still 43% of the
  time; 5% of the time they go over 15 feet a second. NBA tracking puts the
  average man at roughly 4 mph or more, and an NBA player is rarely still.
  (That tape is days old and counts dead balls: remeasured in step 0.)
- **Contact.** Men are kept off each other: never closer than 1.25-1.45
  feet, middle to middle (keepOff), except in a few set pieces (posts,
  box-outs, a screen's 1.35). Contact is something the picture avoids, not
  something it shows.

## Rules for all of it

- **Measure, then change, then measure.** Every piece below names its
  numbers. A change is in when it moves its own numbers the right way,
  doesn't move the others the wrong way, and looks better on film (a strip
  of frames, before and after, sent to the owner).
- **The picture before the staging.** A change to how a path is drawn
  (evaluate.ts, poses.ts) changes nothing the director decides, so the
  games play out exactly as they did and only the picture's own tests can
  move. A change to the staging (director.ts, stepper.ts) reshuffles every
  game after it, and the statistical tests wobble with it (the 2026-11-12
  math change turned up seven). So: draw-time work goes in freely; staging
  work is batched, runs the full suite, and a test that fails because the
  games changed is rewritten to what it was for, never just loosened.
- **Decisions live in the stepper.** Anything a man decides moment to
  moment (where he goes, who gives way, what coverage he plays) belongs to
  the stepper, and the director's patch pass that did it before goes when
  the stepper does it (SIM.md, steps 2-5).
- **Phones.** Staging a game takes 17 s on a desktop and the picture must
  hold its frame rate on a phone. A change that costs either says what it
  costs and pays for it.
- **The same game everywhere.** No randomness but the seeded draws and
  hashes, and no Math call the staging makes outside sameEverywhere.

## Step 0: the scorecard

One tool, run on the real games (11, 22, 33) and test games "a" to "d",
that prints every number below, so each change after it is a before and
after. Built on the game tape (gameTape.test.ts), plus what the tape does
not carry yet: each man's acts and feet, the director's sets and the
coverage played.

| Area     | Measure                                                                                                                      | NBA target                                                             |
| -------- | ---------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| Movement | Feet sliding while planted (inches per planted frame)                                                                        | 0                                                                      |
| Movement | Lean against acceleration (fit of trunk pitch to forward acceleration; bank to turn rate times speed)                        | a clear lean on every hard start, stop and curve                       |
| Movement | Pose jumps (a joint turning faster than a body can, frame to frame)                                                          | 0                                                                      |
| Readable | Ball screens a position-only detector finds, against the screens the director staged                                         | most of them                                                           |
| Readable | Screen set within a step of the man guarding the ball, at an angle to his way                                                | most of them                                                           |
| Readable | Ball used off the screen within a second of it being set                                                                     | most of them                                                           |
| Readable | Coverage depth: the screener's man, as the ball comes off (drop: back at the foul line or below; hedge: out past the screen) | as named                                                               |
| Tempo    | Average speed on the floor; share of time stood still; share over 15 feet a second                                           | NBA tracking (to be set in step 0 from NBA.com speed and distance)     |
| Tempo    | Shot clock used per possession, half court and early offense                                                                 | NBA averages (about 14-15 s a possession overall; to be set in step 0) |
| Tempo    | Lockstep (men setting off within 0.1 s of each other), reaction spread                                                       | down from 35% (SIM.md)                                                 |
| Tempo    | Repeats: the same move run the same way by the same man twice in a game                                                      | rare                                                                   |
| Contact  | Contacts a possession by kind (screen, drive, post, box-out, rebound)                                                        | each kind present where the play has it                                |
| Contact  | Middle-to-middle gap at contact                                                                                              | bodies touching, never through                                         |
| Contact  | Hard contact with a reaction drawn                                                                                           | all of it                                                              |

The tool is scorecard.ts (run with scorecard.test.ts and SCORECARD_GAMES).
Its first numbers, on real games 11, 22 and 33 as they stood on master
(2026-11-17, before any of the work below), only what is shown at real
speed with the ball in play:

| Measure                                              | 11           | 22           | 33          |
| ---------------------------------------------------- | ------------ | ------------ | ----------- |
| Planted foot sliding (inches a 50 ms frame)          | 0.60         | 0.61         | 0.59        |
| Trunk pitch: hard start / steady run / hard stop (°) | 21 / 14 / 10 | 22 / 14 / 10 | 21 / 13 / 9 |
| Banked into a hard turn at speed (°)                 | 2.1          | 2.4          | 2.6         |
| Speed on the floor (mph)                             | 3.28         | 3.28         | 3.19        |
| Share of time stood still                            | 41%          | 41%          | 42%         |
| Share over 15 feet a second                          | 5%           | 6%           | 5%          |
| Set off within 0.1 s of another man (lockstep)       | 66%          | 67%          | 67%         |
| Ball screens staged                                  | 153          | 132          | 130         |
| ... set within 3 ft of the man guarding the ball     | 53%          | 55%          | 57%         |
| ... found by a position-only detector                | 28%          | 42%          | 29%         |
| Drop coverage that looks it (big back in the paint)  | 66%          | 46%          | 40%         |
| Hedge / blitz that looks it (big up at the ball)     | 37% / 21%    | 0% / 9%      | 31% / 13%   |
| Switch that looks it                                 | 78%          | 64%          | 73%         |
| Hard contacts with a reaction drawn                  | 2%           | 5%           | 4%          |
| Closest bodies come (median, middle to middle, ft)   | 1.59         | 1.61         | 1.59        |

Step 0 also films a reel: twenty moments from a real game (a ball screen
each way, a handoff, a pin-down, a post-up, a drive and kick, a closeout, a
box-out, a transition run, a dead ball) as strips of frames, kept as the
"before".

## A. Movement quality (the picture: evaluate.ts, poses.ts)

Draw-time only, so it can start at once and runs alongside everything else.

1. **Lean into acceleration.** Trunk pitched forward as he speeds up, sat
   back and hips down as he brakes, banked into a curve by how fast he is
   going round it; how much from how hard he is speeding up or turning
   (which the runs already know).
2. **Starts and stops as steps.** The first step: a push off the back foot,
   a drop step or a false step on a quick start. A stop: a stride stop or a
   hop into a jump stop, strides shortening into it - never a glide to a
   halt.
3. **Cuts.** Plant the outside foot and push off it; hips turn first; an
   open step or a crossover step by the angle. V- and L-cuts read as cuts.
4. **Defense.** A slide that pushes off the trail foot; beaten, the drop
   step and turn and run; a closeout's chop steps; stance lower or higher by
   what he is doing.
5. **Feet, everywhere.** No slide of a planted foot in any join - run into
   act, act into run, act into act; a stance as wide as the move needs.
6. **Upper body.** Arms swinging by pace, shoulders turning against the
   hips, the head steady and level, eyes on the play; a weight shift and a
   reset when he is stood.
7. **Moving into a move.** A run carried into what it ends in - the gather
   off a dribble or a catch on the move, the layup off a standing catch
   (today 0 to 16 feet a second in a frame), a closeout into a contest.

Measures: feet sliding, lean against acceleration, pose jumps; film.

## B. Readable basketball (the staging: director.ts, stepper.ts, plays.ts)

1. **The ball screen as an event.** The screener runs to a spot set off
   the ball handler's man, at an angle to where the ball is going, and
   plants (feet set, wide, hands in); the ball handler sets his man up
   (a step away first), comes off shoulder to hip, and turns the corner -
   timed so the screen is set before he uses it.
2. **Coverage that shows, chosen as teams choose it.** Drop: the big back
   at the foul line, backpedaling, the guard over the top. Hedge: the big
   out past the screen, then back to his man. Switch: the two swap, and
   it shows. Ice: the ball kept off the middle. Blitz: two on the ball.
   Chosen by the team and the men in it - a slow big drops, a lineup of
   wings switches - so a team has a defense you can recognize.
3. **What follows the screen.** The roll to the rim, the pop, the slip;
   the low man tagging the roller; the kick to the corner and the
   rotations behind it (the x-out).
4. **Off-ball actions.** Pin-downs, curls, flares and staggers with the
   chaser trailing or cutting under; handoffs played over or under.
5. **Transition.** Lanes filled wide, a rim runner, a trailer; getting
   back - sprint, stop the ball, find a man.
6. **The shell.** 5-out and 4-out spacing, the nail and the gaps, help-side
   depth by where the ball is - in the stepper, retiring mark, keepApart
   and stopTheBall as it goes (SIM.md steps 2-3).

Measures: the screen detector, screen placement and use, coverage depth;
film.

## C. Contact (the stepper and the picture)

1. **Separation that allows contact.** Each pair of men gets a distance by
   what they are doing: apart in open play as now, touching in a screen, a
   post-up, a box-out, a drive into a man, a fight for a rebound. Who gives
   way, and how much, by strength, size and who is moving into whom
   (ratings: strength, height and weight).
2. **Screens hit.** The man guarding the ball runs into the screen and is
   held up by it, fights over the top through it or goes under it; the
   screener absorbs it and holds his ground, moved a little.
3. **Drives bump.** Shoulder into chest, a hip check, an arm bar from a
   beaten defender, a finish through contact (the and-ones and charges the
   sim already calls).
4. **Posts and box-outs lean.** The post player backs his man down a step
   at a time by strength; a box-out sits a man on another and both move.
5. **Rebounds and loose balls jostle.**
6. **Every hit shows.** A small move for each - absorb, bump, stumble,
   recoil, push off - drawn where the staging says contact happened.

Measures: contacts a possession, the gap at contact, reactions drawn; film.

## D. Variety and tempo (staging and picture)

1. **The clock.** Shot clock used per possession, half court and early,
   matched to the NBA's; each team at its own pace; late in the clock, the
   rush.
2. **Each man's own speed.** Top speed and how quick he gets there from
   his ratings (speed, size, age) - and slower as he tires, from his
   minutes on the floor since he last sat (the play-by-play has the subs).
3. **When men react.** Spread by who he is and what he reads:
   a sharp defender sometimes early, a poor one late.
4. **No two trips the same.** More ways of each move (`ownWay`),
   and men off the ball that never stand stock still - a shift of weight, a
   step, a look.
5. **Moving more.** The share of time stood still and the average speed
   brought to the NBA's - by the shell and the actions above, not by
   jiggling men in place.

Measures: speed and standing against NBA tracking, shot clock used,
lockstep, repeats; film.

## Order

- **Step 0 first:** the scorecard and the "before" reel. Everything after
  is judged against them.
- **Then two tracks at once.** The picture track (A) needs nothing else and
  shows on every frame. The staging track goes in batches, each one suite
  run: the shell into the stepper (B6) and contact-aware separation (C1),
  which go together; then the ball screen, its coverages and what follows
  it (B1-B3, with screens hitting, C2) - the single most-run action in the
  league, so the biggest win in readability; then tempo (D1-D2); then
  off-ball actions and transition (B4-B5) with drives, posts and rebounds
  (C3-C5); variety (D3-D5) along the way.
- Each batch ships with its scorecard numbers, before and after, and its
  film.

## Done

- **2026-11-17, picture (A1, A6, C6).** All of a man tipped over his feet
  with how hard he speeds up, pulls up and turns (evaluate.ts tipAt, from
  where he goes 0.2-0.4 s either side; poses.ts overFeet, his feet left
  where they were and his legs reaching to them). Two men coming together
  hard rock back off each other; touching a while (a post-up, a box-out)
  they lean on each other (joltAt, worked out two seconds of the game at a
  time). Drawing only: the director's own looks are drawn plain, so the
  games are the same. Real game 11: hard contacts with a reaction drawn
  2% -> 77%, banked into hard turns 2 -> 4 degrees.
- **2026-11-17, fixes found reshuffling the games (none reshuffles them
  itself: late passes, the stepper, the arms).**
  - A run cut down to a sliver (under 60 ms) by what went in after it runs
    on into the one it goes straight on into (noJumps; not at a cut, where
    he is just put in place) - it had a man going 146 ft/s for a frame.
  - Holding his ground at a jump shooter (holdHisGround) never takes out a
    run that carries a man past 9 ft from him: one off after the ball as
    the shot went up was left standing, the ball snapping 17 ft into his
    hands off the floor.
  - Stepped, the way in is cleared once more at the very end (after
    keepOff, the steps aside eased and taken on the floor): contesters
    were carried back to 0.9 ft of a finisher. And no empty step aside
    with endless times when a man never came nearer than GIVE_WAY + 0.3.
  - The stepper's crossovers checked against where the stepping really
    takes him: a pair whose crossing goes against the way he goes is left
    out, and so is one with a hand of his up meanwhile, pointing or calling
    for it (crossesWithHim). Every move across his line gets one to start
    with (CROSS_ON_CUT gone): those left out are the now and then he keeps
    it where it is. Test game a: all 9 counted going his way.
  - Clearing the lane (three seconds) leaves a man where he is when his
    own hands are to be at the ball there before he runs anywhere - a pass
    knocked on off them out of bounds was coming to where he had been.
  - Stripped, a man wants the call; his pass picked off, his hands on his
    head (lostIt, arms only).
  - The "coming together hard" test evaluates each man once a moment
    (603 s -> 84 s here).
- **2026-11-17, the man with the ball and the man on him (part 2).** On
  the ball the defender is marked to an arm's length (CUSHION 3 -> 3.6 ft)
  and gives ground from 3.2 ft out past 12 ft from the rim (was 2.4) - not
  backing a man down. A dribble run that would go straight through a man
  of theirs goes by his hip (atHisHip) - not one into a charge, a strip or
  a block, nor one into backing his man down. Test games a, b: the man on
  him within 2.5 ft 14.4% -> 12.6% of the dribbling, within 1.8 ft 3.5%
  -> 3.2% (as pushed, after the post-up fix). Tried and left out: a deeper spot for him while the play is
  staged (it cut the stepper's crossovers by two thirds), probes in at his
  hip (a man off where the play has him), keeping the man in front of the
  ball 3.2 ft off (the charge taken falls on top of him).
- **2026-11-17, the ball kept away from him (part 1).** Stepped, a man of
  theirs close in front on the side he has it, the man with the ball takes
  it over to the other hand on the beat before (ballAway: a crossover in
  front; between his legs only stood with the man tight on him - a move
  through his legs or round his back set off on the move starts from
  elsewhere than the top of his dribble, and the ball jumps). Back in the
  hand he started in by what comes next - unless that is a pick-up and a
  pass, which can come up from either (a shot, a layup, goes up from the
  hand it was to). Test games a, b: on his side 45.2% -> 39.2%; crossovers
  in front 250 -> 475, through the legs 114 -> 125. The stepper's own
  crossovers are checked against where the stepping takes him, and so are
  not these (marked away on the ball).
- **2026-11-17, B2: the screener's man ready for the coverage.** In the
  step a ball screen is set, the screener's man sits where the coverage
  wants him: in a drop back toward the rim (DROP_BACK 9 ft off the screen,
  between 5 and 13 ft out of it), to hedge, blitz or switch up level with
  it (SHOW_OFF 3). With it, and the games reshuffled by it:
  - A man posting up has his man on his back as he starts backing him down
    (the guard sets off earlier, by how far he has to come), and keepOff
    leaves the two of them together.
  - A defender put right on a jump shooter by the stepping always gets a
    hand up (wallUp once more after stepping).
  - Screens set before the man they are for comes by (setBeforeHeComes): a
    screener stood within 5 ft of him in the 200 ms after the set starts
    sets 500 ms before he comes, if he is still (within 0.8 ft) then -
    staged, his run there squeezed up if it can go 1.5x as fast and no
    faster than 20 ft/s; stepped, only where he stands still. Test games
    (director3, 100 screens): set late 14 -> 13. Left: the screener 2-6 ft
    off his spot as the man comes by (to set it in time he'd have to be
    moving into it).
- **2026-11-17, off the last move he goes.** Measured on B2's games:
  out of a hesitation, a jab or an in-and-out the man with the ball was
  stood there 0.6 s later 25 times in 27. Working his man where he stands
  (workHisMan), when the stretch ends in his drive (a run on his dribble
  of 5 ft or more the rim's way, setting off within 400 ms) or his
  pull-up, the moves he puts on his man stop short of it and a last one -
  into the pull-up the hesitation, into a drive that, the in-and-out or
  the jab - ends as he goes. Few stretches end in one (most end in a pass
  or a short probe forward): hesitations 62 -> 65, in-and-outs 21 -> 25,
  jabs 48 -> 49 in test games a, b. "A handler now and then gets his
  pull-up off a hesitation" passes again. Seen on film: a screener set
  chest to chest between the man with the ball and his man - next.
- **Test timeouts on a 4-core machine.** The footwork tests' 120 s limits
  are tight here: master itself times out on "a shooter drifting along
  the arc side-steps" (152 s) run beside one other job.
