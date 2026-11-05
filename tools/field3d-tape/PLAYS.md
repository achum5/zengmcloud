# Football 3D plays, one play-by-play line at a time

Football's version of `tools/court3d-tape/PLAYS.md`. Every line the football
live sim gives, and how the 3D field plays it. One line is worked at a time:
list the ways it really happens, make the field able to show each of them,
check it looks right every time (the game tape's checks, the tests), and
check that over a game it doesn't feel like the same clip again (the
variety tool).

Per game: how often the line comes up (9 real-sim games, seeds 1-9).
Combinations: distinct whole combinations of the variety tool's parts.

## Order

| #   | Line                                               | Per game | Variety tool | Status  |
| --- | -------------------------------------------------- | -------: | ------------ | ------- |
| 1   | Run from scrimmage (`handoff` → `run`)             |       51 | `run`        | working |
| 2   | Pass complete (`dropback` → `passComplete`)        |       46 | `catch`      | working |
| 3   | Pass incomplete (`passIncomplete`)                 |       15 | -            | todo    |
| 4   | Punt and return (`puntTeam`, `punt`, `puntReturn`) |       10 | -            | todo    |
| 5   | Kickoff and return (`kickoff`, `kickoffReturn`)    |        7 | -            | todo    |
| 6   | Flags (`flag`, `penalty`)                          |       13 | -            | todo    |
| 7   | Sack (`sack`)                                      |        4 | -            | todo    |
| 8   | Extra point, two-point try                         |        4 | -            | todo    |
| 9   | Field goal (`fieldGoalAttempt`, `fieldGoal`)       |        2 | -            | todo    |
| 10  | Interception and return                            |        2 | -            | todo    |
| 11  | Timeouts, two-minute warning, quarters             |       15 | -            | todo    |
| 12  | Injury (`injury`)                                  |        6 | -            | todo    |
| 13  | Kneel, fumble, onside kick, safety                 |     rare | -            | todo    |

## 1. Run

Parts: formation and the quarterback's set; the exchange (handoff, keeper,
a toss); where he hits the line (inside, off tackle, outside, wide, each
side) and a cutback or not; a move to make a man miss (juke, spin, hurdle,
stiff arm); how it ends (tackled, swung down, driven back, stood up, slid,
out of bounds, in); how many bring him down; the gain (the sim's).

Now (9 games, 449 runs): 371 different combinations. A toss on 39 (runs to
the edge under center, to a back lined up deep). A move to make a man miss
on 36 (8%).

To do: a pitch on the option out of the gun, more cutbacks (40), the move
to make a man miss weighted by who is chasing as much as by his elusiveness.

## 2. Pass complete

Parts: formation; the drop; where the throw goes (depth, side) and its arc;
the route; how it is caught (in stride, on the move, sat down; diving,
leaping) and how open; then the same ending as a run.

Now (417 catches): 402 different combinations. Caught standing still 37%
(hitches, curls, comebacks, screens and check-downs). Arcs nearly all on a
line (a lob or touch throw 7 times).

To do: more touch on deep throws, a back-shoulder throw, contested catches
(20), a catch at the sideline with his feet in more often.

## Side notes

- Cover fouls (interference, illegal contact) are acted only when the man the
  sim names is near the target - the coverage isn't arranged so he is, so most
  aren't shown.
