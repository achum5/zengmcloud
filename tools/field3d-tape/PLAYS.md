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
| 3   | Pass incomplete (`passIncomplete`)                 |       15 | `incomplete` | working |
| 4   | Punt and return (`puntTeam`, `punt`, `puntReturn`) |       10 | `punt`       | working |
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

Now (9 games, 449 runs): 378 different combinations. A toss on 39 (runs to
the edge under center, to a back lined up deep). A cutback on 60 (inside
zone made to go one way, the back pressing it and cutting back behind the
line). A move to make a man miss on 36 (8%).

To do: a pitch on the option out of the gun, the move to make a man miss
weighted by who is chasing as much as by his elusiveness.

## 2. Pass complete

Parts: formation; the drop; where the throw goes (depth, side) and its arc;
the route; how it is caught (in stride, on the move, sat down; diving,
leaping) and how open; then the same ending as a run.

Now (417 catches): 402 different combinations. Caught standing still 37%
(hitches, curls, comebacks, screens and check-downs). Deep throws lofted a
little, more the further they go (of 103 tried: on a line 90, touch 10, a
lob 3). A man on him reaching a hand in at it as it gets there on 26 of
the 111 tight catches (in his stride or sat reading it, the arm on its
side).

To do: a back-shoulder throw, a contested catch fought for (both up for
it, a hand on it in the air), a catch at the sideline with his feet in
more often.

## 3. Pass incomplete

Parts: where the throw goes and its arc; how it gets away from him (knocked
away by the man the sim credits with it, off his hands, out of his reach and
laid out for, sailed on over his hands, short into the turf in front of
him, thrown away with a man on the quarterback); how near him it was and
whether he was in his stride; what came after (his hands on his helmet, the
man who broke it up waving it off).

Now (132 incompletions): 128 different combinations. Over his hands 44,
knocked away 30, thrown away 19, dove for it 16, off his hands 14, short in
the turf 9.

To do: batted down at the line, thrown behind an open man, the receiver's
palms up at a ball short of him.

## 4. Punt

Parts: how far and how high it goes and where across the field it comes
down (round the returner, or out toward the numbers; a shank low and end
over end, a pooch up and end over end); how it is fielded (set under it,
going back under one over where he lined up, coming up into it at a run,
juggled, a fair catch, let bounce and downed by the cover, into the end
zone); then the return (as a run's end, and its own yards).

The returner lines up at his usual depth leaning toward where it will go,
not on the spot, so a short one brings him up and a long one sends him
back. On a return of 10 yards or more the gunners are held up at the line a
while longer by the men on them, which gives him room to come up into it.

Now (86 punts): 76 different combinations. Out toward the numbers or the
sideline 43, end over end 20. Set under it 51, going back 8, coming up at a
run or on the move 12, juggled 6. A man made to miss on 11 returns.

The sim credits a tackle on almost every return, even of 0 yards, so fair
catches and punts let bounce are rare.

To do: a muff (the sim has none), the returner's catch with his arms in a
basket, a return that bounces outside off the wall. The game tape has more
sharp turns on punts since (27 of them, from 9: the returner and the cover
cutting across in the return, at about 10 feet a second).

## Side notes

- Cover fouls (interference, illegal contact) are acted only when the man the
  sim names is near the target - the coverage isn't arranged so he is, so most
  aren't shown.
