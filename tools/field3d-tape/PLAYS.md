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
| 5   | Kickoff and return (`kickoff`, `kickoffReturn`)    |        7 | `kickoff`    | working |
| 6   | Flags (`flag`, `penalty`)                          |       13 | `flag`       | working |
| 7   | Sack (`sack`)                                      |        4 | `sack`       | working |
| 8   | Extra point, two-point try                         |        4 | `kick`       | working |
| 9   | Field goal (`fieldGoalAttempt`, `fieldGoal`)       |        2 | `kick`       | working |
| 10  | Interception and return                            |        2 | `pick`       | working |
| 11  | Timeouts, two-minute warning, quarters             |       15 | `break`      | working |
| 12  | Injury (`injury`)                                  |        6 | `hurt`       | working |
| 13  | Kneel, fumble, onside kick, safety                 |     rare | `kneel`      | working |

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
away by the man the sim credits with it, batted down at the line, off his
hands, out of his reach and laid out for, sailed on over his hands, short
into the turf in front of him, thrown away with a man on the quarterback);
how near him it was and whether he was in his stride; what came after (his
hands on his helmet, the man who broke it up waving it off).

Batted down at the line: by the man in the rush the sim credits with it (a
step across into the lane if he has to), or now and then one in the lane
anyway with nobody credited - off his block as the quarterback winds up,
up with his arms as it goes by, the ball off his hands up in the air and
down behind the line, the man it was for pulling up when he sees it. Only
where the throw goes by him low enough to reach.

Now (132 incompletions): 129 different combinations. Over his hands 40,
knocked away 31, off his hands 24, dove for it 14, thrown away 11, short in
the turf 8, batted at the line 4.

To do: thrown behind an open man, the receiver's palms up at a ball short
of him.

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
cutting across in the return, at about 10 feet a second). Most of those
are jukes in the return, which are meant.

## 5. Kickoff

Parts: where it comes down (deep in the end zone, in it, at the goal line,
inside the 15, short of that) and across (round the middle, or aimed out
toward the numbers on the kicker's own count), how high; how it is fielded
(set under it, going back under one over where he lined up, coming up into
one short of him at a run, on the hop, juggled, caught and a knee in the
end zone, or let go into the end zone for the touchback); then the return.

The setup is the sim's: the cover at the receiving 40, the front five yards
off them, two returners at the 5 and the 2, and nobody but the kicker and
the returners moving till it is down.

Now (66 kickoffs): 59 different combinations. Out toward the numbers 33.
Set under it 41, coming up at a run 17, going back 6. On the hop 16,
knelt 10, juggled 2, let go 1.

To do: a kick out of bounds (the sim has none in these games), a return
man reversing field.

## 6. Flags

Parts: when it comes out (before the snap, in the play, after the
whistle), who throws it (and a second flag on it), how (thrown at the
spot, or up high on a dead-ball foul; standing, on the move, on the run),
whether he goes and tells the referee, whether the man flagged argues it,
and the referee's signal (the sim's foul).

Who throws: whichever of the officials who would see that kind of foul is
nearest. Off the ball at the snap or before it (a false start, offside, in
the neutral zone, a formation or motion foul) that is the line judge or the
down judge, on the man's side - the umpire, for a false start in the middle
of the line - and now and then a second of them throws too. A dead-ball
foul (before the snap, after the whistle) goes up high, not at a spot.

Now (115 flags): 63 different combinations. Thrown by the umpire 51 (from
76), the wings 35, the back judge 16, the referee 11; two flags on 8. Told
to the referee 22, argued 35.

To do: the yards walked off after the call.

## 7. Sack

Parts: another of them slipped first or not (spun out, stepped aside), how
long he had, what the quarterback did with it (stood in the pocket, stepped
up, drifted back, slid aside, got out of it), where the man
who got there came at him from, how he was brought down and by how many,
and the celebration.

On his own count the quarterback now and then drops past where they get
him and steps up into it, or - with the room behind the line - gets out of
the pocket away from the man coming and is run down out there. The man who
got the sack celebrates his own way (a flex, a fist pump, a leap, a dance,
a finger to the sky), his mates in on it. Now and then another of the rush
gets off his block and at him first, a moment before - the quarterback
spins out or steps aside from under it, eyes still downfield, and that man
is left on the turf where he was (only one coming from in front or behind,
with the man who gets him on the side he gets away to).

Now (38 sacks): slipped first 7 (stepped aside 6, spun out 1). Drifted back
15, stepped up 9, slid aside 7, stood 4, out of the pocket 3. Celebrations
13 ways (from 6, every one a flex).

To do: a duck under the first man, more from the blind side (2 of 38).

## 8, 9. Extra point and field goal

Parts: how far (a try, or the field goal's yards), how it went (down the
middle, inside an upright, wide left or right, short, off the upright),
how high over the bar, how it bent, and the kicker and the holder after it.

Each kicker's kicks bend his own way - a hook or a fade - more the longer
the kick, and still go through (or by) where they were going.

Now (48 kicks): 38 different combinations. Down the middle 15, inside the
upright 17, wide 13, short 3. Bending left 19, right 17, straight 12.

A two-point try is a run or a pass from the 2, so it is in those lines.

To do: a blocked kick (the sim has none in these games), the kicker's look
after a miss.

## 10. Interception

Parts: how far it was thrown, how the man got it (set, on the run, up off
his feet for it, laid out for it, off the receiver's hands), who got it,
what he did with it after (evades, how he went down, the gain), the
quarterback after it, and the defense's party.

Each man gets his own way: now and then it pops up off the receiver's
hands and he takes it coming down, his hands up for it the whole way; up
in the air for one thrown a long way; laid out for one with little to come
of it after, a dive the length a receiver's would be at his pace, and up
off the turf with it.

Now (14 picks): 14 different combinations. Diving 5, set 4, on the run 2,
off a tip 2, up for it 1 (all set before).

To do: a return with a move in it (no pick return has an evade), a pick
batted up at the line, the quarterback's own look after it.

## 11. Timeouts, two-minute warning, quarters

Parts: who called it (or the two-minute warning), how that side gathered,
how the other side did, the drinks, and the pictures (the coach's T, the
huddle, then the cheerleaders or the crowd).

Each timeout its own way: the side that called it in rows round the head
coach, or the same with the front row on a knee, or the quarterback (the
middle linebacker, for the defense) over to the coach on his own, the rest
in a ring of their own out on the field. The other side in its ring, on a
knee in it, or at its own sideline round its coordinator - always at the
two-minute warning.

Now (95 timeouts): rows 48, the quarterback over 29, on a knee 18. The
other side in a ring 48, at its sideline 31, on a knee 16.

To do: the quarter breaks (the change of ends is the same each time).

## 12. Injury

Parts: how he lies (on his back, on his face), whether one of his own
waved the trainers out, how many went down on a knee round him, how he
went off (helped, an arm over each trainer, or on his own), and how many
clapped him off - and from which sides.

Up off the knee as he is, men of both sides now and then clap him off,
turned to him as he goes.

Now (50 men hurt): on his back 36, on his face 14; waved out 28; helped off
36, on his own 14; clapped off by 3 or more 35, by both sides 32.

To do: a cart for a bad one (the sim gives no word of how bad). (Not the
crowd standing to clap him off: the picture over him going off looks down
at the field, no stands in it.)

## 13. Kneel, fumble, onside kick, safety

Kneels are the only ones of these the nine games have much of (16; one
fumble, no onside kick, no safety). Parts: what the lines did, what the
quarterback did after it, and where the ball went.

Most of the linemen now come up out of their stances into the man across
from them a moment, each pair on its own count, and let go - not all eleven
stood straight up together.

Now (16 kneels): the lines came together 14, stood up 2.

The ball tossed down at the end of any play (a knee too) now goes back to
an official when the picture stays on it long enough - flipped to one near
enough in front, or picked up off the turf by the nearest free one - and is
carried in toward the spot (variety line `back`: 20 of 1005 tosses in nine
games; most come under a second before the cut to the next play).

To do: the umpire setting it down on the spot, a wing flipping it on to
the umpire.

## Side notes

- Cover fouls (interference, illegal contact) are acted only when the man the
  sim names is near the target - the coverage isn't arranged so he is, so most
  aren't shown.
