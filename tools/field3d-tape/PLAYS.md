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
a toss, the option, a draw, a counter); where he hits the line (inside, off tackle, outside,
wide, each side) and a cutback or not; a move to make a man miss (juke,
spin, hurdle, stiff arm); how it ends (tackled, swung down, driven back,
stood up, slid, out of bounds, in); how many bring him down; the gain (the
sim's).

The option: on its own count the end man on that side is left to the
quarterback, not blocked - off the snap a step up the field and in, reading
it if there is time, then at him as he pitches it and into him just after
it is gone. The quarterback is put down (on his back hit from in front, on
along the way he was going), and up a moment later; the end man on after
the ball.

The draw: out of the gun, an inside run with the back beside the
quarterback - about half of them on second or third and 7 or more, a fifth
else. Made to look like a pass first: the line back into its sets, the
back up into his set beside the quarterback, the quarterback back a step
with the ball up looking down the field; then he steps up and hands it to
the back coming up and in, and drifts on back with his fake. The rush
comes up the field into the sets (not as hard as into a run block), and
the linebackers drop back first and come up to fill late.

The counter: now and then on power to a back lined up deep, his first step
a jab the other way, then back across to the handoff and on behind the
pullers - the backside guard and, after him a step deeper, the backside
tackle, round and up through the hole onto the men filling it. A man up
front the tackle has left with nobody near enough to block him comes up
the field after it from the back side.

A man up front off the ball into the man blocking him gathers into him -
slowed over his last strides to about 7 feet a second, harder and a
little quicker if he is short of time - not into him at all he got up to
and stopped dead. So does a pass rusher straight into his man's pass set
(not one round the edge, nor the two on a twist, timed off each other flat
out). Off at a run into a run block (an end, say), his hands get to him a
half step off and he presses on in over the next moment as that takes the
pace out of him, and the shoving that follows comes on gradually from
there, not bending his run in. A linebacker (or anybody) met on the run
by a blocker climbing to him pulls up into it over his last strides, the
same way - and so does the blocker, a lead blocker up through the hole
or one out in front of a screen, breaking down into his man and his
hands pressed in (hard stops at contact on the game tape: 335 before, 39
now).

Stood up, the man with the ball braces for it - pulled up to about 10 feet
a second over his last strides, there a moment later, and on into them as
that takes the rest out of him - before they drive him back, not met at a
full sprint and sent straight back the other way; the men who pile in on
him do the same, along the way they come in, bent round or not. A man in
the secondary left on his feet after his tackle carries on through it off
the pace he got there at, as far as the man he hit goes down; the rest of
the defense after the ball plant first off a backpedal and pull up as
they get there (sudden slowdowns on the game tape 123 before, 25 now).

Now (9 games, 442 runs): 380 different combinations. A counter on 16, a
draw on 9. A toss on 42 (runs to the edge under center, to a back lined up
deep). The option on 3 (a run to the edge out of the gun, the back beside
the quarterback on that side: the quarterback down the line with it, the
back out wide of him and a step behind, pitched to him on the run, the
quarterback easing up after it - or put down by the end man, both of
these). A cutback on 56 (inside zone made to go one way, the back pressing
it and cutting back behind the line). A move to make a man miss on 30
(7%) - more often the more elusive he is, and, in a game with the ratings,
the poorer a tackler the man he goes past (a sure one about two thirds as
often as a poor one; as often as ever over them all).

## 2. Pass complete

Parts: formation; the drop; where the throw goes (depth, side) and its arc;
the route; how it is caught (in stride, on the move, sat down; diving,
leaping) and how open; then the same ending as a run.

Coverage, behind the look the defense shows: zone most of the time (with
two safeties deep 70%, one deep 55%), man the rest - always man with a
corner pressed, or on a back-shoulder throw or a double move. Two-deep
zone: the safeties a deep half each, the corners sitting in the flats, the
linebackers dropping under the routes. One-deep zone: the free safety in
the deep middle, the corners bailing to a deep third each, the strong
safety and the linebackers under them. Man: everybody on a man but the
free safety (two deep, both safeties, a deep half each). A man breaking
on the ball off his backpedal plants first.

A break in a route taken at pace (an out, a dig, a post, a back's angle
out of the backfield) is rounded off as tight as he can take it at the
pace he cuts at - not turned square in a blink; a curl, a comeback or a
double move's jab is still planted. A back carrying out a play-action fake
and staying in to block rounds off it into the line. Sharp turns at speed
on the game tape: 266 before, 178 now (pass catchers 145 to 56, most of
those left jukes after the catch).

Now (417 catches): 403 different combinations. Caught standing still 37%
(hitches, curls, comebacks, screens and check-downs). Deep throws lofted a
little, more the further they go (of 103 tried: on a line 90, touch 10, a
lob 3). A man on him reaching a hand in at it as it gets there on 16 of
the 101 tight catches (in his stride or sat reading it, the arm on its
side). Caught at the sideline with his toes dragged in on 15 (an out or a
comeback that goes out of bounds with next to nothing after the catch; the
variety tool counts them now - it missed them before).

Up for it at its highest on 7 (from 3): thrown deep, covered or into the
end zone, and no more than 6 yards left to go with it after - he comes
down with it and goes on. A jump ball fought for on 2 of those: the man on
him up off both feet with him, arms up at it, on his own count, never
into him.

Thrown to his back shoulder (now and then, to a man split out, 9 to 25
yards down): off the line like a go - a step outside and straight up the
field - pulled up a step past where it goes and back out toward the
sideline to it, turned to the quarterback, the ball there as he turns; the
man on him, his back to it, carried on up the field past him a moment
before he turns back. On 3 (from 0).

A double move (a slant-and-go) on about half the throws 15 yards or more
down to a man split out - caught with room to run after it, in the end
zone, or thrown on over him (not one the sim has somebody getting a hand
to): three steps up and a jab in as if on a slant, planted hard, and up
the field. The quarterback pumps at the jab when he is set in time; the
man on him comes up and in on it, plants, turns and chases him. The
variety tool calls it "off a double move" (in toward the middle quick,
then straight up the field to it, deep - none before it existed). Caught
on 2 to 4 in 9 games (which throws get one moves with the timing of the
plays before).

Not done: his toes in on a deeper throw along the sideline - in 9 games
only one catch out of bounds that deep with next to nothing after it.

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

On his own count, the man it was for now and then pulls up after one short
of him in the turf (about half the time) or sailed on over his hands (a
fifth), turns round to the quarterback, and puts his palms up at him.

Thrown behind him: on his own count, a throw to a man running across in
his stride now and then comes in behind him - one arm back for it, the
ball off the ends of his fingers and on past behind him into the turf,
him running on and pulling up.

Now (132 incompletions): 129 different combinations. Over his hands 42,
knocked away 31, off his hands 14, short in the turf 13, dove for it 12,
thrown away 7, behind him 7, batted at the line 6. Palms up 11, hands on
his helmet 12, waved off 18.

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

Set under it, on his own count he takes it in a basket about half the time:
elbows in, his hands low, the ball into his belly - not always hands up at
it.

A man made to miss in the return can come while he is still getting up to
speed, and one grabbing at him from the side is spun out of (as on an
interception return).

The wall set up for the return goes up to meet a cover man who would
otherwise get there only as it ends, so its blocks happen (in games 1-3,
75 of 104 wall pairs met, from 45). A hold by the punting team or by the
rush, with nobody near the man in the return, is acted at the line as it is
kicked.

Now (86 punts): 71 different combinations. Out toward the numbers or the
sideline 42, end over end 19. Set under it 56 (in a basket 26), going back
4, coming up at a run or on the move 12, juggled 4. A man made to miss on 10
returns (juked 5, spun out of 4, stiff-armed 1).

The sim credits a tackle on almost every return, even of 0 yards, so fair
catches and punts let bounce are rare.

To do: a muff (the sim has none), a return that bounces outside off the
wall. The game tape has more sharp turns on punts since (27 of them, from
9: the returner and the cover cutting across in the return, at about 10
feet a second). Most of those are jukes in the return, which are meant.

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

Now and then on a long return (20 yards or more from where he has it) he
reverses the field: off hard up and out toward one sideline - the other
side from where it ends -, round up the field at the far end of it, and
back across.

Now (66 kickoffs): 58 different combinations. Out toward the numbers 34.
Set under it 45 (in a basket 17, as on a punt), coming up at a run or on
the move 12, going back 7. On the hop 16, knelt 9, juggled 1, let go 2.
Field reversed 3, a man spun out of on 2.

To do: a kick out of bounds (the sim has none in these games), the
coverage turned back after a returner reversing the field.

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

Parts: another of them slipped first or not (spun out, stepped aside,
ducked under), how
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
with the man who gets him on the side he gets away to). Or, on his own
count about a third of those times, he ducks under him - down low with the
ball tucked at his chest as that man comes in high and goes on by over the
top of him, beside him - and up and on.

From his blind side: on his own count, with the man who gets him coming
round from his left, the quarterback's eyes are out to his right at the
last - on a man out there if there is one, side on to him the way he would
be to throw it - his back to the man coming, never seeing him.

Now (38 sacks): slipped first 8 (spun out 4, stepped aside 3, ducked under
1). Drifted back 15, stood 8, stepped up 7, slid aside 5, out of the
pocket 3. Head on 24, from the side 6, from behind 5, from behind on his
blind side 3. Celebrations 13 ways.

## 8, 9. Extra point and field goal

Parts: how far (a try, or the field goal's yards), how it went (down the
middle, inside an upright, wide left or right, short, off the upright),
how high over the bar, how it bent, and the kicker and the holder after it.

Each kicker's kicks bend his own way - a hook or a fade - more the longer
the kick, and still go through (or by) where they were going.

A kick missed (a try too), the kicker as a rule shows it his own way: hands
on his helmet, or on his hips with his head down, or down on a knee a
moment - pulled up first, facing the posts. Stood there with it, the holder
as often as not comes over behind him and pats him on the back.

Now (48 kicks): 39 different combinations. Down the middle 15, inside the
upright 17, wide 14, short 2. Bending left 20, right 18, straight 10. The
kicker after a miss: hands on his helmet 11, on his hips 4 (none on a knee
in these nine games). The holder over to pat him on the back 9.

A two-point try is a run or a pass from the 2, so it is in those lines.

To do: a blocked kick (the sim has none in these games).

## 10. Interception

Parts: how far it was thrown, how the man got it (set, on the run, up off
his feet for it, laid out for it, off the receiver's hands, knocked up at
the line), who got it,
what he did with it after (evades, how he went down, the gain), the
quarterback after it, and the defense's party.

Each man gets his own way: now and then it pops up off the receiver's
hands and he takes it coming down, his hands up for it the whole way; up
in the air for one thrown a long way; laid out for one with little to come
of it after, a dive the length a receiver's would be at his pace, and up
off the turf with it.

Most returns are short and still getting up to speed when they end, the
nearest man the receiver at his side: now and then he gets a hand on him,
and is spun out of.

The quarterback takes it his own way, much the same each time - hands on
his helmet, bent over with his hands on his knees, or his arms out at the
man it was meant for - once the other side's moment with it is over, and
now and then the picture comes in tight on him for it (from the press box,
when nobody is right in front of him and he isn't turned away).

Picked off right at the line, on the man's own count: knocked up there by
one of the rush up off his feet for it - him, if he is one of the front,
and down to him a step on from where he lands; or off another of them near
enough him, and him over under it with his hands up. The man it was meant
for pulls up once he sees it go up.

Now (14 picks): 14 different combinations. Off a tip 6, on the run 4, set
2, up for it 1, knocked up at the line 1 (his own; 2 before the routes
moved on). Spun out of a man's grab in the return on 1. The quarterback
after it: hands on his helmet 6, bent over 5, at the man it was meant for
1, after him 1, in on the tackle 1; the picture tight on him 5. Up off
his feet for it only from near enough to get there in the leap.

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

Between quarters (not at the half), the look round the stadium as the
teams start for the other end, the crowd doing the wave on some; then, on
about three breaks in five, the picture on the huddles - each side gathered
at its own sideline round its coaches, as at the two-minute warning (the 3D
court's huddles at a break) - and on from there to the next play.

## 12. Injury

Parts: how he lies (on his back, on his face), whether one of his own
waved the trainers out, how many went down on a knee round him, how he
went off (helped, an arm over each trainer, or on his own), and how many
clapped him off - and from which sides.

Up off the knee as he is, men of both sides now and then clap him off,
turned to him as he goes.

Now (50 men hurt): on his back 36, on his face 14; waved out 28; helped off
36, on his own 14; clapped off by 3 or more 35, by both sides 32.

How bad it is, where the box score says (the game knows by the time it is
shown): off on his own from a knock that keeps him out a week or none,
helped off from worse, down with the trainers longer from one that keeps
him out a long time; both hands to his helmet for his head or neck, an arm
held for a hand or an arm (the 3D court's hurt men the same way).

Out 8 weeks or more: the cart out for him - white, his team's color down
the sides, a roof over the front, a trainer driving - called for once the
trainers are with him, round to park beside him (whichever way in is
clearest, men stood about in its way stepping aside), him helped round
behind it, turned and sat on the back, and away with him to his sideline.

Not the crowd standing to clap him off: the picture over him going off
looks down at the field, no stands in it.

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
  sim names is near the target. A defender flagged for pass interference now
  lines up across from the man the ball goes to (his group's nearest place)
  and covers him, so he is on him as it comes where he can get there: 3 of
  the nine games' 8 shown (none before). Left: a deep safety named on a short
  throw to the far side, a flag with no stat naming the man, offensive
  interference.
