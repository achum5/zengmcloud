# Realistic Front Offices

Status: design. The old smart AI is gone, the arena (below) runs and the
framework is agreed; the front office itself is being built as a thin slice. Decisions below were made by the repo owner;
anything not written here is still open and gets asked, not assumed.

## Goal

CPU teams that run their franchises the way an elite human GM would, short term
and long term, in every situation a human GM can find themselves in - trading
with humans and with each other, in single-player and in multiplayer. Every CPU
team is elite and wants what is best for its franchise.

There are no GM personalities or characters, and nobody gets fired. You deal
with "the Pistons", not with a named person.

## Ground rules

- **A separate league setting, "Realistic Front Offices" (on/off).** Off is stock
  BBGM, and stock behaviour is not touched by this work - not even where stock
  looks wrong (difficulty also changes what CPU teams ask of each other and how
  readily their players re-sign; that stays). The owner plays a real league on
  stock while this is built.
- **Basketball only.** The setting does not appear in the other sports.
- **In League Settings** (Game Modes), off by default (owner's call, to try
  the mode while it is built). What it changes so far: CPU-to-CPU trades,
  how CPU teams judge a human's trade offer and what they offer humans
  (counters, the trading block, Trade Proposals), re-signing and free agency
  for every team, the draft, free-agency testing and mood terms.
- **Existing leagues never switch on their own.** New leagues default to it only
  once the owner has played it and is happy with it.
- **Stock's difficulty does nothing when it is on.** The trade, signing and
  overall difficulty settings stop applying: no handicap on CPU teams, no
  revenue or free-agent penalty on humans. The mode has its own, unrelated
  scale (owner's call, see trades below).
- **The old "Smart AI Front Offices" system is removed, not reused.** Its
  Franchise Outlook page goes with it; a new page showing this mode's view of
  each team comes later, shown only when the mode is on.
- **Multiplayer is first class.** Everything works across devices.

## What a team is trying to do

- **No fixed point table.** A front office gives itself the best chance to be as
  good as it possibly can - right away if it is in that spot, otherwise as soon
  as it can be - the way a person runs a team. Title chances are the currency;
  a deep playoff run only breaks ties between moves that leave title chances
  the same, and the arena checks the result looks like real NBA behaviour.
- **Patience:** each season out counts 90% of the one before, so a title five
  years away is worth about 60% of one this season.
- **Titles count most**, added up over roughly the next decade.
- **As sure of its read as it has earned.** Title chances come from playing
  the seasons out many times with each team's strength off by as much as
  the read of a team really is: about 1.5 points a game this season,
  measured against how teams did in three arena leagues (the first slice
  guessed 3.5), and spreading as a random walk does seasons out - 3.6, 4.9
  and 6.0 points one, two and three out. Surer of who can win, a contender
  pushes for it and a team that cannot waits. Half the teams each way, 5
  leagues each way round over 5 seasons: 30 titles to 20 (more or as many
  in every league), 368 playoff rounds to 382, and 65 seasons under 20 wins
  to 49.
- **Playoff runs count for something**, but never at the cost of real title
  chances later. Selling the future for a 2% title chance and a 75% chance at the
  conference semis is a franchise disaster, not a strategy.
- **"Stuck in the middle with no direction" is a failure** we measure and test
  against.
- **Money: a small real cost** (owner's call, replacing "the luxury tax and
  profit are ignored"): a team does what is best for the franchise, money
  included. Each season, payroll past the league's minimum costs 0.02 titles
  per cap's worth, and the luxury tax's rate on top past the tax line.
  Payroll counts the free agents a team's room is expected to buy, at their
  market salaries: room it would spend anyway costs nothing more spent on
  someone else, room it could not spend is money kept. In the season under
  way, the tax counts on the payroll as it will stand when the playoffs
  start, when it is charged, and only the rest of the season's salary past
  the minimum. Cap room is used strategically to chase free
  agents when that applies, and a team re-signs its own players over the cap
  (Bird rights) when that is good for its outlook. With money free, a team
  over the cap paid its own players whatever made them likeliest to stay -
  the max to 21-year-olds rated 50. Half the teams each way (2 leagues x 2
  seasons): re-signed at the max 7 against 17, a 56 or worse at 15% of the
  cap or more 0 against 4, re-signings 7.5% of the cap against 9.4%, free
  agent overpays 2 against 5 - and as many players stayed (rookie deals 71%
  against 72%, the top ten veterans 88% against 87%). At 0.05 a cap, fewer
  players finishing rookie deals stayed (63% against 77%). Charged only past
  the cap at first, salary under it was free to a team with room it could
  not use, and once room was measured to buy less, bad teams gave the max
  for three or four years to young players rated 48-56 (6 in 4 leagues x 2
  seasons; charged from the minimum, 1). Half the teams each way: as many
  wins, as many players finishing rookie deals kept (77% against 79% of 58
  or better), long overpays 0 against 2. A team going nowhere may now shed
  a veteran it pays more than he is worth to it.
- **Losing costs what the game charges for it** (owner's call: only the
  game's real costs, whatever is best for the franchise). A player's mood
  toward his team counts its record, twice as hard when it is losing, and his
  mood decides whether he re-signs. Priced from the team as it stands, a move
  that left it far worse was priced as if its players would feel about it as
  they do now - so a team could tear itself down to 3 wins still counting on
  keeping whom it kept. Now a move's change in the team's record each season
  moves the mood of everyone whose contract runs out after that season (the
  game's own formula, regressed as finishes drift back toward the middle),
  and the roster is priced again with it when that comes to a quarter point
  of mood. Half the teams each way, 4 leagues each way round over 6 seasons:
  29 titles to 19, about as many games and playoff rounds (352 to 368), 56
  seasons under 20 wins to 81 and 8 under 12 to 19, with 29% of rosters
  priced twice (an arena season 1,220 s against 974 - kept, owner's call); bad teams signed more free agents (148 of 50 or better to 117) and left big room unspent half as often. Counting hype as well, and
  what the next summer's room buys, teams shied from losing far more - 5
  seasons under 20 wins to 41, in 2 leagues each way - and won 8 titles to
  16: in this game a deep rebuild still wins titles, and those costs were
  overweighted. Left off (losingParts).
- **No tanking through lineups.** A rebuilding team sells for the future through
  trades, but on the floor every team plays to win.

## How players are judged

- **True ratings.** CPU teams see them and never reveal them.
- **By what a player adds to winning in this game's sim, not by ovr** - his
  exact ratings run through simulated games, not this season's stats. A 62 ovr
  can be below league average in production while a 50 ovr is above it; humans
  exploit that gap today. The sim is ground truth and CPU teams can measure any
  ratings profile against it, including lineup fit (the sim's synergy).
- **Lineup fit counts through title odds** (owner's call: a team cares about
  synergy when it is building a winner). A season's fit counts exactly as much
  as that season matters for a title: fully for a contender now, almost
  nothing for a rebuilding team now, and in a rebuilding team's projected
  window, by that window's odds.
- **Projected forward** with the game's own progression model. Checked on
  1,570 draftees (3 leagues x 10 seasons; the arena's drafts log carries each
  one's projection as he was drafted): a year to four years on, their overall
  followed it 0.98, 0.99, 0.96 and 0.93 times as far as projected, and picks
  outside the top five came in 1 to 2.5 points above it by years 3 to 5 - late
  picks are not overrated.
- **Learned ahead of time, never simmed live.** What a set of ratings is worth
  is learned from millions of simulated games and shipped as a model; CPU teams
  do not run sims of their own during play (owner's call: live sims would bog
  the sim down, and a few hundred games is mostly noise - one 400-game swap test
  carries about 0.8 points a game of luck).
- **Any league settings.** The model is fit across a spread of sim settings
  (pace, the shooting, steal, block, turnover, rebound and foul factors,
  quarter length, ...), so it holds in any league, not just default or the
  owner's.
- **The box score adds nothing once the ratings are known.** Measured: a
  season's BPM helps a model that only knows overall, and adds exactly zero to
  one that reads the full ratings.
- **Talent does not add up.** The sim hands each shot to one of the five by
  usage, so two stars dilute each other: summing single-player values over a
  roster overstates the gaps between teams by about 40%. Rosters are priced
  whole, with each player's share of the ball, plus lineup synergy.
- **Positions matter on the floor.** The sim starts 2 guards and 2 forwards
  or centers and won't sub a man in if that leaves fewer than 2 guards (or a
  PG) or 2 forwards (or a C) on the floor, so a roster short of guards plays
  its scrubs and tired starters there. The shark bot found it: CPU teams gave
  their guards away too cheaply. Measured by swapping teams' guards for
  forwards of the same value and back (positions lab): in random-players
  leagues a quarter of rosters are short of guards, and the model missed by
  up to 5 points a game without it; with it, its error on whole teams fell
  from 1.49 to 1.23 points a game on fresh leagues. A player whose position
  is not known yet (a pick, a free agent a team may sign) counts as a
  typical one, and a projected player at the position his ratings will put
  him. Checked again on 3 fresh leagues (2,466 swaps, below), a shortage of
  guards cost 1.28 points a game against 0.95 predicted (bigs: 0.13 either
  way). 1.2 times the guards' cost fit a little better (error 1.210 against
  1.213 points a game) but tipped a CPU team into taking picks 30 and 58 for
  pick 15 (the owner's trading-up exploit) - two picks' players counting as
  part-guards in its future rosters - so it stays at the first fit.
- **Lineup fit, checked against the sim** (synergy lab, owner's concern):
  2,466 swaps of a player for one of the same value at his position with or
  without each of the sim's synergy skills (shooting, ball handling,
  passing, athleticism, post scoring, perimeter and interior defense,
  rebounding), 300 games each, 3 leagues. The sim moved with the model's
  synergy 0.93 ± 0.08 times as much as the model said (its players 0.93 ±
  0.02), and every skill's swaps landed within about 0.3 points a game of
  the prediction: CPU teams read fit as the sim plays it. In the arena's
  trades no contender's fit got worse by more than half a point a game by
  the model (294 trade sides, 3 leagues x 3 seasons, each played out 200
  games).
- **What else it misses is small and spread out.** 1,200 random swaps of a
  top-8 player for one of the same value (swap lab, 2 leagues): the model's
  own error on a swap is about half a point a game, and neither the swapped
  players' ratings, nor their skills, nor where they sat in the rotation,
  nor the size of the swap explains much of it (athletes are underrated by
  about a third of a point).
- **A roster of players who may not be there is priced as it would play.**
  Each place in the pecking order goes to whoever is likely to fill it, given
  everyone's chance of being there; the most valuable uncertain players are
  played out both ways exactly. Pricing everyone as there and taking each
  absence off separately broke down on rosters seasons out, full of players
  who may have left, picks and free agents a team may not land: removing a
  bench prospect could look like it made a team two points better. A place's
  lineup synergy is priced the same way, from the skills of everyone who may
  play it by his chance of playing it: handing it the likeliest player's
  skills made synergy jump whenever that changed hands, so a long shot with
  a 5% chance of being there could move a roster by a point. A pick's player
  or a free agent a team may sign, whose skills are not known yet, brings
  those of a typical player of his overall - not a minimum player's, which
  cost a lineup about half a point for every projected starter in it.
- **Injuries yet to come count.** Everyone is there for 8% fewer games than
  the injuries he has now leave him, played out as the roster plays without
  him: players on a team all season missed 6.5% of its games at 12-18
  minutes a game and 9.5% at 30-36. Counting only the injuries a team had,
  a roster leaning on one star came in worse than read and a deep one
  better - against how 300 team-seasons went, 0.14 points a game worse for
  each point its best player's overall stood above its top eight's - and
  the read missed by 2.05 points a game. Counting them, the star's share
  halves and the miss is 1.97. A star with little behind him loses 1.18
  points a game to injuries to come, a deep roster as good 0.29. Half the
  teams each way, 2 leagues each way round over 5 seasons: as many titles
  (10 to 10), 153 playoff rounds to 147, 12 seasons under 20 wins to 22,
  and 4 to 6 points more of their games won in the third and fourth
  seasons. No slower.
  Weighting it by minutes instead (a starter about 10%, the end of the
  bench almost none) did worse: 3 titles to 7 and 60 playoff rounds to 90
  in a league each way round, and a looser read (2.09 points a game to
  1.98). Left off. So did weighting it by age (the game's injury rate rises
  3% a year past 26; a roster whose top eight were a year older came in
  0.19 points a game worse than read): 8 titles to 12 in 2 leagues each way
  round, and 27 seasons under 20 wins to 20. Left off.
- **Retirement by the game's own rule.** Nobody on a roster retires before
  34; after that, the older and the less valuable, the likelier (a starter
  at 35 almost never, the end of the bench often).

## Framework

One rule for every decision - trade, signing, re-signing, pick, cut, lineup:
take the option with the most expected titles (each season out at 90%, deep
runs as the tiebreak). No tiers and no "if rebuilding, do X" rules; contending,
rebuilding, buying at the deadline and avoiding the middle come out of the
math, because title chances rise steeply with strength and the same player is
worth far more to a contender than to a lottery team.

Five layers:

1. **On-court value**, learned from mass sims: a whole roster (full ratings,
   rotation, sim settings) in, points a game against an average team out.
   Rosters are always evaluated whole, never as a sum of player values - that
   is what stops consolidation exploits and prices fit and roster spots.
2. **The future**, from the game's own code: progression and aging, injuries,
   retirement, mood and contract demands, the lottery. CPU teams know the odds
   of every outcome, never the outcome.
3. **How teams fill holes**: picks become prospects, cap space becomes
   signings, empty spots become minimum players. What those are worth depends
   on how the league's GMs behave, so it is learned from arena leagues run with
   the new front offices, and refreshed as they improve.
4. **League outlook**: every team's strength each season for about a decade,
   plus what each asset is still worth after that, turned into title and
   playoff odds - and from it, what any asset is worth to any team and what the
   market would pay for it.
5. **Decisions**: candidate moves scored by their change in franchise value.
   Deterministic, so every device agrees.

Graded on results: each model against fresh mass sims, the arena against NBA
numbers, version-against-version leagues (a change ships only if the new
version wins more), and a shark bot that hunts for mispriced trades using the
sims as ground truth.

## Direction without rigidity

- Each team keeps a direction (contend now, retool, rebuild) stored in the
  league.
- **Noise does not move it.** A losing streak is not a reason to sell.
- **Real changes move it immediately.** A torn Achilles on the favourite's star
  opens the window for the next tier of contenders, who get aggressive and buy.
- Every move is still judged on its merits, so an opportunity off the plan is
  still taken.

## Always on

Teams work the phones every day, all season. Sims get slower; how much slower is
acceptable is decided once the real cost is measured against how much smarter
it makes CPU teams, but it must not be egregious. Speed work waits until the
behaviour is right (owner's call); the first slice adds about 0.7 s a sim day.
Measured since (arena, one season of a random-players league): about 800 s
against about 50 s stock. Looking for trades is 41% of it (the last week
before the deadline, when every team shops each day, is about 40% of the
in-season looking), free agency 19% (each of the best free agents priced for
every team, again whenever a signing changes the pool), memory cleanup 17%.

## Trades

- **Core rule: a CPU team never makes a trade it would not make on its own.** A
  human's offer can never create a deal it would not take from anyone.
- **A contender does not arm its rivals.** Title chances are shared: what a
  deal adds to the other team's comes out of everyone else's, this team's
  too. Each season a team's title chance is scaled by the share the other
  team leaves, after the deal against before. Priced as if the rest of the
  league stood still, a 7.5% contender made a 72% favorite better for a gain
  of 0.3 hundredths of a title - as it would a human contender. Half the
  teams each way (4 leagues x 2 seasons): those pricing their rivals made
  fewer trades (140 sides to 186), better in hindsight (+22 to -12). The
  rival is priced only into deals that could clear the bar, so the search is
  hardly slower.
- **Friction** (owner's call: deals between CPU teams went too easily). In 5
  arena seasons half of all CPU trades had a first-round pick in them - 38
  went for players rated under 58, one team gave 4 in a season - 6 players
  were traded twice in a season, and some teams made 4-6 trades a year.
  Three frictions real front offices have:
  - **Firsts are scarce.** Each first a team gives costs it more, by its own
    value of the pick, the larger the share of the coming drafts it would
    have none in - less for a team within shooting distance of a title, and
    nothing from 10% title odds this season on: winning now is worth firsts
    and future firsts (owner's call). Late firsts are worth little to a
    contender's own title chances - more to it than to the rebuilding teams
    that would buy them, so pricing them at what others would pay changed
    nothing; it is running low that makes them dear. At first a team also
    never left itself without a first in two drafts running (the Stepien
    rule); taken out (owner's call: valued, not forbidden).
  - **Flipping costs.** Trading on a player traded for in the same stretch of
    trading (a season's games, or the offseason with the preseason after it)
    must gain 0.002 of a title more for each - so a seller still passes on a
    veteran it took as salary filler, but nobody churns. At first it was not
    allowed at all (owner's call: a cost, not a rule).
  - **Deal fatigue.** Between CPU teams, each trade a team has made this
    season raises what its next must gain by 0.002 of a title.

  Half the teams each way, 2 leagues each way round over 5 seasons, with the
  first versions (the Stepien rule, no flips): those with friction made 18%
  fewer trades, gave 112 firsts to 203 (70 to 136 for players under 58),
  flipped 11 players to 50, had 2 team-seasons of 4 or more trades to 17 - and
  won 23 titles to 17, as many playoff rounds (302 to 298), with 39 seasons
  under 20 wins to 52. No slower.

- **The side a deal does least for asks for more.** The search prices a few
  packages and takes the deal that does most for the side it does least
  for; that side then haggles - one asset at a time from what the other team
  puts on the table, or one fewer of its own - as long as it comes out
  further ahead and both still clear their margins. Taking the first deal
  both would make, a team at the deadline gave two starters on minimum deals
  for a 19-year-old rated 34, clearing its margin by a quarter of a
  hundredth of a title while the contender buying them cleared its own by
  two. Half the teams each way (4 leagues x 2 seasons): haggling teams were
  the short side of 67 trades to 111, clearing their margins by 0.81 of what
  the other side did against 0.73 (medians), better in hindsight (+13 to -9).
- **Not the deal best for the team shopping** (measured, left off): taking,
  of the deals both would make, the one that does most for the team out
  shopping - the other still haggling - found one worth 0.045 of a title to
  it where the usual pick was worth 0.005, but half the teams each way, 2
  leagues each way round over 5 seasons: 9 titles to 11, 155 playoff rounds
  to 145, as many games won.
- **Searched as wide as it is** (measured, left as is): picks five drafts
  ahead on the table, not three, 10 titles to 10 and 149 playoff rounds to
  151 (2 leagues each way round); twenty deals a pair priced in full, not
  ten, 6 titles to 4 but 63 playoff rounds to 87 (a league each way round).
  Pricing a team's cuts as cuts.ts makes them, not by lowest value: 16
  titles to 14, 217 playoff rounds to 233 (3 leagues each way round),
  about 10% slower. All left off.
- **Shopping.** When you offer a CPU team a deal, it compares three things:
  keeping its players, your offer, and the best deal it can get elsewhere by
  asking the other CPU teams instantly. Yours is best: it accepts. Someone
  else's is better: "Trade rejected. They have a better offer elsewhere." -
  not whose or what (owner's call), so the other team cannot be worked on
  before coming back to the first. A CPU team never takes less from a human
  than another CPU team would give it.
- **No bidding** (owner's call, replacing matching windows and pending
  deals): what a user sees and does is vanilla's - trade screen, trading
  block, "What would make this work", Trade Proposals. Deals go through as
  soon as both teams agree. Only the CPU teams' judgment is the mode's.
- **A higher bar for a human's deal** (owner's call): a human picks which
  deals to offer, so finds the ones the model gets wrong in his favour; the
  margin of error counts 1.5 times as much in a deal with a human (at the
  realistic difficulty), tuned by the shark bot (src/test/shark): an expert
  human hunting each season for the deals the judge takes that help him most,
  graded on the game's own sim.
  At 1.5 the CPU team still comes out ahead by its own read after its misreads
  (2.5 ± 0.8 hundredths of a title a deal, 7% of deals leaving it worse off,
  4 leagues x 3 seasons); 1, 2 and 3 misread no less, so it stays at 1.5. What
  the shark gains is mostly bought with its future, as a contender would.
  The one blind spot it found, guards, is fixed (positions, above): run again
  on the fixed model (4 leagues x 3 seasons), no deal the shark made left the
  CPU team worse off by its own read after its misreads (7% before), and its
  edge by position is gone (guards and wings were 0.6-1.0 points a game).
  Hunting the CPU's misreads instead of its own gain, it finds deals misread
  by 0.75 ± 0.29 points a game, still a gain for the CPU after them.
  Run again with draft picks' upside and letting picks go (4 leagues x 3
  seasons): the shark gains 1.9 ± 0.5 points a game a deal (2.9 before), the
  CPU misreads by -0.12 ± 0.35 and is still ahead after it (2.5 ± 1.3
  hundredths of a title). Its deals are a rebuilding team's young player (on a
  rookie deal in 68% of them) for older players.
  Run again after the vanilla-mechanics pivot (3 leagues x 2 seasons, 22
  hunts of about 5,000 candidates each): 4 deals made, 0.7 a league-season.
  The shark gains 2.7 ± 0.5 points a game a deal; the CPU loses 0.84 ± 0.45
  this season against 1.23 its model expected (misread +0.39 ± 0.32, none
  beyond the sim's noise), and is ahead after it (5.9 ± 0.8 hundredths of a
  title, carried); no deal left it truly worse off. It gave older, better
  players (27, 59 overall) for younger ones (25, 54) - its own choice of the
  future over this season.
- **What a no says** (owner's call): "They'd rather keep what they have",
  "They have a better offer elsewhere" (only when another CPU team would make
  that deal by its own read) or "They would rather wait for the deadline" -
  never how close the offer came.
- **Undo is unchanged** (owner's call).
- **Front Office Difficulty** (owner's call: the mode's own scale, unrelated
  to stock's, which does nothing to trades here; the trade screen no longer
  prints the CPU's read to the console). Realistic, the default, is the even
  playing field: a CPU team as sharp as a human, no help either way. The
  levels scale how much the margin of error counts in a deal with a human:
  Easy 0.75 times the realistic bar (more willing to cater to him, as stock
  is), Hard 1.25, Insane 2. Everywhere a CPU team decides on a deal with a
  human: his offers, "What would make this work", Trade Proposals and the
  trading block.
- **Human-to-human trades are unchanged** (agreed in chat).
- **CPU offers to humans** (owner's call: vanilla's mechanics, the mode's
  valuation). Each names the CPU team's price: of the deals it would take, by
  its own read over the human bar, the one it gains least from - good for the
  human or not, he decides.
  - **Trade Proposals** works as in stock: the same teams in the same order,
    the same reroll, one of the human's players or picks picked for each team
    the same way, and more added to either side as stock adds them. What the
    team offers for it is its price, adding up to three assets a side, and
    only if it would not rather give its own up elsewhere. To keep the page quick each team gets one
    try (stock: ten) and twelve teams at most are asked; the offers are kept
    until the page would change them anyway (5 offers in about 10 seconds in a
    test league, instant after).
  - **The trading block** (owner's call, replacing the fair rule there, which
    left it empty - a good starter put up for picks drew no offer at all) is
    where the human sees what the market would pay: each CPU team bids up to
    three of its assets, one at least of the kinds asked for (picks alone when
    only picks are), salaries left out - the human makes the money work. The
    stock trade AI's values size the bid (by its own read a contender's bids
    in picks all came out as its three best, for anyone): the biggest they
    call fair for the CPU team, worth a third of their price for what it gets
    at least, and made only if the team would take the deal by its own read,
    over the human bar, and would not rather wait for the deadline. A bid is
    not shopped: whether its assets would fetch more elsewhere is weighed when
    the human offers the deal (shopping every bid took the block from a second
    or two to most of a minute).
  - **"What would make this work"** (owner's call) names the CPU team's
    price: of the deals it would take that keep everything in the human's
    offer (the offer itself if it would take that), the one it gains least
    from, adding at most one asset of its own - good for the human or not,
    he decides. Held to the fair rule as well, it named a price for one ask
    in ten (a good player, not a team's best, asked for nothing); naming its
    price, for seven in ten.
- **The whole calculation behind each CPU-to-CPU trade** is on its trade
  summary page, with God Mode off too for now (owner's call), though it shows
  true ratings.
- **Talking (later).** CPU teams explain their side of a deal the way a real
  front office would, without giving their hand away.
- **Trade volume: whatever the gains say** (owner's call). Every deal both
  sides gain from by their own reckoning gets made; in the arena that is about
  75-80 a season against the NBA's 55-62, and the trade checker confirms the
  on-court gains are real. No minimum is tuned to match the NBA.
- **Timing: teams value waiting** (owner's call). Before the deadline a team
  weighs a deal now against deciding at the deadline - when it knows better
  how good it is, but has lost those regular-season games with the new roster
  and may have lost the deal. Either side preferring to wait puts it off.
  How much of a deal is still on offer later is measured, not assumed (owner's
  call): six weeks out, the best deal a team can make at the deadline is worth
  about 60% of its best today, so waiting rarely pays and trades stay spread
  through the season rather than bunching at the deadline as in the NBA.
- **An injured player back for the playoffs is worth his playoffs.** This
  season's strength counted each player for the share of the regular season
  he would play, so a star out for the rest of it but back for the playoffs
  was nearly worthless this season - a contender could be had at the
  deadline. Before the deadline, this season's strength is now the regular
  season's moved toward the playoffs' (everyone back who will be by then) by
  how much the deadline view's fit of title chances leans on playoff
  strength.
- **A deadline rush** (owner's call: on, then tuned toward the NBA). In the
  last 7 days before the deadline every team shops each day, as in the
  offseason windows - with two teams shopping a day, a given pair was looked at
  about once a month, so a deal put off for the deadline was seldom seen
  again. Every team also shops once ratings progress in the preseason (the
  NBA counts training camp deals as offseason ones), and this season's title
  odds count a team's record as well as its strength, so the standings the
  deadline reveals enter its deals. Arena, 4 leagues x 5 seasons: in-season
  trades 12 -> 16 a season (NBA ~27), the deadline's final week 10% -> 41% of
  them (NBA ~75%), trades 30 -> 36, firsts traded 8.7 -> 11; the trade market
  takes about half again as long.
- **Deals clear the margin of error** (owner's call). A front office only
  makes a deal if each side gains by more than the model's own error on that
  gain: what a player's ratings are worth on the floor (measured against swap
  tests), plus how much further careers land from the projections than their
  own spread allows (measured on every player traded in arena leagues - the
  projections are unbiased, and only a little wider in reality). The luck the
  model already prices in, like a prospect's range of careers, is not error.
  Each moved player's error counts at the minutes he is expected to play each
  season - a prospect's at the place he will hold once he is good, not at the
  end of the bench where he sits today.
  With the margin, trades fell to ~23-32 a season against the NBA's 55-62,
  but stars no longer churn (re-trades 63% -> 32-39%). Pricing prospects'
  doubt at their future minutes, with the roster fixes above, took trades
  from 36 to 19 a season with the deadline rush on (arena, 4 leagues x 5
  seasons: re-trades 36% -> 17%, stars moved by trade 6.4 -> 2.9 a season
  against the NBA's ~3.6, a prospect given for a veteran of 30 or more 2.4%
  -> 0.3% of trades); with the offer market, 43 -> 15 (owner's call: accept
  the lower volume). The trade market takes about a third longer.
- **A prospect's upside counts.** A player's seasons ahead are averaged over
  many simulated careers - calibrated: within a point of how traded players
  turned out, at every age. But a roster gives the player who turns out well
  a starter's minutes and the one who doesn't none, so the average undersold
  anyone whose future is uncertain: a teenager projected to average 44 sat
  at the end of the bench in every season priced, though one in eight of the
  prospects CPU teams traded for veterans of 58 or worse became 60 or better
  within four seasons (3% projected to on average). Each projected season is
  now played as the worse and the better half of his careers, each half as
  likely, where the two are 4 or more apart: a 21-year-old 47/66 costs his
  team about 4.6 times as much to give up. Projections' spread is close to
  real, if a little narrow: in arena leagues, traded players' overalls a
  season to three on fell within their projected sd 58-65% of the time at
  every age (68% would be exact) - upside, if anything, undersold. In a league of half the teams each way (4 leagues x 5
  seasons), teams pricing upside gave a prospect for a mediocre veteran on
  2.2% of their trade sides against 8.0%, and won 13 titles to 7; over ten
  seasons (4 leagues, with picks' halves too), 2.0% against 7.8%, titles
  20 to 20, playoff series won 0.56 a season to 0.44.
- **So does a pick's.** A pick's player was the average of what players taken
  there become, the star and the bust averaged into a bench player: a middling
  team's first was worth almost nothing to a bad team. He is now the worse
  and the better half of their careers too (the 15th pick five seasons on:
  a 48 or a 60, against an average of 51), and the team keeps or lets go
  each half after his rookie deal - by then it knows which he turned out to
  be. A middling team's first is worth 2-16 times what it was, the most to
  the worst teams. Half the teams each way (4 leagues x 5 seasons): those
  pricing picks this way won 15 titles to 5 and 0.60 playoff series a season
  to 0.40, gave a first to take on a weak player on 1.3% of their trade sides
  against 7.5%, and took in 136 firsts to 21 - half of them from good teams
  to bad, as in the NBA.
- **Picks are priced over where they may land** (owner's call). How far a
  team finishes from where it is projected is measured in the arena (about 4
  places a season out, 6.5 three seasons out), and the lottery moves it from
  there. Most of that is luck, so it goes into the pick's value, not its
  margin: a pick is worth the average over every place it may land. Its margin of error is only
  the misread of how good its team will be, by the outlook's own estimate of
  that. From the lottery on, a pick priced as one prospect of the class
  carries his error as a player would; before, deals of this draft's picks
  carried none (39 of 285 arena trades were pick-only deals at the draft), and
  pick 15 went for picks 25 and 31. In the arena, pick-only deals at the
  draft fell from 1.6 a draft to 0.1, trades at the draft from 3.5 to 3.3.
- **Picks land back toward the middle.** Teams drift back toward the middle
  of the draft more than projecting their rosters forward says - others get
  better and worse too, rosters turn over. Measured in the arena against the
  standings (below): of a team's projected distance from the middle, its
  finish really keeps 90% a season out, 60% three seasons out, 27% five out.
  A pick's expected place is pulled toward the middle by that much; taken at
  face value, a bad team's picks years away looked far better than they
  were, and it kept selling.
- **Picks go through the league's lottery.** A pick was priced by where it
  was expected to land in the draft, the lottery lumped in with the luck of
  the standings as if it were even both ways - so once the standings were
  settled the worst team's own pick was priced as the first. Now a team's
  finish is projected - far more surely than its pick: 2 places off half a
  season out, against 5 for where the pick landed, the lottery being most of
  that (4 arena leagues against the standings) - and then put through the
  league's lottery: the game's own odds for each finish, or under COLA (the
  owner's league) the draw for the first four picks by each team's points,
  its own pick in it only while it holds it, and the rest by record. In the
  arena under COLA the worst team drew a top-four pick 40% of the time and
  picked fifth to eighth otherwise; teams finishing 7th-12th from the bottom
  drew one 22% of the time. A team's pick held by another is never drawn.
  **Under COLA each team's chances are projected along its own path**: what
  it has, plus a season's share for each season it is likely to miss the
  final three rounds, less what its likely playoff runs and draw wins cost
  it, as the game charges them - not as if every team added the same each
  season. A contender's own picks seasons out are worth less (it keeps
  spending its chances), a team out of contention's more. Half the teams
  each way, 3 leagues each way round over 5 seasons: as many titles (15 to
  15), 241 playoff rounds to 209 (more in every league), and more seasons
  under 20 wins (29 to 18) - a team out of contention builds its chances
  by staying out of the playoffs, and does. Contenders on it gave their own
  firsts more readily.
  Before this season's lottery is run, its picks go by the final standings
  (they were priced by next season's projection).
- **A deal moves the picks of the teams in it.** A pick's place comes from
  how good its team is, and a deal changes that: both teams' own picks are
  priced where the deal leaves the two rosters. Before this, a team gave up
  its star and priced its own pick as if it still had him - losing its best
  player moves a team's own first about 9 places, worth 0.025 titles on
  average and up to 0.15 - so "their star and their pick" was a steal, and a
  team selling its star priced the buyer's pick as if the buyer had not got
  him. A signing moves a team's own picks the same way. The trade page's
  front office breakdown shows how far a deal moves each team's picks.
- **A pick is priced at places a tenth apart, and in between by how far
  along** - within 0.01% of pricing it at its exact place. Cached by the exact
  place, the picks priced grew by about 20 MB a trading day; by the place
  rounded, a deal's price hung on what had been priced before it, so two
  devices could answer the same offer differently.
  - **It makes tanking pay, and that stands** (owner's call: "Each FO
    should be in charge solely of building a winning franchise"). With picks
    priced honestly, a team going nowhere sells its veterans to contenders
    and a contender buys: arena, 2 leagues x 5 seasons, trades rose from 20
    to 69 a season and the league polarized - win spread 17 against the
    NBA's 12.4, 4 sixty-win and 6.5 sub-25-win teams a season against 1.6
    and 3.7. Limiting it to the next two drafts changed little (49 trades,
    spread 17). No pull toward winning now for fans' or owners' sake is
    added.
  - Version against version, 10 seasons (4 leagues, run before signing a
    player to get worse was stopped): teams pricing their own picks where a
    deal leaves them won 46.5% of their games against 53.5% for teams that
    did not, and as many titles, 20 each - 5 to 15 in the first five seasons,
    15 to 5 in the last five. They sell first and win later.
- **Deals are looked for among what the other team wants.** Packages of two
  or three assets are built from those the other team values most over what
  they cost the team giving them. Built from what each team valued most
  itself, a contender only ever offered its best players, never the young
  players and picks a rebuilding team wants: of every pair of teams in three
  leagues, deals both would make went from 2, 0 and 4 to 22, 6 and 12.
- **Every kind of deal that happens in real life** (owner's call), as long as
  the CPU's judgment holds up: a side may give nothing (a player into another
  team's cap room, a salary dump), picks may go for picks, and every tradable
  player is on the table, not just a team's best. A pick is never worth less
  than nothing: a drafted player not worth a roster place is cut, which costs
  nothing straight after the draft - so no team gives picks for nothing, even
  where its outlook prices one a hair below nothing (2 of 430 arena trades had
  a team hand a second-rounder over for nothing at all). Rosters only have to
  fit once games are played. A side gives up to three assets - three of its six most valuable
  for a star is a deal humans make all the time.
- **Offseason trading windows** (owner's call): every team shops once the
  draft order is set, right after the draft, and once teams have re-signed
  their own players, and free agency's days have more shopping. In the NBA
  about half of all trades are in the offseason. Trading between picks while
  the draft runs comes later, once the structure is settled.
- **Trade penalty** (player mood hit for a team that trades too much) is
  unchanged. CPU teams are subject to it and account for it. The hope is that the
  league can drop it once CPU teams are hard to fleece.

## Free agency: always the offer market

With the mode on, re-signing and free agency are the offer market's, below
(owner's call, 2026-11-02). There was a switch for stock's free agency with
the mode's front offices (players judging every team on the same terms, CPU
teams choosing by the mode's valuation at the market's asks). It was taken
out: on the same terms, a free agent signs only with a team he is willing to,
and few are with a bad one - a team that won under 30% of its games signed no
free agent of 50 or better in 43 summers of 2 arena leagues, with 58% of the
cap to spend on average (stock's CPU teams sign whoever is best, willing or
not: about one a summer), and leagues split far more than stock's (16% of
team-seasons under 20 wins, stock 2). In the offer market money can make up
for losing. Pricing room by what it bought under stock's rules changed
nothing (half the teams each way over 6 seasons: 11 titles to 13, bad teams'
room unused 75 summers of 84).

## Re-signing

- **Every team on the same terms** (owner's call, provisional): a player
  judges a human team and a CPU team by the same mood factors. Stock's extra
  free-agency testing for CPU teams' players, and its damping of their mood,
  do not apply, and difficulty does not change mood.
- **Every team's players test the market the same amount** when their
  contracts run out (owner's call), tuned in the arena so top free agents
  re-sign about as often as in the NBA (~57%). Human teams' players leave more
  often than in stock as a result.
- **Except players finishing rookie deals**, who mostly stay, as with the NBA's
  restricted free agency. Testing them too cancelled their rookie-contract
  mood bonus: a bad team's best prospect was priced as leaving after three
  seasons, so trading him for an old veteran looked smart.
- A front office prices each player's chance of re-signing with the game's own
  mood model, with the team he would be re-signing with - so a player traded
  for is valued on how he would feel about his new team.
- **That chance is turned into how often players really stay.** His chance of
  taking his asking price is not his chance of still being there: the team
  offers what is best for it, often more, and some who test the market come
  back. In the arena players of 50 or better priced at 7% stayed 40% of the
  time, at 91% 82%, about as often whether priced the season their contracts
  ran out or years before. Priced at their asking price, a bad team's good
  players - most of whom want out - were all leaving, so it sold them for
  little: of the CPU trade sides that lost most in hindsight, 58% gave away
  their best player priced at under 25% to stay.
- **And by how good a veteran is.** That table was measured on players of 50
  or better; a team keeps a veteran far more often the better he is. Of 417
  whose contracts ran out (arena, under the current market), those valued
  under 45 stayed 5% of the time whatever the table gave them, those of 60
  and up 72-93%: priced at the table, the bench veterans of bad teams were
  given 45% and stayed 13%. Refit by the table's odds and his value, the
  log-likelihood of what happened went from -0.65 to -0.47 a player.
  Players finishing rookie deals stay as the table says. Half the teams each
  way (4 leagues x 2 seasons): those pricing it won 0.543 of their games to
  0.457 the second season (0.513 to 0.487 the first, before it counted), and
  their trades did better in hindsight (+49 to -35).
- **Refit since, and left as it was.** Logged as each deadline's final week
  opened against where players were the next season (4 arena leagues, 1,810
  veterans), the table and value overrate staying for good players: a
  veteran's team keeps him more often the better he is up to a value of about
  55, then hardly more - of those of 60 and up given under 10% to take its
  offer, 21% stayed where they were priced at 65%. A refit forecast what
  happened better (log-likelihood per player -0.58 to -0.55) - and
  its teams did worse: half the teams on it, two leagues each way round over
  six seasons, they sold veterans for picks (net +33 first-rounders) and won
  2-5% fewer games from the fourth season on, 7 titles to 17. Many who leave
  were let go because they asked for more than they were worth to the team;
  priced as a loss, that undervalues the choice of keeping them only when it
  pays. The refit stays in the code for the arena (retentionFit).
- **A pick's player is kept past his rookie deal only if he is worth his
  second contract** - if the team is better with him than with what his
  salary buys as room over every season ahead, decided the season his rookie
  deal ends (on that season alone, it let young players go who were about to
  get better). Priced as kept at whatever he asks, a mediocre one ate room
  that buys better, and the worst teams valued a fourth extra first below
  nothing. A deal is priced with the team's decisions on its roster as it
  stands, made again only for a pick the deal brings in or a season whose
  room the deal moves by more than a tenth of the cap.
- **And a pick's player not worth his place is cut straight after the draft,
  for nothing**, as the game allows. Kept on his rookie deal regardless, a
  second-rounder who would sit cost room that buys better, and a team gave
  its own second away for nothing. Each of these decisions is by what the
  seasons it touches are worth to the franchise, as its trades are.

## Free agency

- **Cap room is worth what teams with that much room actually sign** (measured
  in the arena, by room as free agency opens), not what a signing at that price
  gets: teams with room do better than that, and what they get rises smoothly
  with room. Pricing it by the price of a signing gave clearing a max slot a
  special value, so teams dumped prospects' future salaries for it. What room
  buys moves with how teams bid, so it is measured again as that changes, a
  half step at a time (teams act on the table they are given): once money
  past the cap cost something, with no room a bad team landed someone better
  than the minimum 30% of the time, not 74% (1,200 team-summers).
- **And by how good the team is.** Free agents want to win: with 30% of the
  cap a team that won a quarter of its games signed a 56 first, an average
  one a 61, a good one a 66 (arena, 1,380 team-summers). Priced the same for
  every team, a bad team valued room it could not use, cleared it by giving
  good players away and stayed bad. A team's room is priced by the share of
  its games its roster alone is expected to win that season, drifting back
  toward the middle seasons out as finishes do - a bad roster now is not a
  bad team for good, and a long contract eats room it may want.

- **CPU teams sign by the valuation they trade with** (owner's call): what
  signing a player on given terms adds to the team's title chances, less the
  margin of error - picking the best of many free agents favours the ones the
  model overrates.
- _Superseded by the offer market below:_ signing at the player's ask with
  teams weighing whether to wait for it to fall, asks brought halfway down each
  day toward the most any team would pay, and the length picked for the team
  from his asks. The market keeps the parts that held up: a CPU team offers the
  money and length worth the most to it given its odds of landing him, and he
  prices each length as contract negotiation does.
- **Players finishing rookie deals mostly stay** (owner's call). Veterans'
  stay rate was tuned to the NBA's unrestricted free agents; in the offer
  market a player weighs his own team's offer against what testing the market
  looks like it would bring - the teams he would like that could pay his ask -
  with no extra pull either way, and teams that ignore money pay to keep him.
- **An offer market, every team on the same terms** (owner's call, replacing
  the board above): any team, human or CPU, offers whatever money and years it
  likes, and the player chooses among his offers with odds - how much he likes
  each one, fitted so the chances add up to 100%. An awful team with a bad
  offer and a bad fit has next to no chance. A human's offer waits for his
  decision like anyone else's; there is no instant signing.
- **No signing a player to get worse, or past the roster limit for free.** A
  signing never moves the team's own picks earlier - a poor player the
  rotation would play ahead of better ones is no way to a better pick - and is
  priced on the roster the team will have once games are played: past the
  limit, it cuts the players it values least and still pays them. Before, one
  team went from 13 players to 33 in a free agency; replayed, 81 signings
  rather than 144, 13 rather than 70 of players under 45. An offseason trade
  is priced the same way, both teams on the rosters they will have; only
  players under contract then count, since one whose contract is up leaves or
  re-signs.
- **Players decide daily, stars early** (owner's call): each day a player with
  offers may sign or wait for more, so the best go in the first days and role
  players later, as in an NBA July.
- **What a player weighs** (owner's call): money and years (pay against what he
  is worth, and the length he wants at his age), winning, his role and minutes
  with the team, and his existing mood traits.
- **Blind bidding, with a daily suggested price** (owner's call): no team sees
  another's offer. Each day the player's agent names a price for each length,
  from a market formula: about the second-highest amount any team would pay
  him (as an auction clears), so it falls as teams spend their room.
- **Offers are ranked, not capped** (owner's call): a team may have out more
  offers than it could afford together, ranked by priority. If two of its
  targets say yes the same day, the higher one signs; an offer that no longer
  fits falls through and that player chooses again.
- **The offer screen shows your odds** (owner's call): your chance if he chose
  today against the offers he has. CPU teams bid knowing the same.
- **How the market is set up** (measured in the arena):
  - A player weighs an offer by the log of its money against his ask for that
    length, plus 0.04 per point of mood toward the team (its winning, hype,
    market, facilities, his traits, and the role he would have there), with
    a temperature of 0.1: a 10% better offer is about 2.6 times as likely to
    be chosen, a point of mood about 1.5 times, as on the multiplayer board.
    An offer at his ask from a team he is neutral on is worth the same to him
    as waiting.
  - The price is the auction's: the second-highest a team after him would
    pay, where a team is after the few players it gains most from at today's
    prices, as many as it has places, within the cap rules - so asks fall as
    teams fill up and spend their room.
  - A CPU team offers, for each length, the money that does the most for it
    times its odds of landing him at that money, among offers that clear the
    margin of error; the margin is a bar, not a cost (as a cost it made long
    deals look worse than short ones). Every free agent is first looked at
    for the length he wants and for a single season.
  - What a signing is worth by its money is priced at the minimum, at the
    player's market salary and at the maximum, in a straight line between.
    Drawn on from his market salary through the maximum, a team with room to
    spare lost nothing to the money, so paid a 55 the max for three years
    for a better chance of landing him (3 times in 24 free agencies; 2.8% of
    signings gave 15% of the cap or more to a 56 or worse, 0.4% in stock).
    Half the teams each way (4 leagues x 2 seasons): those with the third
    price made 0 long overpays to 5 and paid 4.1% of the cap a signing
    against 4.9%, won 0.516 of their games to 0.484 and 5 titles to 3.
  - Room in the free agency under way is priced, at what teams with that room
    sign, shrinking as the best free agents go - left free, a one-year deal's
    premium cost a team nothing and prime players got one-year deals half the
    time.
  - Testing the market, to a player re-signing, is worth what the three teams
    he would like most could pay him: a role player many teams could pay
    expects more from the market than a star few could.
- **Re-signing stays its own phase, on the same odds** (owner's call): the old
  team gets the first chance; he takes its offer or tests the market, weighing
  it against what he expects free agency to bring.
- **Re-signing shows the odds and lets you work them** (owner's call): in the
  re-signing phase each of a human team's players shows his chance of taking a
  default offer; raising or lowering the money or years moves it; one button
  rolls them all and shows who stays. Unrolled offers are decided as free
  agency opens. CPU teams' players decide when the sim reaches the phase. A
  player's roll is fixed for the season, so deciding again changes nothing.

## Draft

- A pick is priced by the prospect the team expects to take there, from its own
  board, and the team drafts off that same board.
- **The board is the prospect's value to the franchise** (owner's call): true
  ratings through the same projections and title odds as everything else, on
  the rookie deal of the pick, so talent rules and fit and timeline break
  near-ties - a rebuilding team leans to upside.
- **Talent first** (owner's call, as above): prospects are ranked with the
  floor's positions left out - he plays for rosters years away, reshaped by
  then - and positions choose only among those within 5% of the best. With
  positions in full, a roster's guard shortage had teams take a point guard
  over a better player (19% of first-rounders were point guards; stock 15%).
  Draft lab (12 leagues, the whole class drafted each way, each pick's
  expected impact in years 2-4 from the game's own development sims, slot by
  slot): talent first takes 0.08 ± 0.05 points a game more than before in
  the first round and 0.13 ± 0.06 more than stock's best-rated (before, 0.05
  ± 0.06 more than stock); the second round alike.
- **From the lottery on, the draft's picks are priced by the actual class**
  (owner's call): by who would go there off a board of the class by what each
  prospect is generally worth, kept up to date as players are taken - not by
  what the slot usually yields, so a deep or a thin class moves its picks'
  prices, and a human who knows the class is strong can't buy its picks at
  the average.
- **Before that, a draft whose class is already in the league could be priced
  by it too** (the owner's league export; built, and left off - knownClasses):
  every league holds the next three
  classes (Draft Scouting), and a real-players league every real draftee to
  come - Wembanyama's 2023, Flagg's 2025. A pick lands where its team is
  expected to finish, moved by the lottery, as always; what each place is
  worth is now who would go there off the class's board, not what the place
  usually yields. Prospects keep their draft-day ratings in the pool, so each
  is projected from his draft on, once a league. In the owner's league at the
  2021 draft, firsts in 2022, 2023 and 2025 rose by up to two or three times,
  2024 and 2026 stayed about where they were, and 2027's - a class of real
  high schoolers whose best is 42/71 at his draft - fell toward nothing.
  (A pick table rebuilt from 100 Realistic Draft Prospects classes came out
  the same as the generated classes' one: it is the real classes that
  differ.) Half the teams each way, 5 leagues each way round over 5 seasons:
  its teams won a little more in the second and third seasons (1-2% of
  games) and a little less in the fourth and fifth, with 23 titles to 27,
  367 playoff rounds to 383 and 64 seasons under 20 wins to 59. Left off.
- Against stock's drafting (arena, 2 leagues x 5 seasons, 6 drafts judged
  three seasons on): pick order predicts a player's overall a little better
  (rank correlation 0.59 against 0.58), the top 5 picks rate 60 against 58,
  and of each draft's 10 best players 5.8 went in the top 10 against 5.0.
- **Moving up in the same draft is never a cost.** Once the order is set, a
  pick is priced as the player who would go there off the class's board - but
  at an earlier pick a team can still take whoever it would have taken at its
  own later one, so the earlier pick is worth no less to it. Priced each as its
  own player, the Bucks found the 12th worth less to them than their 13th
  (they liked the 13th's player more) and, with each pick's doubt counted in
  full, asked the owner's Thunder for a first and a second on top to swap
  (owner's report). Now a deal counts at least what it would without the
  picks a team moves up with, a straight swap up is taken as offered, and two
  picks in the same draft count only the gap between their doubts.
- **Trading between picks comes later** (owner's call). Still to do (owner):
  price a pick this draft by whom the team itself would take there and the
  chance a better prospect falls to it, not one player off a general board.
- Teams understand whatever lottery system the league uses, and build to it.
  The same front offices with no lottery (the worst record picks first) as
  under COLA (2 arena leagues of 5 seasons against 70 COLA seasons): 4.4
  teams a season under 20 wins against 1.9, the three worst averaging 13.1
  wins against 17.9, 7.6 teams in the middle against 10.0, and 12 firsts
  traded a season against 7.5 - the bad teams taking them in. The NBA's 2027
  lottery is priced by the game's odds for each finish; its limits on how
  high a team that was just in the lottery can pick are left out (a few
  hundredths of one or two teams' pick value a season).

## Roster cuts

- **A CPU team over the roster limit releases whoever it can best do
  without**: the player whose release leaves its franchise worth the most,
  his salary still owed unless he was just drafted, his upside gone with him,
  and the floor's positions as they are left. Stock cuts the lowest value,
  salary and fit aside.
- **Short of the roster minimum, it signs the minimum-salary free agent worth
  the most to it** - a guard when it has none - among the best few by value.
- **With a full roster in the season, it still signs a free agent worth more
  to it than his cost**, then releases whoever it can best do without, his
  salary still owed - as a signing is priced. A full roster used to keep its
  fifteenth man all season: as one season opened, 22 teams were full, and
  21 of them would have gained from one of the best free agents left - a 62
  asking 6% of the cap among them - even releasing a player for it.
- **But it signs no one it would release, and one a day.** Before a full
  roster signs, it looks at the whole roster with him on it as it will when
  it cuts: if he is the one it would let go, or the team is no better for
  the swap, it passes. And it signs no more in a day than it had places for
  when it made its offers - one, at a full roster - since each was priced
  as the day's only signing. In the owner's league (18-man rosters), the
  first two weeks of a season had 78 in-season releases, some players cut
  four to six times, a team signing a player every day only to release him
  (priced cutting its least valuable, while the cut looked at everyone);
  with both, 45, nobody cut twice.
- **Still to measure: a pick's player, and a young player, as the best quarter
  of what he may become and the rest** (boom, off), not two halves. In the
  owner's league picks 21-25 became a 65 or better 11% of the time and picks
  31-35 2%, though their averages differ by three points; the better half
  averages that chance away, and to a team far from contending a pick is
  worth only what it might become. In the 2005 replay a contender gave three
  late firsts for a 53 and a rebuilding team took them at about the 53's
  worth; with boom the contender no longer gains from it. A contender's
  first four years out for an early second now (both sides gaining under a
  half a hundredth of a title) is not changed by it: that is the contender
  valuing the seasons it can win in.
- **Still to measure: money short of the league's minimum payroll is free only
  this season, once games are played** (floorNow, off). A team short of the
  minimum pays the rest as a fine when the playoffs start, so salary up to it
  costs nothing - but before then, and for seasons ahead, a team that reads
  itself short nearly always ends up past it (6% of arena teams paid the fine
  in a season): it signs and trades for more, room the outlook expects to buy
  only two free agents. Without, a team with room to spare for years bid the
  most for anyone - in the 2005 replay a 57 got the maximum for three years,
  a 64 for five - and set prices for everyone.
- **A team plays and re-signs only players it wants.** A player worse than
  a minimum free agent never plays ahead of one, and once his contract is
  up, any player is let go when his salary buys more as room. Before, every
  expiring player was re-signed at his odds of staying - in the owner's
  league a 38-overall with an 84% chance - and a thin future roster played
  its worst players, so a scrub cost his team for a decade: teams cut
  prospects (a 21-year-old 39/57) and younger veterans for older ones about
  to retire. Now those cuts gain nothing, and in-season releases fall to
  23-32; about a quarter slower. Half the teams each way, 2 leagues each way
  round over 5 seasons, 4 times: 23 titles to 17, 0.04 more of their games
  won by seasons 3-5, 48 seasons under 20 wins to 63.

## Coaching

Starters, rotation and minutes come from the same judgment of who helps win.
Always played to win.

- **Minutes by the model's read of each player don't help.** The sim hands
  out minutes by value, fatigue and a playing-time dial; turning each CPU
  player's dial (within ±25%) so minutes follow the strength model's read of
  him instead changed nothing (0.01 ± 0.19 points a game, a league of 30
  teams x 300 games). The game's own value already sorts minutes as well, so
  CPU teams keep its rotation.

## How we know it is good

Build the measuring tools before the front office.

- **Arena** (`src/test/arena/`): random-players leagues (owner's call) with the
  owner's multiplayer settings, except that no team is hard-capped and every
  team is run by the CPU. Played through the real game code season after
  season, many leagues at once, then measured on winning, direction, trade
  sanity and cap/roster use against real NBA numbers (2010-2026). Stock is the
  baseline. Reports go to the owner in chat.
- **Exploit list:** every known way to fleece the CPU becomes a test it has to
  pass. From the owner so far:
  - Aging but very good players pried off good teams for a few picks (a 20-5
    team selling its 37-year-old, 60 ovr, elite-producing center).
  - Trading down in the draft for a haul - and then the CPU not even taking the
    player you wanted.
  - Trading up cheaply (picks 30 and 58 for pick 15).
    The first and last are tests (humanTrades, lottery). Trading down, the CPU
    takes about what an NBA draft chart calls fair (12 and 20 for 5) and turns
    down hauls (8 and 12 for 5, 4 and 9 for 1).
- **Trade checker** (`ARENA_CHECK_TRADES`): every trade the front offices make
  is played out - both teams before and after, against the rest of the league -
  next to what the model expected it to do for each side this season.
- **Version against version:** leagues where half the teams run the new front
  office and half the previous one, over many seeds. A change ships only if it
  wins more.
- **A "shark" bot** that plays like an expert human hunting for exploits.
- **A front office log:** a plain-English reason for every move.

## Multiplayer constraints

- Front office state lives in the league database, not in memory: who is in
  charge of simming changes, and draft and free agency steps can run on any
  device.
- Only the device doing the step writes front office state, inside the existing
  capture window.
- Answers to humans are a deterministic function of league state, so asking
  again cannot reroll a no into a yes.
- Every member's device holds the whole league, so there are no secrets.

## Build order

Thin slice first (owner's call): all five layers in rough form, end to end, so
CPU-to-CPU trades run in the arena; then deepen whichever layer the arena and
version-against-version tests show matters most.

1. Remove the old smart AI. Write this doc. (Done.)
2. Arena and stock baseline. (Done.)
3. The thin slice, ending in CPU-to-CPU trades measured in the arena.
4. The market: shopping, matching windows, bidding, the rebuilt Trade Proposals
   page. (Matching windows and bidding later removed, and Trade Proposals back
   to stock's mechanics - owner's call.)
5. Free agency board, re-signing, draft, roster cuts, coaching.
6. Explanations.

## Open questions

Asked as they come up; answers move into the sections above.
