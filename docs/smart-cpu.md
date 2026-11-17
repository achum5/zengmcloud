# Smart CPU (third try): one GM as good as the humans, on every CPU team

Owner, 2026-11-13: "the dream is to get the cpu to do the exact same thing as
us humans do ... it takes having an incredible base to the mode in order to
get it to work well." The two earlier tries (docs/realistic-front-offices.md,
docs/vanilla-plus.md) stalled on patches over a weak base. This one is built
the other way round: a base first, a yardstick before any engine, and no
piece kept unless the yardstick says it helps.

## The owner's rules (2026-11-13)

- **Goal: titles.** CPU teams are run from the GM's chair, not the owner's:
  no playoff revenue, no money motive. Wins and playoff runs count only as
  steps to titles.
- **The CPU sees everything.** Every rating in the file, true values, no fuzz
  ("I don't want the cpu hindered in any way"). Heavy computation behind the
  scenes is welcome ("even if the cpu is like damn near cheating on the back
  end like chess engines do that is ideal").
- **Speed:** a good bit slower is acceptable if it works, but it has a budget:
  the heavy thinking happens at the moments that matter (preseason, the
  deadline weeks, the draft, free agency), not every day, and is cached.
- **Parity, not a number:** "if we played 10k seasons we would average out to
  winning a title once every 30 seasons." The fallback if parity is never
  reached is a plain difficulty slider (what the CPU asks of a human, times a
  factor), as vanilla has.
- **How the humans judge players:** by build (the full ratings profile, not
  just the overall) and whether, with good progression, he becomes a really
  good productive player - or, if older, whether he helps win now.
- **How the humans pick a window:** common sense. Do I have the players? Am I
  within a player or two of competing? Do I have the structure to beat the
  other best teams in a playoff series?

## Why the earlier tries stalled

- **Title-odds engine** (try 1): every asset priced by how much it moved the
  team's own title odds this season and next. Most teams have nearly none, so
  most assets were worth nearly nothing to most teams: three firsts thrown in,
  a good player refused as a cost. Margins, frictions and a market floor were
  patches on that.
- **Vanilla+** (try 2): vanilla's player value with plans, patience, a
  calendar and tweaks on top. Each piece sound alone; together they did what
  nobody designed - in the owner's league (2026 export, CLE 92% to win the
  title) every CPU contender read "no real shot", waited, and sold rotation
  players for prospects, to the humans too.
- **Neither had a yardstick for "as good as a human".** Both were tuned to
  NBA-looking league stats.

## The base

One GM. Every CPU team runs it; a bot running it is also the yardstick.

1. **Strength: what wins in the sim.** A fast scorer, fitted on many
   simulated games, that predicts a roster's results from its players' real
   ratings (build, not just overall) and who plays. Then team strength to
   wins, seeds, series and titles, calibrated the same way.
2. **Players: where each is headed.** His ratings for the next seasons as a
   range of outcomes, from the game's own development code (exists:
   realisticFrontOffice/projection.ts, vanillaPlus/worth.ts seasonValues),
   plus injury and retirement risk.
3. **One goal.** Expected titles over the next several seasons, near ones
   weighed more, over that range of outcomes - so a rebuilding team's 2029
   counts as a contender's this spring does.
4. **Two-way prices.** Every player and pick is worth to a team the more of:
   what it does for the team's goal, and what the best other team would give
   for it, by the same goal. Nothing is ever worth nothing to anyone because
   that team can't win now.
5. **One decision engine.** Trades, free agency offers, re-signing, the
   draft, extensions, cuts: each candidate move scored by the change in the
   team's goal; the other side (team or player) answers by its own. No lanes,
   no patience rules: a team tanks, goes all in or stands pat because that is
   what its goal says.
6. **Human moves.** The searches are the moves humans make: consolidate two
   or three for one, buy injured or slumping players low, sell before the
   age drop, rent at the deadline, clear room for a free-agent class, trade
   into a strong draft. Each is a way to find candidates; the goal decides.

## What try 1 left that works

Try 1's eyes were good; its prices were not. Kept as the base:

- **Strength** (realisticFrontOffice/strength.ts): a roster in points a game
  against an average team, from every player's full ratings, his share of
  the ball, positions on the floor and lineup synergy, fit on the sim. On
  fresh matchups it misses by 1.25 points a game where the game's own spread
  misses by 2.16. On the owner's league (2023, half a season in) it tracks
  each team's real margin better than the game's team rating (r 0.82 against
  0.77).
- **The outlook** (realisticFrontOffice/outlook.ts): every team's roster
  projected ten seasons with the game's own development, picks turned into
  the players they become, retention, injuries; each season's strength
  turned into title chances by playing seasons out.

## The goal, worked out

Try 1 valued an asset by what it did to the team's own title chances. For a
team far from a title that is almost nothing whatever the asset, so a bad
team was indifferent between nearly everything - the root of the giveaways.
A real GM of a bad team isn't: he knows that assets become a contender later,
when his window opens, by drafting, developing and trading them for stars.

So the goal is titles, but a team counts on doing what humans do with
assets: turn them into players when it is ready to win. Concretely:

1. **A market price for everything.** Every player and pick has a price on
   one common scale: what the teams that want him most would give, each by
   its own goal (the second-highest bid, as at an auction). Worked out for
   the whole league at once, and again as rosters change.
2. **A team's window.** For each coming season, the title chances the team
   would have if it spent what it has to spare (assets that won't help then)
   on the strength that season's market sells for that much. Its window is
   the season where that pays most; the title chances it buys per unit of
   price is its exchange rate.
3. **What anything is worth to a team** is the more of: what it does for the
   team's own title chances on the floor, and its market price at the team's
   exchange rate. A pick is never worth nothing to a bad team - it is part of
   the price of the star it will trade for when its window opens - and a
   contender values the same pick by what it could buy now.
4. **Every decision** - accept a trade, offer one, sign, re-sign, draft, cut,
   extend - is the move that raises that worth most. Tanking, going all in
   and standing pat are what that says, not rules.

The title-window question ("am I within a player or two?") is the window
step: the team plays its own seasons out with and without what it could buy
(owner: "decided via the cpu almost being able to cheat and determine via
test sims that they are able to run themselves").

## The yardsticks

- **A. One GM in today's league.** A bot running the GM plays one team as a
  human would (through the human screens' rules, at the league's
  difficulty), against today's CPU, many leagues and seasons. It should win
  titles about as often as the owner's humans do. Until it does, it isn't as
  good as them.
- **B. Everyone runs the GM.** The same bot, or one playing the owner's
  playbook, as one team among 29 CPU teams running the GM. Over many seasons
  its title rate should come down to an average team's.
- **C. Replay DBA.** Every trade the humans made with the CPU in the owner's
  league (24 seasons, rebuilt season by season from the export): would the GM
  have taken it? Every CPU move like Golden State's sale of Tobias Harris:
  would it happen?
- **D. The owner plays it** in a copy of DBA.
- **Speed:** each season's time against today's, by phase.

## Checks the GM must pass (found in today's modes)

From the audit of the realistic mode (2026-11-13, 4 test leagues, exploit
hunting) and the owner's league:

- **Salary dumps.** A human can hand a CPU team a bad contract for nothing or
  a second (a 25-year-old rated 55 owed 26% of the cap for eight years: 7 of
  9 teams with room took him for a second). Contract length barely counted.
- **Rentals.** Contenders paid two or three firsts and a young, nearly equal
  player for a 34-year-old on an expiring deal.
- **Clearing room.** A team gave a 25-year-old rated 75 away for nothing to
  make room for a free agent no better.
- **Flips.** No cooldown after a trade: a player went through five teams in
  20 days; a team traded a player away and bought him back.
- **Rosters.** Teams over the 18-man limit or down to 6-9 players, and under
  the payroll floor with good players unsigned.
- **Feeding the favorite.** With one team far ahead (CLE 92% to win the
  2023 title), every other contender read "no shot" and sold rotation
  players for prospects - to the favorite too.
- **The Golden State trade.** A 37-12 contender sent its 31-year-old 66 to a
  rival for two rookies (vanilla had it "rebuilding").

## The layers (owner, 2026-11-13: "go back to the basics and get the actual engine itself right ... very carefully")

Built from the bottom up. A layer starts only once the one under it passes
its checks, and a layer that fails is fixed at its own level - never patched
from above. No rules that paper over a wrong value ("sign up to 13
players"): if a team does something no GM would, the fault is in what it
thinks things are worth, and that is where it is fixed.

0. **The GM's eyes** - what it sees, deciding nothing: how good a roster is
   (strength, done: tracks the owner's league's margins better than the
   game's team rating), where each player is headed, every team's title
   chances this season and the next ten, and each player's chance of
   re-signing. Checks: said 30%, won about 30% (title chances against who
   won, many test seasons); said 85% re-sign, about 85% did.
1. **What anything is worth to a team** - title chances, a market price for
   everything (what the keenest teams would pay, as a package), and never
   below the best real offer elsewhere. Checks, each a permanent test: no
   giveaways; picks never worthless; no salary dump for free; two decent
   players not worth more than one much better one because their prices add
   up; a contender doesn't sell its core to a rival; a team with an empty
   roster wants cheap players; the owner's league's eight clear fleeces all
   refused (done, DBA replay). And the other side, since a GM that refused
   everything would pass all of those: the same years on more money are
   worth less to every team; teams still sell players for offers both sides
   take; and a team turns down an offer it would take alone when another
   team would really give more.

   Passed 2026-11-13 (smartCpu/worth.test.ts, an arena league one season
   in), margins in titles: best player for nothing -1.76 to -0.11; a first
   for nothing -0.056 to -0.002 (210 picks); a quarter of the cap for six
   years taken for nothing or with a second -0.056 to -0.030 (a second adds
   next to nothing); two lesser players for a star -1.54 to -0.05; a
   contender's core to a rival for prospects -1.54 to -0.24; eight free
   agents for a three-man roster +0.006 to +0.141; twice the money over one
   year -0.0004, over three -0.0095 (later seasons' money is room that
   buys players); 12 sales among each team's six best players (+0.0002 to
   +0.050, every one through the market rule); 17 lowballs turned down
   (-0.048 to -0.001).

   What the numbers say about trading: a team far from a title values what
   it holds at its market price, since its own use is smaller. Two such
   teams never both gain from a swap (1 of 360 one-for-one swaps tried);
   deals come from a team that wants a player more than the market, paying
   at least the price the next keenest would. The six keenest teams set
   prices close to the whole league's (the 29 other teams' second-highest
   bid was higher for 31 of 240 players, by at most 0.008; the top bidder
   was outside the six for 4).

2. **Decisions** - one engine for trades, free agency offers, re-signing, the
   draft, cuts and extensions, every move scored by layer 1.
3. **Whole leagues, every team the GM** - the league has to look like the
   NBA, year after year. Checks: every team a full, legal roster every
   preseason (the owner's all-CPU league had the 2035 Lakers at 3 players
   with $145M of room and 220 free agents unsigned - under the old engine's
   worth, an aging dynasty saw no use in anyone); dynasties age into
   rebuilds, not collapses; trades, contracts and how long teams stay on top
   like the NBA's.
4. **The yardsticks** - the bench (a GM-run team against today's CPU; a bot
   playing the humans' way against a league of GMs), the DBA replay, then
   the owner plays it.

Today's mode stays as it is, and the owner's league untouched, until the GM
beats it on the yardsticks.

## Layer 2: the decisions (plan, 2026-11-13)

Behind a new hidden switch (`smartCpu`, only with realistic front offices
on), every CPU team decision is the GM's: the option worth most to the team
by layer 1, and nothing when nothing is worth more than standing pat. The
other side answers by its own: a CPU team by its verdict, a free agent by
the offer market's choice (unchanged), a human by pressing the button. The
game's rules (cap, roster limits, free agency's stages, CBA-lite) stay.

Where each decision is made today (realistic mode, vanilla+ on) and what
replaces it:

| decision                                                                                                                     | today                                                       | the GM                                                                                                                                              |
| ---------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| free agent offers (stages and in season), re-signing, offer-sheet matching, signing at a full roster, filling a short roster | try 1's `priceSigning` (its franchise value, less a margin) | the GM's worth of signing him on those terms, the roster cut to the limit if over; boards, asks (`priceMarket`), odds and stages built on it as now |
| cuts over the limit                                                                                                          | try 1's franchise value without him                         | the same through the GM's worth, dead money counted                                                                                                 |
| team options                                                                                                                 | a rule of thumb (declined under 80% of market salary)       | his worth at the option's salary against letting him go                                                                                             |
| the draft                                                                                                                    | stock (`value ** 69` at random)                             | the prospect worth most to the team                                                                                                                 |
| CPU-to-CPU trades                                                                                                            | vanilla+'s market                                           | the GM's market (below)                                                                                                                             |
| a human's offer                                                                                                              | vanilla+'s evaluate, trade difficulty on                    | the GM's verdict with the market rule; difficulty off until the bench says otherwise (layer 4)                                                      |
| what would make this work, proposals, trading block                                                                          | makeItWork on vanilla+'s evaluate                           | the same search on the GM's verdict                                                                                                                 |
| clearing room by trade                                                                                                       | try 1's plans re-vetted by vanilla+                         | off for now: room is used by signing and through the market                                                                                         |
| extensions                                                                                                                   | off                                                         | off                                                                                                                                                 |

**The market.** At each market moment every CPU team's tradable players are
priced by their owner's use and the keenest teams' bids. A player is a
candidate sale when a keen team bids above both the owner's use and what
the rest would pay. For each, every keen bidder's best offer (its players
and picks, best for the seller first, while it still gains); the seller
takes the best, if it gains. Deals are made best first; after each, the two
teams are re-read and only what involves them is worked out again. Market
moments: preseason, after the draft, once a free agency stage, in season
every two weeks and the last days before the deadline - the volume checked
in layer 3 against the NBA's.

**Speed.** A franchise value costs 14 ms on a team's own roster and 1.35 ms
on any other roster of its (its let-go decisions reused, 2026-11-13). Values
are kept from one decision to the next while the outlook is the same and the
team's players and contracts haven't changed, so a market moment works out
again only the teams that dealt. Budget: a market moment in tens of seconds,
a free agency day in seconds.

**Checks**, each a permanent test on the test league:

1. Free agency: a team down to three players with room makes offers to
   enough free agents to fill its roster (the 2035 Lakers); no team offers a
   contract worth less than nothing to it.
2. Re-signing: a team re-signs a young, good player at his ask, and lets an
   old, declining one asking a lot go.
3. Cuts: over the limit, the player whose loss costs least goes, dead money
   counted; never a starter ahead of a scrub.
4. Options: a team keeps a bargain option and declines a dear one.
5. Draft: with one prospect clearly best for everyone, the first pick takes
   him.
6. Market: every deal it makes is worth more than nothing to both sides,
   the market rule on; nobody is traded twice in a moment; a moment on the
   test league finishes within the budget.
7. Human offers: the owner's eight clear fleeces still refused (DBA replay);
   the market rule holds against humans.

Passed 2026-11-14: checks 1-7 (`smartCpu/decide.test.ts`; the DBA replay
with the layer 2 GM: 0 of the 8 fleeces taken, Golden State refuses the
Tobias Harris trade by 0.32 titles, 79 of the 156 human-CPU trades taken -
51% - and no unhandled errors).

Then layer 3's leagues, where the re-signing odds get their check (said 85%,
about 85% stayed) with the GM making the offers.

Build order: the view that follows the league (rosters, contracts, cuts,
dead money); signing worth behind `priceSigning` with cuts and options; the
draft; the market; human trades. Each with its checks before the next.

### Money, worked out (2026-11-13)

Building the signings showed that the market price, as layer 1 had it,
couldn't price a contract. A contender's money past the cap costs it next
to nothing in titles (the owner's call: money is a small cost), so its use
for a player hardly falls as his salary rises; with prices set by
contenders, every team was "willing" to pay anyone hundreds of millions a
year. Two changes, each tested against layers 1 and 2:

- **A bid has to be a trade.** A team's bid for a package counts only if it
  can take it under the trade rules and still gain: over the cap after it,
  it sends back matching salary (`softCapTradeSalaryMatch`), the players it
  would miss least first, and bids only if that trade leaves it better off.
  Those players are part of what it pays, so its bid is still its use for
  the package. Salary sent back may come from any player, tradable today or
  not (a price is what the market pays over the coming weeks: counting only
  tradable players, in the preseason contenders could bid for nobody
  expensive and a bad team gave its best player away).
- **A signing is worth what the player does for the team**, his salary
  counted as the room and money it takes - no market-price floor. A
  contract a team creates resells only for players of about its salary, so
  its room is spent either way; with the floor, a 35-year-old's re-signing
  was "worth" billions a year. Trades keep the floor: it is what stops
  giveaways.

What it means: a bad team may decline to re-sign a young starter at the
going rate (six of 40 young starters, all on bad teams, rated 54-58, were
worth less than the salary table to their teams) and he signs where he is
worth more; young stars (60 and up) are always worth their salary to their
team. The checks that pass (`smartCpu/decide.test.ts`): a three-man roster
wants 11 of the 15 best free agents at their asks; young stars worth 1.2 to
11 times the salary table; for every 33-and-over player a long deal is
worth no more a year than a short one (apart from deals at about the
minimum); over the limit, nobody among a team's eight best is cut; bargain
options kept and dear ones declined (9 of 9).

### The draft and the market, built (2026-11-13)

**The draft**: each CPU team takes, of the class's 20 best by value, the
prospect worth most to it on his pick's rookie deal by layer 1 - his use
and at least his market price, so a bad team takes the best asset. Every
team with the first pick took the class's clear best; 0.3 s a pick.

**The market** (`smartCpu/market.ts`), as it came out of three attempts:

- Candidates: every CPU team's tradable players that some keen CPU team
  wants more than their owner and the rest of the market do, and could take
  under the salary rules in an offer of a few players (with room, or with
  its dearest players outside its three best matching his salary).
- Each candidate is put to layer 1's best real offer (every keen buyer's
  best for the seller while it still gains, legal under the trade rules),
  and is a deal if the seller gains. All candidates are asked, once a
  moment: what makes a deal can't be told from the candidate cheaply (28 of
  374 made one, spread from the 41st best to the 313th).
- The deals are made best for the seller first, each team once a moment,
  each checked again as it stands before it is made.
- Settled first: every player's chance of staying with each team that sets
  prices. Left to be filled in as they were needed, a deal's worth hung on
  what had been priced before it.

What didn't work: buyers' offers built from their own assets one at a
time, the seller's favourite first, filled with worthless ones; packages
ranked by adding up what each asset is worth alone, which misses that four
rotation players at once cost far more than each alone (a buyer's "gain" of
+0.01 was -0.28 in full).

On the test league: 3 deals a moment (26 found; four no longer good once
earlier ones were made, the rest sharing a buyer - the keen teams buy most
of them), 72 s. A vet for youngsters to a contender, two like-for-like
swaps. Volume and speed are layer 3's to judge across seasons; 72 s a
moment is too slow for the owner's league as it stands.

## Layer 3: whole leagues (2026-11-14)

### Speed, first

A GM season in the arena took about 20 minutes, two thirds of it the trade
market. One market moment on the test league (src/test/smart/
momentProbe.test.ts: one moment timed step by step, its deals written out
whole so a change meant only for speed can be checked to find the same
ones) worked out 37,071 franchise values in 106 s. Where it went, and what
was done:

- **A player's contract read for every season of every value** (11% of a
  season): read once a value. Exact - the same 30 deals, the same gains.
- **Whom a team lets go of a player it would bring in, decided afresh on
  every roster** (45% of a moment: a veteran's re-signing is weighed over
  each season ahead, and the market tries hundreds of rosters with him):
  decided once, on what the team holds with him, and kept for every roster
  of its he is on - as its own players are decided once, on what it holds.
  106 s to 52 s; 26 of the moment's 28 deals as before (it had 30), the
  rest worth within 0.002 of a title.
- **Offers built without the trade rules on salary**: 1,052 of 1,256 offers
  broke them (the seller over the cap taking in more than it may 426 times,
  the buyer sending back too little 421, the apron 205), each after some
  16 franchise values, and were thrown away. Offers are now built within
  the rules (smartCpu/tradeRules.ts, the trade summary's checks for one
  side): a piece the seller couldn't take in is passed over, and a buyer
  short of salary to send back adds the players it would miss least, as
  NBA teams fill a trade. None broke the rules after; deals found went from
  28 to 82 a moment - most good deals had been lost to salary alone.
- **Every deal found trimmed to beat the next best offer** (82 a moment, 5
  of them made): only those about to be made are trimmed now; the deals
  are ranked by their best real offer, before trimming.
- **Each candidate put to the four keenest teams**: two in the market. 82
  deals found to 74, and the deals made the same but one (a smaller deal
  for the same buyer); a third less work. A human's offer is still judged
  against the four.
- Found along the way: judging a deal against the best offer from the
  others, the market asked the next keen team down in the partner's place -
  one more team than the market itself asks - and a deal the market would
  make looked beaten. The teams asked are now the keenest whoever the deal
  is with.

A moment now: about 30,000 values, 45 s. Still the market's search, mostly
pricing the packages offered (each keen team's bid, its roster cut to the
limit).

Then, by whole seasons (the arena's timers by phase and by decision):

- **Each team shops its five most wanted players a moment**, ranked by a
  rough read (rosters not yet cut to the limit; a deal's worth is always
  worked out in full). One moment: the same deals made, 17,745 values. But
  over a season trades halved (55 to 24): a team's five most wanted barely
  change from one moment to the next, so the same five were asked about
  every time and the rest never. Now a player shopped without a taker sits
  out the next moment while his team shops its others.
- **Free agency's moments never met**: they were set on days 30, 20 and 10
  left, and in stages the days left count the five stages down. The market
  now meets at each stage - most offseason trades are made there (6 and 16
  in the two test seasons).
- **The calendar, by the NBA's** (arena benchmarks: about 27 trades in
  season, three in four in the deadline's last week; about 30 in the
  offseason): in season every 17 games a team (not as the season opens) and
  each of the last five days before the deadline. Deadline week: 74% of
  in-season trades in both test seasons.
- **In-season signings**: each of the top 20 free agents priced for every
  team every few games, a full roster's price weighing whom it would cut -
  a tenth of the season's time for players few sign. The GM looks at the
  top eight in season.

A GM season in the arena: about 500 s (from 1,220). Trades 34 and 39 a
season (in season 19 and 13, offseason 15 and 26): below the NBA's 55-62,
in season most of all.

### Picks in later drafts (2026-11-14)

The bench's GM team in its second league (below) gave away every first it
had, 2028 to 2041, most as the last piece of a deal for a veteran, and
won 6 to 16 games a season for its last four. A probe of what GM teams think
picks are worth (src/test/smart/pickProbe.test.ts) showed why: each draft
further off halved a first's market price. A mid team's next first fetched
0.07 of a title, its first six drafts away 0.002 - discounting alone would
leave half. Seeing 16 seasons ahead instead of 10 doubled what a team
thought its own far firsts were worth to it, but not their price: the
title chances such a pick brings are years off and uncertain for every
bidder.

What was missing is what a GM knows a pick is: something it can keep and
sell once its draft is next. A pick in a later draft is now worth, to any
team, at least what a pick where its team is expected to finish fetches in
the next draft today, discounted for the seasons until its own draft (the
same patience as everything else: 0.9 a season). Far firsts went from
0.15-0.30 hundredths of a title to 2.6-4.4; the next draft's picks are
priced as before.

### The trade screens, on the GM's own offers (2026-11-14)

The trading block, trade proposals and what would make this work used the
game's own search, asking the GM's worth of hundreds of packages: two
minutes for a trading block, and a quicker read of a package from its
pieces' prices offered deals the full worth then refused. They now run on
the GM's own offers (smartCpu/screens.ts) - the offer it would make another
CPU team, built within the salary rules and checked by its full worth:

- **Trading block**: each CPU team's real offer. On the test league 28 and
  25 of 29 teams made one for two of the user's players, in 3 to 13 s; in
  the app (a new league, the GM on, at the deadline) 11 offers in 9 s.
- **Trade proposals**: up to five of twelve teams' real offers for one of
  the user's assets (9 s in the app).
- **What would make this work**: the CPU team first asks for what it takes
  to say yes - the piece the user would miss least where one is enough
  alone, else its favourite - then the side the salary rules want to send
  out more adds the player it would miss least that settles them. Before,
  every package for a player of a team over the cap broke the rules and
  nothing was found; now three of four asks find a legal deal in 0.2 to
  4.5 s (the fourth, a team's best player behind the hard cap, nothing).

### Three seasons with every team the GM (2026-11-14)

The arena league again (seed 503), with the pick floor and the screens:
29, 26 and 37 trades a season (in season 18, 11, 15; offseason 11, 15,
22), 64-72% of in-season trades in the deadline's last week; every team
traded; deadline rosters 11 to 18; win spread 9.3, 12.5, 12.7 (NBA about
12-13); champions won 54, 70, 66 games. About 850 s a season with four
runs sharing four cores. Expiring players (rookie deals aside) who stayed
with their team: 45-48% of those valued 60 and up, 36% of 52-60, 21-25%
below (the NBA's top free agents: about 57%).

Open: the buyers are nearly always contenders - five teams start 87-100%
of a season's trades (as before the pick floor; the market is the
sellers', and a contender's title chances move most) - and they make many
small swaps, near-equal role players for gains of a few ten-thousandths of
a title. Real GMs leave those alone; a margin for the model's own error
would stop them, but it is a judgement call left for the owner.

### Yardsticks A and B, first runs (2026-11-14)

- **B: the shark against a league of GMs** (2 leagues x 5 seasons, the
  bench's random-players league, every other team the GM): 35, 44, 35, 38,
  27 and 42, 47, 25, 27, 35 wins, no titles, 15 trades in all - mostly
  its prospects for veterans, once a contender's expiring 65 and a bad
  contract for a young prospect. The shark's sims found gains of 2 to 10 wins a deal and
  none added up to a contender. It does no better than an average team.
- **A: the GM as a human against today's CPU** (2 leagues x 10 seasons):
  one title (seed 1, season 10, after five seasons of 47-56 wins), and one
  collapse (seed 2: 59-23 in its third season, then 24-58 and 4 to 19 wins
  a season). In seed 2 the GM gave every first it owned for veterans -
  fixed by the pick floor above; with it, the same league still sank, its
  expiring players let go at their human-team asks (0 of 8 one summer) and
  then unwilling to negotiate. Against today's CPU the GM also counts
  prices only GMs would pay (a 36-year-old 61 worth 0.3 of a title to
  resell), so this yardstick says as much about the bench as the GM; the
  bench's re-signings are logged now to see what it turned down.

### One switch (owner, 2026-11-14)

League Settings show one switch for the mode: Smart CPU (Game Modes).
Realistic Front Offices and Front Office Difficulty are off the list: Smart
CPU turned on turns the realistic front offices on with it, and turned off
turns both off (league creation, createStream; saving settings,
updateGameAttributesGodMode). The difficulty never touched the GM. A league
with the realistic front offices on that never had Smart CPU on - today's
mode, the owner's league - runs as it did.

Superseded 2026-11-17: Smart CPU runs on vanilla's rules. Turning it on still
turned the realistic front offices on, so the switch gave the owner their
free agency and contract rules (the MLE and the rest), not the vanilla-rules
mode below. The owner: "i thought we got rid of that stuff and just made it
essentially vanilla+?" Saved on (or turned off), Smart CPU now leaves them
off, and a new league with it on starts with them off.

### Shared leagues (2026-11-14)

The owner: "this needs to work 100% perfectly with mp sync". What was
checked, and what was changed:

- **Human teams.** Every league mate's team is in `userTids` on every
  device (it syncs), and the GM skips them all (`SmartView.humans`): it
  never trades, signs, re-signs or drafts for a human team.
- **One device decides.** The GM's decisions run only where the league is
  advanced (the device in charge of simming, or the one that won a
  ready-up step); the screens (trading block, proposals, what would make
  this work) are local views, checked against the league again before an
  offer stands. The outlook is built from seeded randomness, so it is the
  same on every device.
- **Memory kept with the league.** Two things the GM decided by lived only
  in the worker: the players each team shopped at the last market moment
  (rested the next) and the free agent pool's quality as free agency
  opened (what room buys later in it). A reload, or the sim moving to
  another device, forgot them, and the next decisions came out
  differently. Both are game attributes now (`smartMarketRested`,
  `faPoolAtOpen`), as the market moment already was (`smartMarketMoment`).
- **Leases against a stalled worker.** The GM's work never yields: a market
  moment holds the worker 30-60 s on a desktop and longer on a phone, and
  no timer or write runs meanwhile. The sim-day fence and the ready-up
  claim (90 s) could read a day or step still running as a crashed one and
  let another device run it again. The room's busy mark (3 min, stamped
  once a call) lapsed under long sims, and a multi-day sim dropped it as
  soon as the call that started it returned, so league mates' edits
  published into the sim and were overwritten by its whole records. Now
  (sync/advanceLease.ts):
  - in a league with the smart CPU every such lease is ten minutes;
  - a sim holds the room busy from its first day to its last, the mark
    renewed before each day once half its lease is gone (the write waited
    for);
  - a ready-up step doesn't start on another device while the room is busy,
    and in a smart league holds the room itself.

  The cost: a device that really crashed mid-step holds the room up to ten
  minutes.

- **Not changed:** the GM's work still doesn't yield to the event loop.
  Yielding would let another device's changes, or this device's own
  trades, land in the middle of a decision made from a snapshot; with the
  leases long enough, the stall is safe.

## The DBA replay

Yardstick C, built (src/test/dba/). Every trade of the owner's league from
2013 on, put to a valuation at the moment it was made:

    DBA_LEAGUE=<league.json> DBA_DB=<league-slim.json> DBA_OUT=<dir> \
      npx vitest --run --project basketball src/test/dba/dbaReplay.test.ts

(about 8 minutes with both baselines, under a minute with vanilla alone;
DBA_VALUATIONS picks them, also `vanilla@<difficultyTrade>`; DBA_FROM and
DBA_TO the seasons.)
It writes dba-replay.txt and a row per trade in dba-replay.json. A new CPU
is a `Valuation` (valuations.ts: given the league set to the moment, how
much a side gains) passed to `runDbaReplay` or added to the list.

- **The moments** (timeline.ts, state.ts): rosters from every draft,
  signing, trade, release and retirement event, contracts by when each was
  signed, picks through every trade, injuries from injured/healed events,
  the day of the season from the injury events' steady rate, vanilla's
  strategies rebuilt review by review from 2000. Checked against the
  export: all 1,310 traded assets 2013-23 where the rebuild had them, every
  player's team now right (775/775), every pick's owner at the draft right
  (600/600), the day within 3 games of the traded players' minutes, all 27
  CPU strategies now right.
- **What it can't get right:** ratings are one row a season, with in-season
  progression folded in, so an in-season trade sees where players ended
  the season; a box score's PER is the season's; a team's record that day is
  its final record pro-rated; which prospects were in each future draft
  class back then isn't kept (their draft-year ratings stand in; seven
  classes at once fits vanilla's answers best) - picks are the weak spot;
  difficultyTrade is taken as 3.5 throughout, and God Mode was on at some
  point.
- **Outcomes:** team strength is strength.ts rosterPoints (points a game):
  the roster just before and after; at the start of each of the next three
  seasons, the roster it had against it with the trade undone (a pick counts
  as the player drafted with it; later trades not followed). "Humans clearly
  won": the human up and the CPU down by 0.5 a game or more, now (the rest
  of the season, or the coming one for an offseason trade) and later (the
  two seasons after). A howler: a top-8 team 0.5 a game worse for a CPU
  trade.

Baselines (2026-11-13): 156 human-CPU trades, 119 CPU-CPU.

|                                              | vanilla (3.5) | realistic (master) |
| -------------------------------------------- | ------------- | ------------------ |
| Human-CPU: the CPU says yes                  | 100 (64%)     | 13 (8%)            |
| ... within 0.25 of yes / within 1            | 129 / 136     | 25 / 39            |
| ... players only                             | 16/16         | 0/16               |
| Humans clearly won (8): the CPU says yes     | 5             | 2                  |
| CPU-CPU: the side that had to agree says yes | 83 (70%)      | 52                 |
| ... within 0.25 of yes                       | 112 (94%)     | 82                 |
| CPU-CPU: both sides yes                      | 2             | 2                  |
| Howlers (7): the contender says yes          | 2             | 4                  |
| Howlers: the trade still happens (\*)        | 4             | 0                  |

(\*) Vanilla's own rule for CPU trades: the partner over 0, the team that
started it within 15 either way (betweenAiTeams). The realistic mode's:
both over 0.

Trades are struck at vanilla's margin (a human asks what would make it
work, vanilla adds until it is just over 0), so a small error in the rebuild
flips a yes: of vanilla's 56 noes, 36 are within 1 of yes. About 20 are far
off - most with picks several drafts out, priced from real classes that may
not have been in the league yet; a few look forced (TOR giving the 2015 #1
pick and four players for Michael Beasley is -111 by vanilla). At
difficultyTrade 2 or lower vanilla says yes to 108-110 (in the season 46 of
63, against 38), so the trade difficulty may not always have been 3.5.

The realistic mode turns down almost everything the humans did - fair
deals too - and as a contender would make more of the howlers than vanilla
would (its contender says yes to 4 of 7).

## The bench

Yardstick A's harness, `src/test/bench/` (2026-11-13). A random-players
league with the owner's settings; one team (or a few) a human team run by a
policy, every other team today's CPU; season after season through the
game's own code. One league per process, never in the normal test run:

```
BENCH_RUN=1 BENCH_POLICY=autopilot BENCH_SEED=1 BENCH_SEASONS=10 \
  BENCH_OUT=/tmp/bench npx vitest --run --project basketball \
  src/test/bench/bench.test.ts
BENCH_REPORT=/tmp/bench npx vitest --run --project basketball \
  src/test/bench/bench.test.ts    # pools every run: /tmp/bench/report.md
```

- **Switches.** `BENCH_POLICY` autopilot, shark (`BENCH_SHARK` its options
  as JSON), gm, pro or sharkpro (below); `BENCH_TIDS=7,17` the policy's teams (default one, 7 x seed mod
  30), or `BENCH_TEAMS=3`; `BENCH_FO=1` realistic front offices in place of
  the vanilla CPU (its human flows are offers: the game's default
  re-signing offers stand, a signing day makes an offer at his ask, a short
  roster is filled by the game's own signing; smoke-tested only);
  `BENCH_HARD_CAP=0` no secondary hard cap; `BENCH_LEAGUE='{...}'` settings
  over the owner's; `BENCH_LABEL` what the
  report groups the run by; `BENCH_AUTOPLAY_SIGNING=1` a check on the bench
  (below). Every random number comes from the seed.
- **Settings** (runBench.ts `BENCH_SETTINGS`): the arena's (the owner's
  gameplay settings) with difficultyTrade 3.5, difficultySigning 2.5, soft
  cap, 82 games, deadline at 60%, COLA, 18-man rosters, players refuse to
  negotiate; realistic front offices off. The policy's teams are, as the
  humans in DBA are, under the secondary hard cap at the luxury tax line.
- **Human, not CPU.** Only the policy's teams are user teams (userTids, not
  spectator). The CPU prices them as humans (trade markup and the user-pick
  penalty, which key on userTid - set to the acting team before each of its
  moves), players' moods carry the signing difficulty for them, and they are
  never in the CPU's trades with each other. The autopilot team too: its
  rate is what the CPU's own decisions earn under the human rules.
- **The policy** (policy.ts) is asked at the moments a human acts:
  `preseason`; `inSeason` stops (days before the deadline, or every N days);
  `draftPick` for each of its picks; `reSign` with its expiring players;
  `freeAgencyDay` each day of free agency, before the CPU signs. It may read
  anything and acts only through `ctx`: `tradeVerdict` (trade.propose's
  answer without the trade - a human proposes and presses undo),
  `proposeTrade` (trade.propose itself), `contractOptions` and `sign` (the
  negotiation screen: his ask to this team by length, refused if he won't
  negotiate; with realistic front offices an offer), `release`, and
  `autopilot.*` (the fallbacks, for part of a moment). A policy that does
  nothing costs nothing: its hooks are skipped.
- **Fallbacks:** what the policy leaves is decided as the game decides it on
  auto play, carried out through the human screens: signing days
  (autoSign's odds and getBest's choice among players willing to negotiate,
  at their ask to this team; in the season every 7 days, the days since
  together), roster limits (checkRosterSizes: the lowest valued cut,
  minimum players signed), re-signing (the AI's rule: draft picks, and
  willing players the team is worse off without, the cheap ones half the
  time not), the draft (runPicks's choice).
- **Output** per run, `BENCH_OUT/bench-<label>-<seed>.json`: each season
  every team's record, playoff rounds won and rating (as the season opens
  and the playoffs begin) and the champion; for the policy's teams also
  rating rank, payroll, free agents signed, its expiring players (willing,
  re-signed), trades; every trade it made with both sides; seconds by phase
  and the policy's own. `progress-*.log` as it goes.

### Baselines (2026-11-13)

8 leagues x 10 seasons a policy, seeds 1-8, the policy's team 7 x seed mod
30 (7, 14, 21, 28, 5, 12, 19, 26); each seed's league starts the same for
every policy. The DBA humans: 8 titles in 10 seasons among 3 teams, about
27% a team-season.

| policy                                               | titles      | rate (95%)     | win%       | rating rank | out of playoffs | s a season      |
| ---------------------------------------------------- | ----------- | -------------- | ---------- | ----------- | --------------- | --------------- |
| autopilot                                            | 1 / 80      | 1.3% (0.2-6.7) | 43.2 ± 1.7 | 20.3        | 51%             | 31              |
| shark                                                | 0 / 80      | 0% (0-4.6)     | 47.6 ± 1.6 | 20.6        | 53%             | 66 (33 its own) |
| check: auto play's signing, difficulty 0 (6 leagues) | 1 / 60      | 1.7% (0.3-8.9) | 54.2 ± 1.6 | 11.9        | 38%             | 30              |
| the CPU teams beside them                            | 3.4% a team |                | 50.0       |             |                 |                 |

- **Autopilot** sinks under the human rules: about 1.3 of its players a
  summer won't negotiate (0.05 for the check's), it signs 1.8 free agents a
  season at their mood's price, and its win% runs 37-50 by season.
- **The check** is above parity, not below: auto play's own signing at
  difficulty 0 is fair to the user team and more (a user team's players
  aren't pushed to test free agency as a CPU team's are). The bench costs
  the team nothing; the human rules (difficulty, signing only who will
  negotiate at his mood's price, the hard cap) cost it about 11 points of
  win%.
- **The shark** buys this season: 100 trades, 24.6-year-olds of 51/59 for
  31.1-year-olds of 57/57, 21 picks given. 62, 62, 54% the first three
  seasons, then 38-46%: no titles, 6 lost finals.
- Titles alone need far more: 80 team-seasons can't tell 1% from 5%.
  Win%, rating rank and rounds won say more per season.
- **Speed:** an autopilot season about 24 s on a quiet machine (22 s with
  no stops; in the season the stops cost about 0.2 s each once the bench
  stopped saving after each run of days), 31 s with the machine shared;
  the regular season is 75% of it. The shark's hunts (sims) double it.
  Realistic front offices run (smoke-tested, 2 short seasons) at 4-5
  minutes a 20-game season.

### What made a fair bot hard

- **The CPU doesn't sign as a human does.** freeAgents.autoSign signs any
  free agent at his demand, mood unread. A human signs only players willing
  to negotiate with him - a seeded draw, so asking again changes nothing -
  at his mood's price, up to half again his demand; at signing difficulty
  2.5 every good thing about a team counts a 3.5th and every bad thing 3.5
  times. Auto play for a user team signs as the CPU does; the bench signs as
  the human screens allow. `BENCH_AUTOPLAY_SIGNING=1` with difficulty 0 is
  the check that the bench itself is fair: the same team signing as auto
  play does should do as well as a CPU team (it does a little better).
- **userTid, not userTids.** The trade markup, the user-pick penalty, the
  negotiation screen and the mood's "user" view read the one userTid, as in
  multi-team mode; the bench switches it to the team acting.
- **A user team blocks the sim** if its roster is outside the limits
  (checkRosterSizes), so the bench brings it within them before each run
  of days, and again if a death stops the days short. Free agents can't be
  signed from the playoffs to the re-signing phase; a roster short then is
  filled by the game's own signing, counted as `forced`.
- **Re-signing.** At the re-signing phase a user team's expiring players
  are already free agents in negotiation, so the AI's "worse off without
  him" test runs with him put back on the roster for the moment.
- **Not modelled:** DBA's synced free agency board (waiver rolls); its
  ratings shown to the humans to the tens (the bench's bot, as the CPU,
  sees every rating); the COLA opt-out; owner firing (off).

### A stronger bot, against both CPUs (2026-11-15)

The owner asked whether Smart CPU will be dramatically harder to win against
than vanilla at 3.5/2.5. The shark and autopilot couldn't say: neither beat
vanilla by much. `proPolicy.ts` ("pro") judges as a human does: its team's
strength this season and the next two (the MOV formula over the top ten
projected overalls, seasons weighted 1, 0.65, 0.4, each a logistic against
the best other team), a player off contract counted by his chance to stay
(moodInfo's willing odds). It trades through the human screens - "What would
make this work" for each player who would start for it, its top three
untouchable, and the trading block for spare players (no picks) - and
re-signs, signs and drafts on the same worth. "sharkpro" is the shark's
trades with pro's draft, re-signing and free agency. `ctx.makeItWork` and
`ctx.tradingBlock` are the two screens, vanilla's or the smart CPU's as the
league runs (`BENCH_PRO` pro's options).

Against vanilla (8 leagues x 8 seasons): pro 42-44% (its first version
counted expiring players as gone and gave nine players and four picks for a
34-year-old 67; fixed), sharkpro 52.5% - no titles, out of the playoffs 36%,
two lost finals. The best bot so far, and still far short of the DBA humans
(about 27% titles a team-season).

Against Smart CPU, the same four leagues (seeds 1-4, which start identical
in every run) and their first six seasons, with a team that makes no trades
(autopilot) as the control:

| first 6 seasons, seeds 1-4 | vanilla | Smart CPU |
| -------------------------- | ------- | --------- |
| autopilot win%             | 42.5    | 36.9      |
| sharkpro win%              | 52.9    | 39.9      |
| sharkpro's edge            | +10.4   | +3.0      |
| sharkpro out of playoffs   | 5 / 24  | 16 / 24   |

sharkpro lost ground in all four leagues (7 to 17 points). Of its 13 points,
about 6 are the league itself - Smart CPU teams are better run, and a team
that does nothing wins 5.6 points less - and about 7 are its trading edge,
down from 10 points to 3 (two leagues up on autopilot, two down: noise). So
the trades that made the bot better than doing nothing mostly stop working.
Directional only: the bot is not human-strength, and 24 team-seasons a cell
is small. A Smart CPU season here took about 15 minutes, four leagues
sharing the machine.

## The owner's league, decision by decision (2026-11-17)

The owner asked for Smart CPU ready for the DBA league. Its 2023 export ran
under the GM for ten seasons, every team the GM's, and five reviews read
every decision by type (rosters, signings, the draft, trades). The fixes
below were then run side by side on the same league: run 2 (before), run A
(the fixes), run C (A with picks priced as rookies), each from the 2023
preseason; arms N (A as it was) and F (A with the free agency fix), each
from run A's 2026 preseason, seeded alike, to the 2028 playoffs. Harness:
`src/test/smart/history.test.ts` (moves, every deal's reckoning, each
draft pick's board, good free agents day by day).

**Rosters.** Teams opened seasons with up to 25 players and 19 a season
over the league's 18, and in-season they signed and released the same men
in a loop. Now a CPU team trims to the limit as the preseason ends, while
releasing a draftee still costs nothing (`newPhaseRegularSeason`), fills to
13 rather than the minimum, signs over the limit in the summer only if it
is still worth it once both rosters are cut (`worthSigningOverLimit`),
signs at a full roster in season only for a real gain over whoever it cuts
(`SIGN_AT_LIMIT_MARGIN`), keeps the man it just signed, and doesn't
re-sign in season a player it released. Runs A and C: no team opened over
18 (19 a season before), the biggest opening roster 18 (25).

**Trades.** A CPU-CPU deal must gain each side 0.001 (`minGain`; a quarter
of run 2's deals gained one side less, a 53 for a 53): none did, with
trades a season 31 against 33. Trading away within a year a player a team
traded for, or buying back one it traded away, costs it 0.01 a player
(`flipCost`, 0.005 before): at 0.005 contenders still flipped players
within weeks (Walker Kessler went through GSW twice in one season), and
half the flips gained the flipping side under 0.009. At 0.01 they didn't
flip less (run V below: 23 trades flipping a player, against 22), they
found flips reckoned bigger (median gain 0.018, against 0.007): the churn
is the noise in contenders' values, which no flat cost removes. Still
open.

**Picks priced as the rookies they become** (`picksAsRookies`, now on). By
what a typical pick becomes, run C's 2025 save priced the next draft's
expected top-three picks at 7% of a title and late firsts at 1-1.6%, while
the 2024 first-rounders fetched 19-27% (top four) and about 4% (20-30): a
human could buy CPU picks for a third of what the rookie fetches months
later, and contenders gave firsts away for bench players (21 firsts traded
a season). Priced off each draft's own class as rookies on their slot's
deal, the same picks fetch 23-26% and 2.2-3.6%. Run C against A, 2023-24:
8 firsts traded a season (21.5), players traded again within a year 7
(22), trades a season 13.5 (31) - fewer deals, most of the ones gone were
the reviews' worst.

**Free agency in stages** (`freeAgencySettings.stageAsks`, smart CPU only).
A free agent nobody was after had his ask move half of 15% a stage, a pace
made for thirty days of free agency: in arm N, Troy Brown Jr. (65) asked
$27M, nobody would pay that, he still asked $15M at camp when every team's
room was spent, and he signed for $2.0M - the reviews found 14 players
rated 60 and up signing for 1-2% of the cap. Now, nobody after him, his ask
drops each stage to what the second-keenest team would pay, and by at
least 15%, so the keenest gains from him while it has room. Arm F,
the same summer: Brown $18.4M, Filip Petrušev (60) $14.3M (was $2.1M); of
13 rated 60 and up, one signed for 3% of the cap or less (three), and he
had asked $3.7M.

**All of it on** (run V, from run A's 2026 preseason to the 2028
playoffs, against arm N): no team opened over 18; of 20 free agents rated
60 and up over two summers, one signed for 3% of the cap or less
(Donovan Mitchell, who asked $3.7M); 9 firsts traded a season (11.3),
trades a season 19.7 (25.3), in-season signings and releases 73 (84), no
deal gaining a side under 0.001. A summer signing over the limit no longer
needs a margin (worthSigningOverLimit): with one, an offer the player took
could fall through when the team came to sign him (3 of about 45 good free
agents a run; Marvin Bagley III, 64, lost a $24.3M and a $14.3M offer that
way and signed for $1.4M).

**Tried and left out.**

- A signing worth at least what the player would fetch in a trade (a
  resale floor): bad teams with room bid $13-24M for 48-55 players in
  season (Furkan Korkmaz, 53, had 22 teams after him and got $24.3M) -
  contenders' use for marginal players carried into free agency.
- A better stand-in for an empty roster place (49 rather than 43: the ten
  best free agents in season are 48-55): contenders' use for fringe players
  fell only 10-40% (Moussa Diabaté 41/59, to CLE: 7.8% to 6.7% of a title).

**Still open.**

- Contenders put real worth on fringe players (CLE's 7.8% for Diabaté, who
  would never play for it): near the top, a point of MOV moves title odds
  about 9 points, so a tenth man counts. Their bids set prices, so the deep
  draft board leans to whoever is closer to playable: run N's 2026 draft
  had 7 first-rounders aged 22+ with pot 58 or less, and at #25 Tarris Reed
  Jr. (23, projected 43 every season) was priced 2.5%. The projections
  themselves are right: a 42-rated 19-year-old is projected to 59 and a
  47-rated 22-year-old to 51, as the league's own drafts turned out.
- Free agents paid above their ask (Blake Wesley, 62/69, the max at a $41M
  ask), and five-year deals to 33-year-olds.

## On vanilla's rules (2026-11-17)

The owner's idea: the game as the league plays it today - vanilla at trade
difficulty 3.5, overall 2.5 - but with CPU teams that go after players and
handle things far better; and against humans, two tweaks: a CPU team doesn't
sell out its direction, and a human can't poach a contender's player to
double his own chances.

**The switch.** Smart CPU on, Realistic Front Offices off
(`smartVanillaOn`; `smartBrainOn` is either). Before, that combination did
nothing.

**What stays vanilla:** free agency (daily, asks and mood as vanilla has
them), re-signing (willingness), the trade screens and their answers at the
league's trade difficulty, the draft screen, the sim.

**What is the smart CPU's** (the same engine as with Realistic Front
Offices):

- CPU-CPU trades: its market, at the preseason flip, after the draft, at
  re-signing, three moments in free agency, and every 5 games a team all
  season (`vanillaGamesBetween`; 17 with Realistic Front Offices) plus each
  of the deadline's last five days - so contenders keep going after players
  all season and a human can't take the preseason to buy up the market.
- Free agency: on a day a CPU team signs (vanilla's odds of a day off), of
  the 10 best it may sign under vanilla's cap rules, the one it gains most
  from on his ask, and nobody if no one is worth his ask
  (`smartCpu/vanilla.ts`).
- Re-signing: a willing player is kept if the team is better off with him on
  his ask than without him.
- The draft, cuts, filling a short roster: as with Realistic Front Offices.

**A human's offer** (`smartGuardOffer`, smartCpu/humanTrades.ts): vanilla
must say yes at the league's difficulty, and then

- a contender (3%+ title chances this season and next) turns it down if it
  leaves it weaker over those two seasons by more than 2% of its chances
  (at least 0.005);
- a rebuilding team (0.5% or less) turns it down if it leaves it 0.005
  weaker after that;
- a contender facing a contender turns it down if the human's gain over the
  two seasons is over 0.01 and more than 1.5 times its own.
  What would make this work, the trading block and the CPU's proposals to a
  human never put up a deal the guard would turn down.

On the owner's 2023 export, a human GSW (and BOS) asking contenders CLE,
BOS, TOR and MEM for a starter for picks: every one refused for the
contender's own sake ("We're trying to win now"), most of them refused by
vanilla too; rebuilding teams' fair deals went through.

### Coming after the favorites (2026-11-17)

The owner: "as the human, we should be genuinely scared of other teams
trading to get better ... just bc at preseason we are the favorite does not
at all mean we are going to end the favorite." Ten seasons of the owner's
league on this mode (2023 export to 2033, every team the GM) showed two
things in the way.

**Only the favorites bought.** On title chances alone, a team far behind the
favorite gains too little from getting better to pay for it: 199 of 245
in-season deals were bought by teams at 5%+ preseason title odds, and in
2025, with two teams holding 97% of the odds, one trade was made all season.
With humans the favorites, that leaves nobody coming after them. So with
the smart CPU on vanilla's rules a playoff run counts beside a title
(`outlookSettings.vanillaAmbition`, 0.25): making the playoffs and each
round won, as a share of a champion's run (a champion's is 1, so a title
with its run is worth 1.25; a first-round exit 0.0625). With Realistic Front
Offices on, the playoffs still only break ties.

Paired single seasons (the same league and seed, ambition off / 0.25 / 0.5;
BOS, CHA and CLE kept out of the CPU market as in the league):

| start (favorite's odds)        | in-season deals off / 0.25 / 0.5 | of them by the top 10 |
| ------------------------------ | -------------------------------- | --------------------- |
| 2033, runCTL (87%)             | 14 / 29 / 29                     | 14 / 28 / 24          |
| 2033, humans as CPU (59%)      | 23 / 24 / 30                     | 20 / 22 / 22          |
| 2026, realistic run (56%)      | 17 / 15 / 22                     | 16 / 15 / 22          |
| 2033, realistic run (28%)      | 13 / 17 / 18                     | 13 / 17 / 16          |

Most of what 0.25 adds comes where one team is far ahead - the case that
matters with humans the favorites. 0.5 added little more from the top ten
and mostly bench-for-seconds deals by teams ranked 11th and lower.

**Contenders passed players straight along.** At the flat flip cost (0.01),
a third of the deals in a busy season had a team give up a player it had
got that season; Kendall Brown was traded four times in one, and the side
that flipped gained a median 2.4% of a title by it. A flip now costs
`1 + flipRecent (5)` times the flat cost right after the trade, down to the
flat cost `flipRecentSeasons` (half a season) on - a cost, not a lock. This
holds with Realistic Front Offices on too.

Eleven seasons each, humans out of the CPU market (runCTL: before; runAMB:
both changes):

|                                   | before | after |
| --------------------------------- | ------ | ----- |
| deals                             | 214    | 177   |
| flips                             | 56     | 25    |
| deals that aren't flips           | 158    | 152   |
| players moved twice in a season   | 62     | 28    |
| in-season buyers C / B / L        | 109 / 21 / 24 | 70 / 19 / 17 |
| distinct champions                | 6      | 5     |
| median gain of the side gaining least | 0.42% | 0.90% |

Ten-season runs drift apart after the first year, so the bubble's share
there says little; the paired seasons are the test of ambition. Payrolls
rise past the tax line in both runs (top payroll $318M before, $362M
after, tax line $200M) - money is a small cost by the owner's call; open.
