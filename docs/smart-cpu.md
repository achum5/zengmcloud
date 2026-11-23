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

### Shared leagues, audited again (2026-11-17)

The owner: "can we audit the system as a whole making sure every single
last thing will work perfectly fine with mp sync?" Four read-only reviews
(advances and holds, writes off the simming device, state kept outside the
league, settings and versions). Fixed:

- **One device decides, now everywhere.** A league mate's last draft pick
  ran the after-draft phase change, and its market window, on their phone;
  a league mate's own game that ended a day ran the day's CPU signings and
  market there. The smart CPU's market, its signings and its cuts and fills
  now run only where it decides (`smartCpu/decidesHere.ts`: single player,
  the device in charge of simming, or a ready-up step this device claimed);
  elsewhere the market and signings wait, and roster limits use stock's
  cuts and fills.
- **Holds through long steps.** Phase changes, each draft pick and each
  market moment renew the room's busy mark (they no-op outside an advance),
  and Auto Play holds the room from its first step to its last, published.
- **Sim here** is refused while the device in charge is mid-step.
- **Taking over simming** drops the outlook built while following, so the
  new simmer decides from the league as it stands (`sync/simmerHooks.ts`).
  Cuts release the player as the league has him now, not the GM view's copy.
- **League mates' teams** no longer appear as CPU offers on the trading
  block and in Trade Proposals on vanilla's rules.
- **League Settings** save only what was changed in the form, so a league
  mate's change made while it was open isn't written back over.

The sync engine's own gaps the audit found, fixed in a second round (the
owner: "go ahead ... do the entire list"):

- **A move and a sim on the same records.** Changesets carried whole
  records, so whichever published last won every field: a league mate's
  trade held through a sim took back the sim's games and injuries on the
  traded players (and on the trader's phone the player went back to his old
  team); a sim that lost the race to a trade took the trade back. Now an
  ordinary edit also carries the fields it changed (`SyncChange.paths`,
  sync/fieldPaths.ts), worked out against the record as the edit first read
  it (changeTracker's bases - cloned at the first read, since the cache
  hands out live objects that are edited in place; players, teams,
  team seasons and draft picks only, ordinary capture windows only). A
  device applying it sets those fields; a sim still lands whole. A device
  with its own change to the same record still waiting to publish keeps it
  on top: an edit of ours with fields is laid back over what arrived, a sim
  of ours takes the arriving edit's fields in, and the waiting copy in the
  outbox is rewritten so (applyVersion's resolveWrites, outbox.update), so
  what publishes carries both. Whole records (no base, too many paths, an
  older build) behave as before.
- **The busy mark outlives an unsent advance.** An advance whose upload
  only queued cleared the room at once, and held followers' edits went up
  ahead of it. The mark now stays until the outbox empties (or its lease
  runs out).
- **Ready-up steps run on a league mate's phone.** Only the device in
  charge can stamp the room busy, so a step another device claimed held
  nobody. Every device now watches the claim document (stepClaim.ts): while
  another device's claim is open, edits and publishes wait, and the Play
  menu on the device in charge won't run alongside it. A claimed step that
  was discarded (with a checkpoint to restore from) is not sealed complete.
- **A league mate's own game that ends the season.** The switch to the
  playoffs, or to the offseason after the finals, is left to the device in
  charge of simming (its next sim, with no games left, runs it) instead of
  running on that phone.

Devices on an older app version behave as that version did (no version
check): every league mate should reload after an update.

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

| start (favorite's odds)   | in-season deals off / 0.25 / 0.5 | of them by the top 10 |
| ------------------------- | -------------------------------- | --------------------- |
| 2033, runCTL (87%)        | 14 / 29 / 29                     | 14 / 28 / 24          |
| 2033, humans as CPU (59%) | 23 / 24 / 30                     | 20 / 22 / 22          |
| 2026, realistic run (56%) | 17 / 15 / 22                     | 16 / 15 / 22          |
| 2033, realistic run (28%) | 13 / 17 / 18                     | 13 / 17 / 16          |

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

|                                       | before        | after        |
| ------------------------------------- | ------------- | ------------ |
| deals                                 | 214           | 177          |
| flips                                 | 56            | 25           |
| deals that aren't flips               | 158           | 152          |
| players moved twice in a season       | 62            | 28           |
| in-season buyers C / B / L            | 109 / 21 / 24 | 70 / 19 / 17 |
| distinct champions                    | 6             | 5            |
| median gain of the side gaining least | 0.42%         | 0.90%        |

Ten-season runs drift apart after the first year, so the bubble's share
there says little; the paired seasons are the test of ambition. Payrolls
rise past the tax line in both runs (top payroll $318M before, $362M
after, tax line $200M) - money is a small cost by the owner's call; open.

## League 5: every team reviewed, and the core fixed (2026-11-17)

The owner ran an all-CPU league on the vanilla sub-mode, from 2027 to the 2040
preseason, with BOS as the user team on Auto Play. All 32 teams were then
reviewed season by season, covering every team-building move: 474 trades, 943
free agent signings, 1,118 re-signings, every player who walked, cap room at
each moment (payrolls rebuilt from the events to within $1.4M), the draft, the
deadline and the tax. The reviews and their tools are kept out of the repo
(a scratchpad); what they found, and what changed in the core:

**What it did badly, and why.**

- **Cap room sat idle** (116 summers flagged):
  - CLE $131.9M, HOU $111M and LAL $109M with day-1 stars signing elsewhere.
  - Minimum-payroll penalties paid while teams held room.

  Cause, in what the GM sees: a team's unspent room is priced by the free
  agents it is expected to land (payroll.ts signingsWith), shrinking each day
  as the pool empties - but that pool was only measured in the realistic
  mode's free agency. Under vanilla rules it never was, so every real free
  agent had to beat a signing that no longer existed, all summer. Room was
  spent only once the season began and the imagined signing vanished.

- **Losing teams lost good players for nothing.** CPU veterans rated 55+
  re-signed 49% of the time (BOS, a user team without vanilla's -3: 93%).
  Rookie-deal players rated 55+ walked 33 times, 28 of those from losing,
  low-hype teams; this league lets rookies refuse.
- **Five rookie-deal players on winning teams were let go at near-max asks**
  (Erez Cohen, the next season's MVP; Hoskins; Ortiz; Duru; Mirkovic). Cause:
  the flat money cost (0.03 titles per cap a season) outweighed what they did
  for a team with no title shot.
- **Swaps of similar ratings where production was nothing alike.** WAS gave
  Okokoh (33.9 WS over the next three seasons) for Quinton Wilson (-0.3),
  then PHI gave Bol (27.0) for the same Wilson. On the league's own data,
  production beyond what a player's full ratings predict carries over a
  season (r 0.51), and last season's adds 7 points of R² to next season's
  WS/48 (0.71 to 0.78). Not yet in what the GM sees; next.
- **Deals the GM scored as a big gain for both sides were the most
  lopsided:** a median gap of 6.0 WS against 2.8 for marginal ones. When it
  thinks both sides win big, it is misreading one of them.
- **A long injury counted only in the current season.** GSW gave Arenas $32.4M
  through 2039 on day 1 of free agency while he was out 194 more games.
- **Clearing room for a free agent didn't exist under vanilla rules.**
  Wembanyama (78) signed for $54.9M in 2031 while five teams were $5-17M
  short with spare salary.
- **The trade penalty ignored traits.** A Loyal player minds trades 2.5
  times as much; SA traded all through its 2031 title season and its Loyal
  star refused to re-sign.

**What changed (vanilla sub-mode unless noted).**

1. **The pool is measured every day of free agency.** It is measured before
   CPU teams sign and before each market moment (smartCpu/vanilla.ts
   noteVanillaPool, the same notePoolLeft). The GM's view is rebuilt when the
   pool's share changes, since one outlook serves all of free agency
   (signing.ts viewFor).
2. **CPU teams look every day of free agency** (autoSign.ts). Vanilla's
   random days off (75%, 90% if rebuilding) are gone in free agency; whether
   to sign now or wait is the worth's call.
3. **Retention is vanilla's own willingness** (offers.ts willingChance,
   outlook.ts ensureRetention): his mood toward that team, CPU teams' extra
   testing, the difficulty, his traits. The offer market's stay model and
   KEPT tables are for realistic front offices. A season's record moves it
   on the game's own curve (MOOD_SLOPE).
4. **No money cost** (owner, 2026-11-17: CPU teams are run by their GMs, who
   spend to win; there is no owner). `smartMoneyPerCap` is 0. Salary still
   costs the room it takes and the trades it makes harder. With realistic
   front offices the old cost stays: there a team picks what it offers, and
   free money meant the max for anyone.
5. **A current injury carries into the seasons it covers** (outlook.ts
   injuredShare: what is left of this season, then each offseason's healing
   and each season's games). This applies with either rule set.
6. **Clearing room** (smartCpu/clearRoom.ts). In free agency, for a free
   agent it can't pay who would be among its three best, a team looks for a
   trade sending up to three players (and up to two picks) to a team with the
   room to take them, then signs him. The trade and the signing are judged
   together, against its best signing without trading. The taker must gain
   too.
7. **The trade penalty is scaled per player** (tradePenalty.ts
   tradeMoodScale): Loyal ×2.5, and the difficulty's scaling of mood
   negatives as the game applies it. This applies with either rule set.

**Next.**

- Production in what the GM sees: measure the strength model's per-player
  read against on-court results for persistent misses, then blend a track
  record in.
- Why losing teams don't sell expiring players who will refuse: replay a
  deadline in a fresh run.
- Whether a re-signed contract should count as an asset (its trade value)
  once money is out: wait for the next league to show whether it is still
  needed.
- Vanilla's -3 for CPU teams' players re-signing: removed with the smart CPU
  on vanilla's rules (owner, 2026-11-17; player/moodInfo.ts). CPU teams keep
  their players as a human's team does, and the CPU's own read of who will
  re-sign (willingChance) follows.

## League 6: speed, and a dynasty (2026-11-18)

The owner ran League 6 on build 346: real 2010 rosters, every team the smart
CPU on vanilla's rules, 12 seasons to 2022. It took 5-6 hours.

**The League 5 fixes held.**

- Re-signing: 76-84% across the board, from about 49%.
- Good free agents almost never sat unsigned at opening night.
- 41 room-clearing trades. The player signed produced a median 13.2 WS over
  the next three seasons; the players sent out to make room, 4.6.

**A dynasty.** NYK won 6 of 12 titles at 67-73 wins, on payrolls of
$209-217M against a $112.4M cap, and made 94 trades. Losing teams sold it
young players who became stars: Curry (22, 61/73) for Casspi, Ebanks and two
late picks; Wiggins (19, 57/75, the first pick) for Blake Griffin. As far as
the league's data shows, four things drive it together:

- A team with no title shot values nearly everything at its market price,
  and contenders' bids set that price. They bid for production now, so a
  young player fetches little more than what he does now.
- No money cost: a contender keeps everyone it gets.
- Re-signing at a human team's rate, and mood favors winners.
- Many small edges: contenders came out ahead in 58% of their deals with
  teams with no title shot, a median 1.9 WS over three seasons (League 5:
  54%, 0.4).

Underpriced prospects alone are not the cause. Projections match how these
players developed: prospects (19-21, 53-60/72-78) became 68+ 33-39% of the
time against 8-9% for cheap role players, as projected.

**A floor for young players, tried and left out.** The idea follows the pick
floor: a player up to 24 is priced at least at what the market pays today
for the player he is projected to become, discounted for the wait, since he
can always be kept and sold then. The market's level came from the keenest
teams' real bids for established players (24-30), fitted by overall. It
moved few prices on League 6's 2022 rosters (Kuminga, 59/76: 0.242 to 0.256
titles), since contenders' bids sit above it for most prospects. And it
broke a worth rule: floors add up across a package, so a team sold a
28-year-old 69/69 for a 21-year-old 56/72 and a 19-year-old 36/68 - the
many-for-one hole closed before, and one a human could use to buy CPU stars
with young long shots. Not shipped.

**What changed: speed.** One season of League 6, from the 2022 deadline to
the next, took 18.9 minutes in the test harness; the owner's browser is
slower. The smart CPU was about 95% of it:

- The regular season: 9.8 minutes, of which 8.5 were 13 market moments
  (one every 5 games a team, the owner's call), about 40 s each.
- Free agency: 5 minutes.
- The draft lottery through re-signing: 4 minutes.

The draft classes priced as rookies (the table every pick's price is read
off) were the largest single cost: about 60% of free agency and 40% of an
in-season market moment. Two changes:

1. **Price tables kept while their bidders are** (SmartView.keenest). The
   tables - the draft classes as rookies and the next draft's slots - are
   the keenest teams' bids, and a team's bid is its own worth. They now
   stand while the six keenest teams and what each holds do; other teams'
   signings and trades no longer rebuild them.
2. **Signings don't build the market's tables** (signing.ts
   prepareSignings). A signing is priced by the GM's worth alone
   (smartSigningPrice). The tables were built before every batch of
   signings, and again each time a team that sets prices signed someone.
   They are now built where trades are priced: the market, the draft (which
   prices a pick as a trade), clearing room, and the trade screens.

Both make the very same moves: every roster, contract and pick compared
after free agency and after the first 14 days of the next season. Free
agency went from 354 s to 235 s (175,000 franchise values to 118,000), the
14 days from 113 s to 102 s.

**Next.**

- The in-season market's candidate ranking: every player read for every
  keen team, about 4,000 franchise values a moment.
- The dynasty: what a team with no title shot should count in its own
  future window, measured from the leagues' data, not assumed.
- Re-sign odds are read once an outlook (a week in season), at whatever
  moment first asks for them. With the floor on, that timing alone changed
  in-season trades; they should not depend on it.

## League 7: every team reviewed, and the seller's share (2026-11-18)

The owner ran League 7 from the real 2010 rosters, every team the smart CPU
on vanilla's rules, six seasons to 2015 (two hours). All 30 teams were
reviewed season by season. The league followed the real NBA cap ($58.0M
for the summers of 2010-12, $58.7M in 2013, $63.1M in 2014, $70.0M in 2015),
so the reviews' cap room is on those numbers.

**What worked.** Re-signing at 87-92% for rookie deals and 73-80% for
veterans. All 14 room-clearing trades were followed at once by the
signing, and every top day-1 free agent went to a team that had just
cleared room for him.

**What didn't: the market is run by a few contenders.** Teams with 5%+
title odds started 114 of the 140 market trades, and MIA (26), CHI (25),
OKC (24) and MIL (19) started 94; those four won five of the six titles.
The side that started a deal came out ahead 59% of the time, a median 1.6
WS over three seasons. Bad teams almost never started one (BOS 0 of 10,
BKN 1 of 10). The reviews counted, across the league:

- About 30 deals that fed the best teams. Durant (80/82) went to MIA for
  Beasley (77/80). Harden (70/73) went to MIA for Horford and a late
  first, and was flipped within weeks. Thornton (68) went to the 77-5 MIA
  for a 58 and filler. Holiday (65/72) went to MIL for two expiring
  veterans and two late firsts.
- About 25 prospects sold for little. Isaiah Thomas, CHA's #4 pick, went
  for a 2015 #20 weeks after the draft. UTA's Cory Joseph (58/69) went to
  MIA for three fringe players.
- Bad teams that never sold, and stars who walked (LeBron from CLE,
  Harden from MEM, Drummond from IND).
- 55-win teams that never bought behind a superteam (PHO, POR, BKN, HOU).
- Cap room given away. LAL took a $16.3M dump, and UTA signed Tripp with
  the room it made.
- Churn: CHI bought and resold players within weeks. Contenders traded
  runs of future firsts (CHI six straight).
- Second contracts paid on potential, and multi-year minimum deals for
  washed-up veterans.

**How bad teams become contenders (Leagues 5-7, measured).**

- A bottom-third team contended (top 4, or reached the conference
  finals) within 4 seasons 20% of the time, and within 6 seasons 39%,
  with a median of 3 years.
- What predicted a rise:
  - The best young player's potential (odds ×1.5 per 5 points).
  - Keeping its own firsts (22% when it kept all three, 0 of 13 when it
    had sold two or more).
  - A young best player.
- Cap room and the number of young players predicted nothing.
- The contending roster came 38% from the draft, 30% from trades and 21%
  from free agency, and 62% of its WS arrived after the low point.
- Bad teams that traded young players away gave up 1.36 WS for every 1
  they got back, yet selling youth did not change their own chance of
  rising (23% against 20%). The harm was to the league's balance: it fed
  the contenders.

**The outlook checked against them.**

- Bad teams' title chances: on League 7's 2016 file the outlook gives
  the bottom third 0.0%, 0.05%, 0.17%, 0.9%, 1.3% and 2.3% for seasons 1-6
  ahead. In the finished leagues they won 0.4%, 0.4%, 1.4%, 1.6%, 1.9% and
  3.8% (about 16 titles in all). That is low near term but not far off.
- Where a team's own pick lands: a top-three team's pick lands 0.89 of
  the way down the order a season later, 0.66 three seasons on and 0.56
  four on. The outlook's regression of contenders matches.
- So the bad teams' losses came from how deals are struck, not from what
  the outlook sees.

**The cause, in the market.**

- A winning offer is trimmed to beat the next best one. With one team
  wanting a player far more than the rest, the next best is nothing, so
  the seller kept a sliver.
- Offers are built by the seller's own use of each piece, which is near
  zero for a team out of the race. In-season, nearly all offers were a
  single piece.
- In the deals the market found on League 7's 2016 file, the seller kept
  a median fifth of what the deal made for both sides.

**What changed: the seller's share** (market.ts sellerShare, fairToSeller).

- A seller won't hand a team more than 0.01 titles of gain while keeping
  less than a third of what the deal makes for both: it holds out, or the
  buyer pays more.
- The winning offer is never trimmed below that share.
- Small swaps are left alone.

On League 7's 2016 season, the rule turned down 7 lopsided deals and the
market made as many deals as before (3 against 3). The sellers kept 0.38,
0.66 and 0.53 of the gain, against 0.32, 0.06 and 0.82 without it. MIA's
LeBron went to LAC for Harkless (23y 61/67) rather than for filler. What
it does to dynasties is for the owner's long run to show.

### Offers by the seller's worth, and room with the seller's share (2026-11-18)

**Offers built on what the seller values** (worth.ts offerFrom).

- A buyer builds its offer from its players and nearest picks, best for
  the seller first. "Best" was the seller's use for each piece, which is
  near zero for anything when the seller is out of the race, so it was
  offered whatever came first. Contenders paid for stars with filler while
  their young players and picks stayed home.
- Pieces are now ranked as the seller counts a deal (dealWorthNow): its
  use for a piece, or the market's price if more.
- The price tables are read once for the whole offer, not again for each
  piece. Read for each piece, they had made a moment half again as long.

**Room has the seller's share** (clearRoom.ts).

- The team taking salary so another can sign a free agent is selling its
  room. It now gets the seller's share of what the deal makes, past the
  clearer's best signing without it.
- The clearer adds sweeteners, cheapest first and up to four picks, until
  it does.
- Before, LAL took a $16.3M dump and UTA signed Tripp with the room, when
  LAL could have paid him itself.

**Measured.** League 6, from the 2022 playoffs to the 2023 preseason,
against master:

- Market trades: 9 against 7.
- Room-clearing trades: 2 against 3.
- The seller's median share of each deal's gain: 0.61 against 0.55.
- The buyer's median gain: 0.018 against 0.010 titles.
- DEN's Paul George (32y 67) brought back Ingram (25y 61/63) and a first.
- A moment after the draft took 23.5 s against 20.6 s, for the prices of
  the pieces offered.

### A price only as far as the team can get it (2026-11-18)

**What was wrong.** In a deal, a team counts what it gives and gets at its
use or the market's price, whichever is more (premiumNow). The price is
the guard against selling below what another team would pay later. A
player whose deal ends this season can't be sold later if he walks, but
his price was counted whole. His team held out for a price no real offer
reached, and lost him for nothing. On the leagues' files:

- League 6: Cam Reddish (0.00 to stay) was priced at 0.25 titles against
  0.004 to IND, and turned down four players. Mudiay (0.01) turned down
  D'Angelo Russell (65).
- League 7: Otto Porter Jr. (Fame; 0.25 to stay) was priced at 0.67
  against 0.03 to PHO, and had no offer PHO would take.

**What changed.**

- A player whose deal ends this season counts above his use only as far
  as he stays with the team. Players under contract past it, and picks,
  are as before.
- With it, Reddish's best offer is worth +0.040 to IND (was -0.090) and
  Mudiay's +0.097 (was -0.033).
- A full 2016 season of League 7: 8 deals against 2, four of them
  expiring players sold (Hardaway Jr., Caldwell-Pope, Biyombo, Oladipo),
  the sellers keeping 0.38-0.80 of the gain.
- The worth rule "two lesser players don't buy a star" now holds for a
  star whose deal runs past this season. A 29-year-old 67 with his deal
  ending and 0.63 to stay, sold by a team with no use for him for a
  21-year-old 56/72 and a 19-year-old 36/68, is what a rebuilding team
  should do (Leagues 5-7: sell veterans, keep youth).

**Next.** The same season had LAL selling Caldwell-Pope (23y 67/75) to
the 71-11 LAC for two lesser players, gaining 0.21 titles by its own
count. A team out of the race still counts players it gets at the
contenders' price, whether or not it will ever sell them. That price
arithmetic is the next core item.

### A run counts by the series won (2026-11-18)

The owner: "i dont want teams just happy winning 43 games every year and
being the 8 seed. at that point they are in purgatory and should probably
tear down ... a part of building a championship winner from youth is
getting to the point of making the playoffs ... i want to protect against
purgatory teams."

**What was wrong.** On vanilla's rules a playoff run counts beside a title
(`vanillaAmbition`, 0.25). The run was made up of making the playoffs and
each series won, as a share of a champion's run. With four rounds, going
out in the first round was 0.2 of a champion's run, so 0.05 of a title a
season. A team sure of the 8 seed every year banked 0.05 a season, 0.34 of
a title over the outlook (this season and ten more, at the 0.9 patience).
That is three times what building looks to buy. Over the same seasons, at
the same patience, League 7's bottom third had 0.10 of a title in title
chances, and its middle third 0.12. Staying a 43-win team was worth more to
the GM than building.

The leagues have many such teams. A third of all team-seasons (29% in League 5, 31% in
6, 32% in 7, 38% in 8) were 45-58% wins with no series won. Middle teams
sold a veteran for youth or a first in 0-2 of their 65-101 trades in
Leagues 7 and 8.

**What changed.** Only series won count toward a run
(`outlookSettings.seriesOnly`). Making the playoffs alone counts nothing.
Each series won is a quarter of a champion's run with four rounds, and a
title with its run is still worth 1.25. A team that makes the playoffs on
the way up gains from it only as the series it will win come into reach.
Those series count in every season of the outlook, so a young team still
gains from getting good.

What a player is worth to each team (% of a title; `ambitionProbe`, the run
counted by making the playoffs, then by series won):

| league, team (title odds) | its own best veteran         | a prospect (37-42/62) |
| ------------------------- | ---------------------------- | --------------------- |
| League 8 2013, CLE (0%)   | LeBron James 6.21 to 3.64    | 1.87 to 1.59          |
| League 8 2013, MIA (0%)   | Dwyane Wade 4.57 to 2.89     | 2.08 to 1.79          |
| League 8 2013, TOR (0.1%) | Chris Bosh 4.09 to 2.95      | -                     |
| League 7 2016, TOR (0%)   | Marco Belinelli 6.55 to 4.04 | -                     |
| League 7 2016, MEM (0%)   | O.J. Mayo 4.90 to 3.10       | 0.55 to 0.48          |

To the favorites nothing changes much: a 69-70 star is worth 1.44 to 1.48
titles to OKC (57.6%) and 1.32 to 1.36 to LAC (19.7%). A middle team's
veterans are worth 28-41% less to it, its prospects 13-15% less.

**Seasons played both ways** (`purgatoryProbe`): League 7 from the 2016
preseason to 2017's, League 8 from after the 2013 draft to the 2015
preseason, each once with each count, from the same start.

| run      | trades, before / after | middle teams' trades: veterans got/gave, before / after |
| -------- | ---------------------- | ------------------------------------------------------- |
| League 7 | 17 / 16                | 3/4 / 7/3                                               |
| League 8 | 22 / 17                | 6/1 / 1/0                                               |

No clear change in trading. Middle teams sold a veteran for youth or a
first twice in League 7 before and in none of the other three runs. The
goal was one part of purgatory; the other is in the prices. In League 8
(two favorites, OKC and ATL) the best real offer for the veteran of a team
out of the race was worth 0.16 to 0.31 of a title to the seller: spare
players from one favorite (LaMarcus Aldridge 58 with two bench players and
a second; Jonas Jerebko 54 and a second), counted at what the other
favorite would pay for them. A middle
team gains more by taking a favorite's spare players at that price than by
building. That is the price arithmetic named after the last section, and
the next core item.

## League 10: young players sold for prices nobody pays (2026-11-19)

The owner ran League 10 from the real 2010 rosters to the middle of 2019
(build 1197). Two teams ran the league: Sacramento won 61-79 games every
season from 2012 and Washington four of the last six titles, 46-1 when the
file was saved. Together they held 8 to 10 of the league's 20 best players
every season from 2014 to 2018. Memphis never made the playoffs and sold
Conley, Gasol, Mayo, Kidd-Gilchrist, Jamal Murray and Donovan Mitchell along
the way. The owner: "it feels like they're selling off these potential
stars for not near the asset that star is."

**What was wrong.** Two things, both in how a team counts what it gets.

- What a team got counted at its market price where that was more than its
  use. A price is the second-highest keenest team's use - what it would
  gain, not what it can pay - and in a league with a dynasty it runs far
  above what anyone offers: the market's best real offers for League 10's
  young players came to a quarter to three fifths of their prices. In
  every deal of a League 7 season and of offseasons in Leagues 8 and 10,
  the seller's own goal moved -0.02 to +0.02 of a title; the rest of its
  gain was the price of what it got. In a League 8 run Milwaukee sold Tony
  Wroten (21, 59/76) to OKC for J.J. Redick (30, 41), two bench players and
  OKC's 2018 first and second: -0.22 of a title by its own goal, +0.64 in
  the price of what it got, nearly all of it the picks. In a League 10 run Detroit sold Ja
  Morant (21, 61/76) for D'Angelo Russell (-0.12 by its goal) and Memphis
  sold Zion Williamson (20, 63/75) for Andrew Wiggins and a second.
- A pick in a draft past the next was priced by its own draft's class,
  undiscounted. A real-players league holds every class to come, and their
  prospects priced as the dynasties would use them: MIL's 2021 first 0.71
  of a title against bids of 0.08 (its 2019 first 0.13), a 2021 second 0.25
  against 0.01, and later drafts' picks above sooner ones (PHO's firsts
  0.66, 0.89, 0.91, 1.13 for 2014-2017 in League 8).

**What changed.**

- What a team gets from another team counts at its own use for the players;
  only picks keep their price, as their floor stands in for title chances
  past the outlook (`smartSettings.boughtAtUse`). What it gives still counts
  at least at its price: no team gives a player away below the market.
- A pick in a draft past the next is priced by the next draft's class,
  discounted a season for each draft between
  (`smartSettings.laterPicksByNextClass`). MIL's 2021 first is 0.22, WAS's
  0.075 (was 0.34), a 2021 second 0.04.

From the same starts, before / pick prices only / both:

| run                                 | before | picks | both  |
| ----------------------------------- | ------ | ----- | ----- |
| League 10, mid-2019 to 2021: deals  | 25     | 29    | 16    |
| the 20 best on the top three teams  | 11     | 9     | 9     |
| best 2020 record                    | 77-5   | 77-5  | 69-13 |
| teams winning 50+ in 2020           | 5      | 6     | 8     |
| minutes for the run                 | 40     | 22    | 20    |
| League 8, 2013 draft to 2015: deals | 12     | -     | 7     |
| minutes for the run                 | 33     | -     | 14    |

With both, Morant, Williamson, Ntilikina and Giles stayed home, and in
League 8 Wroten, Tony Snell and Damian Lillard did. The young players still
traded went for young players (Tyus Jones for Wiggins and a first) or were
about to walk (Mudiay, 0.02 to re-sign). The runs are quicker because only
the next draft's class is priced.

**Next: re-signing forecasts.** A team's young players are worth little to
it when it expects them to leave, and bad teams expect it too often. Kept
for sure, Luka Dončić would be worth 0.36 of a title to OKC (0.05 now),
Jayson Tatum 0.21 to CLE (0.03), Marvin Bagley 0.11 to MEM (0.007). For
deals ending this season the forecast held (League 10, 2019: rookie deals
forecast 99%, all 31 re-signed). A season or two ahead it did not: in
League 7, 2016-2017, rookie-deal players on teams under 40% wins were
forecast 49% to re-sign and 83% did (12 players), and those forecast under
20% re-signed 43% (7). More leagues are being measured.

## League 34: three trades nobody should make (2026-11-19)

The owner's first look at League 34 (the real 2026 rosters, a few weeks into 2027) turned up three trades:

- The Lakers, 32% title favorites, gave Jake LaRavia (26, 56/59), two bench
  players and their 2028, 2030 and 2032 firsts to Cleveland for Sam Merrill
  (31, 54): "the most broken trade of all time".
- Denver gave DeMar DeRozan (38, 58) to Philadelphia for Kentavious
  Caldwell-Pope (34, 52): "why would they accept this? ... just hang onto
  DeRozan at that point".
- New York gave Jose Alvarado and three seconds to Detroit for Javonte
  Green (34, 55).

Each trade was undone and played out in the game's own sim: the team with
the trade and without it, 300 games against every other team on the same
random numbers (`src/test/impact/swapLab.test.ts`). The Lakers were 0.33
points a game worse for theirs, Denver 0.35 worse for theirs, New York 0.52
better for theirs. New York's trade is sound - Green helps them more than
Alvarado did, and the seconds are what that costs.

**Denver: a player let go came back.** Whom a team lets go past his
contract (wantedOnly, letGo) was decided by option, and a veteran was one
option while his projection was one line and two - each half on its own -
once his future was uncertain enough to split. DeRozan, let go after his
contract ran out in 2027, was gone in 2028 and back in 2029, at 40, paid
his market salary out of Denver's room: a point of 2029 strength Denver
would never have lost, which trading him "saved". This was in every team's
outlook, for every veteran whose projection splits a few seasons after his
contract: a standing reason to sell old players for less than they are
worth. Now a player let go is gone for good, whatever his line does later,
and so is one too unlikely to be there to decide on that season.

**The Lakers: the roster model believed too much.** What it says a player
does is fit on the sim, and much of what sets one apart from another of his
overall is the fit's own noise. On 600 removals (a rotation player taken
off his team, 1,000 games each way) and 720 swaps (a rotation player for
another team's of the same overall, 1,160 games each way) in Leagues 11 and
34 (`src/test/impact`: `removal`, `swapLab`, `profileLab`):

- in the removals, the part of a change a player's overall explains came
  in as the model said (1.05 +- 0.03 of it); the part from the rest of his
  ratings at 0.77 +- 0.04;
- a swap of players of the same overall moved the sim 0.75 +- 0.05 of what
  the model said in League 34, 0.88 +- 0.06 in League 11;
- the model's "typical player of an overall", an even profile, was far
  below the real players of that overall (0.5 points of value at 40-45, 2.3
  at 55-60, 3.7 at 65-70, the same in three leagues);
- League 34's real ratings run well past the random players the model was
  fit on (three-point to 93, field goal and endurance to 100, where the fit
  saw 77), and past that range its squared terms only guess.

A market that picks the best-looking deals out of thousands picks exactly
the model's overstatements. Now (strength.ts):

- each player is the average line of real players of his overall and 0.85
  of where his own ratings part from it, in his skills as in the rest
  (`strengthSettings.profile`, `profileSynergy`; the average line
  `AVERAGE_OVER_EVEN`, fit on Leagues 10, 11 and 34);
- each rating's curve carries on straight past the range the fit saw
  (`FIT_SUPPORT`, `supportTails`).

On the 1,320 changes, the model's error past the sim's own noise is 11%
smaller (0.154 to 0.137 points squared): League 34's swaps 17%, its
removals 22%, League 11's removals 18%, League 11's swaps the same. The
model's changes are now as big as the sim's (0.99 of them; swaps 0.90 in
League 34, 0.96 in League 11), across the whole range - the tenth of swaps
the model likes most came in at +1.10 +- 0.10 in the sim, where it now says
+1.10 and said +1.25. Of the 11%, the straight curves are 3%. League 11's
swaps were measured after the shrink was first chosen, as a check.

The three trades, priced again as they were offered:

| trade                     | worth to the buyer before | now     |
| ------------------------- | ------------------------- | ------- |
| Lakers get Merrill        | +0.085                    | +0.034  |
| Denver gets Caldwell-Pope | +0.0006                   | -0.0018 |
| New York gets Green       | +0.009                    | +0.003  |

League 34 played on from the same start to the next preseason, before and
after (one run each): 34 deals and 26, about the same at a run each. Gone
are Denver selling DeRozan and Minnesota, 0.5% to win the title, paying two
firsts and four players for Jalen Smith (51, expiring); the runs took 13
and 13.5 minutes.

Denver no longer makes its trade; New York still makes its sound one. The
Lakers still make theirs, for half
the gain: the model has it at +0.39 points this season, the sim at -0.33
+- 0.18 - a miss of about two of the model's standard errors on a
three-for-one. What is left of it is not the shrink: elite shooters
(three-point 80 and up) are still rated 0.37 +- 0.12 points too high when
taken off a team (0.43 before), most of it inside the fit's range. That is a fault in the model's
fit itself, from random players' leagues; refitting it on real players'
leagues is next.

## League 30: firsts for players who barely move a team (2026-11-19)

The owner's League 30 (the real 2026 rosters again) had the Lakers give two
firsts and three players for Sam Merrill (54), the Lakers a first and Jarred
Vanderbilt (51) for Robert Williams (47, $14M through 2029), and Houston two
firsts and three young players for Luke Kennard (51): "those pick teams are
trading away could be incredibly valuable ... unless it's a genuine way to
actually contend this year/next. Not if it's for a 50 overall -5 bpm guy".

**What the sim says.** Each trade played out both ways, 300 games against
every team: Robert Williams made the Lakers 0.19 points a game worse (the
model said 0.24 better); Kennard made Houston 0.43 better (the model said
0.45). Over 100 days of Leagues 30 and 34 from the same files, every trade
the market made was played out the same way for both sides
(`src/test/smart/tradeAuditProbe.test.ts`): where the model said a side
gained on the floor (+0.55 on average, 24 sides) the sim gave +0.57 +-
0.09, and of the 9 sides that gave firsts for it 7 got better on the floor.
Huerter for OKC was +1.11, Zach Edey +1.66, Paul Reed for Minnesota +0.71.
The players bought are mostly real upgrades; their overall undersells them.

**The roster model, refit.** The removal and swap labs on two more leagues,
30 and 7 (`src/test/impact/refitLab.test.ts`, 2,620 changes in all with 11
and 34), as rows of a ridge
regression on the model's coefficients, around the fit as it is, the random
players' leagues' average line by overall held fixed so the pick and free
agent tables stay true: held out a league at a time (Leagues 30 and 34 as
one, their rosters the same), 0.149 of error past the sim's noise against
0.150 for the model as shipped. Diminishing returns on the sim's own team
composites (rebounding, defense, ...) added nothing that held from league
to league. The model is as good as these labs can make it; what is left is
a few tenths of a point a player either way, and a market that buys on it.

**Picks.** A late first is priced at about 0.02-0.03 of a title and a top
pick 0.10-0.16. Priced against what they become, picks are consistent: the
players a slot took one to four seasons ago are worth what the slot is now,
discounted for the wait (`pickConsistencyProbe`: League 5's 21-30 picks
0.99 and 1.12). In the leagues' own drafts picks 21-30 became a 65 within
eight seasons 5-11% of the time and a 70 1-5%; as the best quarter and the
rest (`boom`) rather than two halves, a late first is worth 10-20% more,
the class as a whole 3-6%.

**What changed: the GM's patience** (the owner's call): a title five years
away counts 77% of one now, not 59% (`PATIENCE`, 0.95 a season). A first
pays off four to eight seasons on, so at 0.9 it was worth about half as
much to a GM as what it turns into. The Lakers' 2027-2033 firsts are now
0.026-0.057 (0.020-0.036 before), Indiana's 0.080-0.117 (0.047-0.095); the
Lakers no longer give two for Merrill (their gain -0.003).

From the same League 34 start to the next preseason: 26 deals at 0.9 and
31 at 0.95, 15 firsts traded and 19 - the new ones mostly for players 19-23
with 63-69 potential (Washington's for Jordan Smith Jr., 20, 48/69, and
Tyran Stokes, 19, 42/68); the same champion; 1,143 s and 1,196 s side by
side. A team at 0.4% still gave two firsts for Josh Minott (52), as at 0.9.

**Tried and left off: a run counted by its depth squared** (`runDepth`), so
a first-round win counts a sixty-fourth of a title, not a sixteenth: the
same season had the same middle-team buys (Minnesota at 1.1% two firsts for
Paul Reed, 52; Houston at 2.2% two for Cam Spencer, 54).

### The buyer's choice (2026-11-19)

The owner, of Minnesota's two firsts for Josh Minott (52): "is that REALLY
the best they could get for their 2 1sts? ... that buying team needs to be
smarter and try to get a better upgrade".

It wasn't. Offered to every team for each of its players, the same package
(the two firsts, Terrence Shannon Jr. and Trey Kaufman-Renn) would have
bought 48 players their teams would sell, and by Minnesota's own worth
Tristan Da Silva (56/59) and Jaylen Wells (57/61) were each about 0.03 of a
title better buys than Minott (`buyerOptionsProbe`). Da Silva was on offer
to Minnesota that moment (`marketMomentProbe`).

**What was wrong.** A team makes one deal a moment. The market found every
deal - each its seller's best offer for the player - and made them in order
of what the seller gained, so a buyer with several open took whichever its
seller wanted most. At one League 34 moment the Lakers were the winning
offer in 16 of 26 deals and made the one worth 0.025 of a title to them
with others worth 0.050 and 0.044 open (`momentDealsProbe`).

**What changed.** Every deal found is trimmed as it would be made, and they
are made in order of what the buyer gains (`marketSettings.buyersChoose`):
each seller still takes the best offer for its player, and each buyer the
best deal open to it.

From the same starts to the next preseason, before and after:

| run                            | before | after |
| ------------------------------ | ------ | ----- |
| League 34: deals               | 31     | 23    |
| League 34: firsts traded       | 19     | 10    |
| League 30: deals               | 31     | 26    |
| League 30: firsts traded       | 21     | 16    |
| minutes for the run, League 34 | 18.4   | 19.2  |
| minutes for the run, League 30 | 17.8   | 17.6  |

Firsts bought more: Mikal Bridges (65), Stephon Castle (59/67), Dyson
Daniels (62), and for a team building, AJ Dybantsa (20, 54/72). Minnesota
no longer gave two for Minott (Orlando, in the playoff race, gave one).
Still made: Minnesota at 0.3% two firsts for Ryan Kalkbrenner (51) in
League 30 - its best deal open - and OKC three for Anfernee Simons (55),
a contender's.

### League 36: two firsts and Sexton for Kevin Huerter (2026-11-19)

The owner, of the Lakers giving Collin Sexton and their 2030 and 2032
firsts for Kevin Huerter (55): "huerter sucks. like he actually performs
like a bad nba player. in what world are the lakers giving away 2 1sts plus
sexton for him???"

**On the floor he helps them.** Huerter for Sexton, played 8,700 games a
side against the league as it stood, made the Lakers 0.73 points a game
better (`swapLab`); the roster model said 0.81. The box score reads him at a
BPM of -3.0; the Lakers were +3.1 a game with him on the floor. The deals
open to them that moment were real upgrades too: Gafford +1.23, Jay Huff
+1.00, Duncan Robinson +0.44. What was wrong was the price, and why each
buyer paid it.

**What a point is worth to the favourite was overstated.** Title chances
came off one logistic curve fit across all thirty teams. With one team
played 0.8 points stronger and everyone else as they were
(`curveSlopeProbe`), the favourite's chances rose 6-7 points in Leagues 30,
34 and 36 where the curve said 11; seasons ahead the curve said 1.3-1.5
times what the seasons did; and an East team as good as a West one got the
West's chances (Boston 8% by the curve, 12% played). Now each team prices
by its own curve (`outlookSettings.ownCurves`): the season's simulated runs,
each reweighed by how much likelier its draw of the team's strength would
be with the team half or one doubt stronger or weaker (`simulateSeason`'s
`shifts` - no season played again), fit as the league's was. A team with
few titles in those runs is read partly off the league's curve, each title
against 20. Read off a thousand runs, a contender's curve moved a fifth from
one set to the next - enough to trade on the noise - so this season and
next are now played four times as often (`ownNearRuns`, about a second more
each week the outlook is built). Against the seasons played again, the
Lakers' 0.8 points are now 7.9 points of title (played 7.3), Boston's level
11.2% (played 12.3%). Huerter was 0.22 of a title to them; now 0.17.

**The seller took a third of a gain it couldn't sell alone.** At that
moment the Lakers had 23 deals open, a role player in each, and nobody
bidding against them for any: the next best offer was worth 0.001 to every
seller (`marketMomentProbe`). What set the price was the seller's share - a
seller keeps a third of what a deal makes for both. With the Lakers gaining
a lot from any of them, each of the 23 cost two firsts and a player, as if
Detroit were the only team with a shooter to sell. Now the share is of what
the deal makes past the buyer's next best (`marketSettings.
buyerAlternatives`): its best deal with another seller, that seller taking
only what beats keeping the player. A seller with something nobody else can
sell still holds out for its share; one of 23 sells for what beats keeping
him. Huerter would have cost Sexton and the 2030 first.

**And buyers took gains they couldn't be sure of.** Even then, a third to a
half of the deals paying firsts gained the buyer under 0.01 of a title -
Oklahoma City three firsts for Cedric Coward, 0.002 - while its read of a
one-player swap is off by about 0.04. The design left a margin for the
model's own error to the owner; the owner chose it (2026-11-19). Now each
side must gain by more than its own read of the deal could be off
(`marketSettings.margins`, `SmartView.marginNow`): the older front office's
reckoning (`margin.ts`) - each player who moves, his on-court value and his
career, and each pick, through every season he is expected to be there - by
what a point is worth to that team then, off its own curves. The on-court
error is the roster model's as it is now: against 600 players taken off
their rosters in the sim (the removal lab, Leagues 34 and 11), by most
likelihood with each sim's own noise, 0.52 points a full game at every
overall (the first model's grew from 0.4 at 52 to 1.1 at 70). At the
Lakers' moment Sam Hauser for Mamukelashvili and two firsts (a gain of
0.004 against a margin of 0.107) and Day'Ron Sharpe (0.0002 against 0.034)
fall away; Huerter (0.104 against 0.026), Robinson, Gafford and Huff (0.14
against 0.034) stay.

Measured otherwise and not built: buyers asking after their own best
targets each moment (no new deals - the targets were stars nobody would
sell or players who hardly mattered), and the value of keeping a first for
a better deal later (in League 30 frozen for a season, a buyer's best deal
paying a first was beaten later 89% of the time; in League 34, half).

The owner's league files were lost with the container before the runs on
them finished; on three random-players leagues (`arena:301-303`), one
season each, before and after:

| run                                       | before | after |
| ----------------------------------------- | ------ | ----- |
| deals                                     | 106    | 75    |
| firsts traded                             | 72     | 52    |
| deals paying firsts for veterans under 60 | 13     | 7     |
| deals paying firsts for players under 26  | 36     | 26    |
| minutes for the three seasons             | 34.1   | 34.6  |

What fell away besides were mostly near-equal swaps between teams out of
the race (a 48 for a 50 and seconds). Still made: Denver at 9% two firsts
for a 30-year-old 54 - a gain past its margin, to look at again on the
owner's leagues.

## League 12: young players kept, prices not counted on, drafts for the team itself (2026-11-20)

The owner ran League 12 from the real 2010 rosters to the 2018 draft on the
371 build. Rebuilding teams sold the young players they should have built
around: Utah Andrew Wiggins (22, 70/79) for Hernangómez, Reggie Jackson and
a #28 pick; Cleveland D'Angelo Russell (22, 65/75) straight up for Malcolm
Brogdon to the 71-11 champion; Milwaukee Michael Carter-Williams and Bruno
Caboclo, New Orleans Giannis - each in the last season of his rookie deal.
The owner: they "should basically be untouchable for these teams ... they
should also judge what they need to do in order to get the player to stay
... maybe the team needs to buy to make the player happy". And of the rest
of the review: firsts for role players "very very very very bad", churn to
go ("both sides must genuinely benefit"), teams clearing room only when they
truly gain, bad teams drafting older players with less upside. Played on
from the 2018 draft on current master, Detroit sold Luka Dončić (22, in the
last season of his rookie deal) to Chicago in the 2021 preseason for JaKarr
Sampson and two seconds.

### The re-signing forecast, for the season his contract runs out

On vanilla's rules a player re-signs only if he is willing, and the game
decides that from his mood toward the team the summer his contract runs out:
its record that season, its hype after it, his seasons with it. The outlook
took his chance from his mood today, as if the team would lose as it is
losing now until then. On the 2018 draft's file John Collins (21, 59/71,
through 2020) stood 0% to re-sign with Washington, Zach LaVine 1% with
Phoenix, Jayson Tatum 22% with Indiana: a bad team's young players were worth
next to nothing to it, and in the last season of a rookie deal their price
counted only as far as they would stay - so almost anything beat keeping
them. And a team's own draft picks read 100% (the game can't let a pick
refuse his rookie deal) until the draft was over, then fell to their mood.

Now his mood is forecast for the season his contract runs out
(`outlookSettings.retentionAtExpiry`, `resignForecast.ts`): the team's record
that season as the outlook expects it, drifting back toward the middle as
finishes do, and as far off as it may land; its hype built up season by
season by the game's own rules (each game against 0.55 and the seasons
before, and 0.05 for making the playoffs - in League 12 playoff teams ended
their seasons 0.065 above the per-game rule alone); his seasons with it by
then; the rest of his mood as it is now - each part counted as the game
counts it (`finishedComponent`, now shared with `moodComponents`; every
rostered player's mood toward three teams in League 12 is the same before
and after, user team and CPU teams). A move that makes the team better by
then makes him likelier to stay, one that makes it worse less, player by
player and trait by trait (a W cares two and a half times as much about the
record): buying to keep a young player happy is worth what it does to his
chance of staying. Collins is now 72% to re-sign, Tatum 64%, LaVine 32%,
Dončić 64% (not 100%).

Against what the game decided, three leagues each played three seasons on
(`resignForecastProbe`: each preseason, deadline and draft's end, every
player whose contract ran out within three seasons, forecast; then each
player's own chance of being willing as the game asked him when it ran
out), players rated 55 or better still with the team then - log loss of the
forecast, new against old (lower is better):

| league                  | 1 season out, rookie deals | 1 season, veterans | 2 seasons, rookie deals | 2 seasons, veterans |
| ----------------------- | -------------------------- | ------------------ | ----------------------- | ------------------- |
| League 12, one run      | 0.32 / 0.64                | 0.37 / 0.56        | 0.38 / 0.75             | 0.51 / 0.75         |
| League 12, another run  | 0.26 / 0.42                | 0.30 / 0.55        | 0.22 / 0.74             | 0.49 / 0.97         |
| a random-players league | 0.21 / 0.24                | 0.32 / 0.54        | 0.52 / 1.73             | 0.43 / 0.72         |

Those the old forecast gave under 20% were willing 30-37% of the time; the
new forecast's under-20% group, 8-13%. Two seasons out the new forecast still
ran 5-10 points low on veterans; those runs were before the playoffs' hype
was counted.

### Prices a team can be sure of

Even so, played on from the 2018 draft, teams out of the race sold their
young players for picks years away from the favourites, their own goal
falling: Golden State Cedi Osman (23, 56/65) for Dallas's 2022 first,
Milwaukee Tyler Dorsey (23, 54/62) for four scrubs and Dallas's 2023 first
and second, Cleveland Patrick McCaw (23, 53/62) for John Henson and a 2022
first (master too). In each the seller's goal fell (-0.014, -0.118, -0.037)
and its gain was the pick's price - what the keenest teams would gain from
it, 0.18 of a title for that 2022 first against 0.010 of use to Cleveland.
Picks got from another team keep their price (their floor stands in for
title chances past the outlook), and the market's best real offers come to
a quarter to three fifths of a price (League 10). Now each side must clear
its margin of error counting what it gets past what it gives in price at a
quarter (`smartSettings.premiumRealized`, `SmartView.sureWorthNow`); a price
given up still counts in full, so nothing a team holds sells for less than
before. Clearing room counts the same, the team clearing and the team
taking the salary each past its own margin (`clearerGains`, `takerGains`):
in League 12, of 28 rooms cleared, six swapped like for like and paid a
second to do it, and Philadelphia paid a first and three seconds for Corey
Brewer.

**A package no team could take whole.** A team gives what it gives at least
at its price - but a first sent with salary no keen team could absorb under
the trade rules had no price at all as a package, and went at its bare use:
Dallas a 2024 first (priced 0.31, of use 0.02 to it) with two players on the
minimum for Aaron Nesmith (21, 42/58), Chicago two firsts (0.38 each) with
Jabari Brown for Marvin Bagley, San Antonio a 2023 first with Austin Rivers
and Emanuel Terry for Jordan Bell (47/52) - and a young star sent with such a
contract would have gone the same way. Now such a package is priced piece by
piece, each at its own price or, where nobody could take it either, its use
to the team (`SmartView.packagePriceNow`).

**Half a percent of a title.** Teams out of the race still swapped bench
players: to a team with no title in sight every player moves its chances
little, and its margin of error as little - New Orleans gave Nate Wolters
(48) to Oklahoma City for four players of 41 to 48 and a second (OKC +0.0031
against a margin of 0.0005), Phoenix Robin Lopez for Isaiah Whitehead
(+0.0048). Each side must now gain past half a percent of a title, counting
what it can be sure of (`marketSettings.minGain`, 0.001 before).

**What contenders pay for depth stays theirs.** A contender values a role
player highly: Dallas, the favourite, Patrick McCaw (53) at 0.36 of a title.
Played 2,320 games against the league as it stood (`swapLab`), McCaw made
Dallas 1.18 points a game better (the roster model said 0.90), and even Noah
Vonleh (48) 0.17 - the sim plays the 8th to 10th men real minutes. A late
first for that is the favourite's call.

### The draft

A prospect a team drafts counted at the market's price: what the keenest
teams, contenders, would gain from him - and they gain most from players
ready now. So bad teams drafted what contenders wanted: Álex Abrines (23,
50/58, 34th on the board) at #7 and Dāvis Bertāns (24, 53/57, 38th) at #10 in
2016, Kevin Huerter (16th) at #4 and Landry Shamet (24th) at #7 in 2018. By
its own use alone, tried first, a bad team passed on the draft's best
prospect where he would likely not re-sign with it, as if it could not trade
him: Memphis took Tyrese Maxey (19th on the board) at #2 and LaMelo Ball
(first) fell to #4. Now a team drafts by its sure worth: the prospect's use
to it - his chance of re-signing forecast for the end of his rookie deal -
and his price past that at a quarter. And between prospects within its
margin of error of the best, the board decides (`boardChoice`): far from
contending every prospect does a team little, and by its own read alone
Detroit took Bol Bol (38/63, 20th) at #8 over Jaxson Hayes (48/76, 3rd),
0.065 to 0.057. On a random-players draft, a bad team at #3 now has a
22-year-old 55/65 7th on its board (6th by price, 9th by use alone).

### Played on

League 12 from the 2018 draft to the 2021 preseason, current master against
all of this, the same start twice:

| League 12, 2018 draft to 2021                              | master                             | after, one run | after, another |
| ---------------------------------------------------------- | ---------------------------------- | -------------- | -------------- |
| trades                                                     | 26                                 | 24             | 23             |
| market deals one side gained under 0.005 by (sure)         | 9 of 14                            | 0 of 14        | 0 of 15        |
| young stars sold for spare parts                           | Dončić for Sampson and two seconds | none           | none           |
| top-ten picks: average place on the board, 6+ places early | 7.2, 4                             | 6.3, 1         | 7.0, 2         |
| minutes for the three seasons                              | 27.4                               | 29.8           | 28.7           |

Young players still moved, for what they were worth: Memphis Landry Shamet
(21, 51/63) for three firsts, Denver Lonnie Walker (22, 62/75) for three
firsts, Phoenix paid three for Kobi Simmons, Chicago two for Deandre Ayton.
The 2018 draft's players finishing their rookie deals with bad teams were
sold in the 2021 preseason - Jaren Jackson Jr. (22, 67/77) by Indiana for
Chicago's 2024 first and two seconds, Collin Sexton, Zhaire Smith - each
forecast 3% to 24% to re-sign: on the league's rules (rookies can refuse)
a 20-win team keeps such a player only if he is willing, and none of them
would be; a first beats losing him for nothing. Rooms cleared were each
followed by a clear upgrade: Golden State Brandon Ingram (22, 71/82),
Atlanta Jonathan Isaac (23, 74/77), Utah Ricky Rubio, Charlotte Jarrett Allen.

## League 13: players left for nothing, offers that never added up (2026-11-20)

The owner asked where League 13's teams (2010-2023, every team CPU, the
399-401 build) lost out by doing nothing. The worst: Oklahoma City let Kevin
Durant (28, 79, $19.4M in his last season) leave for nothing from a 29-53
team in 2016 - no trade all season - and went 17-65, 3-79, 14-68; Charlotte
Greg Monroe (69), Orlando Ryan Anderson, Milwaukee Kelly Oubre the same.
Contenders stood pat as well: Golden State, 60-22 and 39% to win the title in
the preseason in 2013, made no trade and went out in the first round to a
41-41 team.

### Salary that never added up

On League 13's 2023 file, San Antonio's Karl-Anthony Towns (28, 61, $30.9M
in his last season, 11% to re-sign) was worth 0.004 of a title to San
Antonio and 0.06 to 0.17 to each of the six keenest teams - and five of the
six had no legal offer for him. An offer was the buyer's players and picks
best for the seller, then, short of salary, the players it would miss
least - minimum contracts - up to six pieces in all: never a big salary. At
one market moment 124 of 296 offers had no way to make the salaries work,
and 46 more failed on what the filler cost the buyer.

Now (`SmartView.fillSalary`, `smartSettings.fillByDollar`) a buyer short of
salary sends it the way that leaves it best off - its least missed players
first, those it would miss least for each dollar they send first, or the
one contract it would miss least that does it alone - and where what it
would pay plus the salary is more than the player is worth to it, the offer
is built around the salary first, then what the seller wants most on top
while the buyer still gains. At the same moment 7 of 296 offers had no way
to match, and 16 deals were open instead of 3; Towns drew offers from
Boston (Derrick Williams' $16.1M, three fillers and two firsts, 0.125 to San
Antonio) and Chicago.

### Clearing room, at the market's price

A team clearing room counted the players it sent at their use to it alone,
so a team with no title in sight - to which nearly any player is worth
little - sent players for nothing whom contenders would have paid firsts
for: in League 13 New Orleans gave Chris Paul (25, 60/63) to clear room for
Andray Blatche. Now (`clearRoomSettings.atPrice`) what it sends counts at
least at the market's price, as in any trade (`sureWorthNow`), its salary is
sent in the players that cost it least that way, and the team taking it may
send one of its cheapest picks back.

### Played on

League 12 from the 2018 draft through 2023, the same start twice (two
seeds), before and after:

| League 12, 2018 draft to 2023                | before | after  |
| -------------------------------------------- | ------ | ------ |
| market trades                                | 30, 28 | 46, 50 |
| players 60+ on $15M+ traded                  | 6, 6   | 15, 7  |
| best three teams by record buying, of 15     | 8, 12  | 12, 13 |
| 4th to 8th buying, of 25                     | 6, 5   | 6, 4   |
| clearing-room trades                         | 21, 22 | 9, 12  |
| players 58+ leaving losing teams for nothing | 23, 17 | 17, 18 |
| firsts paid for players 27+ rated under 60   | 4, 4   | 7, 4   |

Rooms cleared still went to clear upgrades (win shares over the next seasons,
sent against signed: 35 against 145 and 36 against 132), and takers sent
seconds back for Kyle Anderson and D'Angelo Russell. Stars still left losing
teams for nothing in both (Durant again at 70, Marcus Smart at 75), and teams
4th to 8th still rarely buy: with one or two teams holding most of the title
odds, a role player moves a 4th-best team's odds by less than a deal must
gain (opening a moment's market to every team changed nothing for them).

### Phase changes, timed (2026-11-20)

The owner: some phase changes are "sooo heavy". `phaseTimesProbe` plays a
league file through a season, each step timed and split by the smart CPU's
clock. From the 2010 preseason of the owner's League 25, on a busy machine:
into the draft 37 s, the draft 67 s (the after-draft window's 34 s in it),
into re-signing 41 s, free agency 164 s, every in-season week about 55 s and
the deadline's week 130 s - nearly all of it the market, 35 to 60 s a moment.
A moment's profile: team strengths (`teamPoints`, `fillRest`, `byPlace`,
synergy) about a third, the garbage collector a fifth; 172 of its 299
offers were turned down by their own buyer at the last check.

- The market no longer meets as the draft opens or right after it
  (`WINDOW_PHASES`): in League 12 played on six seasons twice those windows
  made one or two deals each in all.
- An offer's buyer is asked whether it still gains before the trade rules
  are read.
- Clearing room asks the taking team for its worth only for a trade the
  clearing team would make, and prices what it would send only once a free
  agent is worth clearing for: counting players sent at their price had made
  it three times slower (League 12's free agency, 14 s to 47 s; now 23 s).
- A moment renews the room's busy mark as it goes, not only as it starts: a
  phone in charge of simming can spend minutes in one, and a mark that ran
  out let another device run the same step.

### Picks, ranked against the league (2026-11-20)

The owner, of League 13's far-off firsts that became top picks (Oklahoma
City's 2016 first for Greg Oden became Ben Simmons at #1): do teams see their
own decline coming? Each preseason of League 12 played on six seasons twice
(`forwardAudit` FA_PICKS: every team's own first in the next four drafts as
the outlook expects it to land, 840 set against where they did), the outlook
put every team's finish on a bell curve of its own around where it expected
it. Summed over the league that gave out four top-five picks a draft, not
five - too little at either end - and a team over .600 had its own first one
or two drafts out at a 2.5% and 4.8% chance of the top five where 7.5% and
9.8% got there; bad teams stayed bad more often than it said.

Now (`outlookSettings.rankedFinish`, `rankedLanding`) every team's finish in a
draft is drawn around where it is expected to finish, at the same spread as
before, and ranked: each place is some team's, once, and a pick lands by how
many teams finish worse than its own. The field is every team's expected
place in that draft as the league stands (`rankField`, set as the outlook is
built), so a deal's picks are ranked against the same league whatever was
priced before them. On the same 840 the log-likelihood was 0.03 to 0.06
better a pick at every horizon, at the old spread (a spread 0.8 or 1.2 times
it did no better).

Played on twice more with it, against where those picks then landed:

| own firsts, drafts out | log-likelihood a pick | .600+ teams, top five: forecast / landed |
| ---------------------- | --------------------- | ---------------------------------------- |
| this season's          | 2.66 (was 2.72)       | 0.5% / 0%                                |
| 1                      | 2.91 (was 3.10)       | 3.2% / 1.8%                              |
| 2                      | 3.12 (was 3.18)       | 7.3% / 7.3%                              |
| 3                      | 3.27 (was 3.27)       | 10.3% / 15.4%                            |

Trades otherwise as before (market deals 55 and 37 against 46 and 50, the
best three teams buying 11 of 15 both times); firsts paid for players 25 or
older rated under 55, none in either run against one in each before.

## Random-players leagues: stars that never moved (2026-11-20)

The owner: "it feels as tho buyers aren't quite making the splashes that
you'd hope for ... maybe it's best if you're testing in random players
leagues". Random-players leagues with the owner's settings (rookies can
refuse), each played four seasons from the start (`forwardAudit`
FA_LEAGUE=arena:<n>, FA_STARS=30: every time one of the league's 30 best
players was shopped, what came of it and the seller's side of it in parts).

In the first, every best real offer for a top-30 player was worth -0.1 to
-1.5 of a title to his team. Washington's 23-year-old 68 (0.16 of a title
of use to it, a price of 0.82) for two young players and two firsts priced
0.71 together - 0.38 of use to it, its own goal up 0.05 - was -0.57: what
it gave counted at least at his price, the players it got at their use to
it alone (`boughtAtUse`), so a team out of the race could sell its star
only for picks, and contenders' picks land late. The buyer would have
gained 1.5.

- **Value for value** (`smartSettings.valueForValue`): the players it gets
  count at their price as far as that makes up for the price of what it
  gives - never past it, so no deal is made on the gap between players'
  prices and their use alone (what `boughtAtUse` was for).
- **What it gets priced as the team will stand** (`gotAsItWillStand`): past
  its use to the team once what it gives is gone, not beside it. Beside it,
  a package that replaced a star read as of more use than it would be, and
  its price past that as less: New York's 24-year-old 67 for a package at
  his price read -0.055 with its own goal up 0.055.
- **Six pieces** (`offerSize` 6, `offerPicks` 6): the best real offers for
  top-30 players came to a median 69% of their price, 99 of 227 turned
  down at four pieces.

Three leagues, four seasons each, before and after:

| three random-players leagues, four seasons   | before | after   |
| -------------------------------------------- | ------ | ------- |
| trades                                       | 134    | 130     |
| top-30 players traded                        | 7      | 14      |
| ... buyer better the next season             | 2 of 5 | 8 of 12 |
| firsts paid for players 27+ rated under 60   | 35     | 16      |
| players 58+ leaving losing teams for nothing | 29     | 27      |
| players bought back within a year            | 2      | 1       |
| champions, different                         | 7      | 8       |
| a market moment, idle desktop                | 48 s   | 67 s    |

Pittsburgh bought the league's third-best player (28, 74) from a 31-win
team for a package and won the title; contenders bought 26- to 29-year-old
69s from losing teams for two or three rotation players and a first.
Without the six pieces (value for value and as it will stand alone), two
of those leagues moved 6 top-30 players to 5 and paid 21 firsts for older
role players to 27; at six, 12 and 13.

Tried and left out: the margin of error at half a deviation moved 20
top-30 players in two leagues to 12, but with like-for-like star swaps (a
69 for a 69, then the first 69 bought back with two firsts; 67s swapped
twice) and players traded twice within a year 16 and 18 to 13 and 5 - the
margin stays the owner's (2026-11-19); counting a star's price given up at
the 70% real offers reach, no better. Each team's chance of re-signing
there still lets about ten 58+ players leave losing teams a league's four
seasons, and two leagues grew dynasties (Portland, Baltimore) before and
after.

### A pick's upside, measured (2026-11-20)

The four random-players leagues played on the shipped build still paid
firsts for older role players - 37 in four leagues' four seasons: the
reigning champion Vancouver three firsts and a 46 for a 37-year-old 54, New
York two for a 37-year-old 53, a 33-49 Houston a first for a 33-year-old 45.
Contenders' own near firsts land late, and a late pick's player was priced
as two halves of what he may become, his chance of being a star averaged
away (`outlookSettings.boom`, off while it was measured). The same four
leagues played again with it on (`smartBoom`, with the smart CPU on
vanilla's rules, where it was measured):

| four random-players leagues, four seasons    | off | on  |
| -------------------------------------------- | --- | --- |
| trades                                       | 140 | 154 |
| firsts paid for players 27+ rated under 60   | 37  | 23  |
| top-30 players traded                        | 10  | 17  |
| players 58+ leaving losing teams for nothing | 36  | 24  |
| champions, different                         | 11  | 8   |

No slower. One league's Toronto, 69-13 and champion twice with it off,
won all four with it on after buying a 29-year-old 68 for seven players and
a first - a dynasty to watch. Of the players who still leave losing teams
for nothing, seven in ten had a deal both teams wanted that a margin of
error stopped - most often the buyer's (a contender at 33-43% for a
27-year-old 69 in his deal's last season: +0.03 to +0.06 against a margin of
0.19).

Both this and the ranked finish (above) now apply with the smart CPU only:
realistic front offices fail their own checks with them (ranked, their
trading block's bids for a team's best player held no picks; with the
upside, a free agent who makes a team worse every season worth 0.00002 of a
title).

## The front office: the market rebuilt (2026-11-21)

Two trades from the owner's League 27 started it. Boston, 5-9, gave its 2031
first, two seconds and two bench players for Luke Kennard (31, 51/51):
"Seems like an awful trade for the celts". And Brooklyn never moved Michael
Porter Jr. (29, 63, $40.8M, his deal ending, under 1% to re-sign) before the
deadline: "These are the common sense things that I want cpu gms to nail
every time". Then: "Let's take a step back and just think about how real
GMs and front offices approach team building ... these cpu gms need to be
acting in that exact way." The owner's calls (2026-11-21): the margin of
error may be smaller where doing nothing is the worse risk; a market moment
may take up to twice as long; plans stay internal; each stage pushed once it
tests well. And on the first draft: "Should not be capped at just top 6 ...
I don't want explicit rules like that. I want the engine to be fluid and
natural."

### What was wrong

The worth of anything to a team - its title chances, season by season - was
sound: it knew Porter was worth 0.38 of a title to the Lakers and to
Oklahoma City and nothing to Brooklyn. The market around it was not.

- **Prices by six teams.** A piece's price was the second-highest bid among
  the six most valuable franchises, a package's their bid for all of it. A
  first sent with players nobody bid for lost what it fetched alone (Boston's
  2031 first: 0.06 alone, 0.02 with Ron Harper Jr. and Amari Williams, and
  Boston counted it at its own use); a star whose salary none of the six
  could send back had no price, and his team sold him at his use to it.
- **Two buyers, one offer each.** Each shopped player went to the two teams
  that wanted him most by their use alone, each built one offer - what the
  seller wanted most, piled on until the buyer broke even - and that offer
  died at the buyer's own margin of error. Porter went to the Lakers and
  Oklahoma City: the Lakers could send back his salary for a gain of 0.013
  against a margin of 0.098, Oklahoma City offered Jalen Williams (64, $41.5M)
  for a gain of 0.0003. New York, whose offer cleared both sides, was never
  asked.
- **Rested at the deadline.** A player shopped without a taker sat the next
  moment out - the deadline's last days too, for a player about to leave.

### What a front office does now

- **A plan** (`plans.ts`): each moment every team reads where it stands from
  its title chances as its trades count them - this season's on what it
  holds (a star hurt now back for the playoffs), the next three's:
  contending, pushing, building, rebuilding.
- **The scan**: every CPU team reads every tradable player on every other
  team; every player some team would trade for is on the block, those that
  must move first (his deal ending, his team not contending, unlikely to
  re-sign) - shopped every moment, never rested.
- **The league's price** (`SmartView.leagueBidNow`): the second-highest real
  bid of all teams, read keenest first and stopped once no team left could
  beat the second; a pick at least what it will fetch once its draft is next;
  a package, its pieces apart.
- **The auction** (`SmartView.bestRealOffer`): each player put to every team
  whose want for him once it has sent back his salary (`matchedNow`) is more
  than his team needs for him, the keenest first, for as long as one could
  still beat the offers in hand.
- **Offers a buyer would make** (`offerClears`): its offer cut back, the piece
  the seller wants least first, to the most it would give that clears its
  own margin - built salary first too, where his salary must go back.
- **Risk by the plan** (`marginShare`): as the deadline nears, a contender
  buying and a team selling a player who must move ask less of their margin
  of error - half on the deadline's last day.

### Measured

The owner's League 27, replayed from 14 games in to the deadline, twice:
Porter went to Atlanta for Dyson Daniels (24, 60/66), Dorian Finney-Smith
and a 2027 first, and to San Antonio for Keldon Johnson, Tobias Harris, a
20-year-old prospect, a first and a second; 13 and 11 trades in all (the
owner's own game made 10 there). With prices still set by the six most
valuable franchises, the same replay made 29, Nikola Jokić (32, 76) going for
four players and no picks and Tyrese Haliburton (27, 67) for Desmond Bane and
a 2032 first.

Three random-players leagues (rookies can refuse), four seasons each, against
the shipped market:

| three random-players leagues, four seasons   | before | after  |
| -------------------------------------------- | ------ | ------ |
| trades                                       | 118    | 158    |
| top-30 players traded                        | 12     | 14     |
| ... buyer better the next season             | 3 of 7 | 6 of 7 |
| champions, different                         | 5      | 9      |
| players traded twice within a year           | 16     | 15     |
| players 58+ leaving losing teams for nothing | 16     | 16     |
| firsts paid for players 27+ rated under 60   | 20     | 29     |
| minutes a season, idle desktop               | 20.4   | 25.2   |

A market moment on the owner's League 27 file: 88 s, 80 before. Firsts paid
for players 27 or older rated under 60 rose with the trades - 17% of trades
before, 18% after - nearly all a contender's own late first with players it
could spare for a rotation veteran; one is Atlanta (0.3% to win the title)
giving three firsts for a 28-year-old 58/59 near sure to re-sign, worth 0.24
of a title to it and 0.19 to the league (the price of a starter in his
prime). Of the players who still left losing teams for nothing, most were
shopped every moment to the deadline: either the best offer fell just short
of what his team needed, or his team counted on re-signing him - Cincinnati
a 25-year-old 69 at 89% to stay, who left.

Tried and left out: offers built from what is worth more to the seller than
it costs the buyer first (they cost Brooklyn its best offer for Porter, and
bought Moussa Diabaté, 25, 55, for three firsts).

### Who a team would really keep (2026-11-21)

Of the 16 players 58+ who still left losing teams for nothing in those three
leagues, most had been shopped every moment to the deadline and turned down
by their own team, by a hair or by a lot. Cincinnati's 25-year-old 69 on a
rookie deal: nine offers turned down, one of them three firsts and four
players (-0.49 to Cincinnati), because it counted him 89% kept - and in the
summer it never offered him what he asked; he signed with St. Louis for
$36.3M a year. A team's chance of keeping a player whose deal ends was his
chance of being willing alone; whether it would pay his ask itself it
already decides in its own outlook (letGo: a player past his contract, kept
or let go at his market salary).

- **The chance of keeping him** (`SmartView.keptChanceNow`): his willingness,
  and whether the team would re-sign him at his market salary itself - each
  of his halves on its own where his future is split. A team holds out for
  his price after his deal only as far as it would keep him (`premiumNow`).
  Counted among the players that must move too (shopped first, the margin
  eased toward the deadline), in the same three leagues players leaving for
  nothing went 16 to 13 for 22% more trades (players traded twice within a
  year 15 to 25) and a season a fifth slower - so it is not: those are still
  the players unwilling to stay.
- **The scan's first look**: a player below a team's tenth man by the game's
  own value, by more than three, is read as worth nothing to it without
  working it out - most of the scan's work was bench players no contender
  would play. Same deals at a League 27 moment, the scan 43 s to 35 s.
- **A human's offer judged to the first better one**
  (`bestRealOffer(..., beat)`): the CPU asks the teams it would ask in the
  market until one would give it more than the human, and no further. A
  CPU answer to a human's offer on League 27: median 0.39 s, against 0.55 s
  before the rebuild. Skipping teams by their want alone (0.30 s) missed
  better offers - a team can give a seller more than its want past what the
  seller needs, in pieces worth more to the seller than to it - and failed
  two of the checks the GM must pass. The day's first answer, pricing the
  draft classes by the whole league: 15 s against 11 s.
- **Rooms cleared, watched** (`clearRoomWatchers`): every trade made to clear
  room for a free agent is logged in the forward audit (CLEAR) with what
  each piece was worth to the team. Clearing room still gives good players
  on big contracts away for nothing at times, before the rebuild as after
  (Tampa a 25-year-old 62 on $33M, Portland a 30-year-old 64 on $32M; after
  it Cincinnati a 33-year-old 74 on $33M through 2030).

Still leaving for nothing: stars on big deals ending, whom no buyer can take
without sending back rotation players worth more to it over the seasons
ahead than one season of him - Sacramento's 29-year-old 68 ($36.4M) shopped
15 moments with one team ever able to bid, Phoenix's 26-year-old 66
($25.8M) with one.

The same three leagues, four seasons each:

| three random-players leagues, four seasons   | before | front office | and this |
| -------------------------------------------- | ------ | ------------ | -------- |
| trades                                       | 118    | 158          | 181      |
| top-30 players traded                        | 12     | 14           | 11       |
| players 58+ leaving losing teams for nothing | 16     | 16           | 6        |
| players traded twice within a year           | 16     | 15           | 19       |
| firsts paid for players 27+ rated under 60   | 20     | 29           | 28       |
| champions, different                         | 5      | 9            | 11       |
| minutes a season, idle desktop               | 20.4   | 25.2         | 28.0     |

Teams out of the race paying a first for a player 35 or older, three in
twelve seasons, read as their outlooks have them: Montreal (0.3% to win the
title now, 10% next season, 17% the one after) gave its own 2028 first - to
land late - for a 40-year-old 53 it would start this season, short-handed,
2.2 points of strength.

### Stars' prices, tried and left out (2026-11-21)

Splashes stayed few: top-30 players traded 12 in three random-players
leagues' four seasons before the front office, 11 after. Of 4,873 times a
top-30 player was shopped without an offer, 4,674 had no team whose gain from
him past the players it would send back for his salary reached his price -
the league's second-highest use for him, as if he came free. Two ways to let
the market set a price buyers could reach, each played on the same three
leagues:

| three random-players leagues, four seasons   | shipped | bids net of salary |
| -------------------------------------------- | ------- | ------------------ |
| trades                                       | 181     | 235                |
| top-30 players traded                        | 11      | 21                 |
| ... buyer better the next season             | 3 of 3  | 11 of 13           |
| players traded twice within a year           | 19      | 43                 |
| firsts paid for players 27+ rated under 60   | 28      | 40                 |
| players 58+ leaving losing teams for nothing | 6       | 12                 |
| minutes a season, idle desktop               | 28.0    | 33.4               |

- **Every bid net of the salary sent back**: nearly twice the stars moved, and
  their buyers were better for it - but every big contract's price fell with
  them: twice the churn (filler and role players recycled through the bigger
  deals), more firsts for older role players, and rooms cleared by giving
  away good players for nothing (Washington a 29-year-old 68, the Lakers a
  30-year-old 66).
- **A player's price in his own auction set by the teams competing for him**
  (the second-highest gain past salary, where two or more could gain): the
  same star trades through two seasons (9 and 9), seasons a quarter slower.
  Offers came in - shopped without one, 731 times to 327 - and were turned
  down: what contenders hold falls short of what a star is worth, not his
  salary. New York offered the Lakers five spare players for a 24-year-old
  69, Houston a 33-year-old, a prospect and three late firsts for its
  27-year-old 69 - 0.6 of a title on the market each, for players priced 0.9
  and up. The league's best player (26, 77) drew a 71, a prospect and a
  35-year-old, 2.3 against 2.6.

A star stays where he is unless a buyer can pay what he is worth - which, a
rebuilding team holding a young star, is the GM's call. Whether to trade more
stars for more churn is the owner's.

### Holding out a season, tried and left out (2026-11-22)

A team out of the race asks for a player his price past his use to it - what
it could get holding out for a better offer. But holding out spends a season
of him, and a GM selling a 31-year-old knows he fetches less next year. Three
ways to count that, each played on the same three random-players leagues:

| three random-players leagues, four seasons   | shipped | both sides | held only | share ahead |
| -------------------------------------------- | ------- | ---------- | --------- | ----------- |
| trades                                       | 181     | 229        | 271       | 229         |
| top-30 players traded                        | 11      | 20         | 31        | 15          |
| ... buyer better the next season             | 3 of 3  | 7 of 13    | 13 of 21  | 6 of 10     |
| players traded twice within a year           | 19      | 26         | 52        | 34          |
| firsts paid for players 27+ rated under 60   | 28      | 33         | 46        | 45          |
| players 58+ leaving losing teams for nothing | 6       | 10         | 10        | 11          |
| trades flagged by the sanity check           | 8       | 8          | 16        | 9           |
| minutes a season, idle desktop               | 28.0    | 32.1       | 32.4      | 31.5        |

- **Both sides**: what a team holds out for is his price after this season
  past his use to it then - his price less this season of him to the team
  whose bid sets it, past this season of him to his own team. The same cut
  to what a deal brings in, which counts at a share of its price already
  (premiumRealized): Washington turned down a 25-year-old 58 for its
  expiring 23-year-old 60 it had taken (+0.001 to it before, -0.009).
- **Only what a team holds**: back at the first trade moment - and young
  players' prices gone. Each season ahead counted by its title chances from
  here, which flatten for seasons further off only because they are less
  sure, so a contender's gain from a 25-year-old was mostly this season's:
  Chicago, out of the race, gave its 25-year-old 67 (price 0.75, use to it
  0.04) away to clear room. Churn and flagged trades doubled.
- **By the share of him still ahead** (players under contract past this
  season): what he adds to the bidder's strength each season, each counted
  as the season at hand once it is. Young stars kept their prices, and
  Boston and Pittsburgh sold veterans under contract (32, 56; 34, 57) to
  contenders at the first trade moment.

In every version teams out of the race sold more role players 27 and older
to contenders for firsts - 45 against 28, 33 of them from losing teams:
Toronto gave two firsts for Washington's 37-year-old 54, Vancouver two and
five players for Chicago's 30-year-old 59. The contenders' bids hardly
change; on master the sellers' asks held them off. Holding out a season is
how a seller's ask comes down, and it goes in once contenders stop bidding
firsts for role players.

### Firsts as currency, measured and not built (2026-11-22)

The owner approved valuing a first as currency for a better deal later: a
GM doesn't spend one on a role player while a bigger use for it may come.
That is worth something only where firsts run short, and in this market they
don't. Every deal priced at every moment of three random-players leagues'
first two seasons (a measuring hook, not shipped): a deal paying firsts open
and passing every check 67 times, 62 of them made - about one chance a team
in three seasons. Where the same buyer had another within a year (32 of 62),
the later one gained more per first 19 times.

Firsts spent in market trades a team-season, over four seasons of the same
leagues as shipped:

| team that season | team-seasons | firsts spent a team-season |
| ---------------- | ------------ | -------------------------- |
| under .450       | 132          | 0.02                       |
| .450-.550        | 96           | 0.16                       |
| .550-.650        | 68           | 0.41                       |
| .650+            | 64           | 1.45                       |

A team holds seven firsts and gains one each draft: the league's best spend
half a first a season past that, over a decade of contending before they run
out. Before paying firsts the median buyer held five; 17 of 98 deals left it
one or none. A first kept for a later deal is worth nearly nothing to
nearly every team, so building it would change almost nothing. Firsts for role
players are decided by what a late first is worth against a rotation player:
a contender's title chances off its own curve (checked against the sim)
against picks priced by what they become - a 57 at about two and a half
late firsts.

### A first counts for a little more (2026-11-22)

The owner: "I think maybe we just need a slight buff to 1sts value". Now
(`smartSettings.firstsBuff`) every team trading a first counts it at a
quarter more than its price, given or got (`SmartView.dealPartsNow`): a
contender pays fewer of them for a role player, and a seller takes them for
its good players more readily.

Three random-players leagues played on four seasons each, master against the
buff (same seeds):

| three leagues, four seasons each             | master | buff |
| -------------------------------------------- | ------ | ---- |
| trades                                       | 181    | 192  |
| top-30 players traded                        | 11     | 15   |
| firsts paid in trades                        | 149    | 167  |
| firsts for a player 27+ rated under 60       | 37     | 27   |
| ... for one 27+ rated under 55               | 7      | 2    |
| firsts for a player rated 55-59              | 62     | 55   |
| firsts for a player rated 60-64              | 39     | 55   |
| firsts for a player rated 65+                | 16     | 26   |
| 58+ players leaving losing teams for nothing | 6      | 7    |
| players traded twice in a year               | 19     | 20   |
| trades a GM would call awful (sanity)        | 8      | 7    |

The owner's League 14 (real players, 2010 start) played on from 2016 to 2019
both ways: 31 trades and 21 firsts on master, 34 and 20 with the buff; firsts
for players 27 and older rated under 60, 3 against 2; on master Milwaukee
paid three bench players and three firsts for a 26-year-old 60, and nothing
was flagged with the buff. Three firsts for a young player under 62 still
happen (four on each side in the random leagues): late firsts are priced so
low that a quarter more hardly moves them.

### League 14: older free agents and far-off firsts, measured and left (2026-11-22)

The owner's League 14 (real players, 2010-2016) had two more things to
check after the firsts buff.

**Long deals for free agents past 30.** In 2015 Charlotte (22-60) gave
Brandon Roy (31) $21M a season for four years, Atlanta Marc Gasol (30) four,
Indiana Joe Johnson (34) and New Orleans Dirk (37) three each; the master
runs had an 18-64 team give a 34-year-old $36M a season for five. Every
free agent a team weighed in two random-players leagues' first offseason
was logged (`forwardAudit` with a signing hook, not shipped) with its gain
on his terms and on one season. The later years are counted: for teams that
had won 34 to 41 games, a 34-year-old's four seasons at $21.1M (or a
31-year-old's three at $16.9M) came to 0.01 to 0.04 of a title less than
one season, and they passed. For the worst teams they came to 0.005 less
or under: their room buys players about 57
and 52 (`HAUL_BY_RECORD`), his contract ends when he retires, and money
itself costs a CPU team nothing (owner, 2026-11-17). Such a signing is
about even for them, and it makes them better at once (the 18-64 team went
41-41). Only a cost for money would stop it.

**An aging team's far-off firsts.** San Antonio (55 wins, Duncan 34) paid
its 2012 and 2013 firsts for Corey Brewer in 2010; they became #4 and #2.
Own firsts traded two or more drafts out by winning teams in Leagues 12, 13
and 14 landed in the top five about 7% of the time, as forecast. The
owner's League 25 (2010) played on six seasons with every team's own firsts
projected each preseason (`FA_PICKS`): teams over .550 whose minutes went
to players averaging 29 or older expected a first two drafts out at pick
10.5 on average and a 31% chance of the top five, and it landed at 14.5,
none in the top five (6 picks); three out, 10.8 against 9.6. Old teams see
their decline coming - a little more than it comes - and price their own
far-off firsts accordingly.

## The market on an NBA calendar (2026-11-23)

On the NBA calendar (a league setting, off by default: `calendarTicks.ts`)
each sim day is one night, about 165 a season, and the end-of-day rules -
the AI's trades among them - run once per round of games, half the league's
worth. A night that ends no round never asked the smart CPU's market, though
it keeps its own clock: a moment every so many games, and one on each of the
deadline's last five days. A random-players league on a calendar, played to
its 2026 deadline (`calendarMomentsProbe`), met on three of those five nights
(four, two and none to go); the other two had seven and six games. Now the
market is asked every regular-season night (`betweenAiTeams({ smartOnly })`
from `game/play.ts` on a night with no round). The same league met on all
five, and at the same nine moments before them.

Since the calendar ticks at the classic pace (459), its deadline window
counts classic days too (`game/classicDaysLeft.ts`): the deadline's last
five days had been five nights, about two and a half classic days of games;
now they are the last five classic days, about ten nights, a moment on each
(a night that is two classic days meets twice, `deadlineMomentDay`). The
same for the realistic market's deadline window and the outlook's days to
the deadline (how likely a deal is to still be on offer then,
`stillOnOffer`, was measured in classic days). Classic leagues are as they
were.

## Speed: the same moves, faster (2026-11-23)

The owner asked (2026-11-23) for faster sims without making the engine any
less effective or dynamic. So nothing here changes a decision: every change
was checked to make the very same moves.

**Where the time goes.** One season of a random-players arena league
(`phaseTimesProbe`, `PT_PROFILE` for a CPU profile) was about 95% the smart
CPU: the in-season market about 70% (a moment every 5 games a team, each
40-65 s here), the draft about 10% (the class priced as rookies again after
each pick, since every pick moves the slots and so the rookie deals), free
agency about 30% (signings, clearing room and its market moments). A market
moment (`momentProbe`, `MOMENT_VANILLA=1` for vanilla's rules, `MOMENT_HEAP=1`
for where it allocates) works out about 37,000 franchise values - and read
values it had already worked out 600,000 more times, each read building the
holdings' key again from every player's contract (15 million contract reads a
moment). It made about 39 GB of short-lived objects; the garbage collector
was 22% of its time.

**What changed.**

- A package's rough want by each team (`leagueBidNow`) and each team's bid as
  the trade rules let it take it in (`matchedNow`) are read once while the
  teams' holdings stand, and read again if any player's value or terms or any
  pick has moved since (`bidInputs`). A package was read again by every team
  for every deal it was in. The first try kept them on the holdings alone and
  a whole season found one deal that came out different: a roster over the
  limit cuts its least valuable player first (`afterCuts`), and a player's
  value moves with every game he plays, so a bid kept across nights of games
  went stale.
- A team's own holdings key is reused while its players' terms and its picks
  are as they were (checked each time); values are kept by team, then by what
  it holds, then by the holdings valued, not under one long joined key.
- Roster pricing (`expectedRosterPoints`, `asPlayed`) reuses its arrays
  instead of making new ones for each of the near million rosters a moment
  prices; the free agents a season's room buys are worked out once for each
  room; the keys of whom a team may let go are made once.
- The game's mood components keep their bounds and trait tables, made once,
  and a mood ranks its own team's market size without copying every team.
- Smaller: a synergy's input kept on the synergy (it was a WeakMap lookup,
  4%), the market salary memo keyed by whole tenths.

**Checked.** One market moment on vanilla's rules and one on realistic front
offices: the same candidates with the same surpluses to the last digit, the
same deals, the same number of franchise values (37,190 and 36,512). A whole
season from the same seed (`forwardAudit`, `arena:217`, preseason to the
next preseason): every trade, signing, re-signing, release and pick the same - the
audit's lines and the league exports (players, teams, all 1,543 events)
compared field by field. Master run twice gave the same league, so the check
can tell.

**Faster.** A market moment: 17-23% less time. The whole season above: 17.1
minutes on master, 14.6 with these (run side by side), 15% less.

**Left.** Most of what is left is the franchise value itself: each roster
over eleven seasons, whom it lets go each season, the free agents its room
buys. A large cut there would mean pricing fewer rosters or fewer seasons -
less of the engine, so not done.

## Clearing room: something back (2026-11-23)

A team clearing room for a free agent sent the salary to the first team with
room that would take it, for nothing back, however much that team gained:
in a random-players league Pittsburgh (rebuilding) gave its 29-year-old 70
to Los Angeles, the one team with the room, for room for a 24-year-old 65.
Now (`clearRoomSettings.asks`) every team with the room is weighed at the
fewest sweeteners, the best for the team clearing taken, and where the
taker would still gain past its margin, the team clearing asks for picks
back - its best for itself first, of the picks the taker could pay for from
what it gains past its margin, up to four asked.

`forwardAudit`, `arena:217`, two seasons each way: on master three rooms
cleared, all for nothing; with this five, two of them for a second-round
pick (Pittsburgh's 70 for Los Angeles's 2032 second; Cincinnati's
34-year-old 70 for St. Louis's 2030 second), the rest for nothing as
before - the takers were rebuilding, and a player they would only have for
his decline was worth little more to them than the room. Clearing room is
2% of the GM's time (0.96 of 40.5 minutes over the two seasons, by
`smartClock`, now written at the end of the audit); the first season took
20.2 minutes against master's 20.5.

### Measured and left (2026-11-23)

- **Contenders and fringe players.** In half a season of the same league,
  teams at 5% or more to win the title took four players rated 53 or less
  in deals (`FA_FRINGE`): three were 52-53s who came in 8th to 10th, two of
  them to teams with starters hurt; one a 48 who came in 11th (New York,
  for five seconds). What a contender's GM puts on young cheap players
  rated 44-50 (0.06-0.09 of a title to New York, `FA_FRINGE_TID`) is mostly
  two and three seasons on, when they project as rotation players a team
  over the cap could otherwise only add at the minimum. Nothing changed.
- **Long deals to older free agents.** The owner again (2026-11-23): no
  owner, so money is nothing in itself; only what a contract does to the
  team on the court counts. It counts already: the room a contract takes in
  each later season is priced at what it would buy then, by how good the
  team expects to be (`roomByRecord`), and a contract ends when the player
  retires.

## League 15: six seasons reviewed (2026-11-23)

The owner's League 15 (real players from 2010, spectator, the smart CPU on
vanilla's rules, rookies can refuse), played to the 2016 preseason on the
code of the morning of 2026-11-23: all 79 trades, the big free-agent
signings, everyone who left in free agency, releases and every first round.
Nothing changed for it.

- **Teardowns and pushes paid off.** San Antonio, read at 2% to win the title
  in the first weeks of 2010, sold Duncan, Ginóbili and Jefferson for five
  firsts; with them and its own #1 (Wall) it won 59 in 2013 and 50+ since.
  Dallas bought Ginóbili, Jason Richardson and Rasual Butler and won the
  2010 title.
- **Few walked.** Nine players rated 56+ left in free agency in six years,
  most of them the real 2010 class (Bosh, Stoudemire); nobody 52+ was
  released. The first rounds follow the ratings.
- **Far-off firsts.** Toronto (2-5% to win the title) gave its 2011-2013
  firsts for Troy Murphy (30, 62, expiring) and Earl Watson (31, 55); they
  were #8, #6 and #4 - Bosh and Murphy left that summer, and Toronto has not
  had a winning season since. Of 22 own firsts winning teams traded two or
  three drafts out, 3 landed in the top five (14%), against about 7% in
  Leagues 12-14: too few to call a bias.
- **Bad teams' room.** Milwaukee gave Josh Selby (23, 49) $19.7M a season
  for three, Toronto Monta Ellis (29, 56 and falling) $11.3M for three
  (-6.6 WS): asks a bad team pays (vanilla's mood), with room that buys it
  little else.
- **A dynasty.** Oklahoma City won 2011-2014 on its real young core and kept
  adding (A.J. Price for three late firsts, Darren Collison, Nowitzki at
  35).

## Bad teams' room: a margin on new signings (2026-11-23)

The owner, on Milwaukee giving Josh Selby (23, 49) $19.7M a season for
three in League 15: no 49 should get that, and a GM in that spot "might as
well keep his payroll open in case anything else opens up". On vanilla's
rules a CPU team signed the free agent it gained most from at his ask, if it
gained at all. For a team with no title shot for the length of the deal,
the player and the room he takes both come to next to nothing in titles, so
the call came down to rounding - and picking the best of ten free agents a
day all summer favours the ones its read overrates. Late in free agency it
is worse: the free agents left are the ones nobody would pay, and the
season's room is priced by a pool that is nearly gone, so a long deal at a
price nobody else paid looks free this season and costs only seasons the
team does not expect to matter.

Now a new signing must gain by more than the team's read of him could be
off (`vanillaSigningSettings.margin`, `SmartView.marginNow`): the rule its
trades have had since 2026-11-19, and the realistic mode's free agency
(`priceSigning`). Re-signing has none: he is no pick of many, and the other
side of it is losing him for nothing. In-season signings go by the same
rule; a short roster is still filled by the game (`checkRosterSizes`).

Measured, each way (`forwardAudit`, `VANILLA_SET='{"margin":false}'` for
master), free-agent signings rated 54 or less for 10% of the cap or more:

| league                              | master | margin |
| ----------------------------------- | ------ | ------ |
| League 15, its 2016 summer          | 1      | 1      |
| League 40 (real players), 2010-2012 | 2      | 0      |
| random players 217, 2026-2027       | 2      | 0      |
| random players 301, 2026-2027       | 1      | 0      |

- League 15's 2016 summer, every signing for 5% of the cap or more: master
  Durant, Giannis, Julius Randle (22, 56/67) 31% from Chicago, Marcus
  Georges-Hunt (22, 37) 8% and Joel Bolomboy (22, 38) 9% from teams under
  .450, Sheldon Mac (24, 42) 5%, Doron Lamb (25, 53) 22% to 2019 from
  Cleveland; with the margin Durant, Giannis and Lamb (a week earlier). Lamb
  still clears it: by day 13 the room he takes this season buys next to
  nothing, and he is a 53 for three more.
- The deals that went: Josh Howard (30, 52) 13% for three from 26-win San
  Antonio, Alonzo Gee (24, 53) 14% for three (League 40); 18-64 Sacramento's
  23-year-old 53/62 at 22% for four (301); 17-65 Baltimore's 23-year-old
  50/61 at 18% for three and 25-57 Los Angeles' 54/59 at 17% for four (217).
- Good free agents were not left out. The best still unsigned when each
  run ended: League 40 a 34-year-old 50 (master 55), 301 a 27-year-old 58
  (54), 217 a 59/62 and a 57/63 asking 11% and 9% (a 57). On master too,
  good free agents go unsigned in vanilla's free agency at the asks
  vanilla sets: League 40's 2010 summer a 27-year-old 63 asking 32% and a
  59 asking 25% with six teams holding 20% of the cap in room.
- Summer signings 42 against 61 (League 40), 16 against 15 (301), 10
  against 28 (217); in-season 53 against 88 (League 40), mostly minimum
  deals.

**Measured and left: what room buys on vanilla's rules** (`FA_HAUL`, 270
team-summers in Leagues 15 and 40 and both arms of random league 301).
The 84 teams with 15-45% of the cap in room as free agency opened signed
someone better than the minimum a third of the time and spent 6% of the cap
on average; the offer market's table (`HAUL`) has 95-98%. Room is not
repriced from it: it is low partly because teams hold out for what the model
says room buys, and priced lower, room would go on exactly the late
leftovers the margin stops.

## Human trades on vanilla's rules: smart CPU on and off (2026-11-23)

The owner asked how a person's trades with CPU teams differ with the smart
CPU on and off at a trade difficulty of 3.5. On vanilla's rules both judge a
person's offer by vanilla's value at the difficulty (`trade/propose.ts`):
the CPU team's own players count 1.35 times over, and the person's picks
are valued as the last pick of their round. The smart CPU only adds a veto
on top (`smartCpu/humanTrades.ts`, `smartGuardOffer`): a contender (3% or
better to win the title this season or next) won't give up near odds, a
rebuilding team (0.5% or less) won't give up later odds, and a contender
won't hand another contender a bigger gain than its own. The same veto
filters "What would make this work", the CPU's proposals and the trading
block. Teams between 0.5% and 3% have no veto, and the smart GM's own
read of value is not asked.

Measured (`humanTradeAudit`, League 40 20 days into 2010): for three
person's teams, every 1-for-1 and 2-for-1 deal with every CPU team among
the best players each side has, and each 1-for-1 again with the person's
first added - 9,048 deals each.

| person's team (title odds) | within the salary rules | vanilla takes | smart CPU vetoes |
| -------------------------- | ----------------------- | ------------- | ---------------- |
| Atlanta (12.2%)            | 1,751                   | 980           | 24               |
| Miami (4.3%)               | 1,420                   | 721           | 62               |
| New Orleans (0.2%)         | 1,683                   | 785           | 66               |

Of the deals vanilla takes, those the CPU team loses by its own smart read
(all / by more than 0.05 titles), off against on: Atlanta 46/19 against
31/12, Miami 54/24 against 24/8, New Orleans 69/24 against 37/6. The
person's best gain by the smart read: Atlanta 0.082 titles against 0.046,
Miami 0.014 against none, New Orleans 0.017 both ways. What the veto stops
are contenders selling their best for the future: 10.7% Houston's Tim
Duncan for Chris Paul and Emeka Okafor, 5.6% Denver's Chauncey Billups for
Paul. What it lets through are teams in the middle: 1.9% Dallas giving
Jason Kidd (37, 60) for Marvin Williams (24, 57/62, $7.5M to 2014), -0.177
titles to Dallas, +0.046 to Atlanta.

**Then: the team's own read too (the owner's pick, 2026-11-23).** A
person's offer vanilla takes at the difficulty, a CPU team now also turns
down if by its own worth it comes out behind (`humanTradeSettings.ownRead`,
reason `value`), whatever its spot - not on an easy trade difficulty (below
0), which is there to give the person a discount. Measured again the same
way, it turns down 31, 25 and 37 more: about the deals above each team
loses by its read, none left.

"What would make this work", the trading block and trade proposals search
with vanilla's value (`trade/makeItWork.ts`), and the search runs right up
to vanilla's line, where vanilla and the team's read part most: of what the
search found for each CPU team's best three (what would the team want for
him) and for each of the person's best three (what would it give), the team
would then take 20 of 163 and 61 of 261. The search now asks the team's
read too (`ownReadValuer`): 141 of 163 and 246 of 261, the rest turned down
for the team's direction. Each piece it tries is sized by vanilla alone,
and the team's read asked only of the ones it would pick (`evaluatePiece`):
the same deal as asking it of every piece in all 522 searches, at about a
quarter of the time: "What would make this work" 0.3-0.6 s against 0.12 s
on vanilla alone, the trading block 0.06 s a team against 0.01 s.
