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

| decision | today | the GM |
|---|---|---|
| free agent offers (stages and in season), re-signing, offer-sheet matching, signing at a full roster, filling a short roster | try 1's `priceSigning` (its franchise value, less a margin) | the GM's worth of signing him on those terms, the roster cut to the limit if over; boards, asks (`priceMarket`), odds and stages built on it as now |
| cuts over the limit | try 1's franchise value without him | the same through the GM's worth, dead money counted |
| team options | a rule of thumb (declined under 80% of market salary) | his worth at the option's salary against letting him go |
| the draft | stock (`value ** 69` at random) | the prospect worth most to the team |
| CPU-to-CPU trades | vanilla+'s market | the GM's market (below) |
| a human's offer | vanilla+'s evaluate, trade difficulty on | the GM's verdict with the market rule; difficulty off until the bench says otherwise (layer 4) |
| what would make this work, proposals, trading block | makeItWork on vanilla+'s evaluate | the same search on the GM's verdict |
| clearing room by trade | try 1's plans re-vetted by vanilla+ | off for now: room is used by signing and through the market |
| extensions | off | off |

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

Then layer 3's leagues, where the re-signing odds get their check (said 85%,
about 85% stayed) with the GM making the offers.

Build order: the view that follows the league (rosters, contracts, cuts,
dead money); signing worth behind `priceSigning` with cuts and options; the
draft; the market; human trades. Each with its checks before the next.

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

- **Switches.** `BENCH_POLICY` autopilot or shark (`BENCH_SHARK` its options
  as JSON); `BENCH_TIDS=7,17` the policy's teams (default one, 7 x seed mod
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

| policy | titles | rate (95%) | win% | rating rank | out of playoffs | s a season |
|---|---|---|---|---|---|---|
| autopilot | 1 / 80 | 1.3% (0.2-6.7) | 43.2 ± 1.7 | 20.3 | 51% | 31 |
| shark | 0 / 80 | 0% (0-4.6) | 47.6 ± 1.6 | 20.6 | 53% | 66 (33 its own) |
| check: auto play's signing, difficulty 0 (6 leagues) | 1 / 60 | 1.7% (0.3-8.9) | 54.2 ± 1.6 | 11.9 | 38% | 30 |
| the CPU teams beside them | 3.4% a team | | 50.0 | | | |

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
