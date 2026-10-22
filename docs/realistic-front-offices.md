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
- **Difficulty does nothing when it is on.** The trade, signing and overall
  difficulty settings stop applying: no handicap on CPU teams, no revenue or
  free-agent penalty on humans. (A slider for this mode may be revisited later.)
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
- **Playoff runs count for something**, but never at the cost of real title
  chances later. Selling the future for a 2% title chance and a 75% chance at the
  conference semis is a franchise disaster, not a strategy.
- **"Stuck in the middle with no direction" is a failure** we measure and test
  against.
- **Money:** the luxury tax and profit are ignored. Cap room is used
  strategically to chase free agents when that applies. Reaffirmed with the
  offer market (owner's call): a team re-signs its own players, over the cap
  as far as it likes (Bird rights), whenever that is good for its outlook, and
  clears room only for a free agent worth it - so players stay with their
  teams more often than in the NBA, where the tax pushes role players out.
- **No tanking through lineups.** A rebuilding team sells for the future through
  trades, but on the floor every team plays to win.

## How players are judged

- **True ratings.** CPU teams see them and never reveal them.
- **By what a player adds to winning in this game's sim, not by ovr** - his
  exact ratings run through simulated games, not this season's stats. A 62 ovr
  can be below league average in production while a 50 ovr is above it; humans
  exploit that gap today. The sim is ground truth and CPU teams can measure any
  ratings profile against it, including lineup fit (the sim's synergy).
- **Projected forward** with the game's own progression model.
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
  him.
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

## Trades

- **Core rule: a CPU team never makes a trade it would not make on its own.** A
  human's offer can only add a bidder; it can never create a deal.
- **Shopping.** When you offer a CPU team a deal, it compares three things:
  keeping its players, your offer, and the best deal it can get elsewhere by
  asking the other CPU teams instantly.
  - Yours is best: it accepts.
  - Someone else's is better: it says so, shows you that offer in full, and
    shows its counter - what it would need from you to beat it.
  - You walk away: it does the other deal, because it would have anyway.
- **Matching window.** A deal involving a CPU team is held until the next sim
  day, and other teams (humans included) can bid on it. Bidding wars are fine;
  a bid cannot be withdrawn. Everything is public - every team sees the bids.
- **Which deals get a window** (every deal, or only notable ones) is a user
  setting.
- **A higher bar for a human's deal** (owner's call): a human picks which
  deals to offer, so finds the ones the model gets wrong in his favour; the
  margin of error counts 1.5 times as much in a deal with a human, tuned by
  the shark bot (src/test/shark): an expert human hunting each season for the
  deals the judge takes that help him most, graded on the game's own sim.
  At 1.5 the CPU team still comes out ahead by its own read after its misreads
  (2.5 ± 0.8 hundredths of a title a deal, 7% of deals leaving it worse off,
  4 leagues x 3 seasons); 1, 2 and 3 misread no less, so it stays at 1.5. What
  the shark gains is mostly bought with its future, as a contender would.
  The one blind spot it found, guards, is fixed (positions, above).
- **What a no says** (owner's call): no, a better offer (shown in full), or
  not before the deadline - never how close the offer came.
- **Undo is unchanged** (owner's call).
- **Difficulty does nothing to trades in this mode**: the CPU's read of a
  deal is its own, and the trade screen no longer prints it to the console.
- **Human-to-human trades are unchanged** (agreed in chat).
- **CPU offers to humans** appear on the Trade Proposals page, which is rebuilt
  for this mode, with a notification so the user knows an offer is waiting. The
  user can accept, decline, dismiss or counter.
  - **What it asks** (owner's call): the deal best for the CPU team, fair by
    what the human can see - public ratings, not the true ones the CPU judges
    by. Best for it by its own read, over the same bar as a human's deal, so
    it takes the deal when it is offered back; fair when the human's side
    gains by the ratings he sees, scouting fuzz and all, through the same
    projections and title odds. The same goes for "What would make this
    work" (the deal best for it that keeps everything in the human's offer -
    the offer itself if it would take that) and the trading block (what each
    CPU team would give for the assets on it). Deals both sides gain from are
    as rare as between CPU teams, so the trading block often comes back
    empty.
  - **How many** (owner's call): no fixed count and no reroll. A CPU team
    makes an offer when it wants a deal, and the offer stays while it still
    does; it goes away once the team loses interest or finds something better
    elsewhere. Two teams a day look again at what they would offer each
    human team, the day's first shoppers; an offer is off as soon as any of
    its players or picks moves.
- **The whole calculation behind each CPU-to-CPU trade** is on its trade
  summary page, in God Mode only (owner's call): it shows true ratings.
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
- **Picks are priced over where they may land** (owner's call). How far a
  pick lands from where it is projected is measured in the arena (about 6
  places a season out, 9 three seasons out, the lottery included). Most of
  that is luck, so it goes into the pick's value, not its margin: a pick is
  worth the average over every place it may land. Its margin of error is only
  the misread of how good its team will be, by the outlook's own estimate of
  that.
- **A deal moves the picks of the teams in it.** A pick's place comes from
  how good its team is, and a deal changes that: both teams' own picks are
  priced where the deal leaves the two rosters. Before this, a team gave up
  its star and priced its own pick as if it still had him - losing its best
  player moves a team's own first about 9 places, worth 0.025 titles on
  average and up to 0.15 - so "their star and their pick" was a steal, and a
  team selling its star priced the buyer's pick as if the buyer had not got
  him. A signing moves a team's own picks the same way.
- **Every kind of deal that happens in real life** (owner's call), as long as
  the CPU's judgment holds up: a side may give nothing (a player into another
  team's cap room, a salary dump), picks may go for picks, and every tradable
  player is on the table, not just a team's best. A pick is never worth less
  than nothing: a drafted player not worth a roster place is cut, which costs
  nothing straight after the draft. Rosters only have to fit once games are
  played.
- **Offseason trading windows** (owner's call): every team shops once the
  draft order is set, right after the draft, and once teams have re-signed
  their own players, and free agency's days have more shopping. In the NBA
  about half of all trades are in the offseason. Trading between picks while
  the draft runs comes later, once the structure is settled.
- **Trade penalty** (player mood hit for a team that trades too much) is
  unchanged. CPU teams are subject to it and account for it. The hope is that the
  league can drop it once CPU teams are hard to fleece.

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

## Free agency

- **Cap room is worth what teams with that much room actually sign** (measured
  in the arena, by room as free agency opens), not what a signing at that price
  gets: teams with room do better than that, and what they get rises smoothly
  with room. Pricing it by the price of a signing gave clearing a max slot a
  special value, so teams dumped prospects' future salaries for it.

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
- **From the lottery on, the draft's picks are priced by the actual class**
  (owner's call): by who would go there off a board of the class by what each
  prospect is generally worth, kept up to date as players are taken - not by
  what the slot usually yields, so a deep or a thin class moves its picks'
  prices, and a human who knows the class is strong can't buy its picks at
  the average.
- Against stock's drafting (arena, 2 leagues x 5 seasons, 6 drafts judged
  three seasons on): pick order predicts a player's overall a little better
  (rank correlation 0.59 against 0.58), the top 5 picks rate 60 against 58,
  and of each draft's 10 best players 5.8 went in the top 10 against 5.0.
- **Trading between picks comes later** (owner's call).
- Teams understand whatever lottery system the league uses.

## Coaching

Starters, rotation and minutes come from the same judgment of who helps win.
Always played to win.

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
   page.
5. Free agency board, re-signing, draft, roster cuts, coaching.
6. Explanations.

## Open questions

Asked as they come up; answers move into the sections above.
