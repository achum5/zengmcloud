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
- **Hidden until playable.** The setting does not appear in League Settings
  until the mode is worth trying; until then it is tested in the arena.
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
  strategically to chase free agents when that applies.
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
- **Human-to-human trades are unchanged** (agreed in chat).
- **CPU offers to humans** appear on the Trade Proposals page, which is rebuilt
  for this mode, with a notification so the user knows an offer is waiting. An
  offer stays until the CPU team no longer wants the deal. The user can accept,
  decline, dismiss or counter.
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
- **Deals clear the margin of error** (owner's call). A front office only
  makes a deal if each side gains by more than the model's own error on that
  gain: what a player's ratings are worth on the floor (measured against swap
  tests), plus how much further careers land from the projections than their
  own spread allows (measured on every player traded in arena leagues - the
  projections are unbiased, and only a little wider in reality). The luck the
  model already prices in, like a prospect's range of careers, is not error.
  With the margin, trades fell to ~23-32 a season against the NBA's 55-62,
  but stars no longer churn (re-trades 63% -> 32-39%).
- **Picks are priced over where they may land** (owner's call). How far a
  pick lands from where it is projected is measured in the arena (about 6
  places a season out, 9 three seasons out, the lottery included). Most of
  that is luck, so it goes into the pick's value, not its margin: a pick is
  worth the average over every place it may land. Its margin of error is only
  the misread of how good its team will be, by the outlook's own estimate of
  that.
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
- A front office prices each player's chance of re-signing with the game's own
  mood model, with the team he would be re-signing with - so a player traded
  for is valued on how he would feel about his new team.

## Free agency

- **CPU teams join the existing free agency board.** A player has an ask; a team
  says it will pay it; the day sims; a mood-weighted roll decides between the
  teams that claimed him. One signing per team per day. Same terms as humans.
- **Single-player uses the same board**, the user against CPU teams.

## Draft

- A pick is priced by the prospect the team expects to take there, from its own
  board, and the team drafts off that same board.
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
