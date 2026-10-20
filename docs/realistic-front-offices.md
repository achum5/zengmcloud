# Realistic Front Offices

Status: design. The old smart AI is gone and the arena (below) runs; the front
office itself is not built yet. Decisions below were made by the repo owner;
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
acceptable is something the owner will feel out, but it must not be egregious.

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
- **Trade penalty** (player mood hit for a team that trades too much) is
  unchanged. CPU teams are subject to it and account for it. The hope is that the
  league can drop it once CPU teams are hard to fleece.

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

- **Arena** (`src/test/arena/`): real-players leagues starting in 2019 with the
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

1. Remove the old smart AI. Write this doc. (Done.)
2. Arena and stock baseline.
3. Player impact model and forecasting (players, team strength, cap).
4. Trade judgment.
5. The market: shopping, matching windows, bidding, the rebuilt Trade Proposals
   page.
6. Free agency board, re-signing, draft, roster cuts, coaching.
7. Explanations.

## Open questions

Asked as they come up; answers move into the sections above.
