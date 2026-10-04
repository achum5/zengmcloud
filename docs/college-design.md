# College basketball: design spec

Decisions from the Q&A. Numbers marked **(default)** are my starting values. Change any of them.

## League basics

- Every D1 school is a fictional stand-in (for example, Lexington Wildcats), and all of them are editable.
- College is a runtime league mode. It never changes pro BBGM.
- **Roster limit:** a league setting, default **15**. Every roster player can be on scholarship (House settlement rules).
- **Eligibility: 5 seasons in 5 years.** This is the NCAA rule adopted for fall 2027 enrollees.
  - Classes: Fr / So / Jr / Sr / 5th.
  - No redshirts and no waivers.
- **Ratings use BBGM's prospect scale.** College stars are about 45–55 ovr, like real BBGM draft prospects, so players move into a pro league unchanged.
  - Today's generated rosters run a bit hot. I'll recalibrate them.

## Calendar

1. Regular season. High school recruiting runs weekly.
2. Conference tournaments, then the NCAA tournament and the NIT.
3. After the season:
   - Early NBA departures. Players also decide on yearly NIL renegotiation.
   - Pre-portal retention talks.
   - The portal opens.
4. **4 offseason recruiting weeks.** HS recruits and portal players are recruited together.
5. Signing day for HS recruits. Portal players sign whenever they commit.
6. Preseason. Unsigned players walk on where there's room, and the portal closes.

## Recruiting (HS and portal)

- **Effort:** a weekly hours pool (default **100/week, max 25 per recruit**). Hours build interest with diminishing returns.
- **Official visits:** limited to **8 per year (default)**.
- **Scouting:** hours spent on a recruit also sharpen his ratings, from a vague range to exact. Stars, position and height always show.
- **Priorities:** every recruit shows his top 3 priorities. Each recruit weighs them differently. The full list:
  - Prestige, Winning, Proximity, Playing time, Pro potential, NIL, Conference, Coach stability, Facilities.
- **Interest:** fit with each school, scored on his weighted priorities, plus hours, visits, offer, promises, and NIL relative to his ask.
- **Commitments:** he commits once an offering school pulls clearly ahead. The best recruits take longer.
- **Decommits:** a commit can flip if another school gets well ahead before he signs. He also leaves if you cut his NIL.
- **AI schools use the same rules as you.** The difficulty slider tunes how smart they are, not extra resources.
  - AI target choice is tiered: schools chase players they can realistically land, so the best programs fight over the top recruits and everyone else works further down the board.

## NIL

- **Budget:** a yearly pool per school based on prestige. It's spent on recruits plus returning players.
- **Negotiation is offer and counteroffer:**
  - You see only a rough range of his ask, never the exact number.
  - He counters with a number.
  - Talks go as many rounds as his patience allows. Patience depends on his personality, his interest in you, how bad your offers are, and competing offers.
  - A slightly low offer costs a small, permanent interest hit. A way-low offer makes him walk away from your school.
- **Overpaying** eats your budget, and he expects bigger raises at each renegotiation.
- **Returning players renegotiate yearly.** Players who improved want raises, and a bad renegotiation raises portal risk.

## Promises

- **Types:**
  - Starter (start X% of games).
  - Minutes (at least N per game).
  - NIL raise next year.
  - Won't sign another player at his position in his class.
- A broken promise hurts his happiness, raises his portal risk, and costs your recruiting reputation with future recruits.

## Transfer portal

- **Who enters, like real life:**
  - low playing time
  - underpaid or unhappy NIL
  - stars at small schools moving up
  - after losing seasons or prestige drops
  - broken promises
- **Size:** realistic by default, about **30–35%** of players with eligibility left. Adjustable with a slider.
- **Retention:**
  - Before the portal opens, at-risk players say they're considering it. You can talk them out of it with playing time or a promise, or with an NIL raise.
  - After they enter, you can recruit them back like anyone else, with a familiarity bonus.

## Early departures and the pro hand-off

- **Players leave by draft stock:** projected first-rounders mostly go, and elite freshmen are often one-and-done. A slider tunes the rate.
- **Two ways into a pro league:**
  - Export each year's departing class as a BBGM draft class file.
  - Link a pro league so departing players automatically become its draft prospects.

## You as coach

- You're the only coach character. There are no coach ratings and no carousel.
- You have a contract and a hot seat, and job offers come from other schools.
- Firing and job offers can each be turned off in settings.

## Prestige

- Moves slowly, based on results: wins, tournament runs, and recruiting class quality.
- Blue bloods have a floor.
- A slider sets how fast it moves.

## Rankings and postseason extras

- Weekly Top 25 poll.
- Bracketology during the season, then a Selection Sunday reveal.
- The NIT for the best teams left out of the NCAA field.
- Team recruiting class rankings.

## Customization sliders

- Portal size and how easy retention is.
- Recruiting difficulty: AI smarts, how hard recruits are to sway, and commit speed.
- NIL scale and budgets, and how much NIL weighs against fit.
- Early NBA departure rate and how fast prestige moves.
- Roster limit, coach firing on/off, and job offers on/off.

## Not in v1

- Coach ratings and the AI coaching carousel.
