# Free agency overhaul and CBA-lite (Realistic Front Offices)

Owner's calls, 2026-11-10. Applies only with Realistic Front Offices on;
stock leagues keep vanilla free agency. Built on upstream's per-season
contracts (`p.contracts[].amounts`, `p.contractDemand`).

Goal: about 80% of what the NBA's CBA does to team-building at about 5% of
its complexity, and a 2K-style free agency in stages instead of 30 days.

## Contract rules

A league's own hard cap (League Settings) stays, where lower - the owner's
league caps the human teams at the tax line.

All amounts scale with the league's settings: the top max tier is
`maxContract`, the others are that times 25/35 and 30/35; the apron is
`luxuryPayroll`; the hard cap is `luxuryPayroll + 0.10 * salaryCap`.

1. **Bird rights.** Over the cap a team may sign only its own players
   (re-sign or extend), up to the max. "Own" = played for the team last
   season or was under contract with it when the season ended.
2. **Max tiers by service** (seasons played): 0-6 -> 25%, 7-9 -> 30%,
   10+ -> 35% of the cap (scaled as above).
3. **Mid-level exception (MLE).** Once a season a team over the cap may sign
   one outside free agent for up to 9% of the cap, up to 4 years, if the
   signing leaves it under the apron.
4. **Minimum deals** are always allowed (1-2 years), even past the hard cap
   while the roster is below the minimum.
5. **Apron = luxury tax line.** Above it after a move: no MLE, and a trade
   may not bring in more salary than it sends out.
6. **Hard cap** (tax line + 10% of the cap). No signing, re-signing,
   extension, match or trade may take payroll above it. Contracts already
   signed (raises, player options) may; then only minimum deals to fill
   the roster.
7. **Contract shape.** Up to 5 years with an own player, 4 otherwise. Yearly
   raise (or cut) up to 8% of the first year with Bird rights, 5% without.
   Optional player or team option on the final year. A human team decides
   its team options in the re-signing phase (declined, the player joins its
   players to re-sign); undecided ones are picked up as free agency opens.
8. **Restricted free agency.** A player finishing a rookie contract is
   restricted unless his team renounces him: he takes offers, and his team
   may match the winning one on the same terms.
9. **Extensions.** Only in the re-signing phase, before free agency
   (owner's call, 2026-11-10: not in the season). In-season extensions are
   built (extensions.ts) but off.
10. **Luxury tax** as BBGM has it.

11. **Cap holds.** A team's own unsigned free agents count against its cap
    room at their asking price until it renounces them or they sign
    elsewhere - so room is not spent on outsiders first and the team's own
    players re-signed over the cap after.

Existing contracts stay as they are (flat, no options).

## Calendar

- **Re-sign Players phase -> "Before free agency."** No separate exclusive
  negotiation: decide options (player and team), renounce restricted rights,
  last chance for extensions. Unsigned expiring players then become free
  agents; their teams keep Bird rights (and match rights if restricted).
- **Re-signing is one chance per player** (owner's call, 2026-11-10): set the
  offer, see his chance of taking it, Submit, and he signs or tests free
  agency on that roll at once (fixed for him for the season). Offers not
  submitted are decided as free agency opens. Each decision is logged for
  the player and the team - a refusal too - with the offer and the odds.
- **Free agency phase, in stages** (each is one Play; the header shows the
  stage, not days left):
  1. _Negotiation window_ - every team makes offers; players answer
     (accept / counter / reject); nobody signs.
  2. _Decision day 1_ - the top tier decides. Each player shows the number
     to beat (money and years of his leading offer, not the team); every
     team submits a final offer, sealed; at the end he signs. Restricted
     players' teams may then match.
  3. _Decision day 2_ - the next tier, the same way.
  4. _Open market_ - everyone left signs as acceptable offers arrive;
     asks come down.
  5. _Camp invites_ - minimum deals to fill rosters; then preseason.
- In the season, the remaining free agents sign as now.

## How a player chooses

- **Motivations.** Five: money, winning, role (playing time), loyalty,
  market. Weights from his mood traits ($, W, F, L); the top two show on
  his page.
- **Interest** in a team, 0-100: the team graded on each motivation
  (winning from its outlook, role from its depth chart, loyalty for his own
  team, market from size and hype), weighted by his. Shown on the free
  agents screen.
- **Offer score** = money against his ask (past the ask counts half, short
  of it double - shipped 2026-11-10), with raises, years and option valued
  (a player option is worth money to him, a team option costs), plus
  interest.
- **Ask.** Set when free agency opens from what the market can pay him -
  cap room, the MLE for teams over the cap, his tier's max - and eased a
  little each stage he is unsigned, never below half his market value (no
  more 61/71 23-year-olds on six-year minimums).
- **Young stars** in the 25% tier prefer short deals or a player option, to
  reach 30% sooner; a long deal must pay them for it.
- **Patience.** Offers far below his ask (under ~80%) use it up; out of
  patience, he will not talk to that team again until the next stage.
  Offers between that and his ask get a counter.
- **Decision.** Odds by offer score, as now but much sharper (owner): a
  clearly better offer wins about 85-90% of the time, close ones are near
  coin flips. Rolled once per player per stage, so deciding again changes
  nothing. Each team sees its own odds.
- **On his decision day he signs** with one of the offers at his ask or
  more, rather than wait; his ask never rises past what he asked as free
  agency opened. At camp, the last stage, he signs with one at the least he
  asks for (the floor) or more - the season is about to start. From camp on,
  a deal for less than his opening ask is for one season only (without,
  veterans signed at camp for 2% of the cap for four years). The number to beat counts only offers whose teams can still
  sign him. (2026-11-10, from an audit of the owner's league: its best free
  agent went unsigned into the season, his ask chasing the offers up.)
- **Prospects.** CPU teams look at the best free agents by what they are
  now and, a quarter as many again, by what they may become - without, a
  20-year-old who might start went unsigned through camp.
- **Contract lengths are enforced** for every signing: 5 years with Bird
  rights, 4 in cap room or by the mid-level, 2 by the taxpayer mid-level or
  the minimum.

## CPU teams

Everything by the same valuation they trade with: which free agents to
offer, what shape (years, raise, option), counters, final offers, matching a
restricted player's offer sheet, option decisions, extensions, which
exception to use, staying under the apron / hard cap. Measured in the arena
like every other front-office change.

## Trades

Apron rule (rule 5) and hard cap (rule 6) apply to every trade, human and
CPU, on top of the usual salary rules.

## Build order

1. Upstream merge (variable contracts).
2. Rules engine (pure functions + tests): tiers, service, Bird rights, MLE,
   apron, hard cap, raise and length limits, options, restricted status.
3. Calendar: stages, "Before free agency", header, Play menu.
4. Player side: motivations, interest, ask, offer score, patience.
5. Negotiation and offers (human UI + CPU), decision days, matching, open
   market, camp invites.
6. Extensions and options.
7. Trade rules.
8. CPU under the rules: arena A/B, owner-league audit, then ship.
