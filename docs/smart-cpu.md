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

## Phases

1. The strength scorer; the yardstick harness (A and C) on today's game.
2. Players' ranges; the goal; two-way prices.
3. Trades on the GM; run A and C.
4. Free agency, re-signing, the draft, extensions on the GM; run A and C.
5. The human moves; then B, then D.

Today's mode stays as it is, and the owner's league untouched, until the GM
beats it on the yardsticks.
