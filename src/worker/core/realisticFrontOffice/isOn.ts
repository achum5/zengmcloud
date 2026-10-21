import { g } from "../../util/index.ts";

// Whether this league runs the new front offices (docs/realistic-front-offices.md).
// Basketball only; every stock code path that behaves differently checks this.
export const realisticFrontOfficesOn = () =>
	__SPORT === "basketball" && !!g.get("realisticFrontOffices");

// How much more players test free agency when their contracts run out, the
// same for every team, human or CPU (owner's call, in place of stock's extra
// testing for CPU teams' players only) - except players finishing rookie
// deals. In player.moodInfo's units; tuned in the arena so top free agents
// re-sign about as often as in the NBA (~57%).
export const MARKET_TESTING = 10;
