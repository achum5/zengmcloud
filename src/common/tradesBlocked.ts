import { PHASE } from "./constants.ts";

// Is trading shut for the stage of the season the league is in? After the
// deadline it is, unless God Mode has reopened it; in the playoffs it always is.
export const tradesBlocked = (
	phase: number,
	tradesAfterDeadline: boolean | undefined,
): boolean =>
	(phase === PHASE.AFTER_TRADE_DEADLINE && !tradesAfterDeadline) ||
	phase === PHASE.PLAYOFFS;
