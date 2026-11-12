// VANILLA+ FRONT OFFICES (owner's call, 2026-11-11; docs/vanilla-plus.md):
// with realistic front offices on, trades run on vanilla's engine - its
// values, its makeItWork - with teams given a direction and small, capped
// tweaks for fit and production, in place of the title-odds engine. Free
// agency stays the realistic mode's.
export const vanillaPlusSettings = {
	// Every realistic league's trades (owner's call; measured: docs/
	// vanilla-plus.md, "What the checks found").
	on: true,
	// How much a CPU team overvalues its own players in a deal with another
	// CPU team (stock: 5%, which after vanilla's 7th power is ~41% of trade
	// value - CPU teams with each other then almost never deal).
	cpuMarkup: 1.02,
	// "A market that never stops", step 1 (calendar.ts): contracts, this
	// season's games left and playoffs, and the tank effect on a team's own
	// pick; selling to a human, never for less than any team's real offer.
	// On with steps 2-4 (2026-11-12; measured: docs/vanilla-plus.md).
	calendar: true,
	// Selling to a human (calendar on), the teams asked what they'd give
	// instead: the keenest this many (every team).
	bidders: Infinity,
	// "A market that never stops", steps 2-3 (calendar.ts patience,
	// market.ts tradeDay): patience to the deadline, every team shopping. Needs
	// the calendar.
	patience: true,
	// "A market that never stops", step 4 (market.ts auction): before a CPU
	// seller says yes, the keenest other teams bid; the most wins, paying
	// just enough to beat the next. Needs the calendar.
	auctions: true,
};
