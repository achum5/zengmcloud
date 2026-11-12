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
	// pick. Off until measured.
	calendar: false,
	// Selling to a human, the teams asked what they'd give instead (the
	// keenest this many).
	bidders: 4,
};
