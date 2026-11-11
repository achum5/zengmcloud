// VANILLA+ FRONT OFFICES (owner's call, 2026-11-11; docs/vanilla-plus.md):
// with realistic front offices on, trades run on vanilla's engine - its
// values, its makeItWork - with teams given a direction and small, capped
// tweaks for fit and production, in place of the title-odds engine. Free
// agency stays the realistic mode's.
export const vanillaPlusSettings = {
	// Off until it has been measured; then every realistic league's trades.
	on: false,
	// How much a CPU team overvalues its own players in a deal with another
	// CPU team (stock: 5%, which after vanilla's 7th power is ~41% of trade
	// value - CPU teams with each other then almost never deal).
	cpuMarkup: 1.02,
};
