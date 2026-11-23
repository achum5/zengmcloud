import type { RecapPlay } from "./recapReel.ts";

// HOW LONG A NIGHTLY RECAP RUNS: picked on each device. Every length keeps
// the biggest plays - a shorter recap's plays are always the top of a longer
// one's, never others - and every game gets at least its one biggest play
// and its FINAL card.

export type RecapLength = "full" | "medium" | "short" | "one";

export const RECAP_LENGTHS: {
	key: RecapLength;
	label: string;
	// Plays a game.
	plays: number;
}[] = [
	{ key: "full", label: "Full", plays: 14 },
	{ key: "medium", label: "Medium", plays: 6 },
	{ key: "short", label: "Short", plays: 2 },
	{ key: "one", label: "One play a game", plays: 1 },
];

export const DEFAULT_RECAP_LENGTH: RecapLength = "full";

export const playsPerGame = (length: RecapLength) =>
	RECAP_LENGTHS.find((row) => row.key === length)?.plays ?? 14;

// A game's plays at a length, in the order they happened: the play that
// decided it first in line (the shot that won it, else the one that forced
// overtime), else the best of the game's MVP (`mvp`, their pid). Then, a
// recap long enough for it running through the whole game as the NBA's do:
// each period's best, and the rest biggest first, no period more than its
// share - the late game outweighs the rest on a play's own size alone.
export const pickRecapPlays = (
	plays: readonly RecapPlay[],
	n: number,
	mvp?: number,
): RecapPlay[] => {
	const ranked = [...plays].sort((a, b) => b.w - a.w || a.i - b.i);
	const lead =
		ranked.find((p) => p.winner === "won") ??
		ranked.find((p) => p.winner === "tied") ??
		(mvp === undefined ? undefined : ranked.find((p) => p.pid === mvp)) ??
		ranked[0];
	const out = new Set<RecapPlay>();
	const inPeriod = new Map<number, number>();
	const take = (p: RecapPlay) => {
		out.add(p);
		inPeriod.set(p.period, (inPeriod.get(p.period) ?? 0) + 1);
	};
	if (lead && n > 0) {
		take(lead);
	}
	const periods = [...new Set(plays.map((p) => p.period))].sort(
		(a, b) => a - b,
	);
	if (n >= periods.length + 2) {
		for (const period of periods) {
			const best = ranked.find((p) => p.period === period);
			if (best && !inPeriod.has(period) && out.size < n) {
				take(best);
			}
		}
	}
	const share = n >= 4 ? Math.ceil(n / Math.max(1, periods.length)) + 1 : n;
	for (const p of ranked) {
		if (out.size < n && !out.has(p) && (inPeriod.get(p.period) ?? 0) < share) {
			take(p);
		}
	}
	// (Room left over, the shares aside.)
	for (const p of ranked) {
		if (out.size < n && !out.has(p)) {
			take(p);
		}
	}
	return [...out].sort((a, b) => a.i - b.i);
};
