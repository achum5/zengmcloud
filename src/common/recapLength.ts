import type { RecapPlay } from "./recapReel.ts";

// HOW LONG A NIGHTLY RECAP RUNS: picked on each device. Every length is cut
// from the same reel of each game's plays (see recapReel.ts), and every game
// gets at least its play of the game and its FINAL card.

export type RecapLength = "full" | "medium" | "short" | "one";

export const RECAP_LENGTHS: {
	key: RecapLength;
	label: string;
	// (On a phone.)
	short: string;
	// Plays a game.
	plays: number;
}[] = [
	{ key: "full", label: "Full", short: "Full", plays: 14 },
	{ key: "medium", label: "Medium", short: "Medium", plays: 6 },
	{ key: "short", label: "Short", short: "Short", plays: 2 },
	{ key: "one", label: "One play a game", short: "1 play", plays: 1 },
];

export const DEFAULT_RECAP_LENGTH: RecapLength = "full";

export const playsPerGame = (length: RecapLength) =>
	RECAP_LENGTHS.find((row) => row.key === length)?.plays ?? 14;

// A play that turned the game: it moved its team's chance of winning this
// much (see recapReel.ts's weighPlays) - only ever in a tight finish.
export const TURN_LEV = 0.15;
// A steal shown as a play of its own only when it moved the game this much.
// (A steal and the basket off it are that basket's play, the steal shown
// with it.)
const STEAL_LEV = TURN_LEV;
// Each play more of the same man's counts for this much less, and each more
// of the same kind (a dunk, a three).
const SAME_MAN = 0.8;
const SAME_KIND = 0.85;
// A period's best is shown however the others outweigh it only when it is at
// least this big (see weighPlays) - not a blowout's third quarter's jumper.
const PERIOD_MIN = 4;
// The game's MVP's best play leads a one-play recap unless another was at
// least this many times bigger.
const MVP_EDGE = 2;

const lev = (p: RecapPlay) => p.lev ?? 0;
const isDefense = (p: RecapPlay) => p.kind === "block" || p.kind === "steal";
// A turning point, or the shot that won it: never held back by the shares.
const pivotal = (p: RecapPlay) => p.winner !== undefined || lev(p) >= TURN_LEV;
// What a recap leaves out but for a reason: garbage time, and a steal that
// turned nothing.
const shown = (p: RecapPlay) =>
	(!p.garbage || p.winner !== undefined) &&
	(p.kind !== "steal" || lev(p) >= STEAL_LEV);

// THE PLAY OF THE GAME, the one a one-play recap shows and every longer one
// has: the shot that won it (else the one that forced overtime); else, in a
// tight finish, the play that swung it most - a make, unless a stop swung it
// half as much again; else the best of the game's MVP (`mvp`, the winners'
// best player), unless another was twice as big.
const playOfTheGame = (
	ranked: readonly RecapPlay[],
	mvp: number | undefined,
): RecapPlay | undefined => {
	const won = ranked.find((p) => p.winner === "won");
	const tied = ranked.find((p) => p.winner === "tied");
	if (won ?? tied) {
		return won ?? tied;
	}
	const byLev = [...ranked].sort((a, b) => lev(b) - lev(a) || b.w - a.w);
	const make = byLev.find((p) => !isDefense(p) && shown(p));
	const stop = byLev.find((p) => isDefense(p) && shown(p));
	if (
		stop &&
		lev(stop) >= TURN_LEV &&
		lev(stop) >= 1.5 * (make ? lev(make) : 0)
	) {
		return stop;
	}
	if (make && lev(make) >= TURN_LEV) {
		return make;
	}
	const best = ranked.find((p) => !isDefense(p) && shown(p));
	const mvps =
		mvp === undefined
			? undefined
			: ranked.find((p) => p.pid === mvp && !isDefense(p) && shown(p));
	if (mvps && (!best || best.w < MVP_EDGE * mvps.w)) {
		return mvps;
	}
	return best ?? ranked.find(shown) ?? ranked[0];
};

// A game's plays at a length, in the order they happened: the play of the
// game (see playOfTheGame), then, a recap long enough for it running through
// the whole game as the NBA's do, each period's best, then the rest biggest
// first - mostly the offense's (a block or two in a long recap, a steal only
// when it turned the game), never garbage time, no man's plays all of it,
// no period more than its share but for the plays that swung the game. A
// shorter recap's plays are picked from the next longer one's: switching to
// a shorter one never shows others.
export const pickRecapPlays = (
	plays: readonly RecapPlay[],
	n: number,
	mvp?: number,
): RecapPlay[] => {
	const longer = RECAP_LENGTHS.map((row) => row.plays)
		.filter((m) => m > n)
		.sort((a, b) => a - b)[0];
	return pickFrom(
		longer === undefined ? plays : pickRecapPlays(plays, longer, mvp),
		n,
		mvp,
	);
};

const pickFrom = (
	plays: readonly RecapPlay[],
	n: number,
	mvp: number | undefined,
): RecapPlay[] => {
	if (n <= 0 || plays.length === 0) {
		return [];
	}
	const ranked = [...plays].sort((a, b) => b.w - a.w || a.i - b.i);
	const out = new Set<RecapPlay>();
	const inPeriod = new Map<number, number>();
	const byMan = new Map<number, number>();
	const byKind = new Map<string, number>();
	let stops = 0;
	const take = (p: RecapPlay) => {
		out.add(p);
		inPeriod.set(p.period, (inPeriod.get(p.period) ?? 0) + 1);
		byMan.set(p.pid, (byMan.get(p.pid) ?? 0) + 1);
		byKind.set(p.kind, (byKind.get(p.kind) ?? 0) + 1);
		if (isDefense(p)) {
			stops += 1;
		}
	};
	const lead = playOfTheGame(ranked, mvp);
	if (lead) {
		take(lead);
	}
	const periods = [...new Set(plays.map((p) => p.period))].sort(
		(a, b) => a - b,
	);
	const share = n >= 4 ? Math.ceil(n / Math.max(1, periods.length)) + 1 : n;
	const stopsMost = n >= 10 ? 2 : n >= 5 ? 1 : 0;
	// (A stop that swung the game goes past the stops' share, but by one.)
	const fits = (p: RecapPlay) =>
		!out.has(p) &&
		shown(p) &&
		(!isDefense(p) || stops < stopsMost + (pivotal(p) ? 1 : 0)) &&
		(pivotal(p) || (inPeriod.get(p.period) ?? 0) < share);
	const worth = (p: RecapPlay) =>
		p.w *
		SAME_MAN ** (byMan.get(p.pid) ?? 0) *
		SAME_KIND ** (byKind.get(p.kind) ?? 0);
	const best = (ok: (p: RecapPlay) => boolean) => {
		let top: RecapPlay | undefined;
		for (const p of ranked) {
			if (ok(p) && (!top || worth(p) > worth(top))) {
				top = p;
			}
		}
		return top;
	};
	if (n >= periods.length + 2) {
		for (const period of periods) {
			if (out.size < n && !inPeriod.has(period)) {
				const p = best(
					(q) => q.period === period && q.w >= PERIOD_MIN && fits(q),
				);
				if (p) {
					take(p);
				}
			}
		}
	}
	for (let p = best(fits); p && out.size < n; p = best(fits)) {
		take(p);
	}
	// (Room left over, the periods' shares aside - not the stops'.)
	const left = (p: RecapPlay) => !out.has(p) && shown(p) && !isDefense(p);
	for (let p = best(left); p && out.size < n; p = best(left)) {
		take(p);
	}
	return [...out].sort((a, b) => a.i - b.i);
};
