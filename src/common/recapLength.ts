import { GARBAGE_W, LEVERAGE_W, type RecapPlay } from "./recapReel.ts";

// HOW LONG A NIGHTLY RECAP RUNS: picked on each device. Every length is cut
// from the same reel of each game's plays (see recapReel.ts), and every game
// gets at least its play of the game and its FINAL card. After the games,
// the longer ones have the Player of the Night's plays, the night's top
// plays counted down, and the standings.

export type RecapLength = "full" | "medium" | "short" | "one";

export type RecapLengthRow = {
	key: RecapLength;
	label: string;
	// (On a phone.)
	short: string;
	// Plays a game.
	plays: number;
	// The Player of the Night's plays (none: no Player of the Night).
	star: number;
	// The night's top plays, counted down.
	top: number;
	standings: boolean;
};

export const RECAP_LENGTHS: RecapLengthRow[] = [
	{
		key: "full",
		label: "Full",
		short: "Full",
		plays: 14,
		star: 6,
		top: 10,
		standings: true,
	},
	{
		key: "medium",
		label: "Medium",
		short: "Medium",
		plays: 6,
		star: 3,
		top: 5,
		standings: true,
	},
	{
		key: "short",
		label: "Short",
		short: "Short",
		plays: 2,
		star: 0,
		top: 3,
		standings: true,
	},
	{
		key: "one",
		label: "One play a game",
		short: "1 play",
		plays: 1,
		star: 0,
		top: 0,
		standings: false,
	},
];

export const DEFAULT_RECAP_LENGTH: RecapLength = "full";

export const lengthRow = (length: RecapLength): RecapLengthRow =>
	RECAP_LENGTHS.find((row) => row.key === length) ?? RECAP_LENGTHS[0]!;

export const playsPerGame = (length: RecapLength) => lengthRow(length).plays;

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

// THE PLAYER OF THE NIGHT'S PLAYS, in the order they happened: his own from
// his game's reel, picked the way a game's are (his biggest, through the
// whole game, not all of one kind). A shorter list is the top of a longer.
export const starPlays = (
	plays: readonly RecapPlay[],
	pid: number,
	n: number,
): RecapPlay[] =>
	pickRecapPlays(
		plays.filter((p) => p.pid === pid),
		n,
		pid,
	);

// His plays a recap would show: enough of them makes him one to show.
export const starPlayCount = (plays: readonly RecapPlay[], pid: number) =>
	plays.filter((p) => p.pid === pid && shown(p) && !isDefense(p)).length;

// THE NIGHT'S TOP PLAYS, best first (#1 first; the recap counts them down):
// the plays of the night that look the best - the poster, the and-one dunk,
// the deep three, the block at the rim - with the shot that won a game on
// top. A play's swing of its game counts here for far less than in its
// game's own recap: a top ten is the night's best plays, not its tightest
// finishes. No game, man or kind fills it, a block or two at most, a steal
// only when it turned a game, and never garbage time. A shorter list is the
// top of the longer one.
export const TOP_PLAYS_MOST = 10;
// How much of a play's swing of its game counts here (LEVERAGE_W in its
// game's).
const TOP_LEV_W = 15;
const TOP_PER_GAME = 3;
const TOP_PER_MAN = 2;
const TOP_STOPS = 2;

export type NightPlay = { gid: number; play: RecapPlay };

// How good a play looks (see weighPlays: its weight, less most of its
// swing).
export const flashOf = (p: RecapPlay) =>
	p.w -
	lev(p) *
		(LEVERAGE_W - TOP_LEV_W) *
		(p.garbage && p.winner === undefined ? GARBAGE_W : 1);

export const pickTopPlays = (
	games: readonly { gid: number; plays: readonly RecapPlay[] }[],
	n = TOP_PLAYS_MOST,
): NightPlay[] => {
	const all: (NightPlay & { flash: number })[] = [];
	for (const { gid, plays } of games) {
		for (const play of plays) {
			if (shown(play)) {
				all.push({ gid, play, flash: flashOf(play) });
			}
		}
	}
	all.sort((a, b) => b.flash - a.flash || a.gid - b.gid || a.play.i - b.play.i);
	const out: NightPlay[] = [];
	const inGame = new Map<number, number>();
	const byMan = new Map<number, number>();
	const byKind = new Map<string, number>();
	let stops = 0;
	const fits = (x: NightPlay) =>
		(inGame.get(x.gid) ?? 0) < TOP_PER_GAME &&
		(byMan.get(x.play.pid) ?? 0) < TOP_PER_MAN &&
		(!isDefense(x.play) || stops < TOP_STOPS);
	const worth = (x: NightPlay & { flash: number }) =>
		x.flash *
		SAME_MAN ** (byMan.get(x.play.pid) ?? 0) *
		SAME_KIND ** (byKind.get(x.play.kind) ?? 0);
	const taken = new Set<NightPlay>();
	while (out.length < n) {
		let top: (NightPlay & { flash: number }) | undefined;
		for (const x of all) {
			if (!taken.has(x) && fits(x) && (!top || worth(x) > worth(top))) {
				top = x;
			}
		}
		if (!top) {
			break;
		}
		taken.add(top);
		out.push({ gid: top.gid, play: top.play });
		inGame.set(top.gid, (inGame.get(top.gid) ?? 0) + 1);
		byMan.set(top.play.pid, (byMan.get(top.play.pid) ?? 0) + 1);
		byKind.set(top.play.kind, (byKind.get(top.play.kind) ?? 0) + 1);
		if (isDefense(top.play)) {
			stops += 1;
		}
	}
	return out;
};
