import { finishOf, type Finish } from "./liveGameWording.basketball.ts";
import { momentsOf } from "./winProb.ts";

// A GAME'S RECAP PLAYS (see the Nightly Recap): the game cut down, as soon as
// it is simmed, to the plays a recap of the night could want - its biggest,
// the game-winner, each team's best player's best - each with the lines of
// its own possession that lead into it, and what is left of the rest of the
// game boiled down to what the 3D court and its score bug need to pick up
// from there: the score, who is on the floor, the team fouls, the timeouts.
// A whole game's play-by-play is about 300 KB and takes the 3D court seconds
// to stage; this is a few dozen KB, each play staged as its own clip.
//
// The same cut makes a shorter recap out of a longer one (`cutReel` again, on
// the reel, with fewer plays kept) - so a recap of any length is always the
// top of the full one, never other plays.
//
// Basketball's play-by-play only. Presentation only: it reads the game, it
// never touches the sim.

// A dunk, a make at the rim that is no dunk (a layup, a tip), a three, any
// other make, a block, a steal. (A reel saved before dunks were told apart
// has every make at the rim a "rim".)
export type RecapPlayKind =
	| "dunk"
	| "rim"
	| "three"
	| "block"
	| "steal"
	| "basket";

export type RecapPlay = {
	// The line the play is (the make, the block, the steal), in the reel's
	// events, and the first and one-past-last of the lines shown with it.
	i: number;
	from: number;
	to: number;
	// How big a play it is (see weighPlays), and how much of it is how much
	// it moved its team's chance of winning (0 to 1).
	w: number;
	lev?: number;
	// Once the game was out of reach in the last period.
	garbage?: true;
	kind: RecapPlayKind;
	pid: number;
	// The raw box score team of the man who made it (0: home).
	t: 0 | 1;
	period: number;
	clock: number;
	// The shot that won it (in the last 24 seconds, the lead for good) or
	// tied it at the end of a period to force overtime.
	winner?: "won" | "tied";
};

export type RecapReel = {
	events: any[];
	plays: RecapPlay[];
};

// How many of a game's plays are kept, biggest first - more than a full
// recap of it shows, for the night's top plays to come from too - and how
// many of each team's best player's own.
export const REEL_TOP = 18;
export const REEL_STAR = 6;
// And each period's own best, however the late game outweighs it: a recap
// runs through the whole game.
export const REEL_PER_PERIOD = 3;

const MADE = /^(fg|tp)/;
export const isMade = (type: string) =>
	MADE.test(type) && !type.startsWith("fga");
const isRim = (type: string) =>
	type === "fgAtRim" ||
	type === "fgAtRimAndOne" ||
	type === "fgPutBack" ||
	type === "fgPutBackAndOne" ||
	type === "fgTipIn" ||
	type === "fgTipInAndOne";
const isThree = (type: string) => type === "tp" || type === "tpAndOne";
const isBlock = (type: string | undefined) => !!type?.startsWith("blk");
// A play a recap is made of: a basket, a block, a steal.
const isPlay = (type: string) =>
	isMade(type) || isBlock(type) || type === "stl";

// Not a line of the play-by-play: applied on the way (see the live game's
// processLiveGameEvents and the 3D court's isLineItem).
const isLine = (e: any): boolean =>
	typeof e?.type === "string" &&
	e.type !== "init" &&
	e.type !== "stat" &&
	e.type !== "timeouts" &&
	!(e.type === "sub" && e.silent);
// Lines kept wherever they are: where the game is (a period starting) and
// that it is over.
const ALWAYS = new Set(["period", "overtime", "gameOver", "elamActive"]);
// What ends a possession: the next line is a new one's.
const ENDS = new Set([
	"drb",
	"stl",
	"tov",
	"jumpBall",
	"endOfPeriod",
	"timeout",
	"gameOver",
	"period",
	"overtime",
]);
// A line of the ball in play: a shot, its miss, a block, a board off it.
const inPlay = (type: string) =>
	type === "orb" || (/^(fga|miss|blk)/.test(type) && type !== "missFt");
// The most lines of its own possession a play is shown after; a play off a
// steal, the steal too if it was this recently (game seconds).
const LEAD_MOST = 3;
const BREAK_MOST = 8;

// The lines shown with the play at line k: a few of its own possession
// before it (the shot it is, the misses and boards it came off, or the steal
// it came straight off) - and after a block, who came up with the ball; after
// a steal, the basket at the other end if it came straight off it. A basket
// is over when it is through the net: the reel cuts to the next play there.
export const clipLines = (events: any[], k: number): number[] => {
	const out = [k];
	const own = events[k];
	const clock = own?.clock;
	let lead = 0;
	for (let i = k - 1; i >= 0 && lead < LEAD_MOST; i--) {
		const e = events[i];
		if (!isLine(e) || e.type === "sub") {
			continue;
		}
		if (
			e.type === "stl" &&
			typeof clock === "number" &&
			typeof e.clock === "number" &&
			e.clock - clock <= BREAK_MOST
		) {
			out.push(i);
			break;
		}
		if (ENDS.has(e.type) || isMade(e.type) || !inPlay(e.type)) {
			break;
		}
		out.push(i);
		lead++;
	}
	const next: number[] = [];
	for (let i = k + 1; i < events.length && next.length < 3; i++) {
		const e = events[i];
		if (isLine(e) && e.type !== "sub") {
			if (ALWAYS.has(e.type)) {
				break;
			}
			next.push(i);
		}
	}
	if (isBlock(own?.type) && next[0] !== undefined) {
		out.push(next[0]);
	} else if (own?.type === "stl") {
		// (The break: the shot, and it in.)
		const [shot, made] = next.map((i) => events[i]);
		if (
			shot?.type?.startsWith("fga") &&
			made &&
			isMade(made.type) &&
			made.t === own.t &&
			typeof clock === "number" &&
			typeof made.clock === "number" &&
			clock - made.clock <= BREAK_MOST
		) {
			out.push(next[0]!, next[1]!);
		}
	}
	return out.sort((a, b) => a - b);
};

// The stats a reel carries: the points (the score, by period), who started,
// and the fouls. The rest - minutes, rebounds, shots - a recap takes from the
// box score, and they are most of a game's play-by-play.
const STATS_KEPT = new Set(["pts", "gs", "pf"]);

// THE CUT: every line in `keep` (indices into `events`), and the rest of the
// game boiled down between them - each stretch cut out becomes, at its end,
// one line of net substitutions a team (silent), the last timeouts, and the
// stats summed by player (split at each period, for the score by quarter),
// and the team fouls in it carried on the first line after it
// (`clipFouls`, as a play shown on its own carries them - see clipOfPlay).
// The first line kept after any cut out says so (`clipStart`): the 3D court
// cuts to it. The stats of the stretch cut out are marked (`gap`), ahead of
// the line's own: the score as it stands when the picture cuts, the line's
// own points as it happens (see HighlightClip's ClipCourt). Lines that are
// kept whatever (a period starting, the end) are kept. Works on a reel as on
// a whole game, its own `clipFouls` folded in.
export const cutReel = (
	events: any[],
	keep: ReadonlySet<number>,
): { events: any[]; map: Map<number, number> } => {
	const out: any[] = [];
	// Where each kept line went.
	const map = new Map<number, number>();
	// What is on the floor as the game has been shown, and as it really is.
	const shown: [Set<number>, Set<number>] = [new Set(), new Set()];
	const real: [Set<number>, Set<number>] = [new Set(), new Set()];
	// The stats since the last line, and those of the lines cut out before
	// them.
	let stats = new Map<string, any>();
	let gap = new Map<string, any>();
	const add = (into: Map<string, any>, e: any) => {
		const key = `${e.t}|${e.pid}|${e.s}`;
		const had = into.get(key);
		if (had) {
			had.amt += e.amt;
		} else {
			into.set(key, { ...e });
		}
	};
	let timeouts: any;
	let fouls: { t: 0 | 1; clock: number }[] = [];
	let skipped = false;

	const flush = () => {
		if (timeouts) {
			out.push(timeouts);
			timeouts = undefined;
		}
		for (const s of gap.values()) {
			if (s.amt !== 0 || s.s === "gs") {
				out.push({ ...s, gap: true });
			}
		}
		for (const s of stats.values()) {
			if (s.amt !== 0 || s.s === "gs") {
				const { gap: _, ...own } = s;
				out.push(own);
			}
		}
		stats = new Map();
		gap = new Map();
		for (const t of [0, 1] as const) {
			const on = [...real[t]].filter((pid) => !shown[t].has(pid));
			const off = [...shown[t]].filter((pid) => !real[t].has(pid));
			if (on.length > 0 || off.length > 0) {
				out.push({ type: "sub", t, pids: on, pidsOff: off, silent: true });
				shown[t] = new Set(real[t]);
			}
		}
	};
	const sub = (e: any, into: [Set<number>, Set<number>]) => {
		const t = e.t === 1 ? 1 : 0;
		for (const pid of e.pidsOff ?? []) {
			into[t].delete(pid);
		}
		for (const pid of e.pids ?? []) {
			into[t].add(pid);
		}
	};

	for (let i = 0; i < events.length; i++) {
		const e = events[i];
		const type = e?.type;
		if (typeof type !== "string") {
			continue;
		}
		if (type === "init") {
			// (The players come from the box score, not from here.)
			out.push({ type: "init" });
			continue;
		}
		if (type === "stat") {
			// (The starters, on the floor from the first line on - see the 3D
			// court's director.)
			if (e.s === "gs" && e.amt > 0 && (e.t === 0 || e.t === 1)) {
				real[e.t as 0 | 1].add(e.pid);
				shown[e.t as 0 | 1].add(e.pid);
			}
			if (!STATS_KEPT.has(e.s)) {
				continue;
			}
			add(e.gap === true ? gap : stats, e);
			continue;
		}
		if (type === "timeouts") {
			timeouts = e;
			continue;
		}
		if (type === "sub") {
			sub(e, real);
			continue;
		}
		// (A line of a reel cut out: the fouls it carried, carried on.)
		if (Array.isArray(e.clipFouls) && !keep.has(i) && !ALWAYS.has(type)) {
			fouls.push(...e.clipFouls);
		}
		if (keep.has(i) || ALWAYS.has(type)) {
			flush();
			const { clipStart, clipFouls, ...rest } = e;
			const own: { t: 0 | 1; clock: number }[] = Array.isArray(clipFouls)
				? clipFouls
				: [];
			const line: any = { ...rest };
			if (type === "period" || type === "overtime") {
				// (A new period: its own fouls from none. After the first clip,
				// it is no more than a cut - see the director's period - even
				// right after a shot at the horn: nobody walks off.)
				fouls = [];
				if (skipped || map.size > 0) {
					line.clipGap = true;
				}
			} else {
				// (A clip starts where lines were cut out before it - and where one
				// started in the reel this is cut from.)
				if ((skipped || clipStart === true) && keep.has(i)) {
					line.clipStart = true;
					skipped = false;
				}
				if (fouls.length + own.length > 0) {
					line.clipFouls = [...fouls, ...own];
				}
				fouls = [];
			}
			map.set(i, out.length);
			out.push(line);
			continue;
		}
		// Cut out, and its stats with it.
		skipped = true;
		for (const st of stats.values()) {
			add(gap, st);
		}
		stats = new Map();
		if (
			type.startsWith("pf") &&
			(e.t === 0 || e.t === 1) &&
			typeof e.clock === "number"
		) {
			fouls.push({ t: e.t, clock: e.clock });
		}
	}
	flush();
	return { events: out, map };
};

// HOW BIG A PLAY IS. What a recap wants is the plays that mattered and the
// plays worth seeing, and which of the two a game is about: a close one's
// plays are the ones that swung it, a blowout's the best of what there was
// to see while it was still a game. So a play is weighed by
//
// - how much it moved who was going to win: its team's chance of winning
//   before it and after (LEVERAGE, see winProb.ts) - the turning points of a
//   tight finish outweigh anything else, while in a game that was never
//   close no play moves it much and the rest decides;
// - what it was: a dunk (the finish the 3D court will show - see
//   liveGameWording's finishOf), a three, an and-one, a shot at the horn,
//   over a layup or a jumper; a block now and then; a steal hardly at all
//   on its own (a steal and the basket off it show as that basket);
// - who made it: each team's best player in the game a little more;
// - and garbage time: once the game is out of reach in the last period, the
//   benches emptying, hardly at all.
//
// (A reel saved before plays had a `lev` was weighed by the kind and the
// swing alone - see pickRecapPlays.)
const LEVERAGE_W = 60;
const WINNER_W = 12;
const STAR_W = 1.5;
const BUZZER_W = 4;
// The last period, the leader this sure of it: garbage time, and how much
// of a play is left then.
const GARBAGE_P = 0.99;
const GARBAGE_W = 0.25;
const SHOW: Record<RecapPlayKind, number> = {
	dunk: 3.5,
	rim: 1,
	three: 3,
	basket: 1,
	block: 2,
	steal: 0.5,
};
const POSTER_W = 2;
const AND_ONE_W = 2.5;
// A block of a shot at the rim (a dunk or a layup) over one of a jumper.
const BLOCK_AT_RIM_W = 1;

type Weighed = Omit<RecapPlay, "i" | "from" | "to">;
type Moment = ReturnType<typeof momentsOf>[number];

// Each line's stats come just ahead of it (the points of a basket, then the
// basket): how the game stood before a line, and after it.
type Standings = {
	before: (k: number) => Moment;
	after: (k: number) => Moment;
};
const standings = (events: any[], moments: Moment[]): Standings => {
	// (Each line's from, or -1.)
	const pre = new Int32Array(events.length).fill(-1);
	let from = 0;
	events.forEach((e, k) => {
		if (isLine(e)) {
			pre[k] = from;
			from = k + 1;
		}
	});
	return {
		before: (k: number): Moment => moments[pre[k]! >= 0 ? pre[k]! : k]!,
		after: (k: number): Moment => moments[k + 1] ?? moments.at(-1)!,
	};
};

// The line before (`dir` -1) or after (1) line k, a substitution aside.
const lineNear = (events: any[], k: number, dir: 1 | -1) => {
	for (let i = k + dir; i >= 0 && i < events.length; i += dir) {
		const e = events[i];
		if (isLine(e) && e.type !== "sub") {
			return i;
		}
	}
	return undefined;
};

const kindOf = (type: string, finish: Finish | undefined): RecapPlayKind =>
	isRim(type)
		? isMade(type) && (finish === "dunk" || finish === "poster")
			? "dunk"
			: "rim"
		: isThree(type)
			? "three"
			: isBlock(type)
				? "block"
				: type === "stl"
					? "steal"
					: "basket";

export const weighPlays = (
	events: any[],
	{
		numPeriods = 4,
		spread,
		gid,
		gender = "male",
		stars = [],
	}: {
		numPeriods?: number;
		spread?: number;
		// The game's, and the league's: how each finish is called (see
		// finishOf), as the 3D court will stage it.
		gid?: number;
		gender?: "male" | "female";
		stars?: readonly number[];
	} = {},
): Map<number, Weighed> => {
	// (The win probability is wanted only either side of a play - and of a
	// block's stop, the shot before it and the board after.)
	const pre = new Map<number, number>();
	let from = 0;
	events.forEach((e, k) => {
		if (isLine(e)) {
			pre.set(k, from);
			from = k + 1;
		}
	});
	const only = new Set<number>();
	events.forEach((e, k) => {
		if (typeof e?.type === "string" && isPlay(e.type)) {
			only.add(pre.get(k) ?? k);
			only.add(k + 1);
			if (isBlock(e.type)) {
				const shot = lineNear(events, k, -1);
				const board = lineNear(events, k, 1);
				if (shot !== undefined) {
					only.add(pre.get(shot) ?? shot);
				}
				if (board !== undefined) {
					only.add(board + 1);
				}
			}
		}
	});
	const moments = momentsOf(events, {
		numPeriods,
		...(spread === undefined ? {} : { spread }),
		only,
	});
	const stood = standings(events, moments);
	const { before, after } = stood;
	const lead = (s: [number, number]) => Math.sign(s[0] - s[1]);
	const winners = findWinners(events, moments, stood, numPeriods);
	const out = new Map<number, Weighed>();
	events.forEach((e, k) => {
		const type = e?.type;
		if (typeof type !== "string" || !isPlay(type)) {
			return;
		}
		const pid = e.pid;
		const t = e.t;
		if (typeof pid !== "number" || (t !== 0 && t !== 1)) {
			return;
		}
		const st = before(k);
		const made = isMade(type);
		// (A block is a stop once the defense has the ball back: from the
		// shot it blocked to the board after it.)
		const shot = isBlock(type) ? lineNear(events, k, -1) : undefined;
		const board = isBlock(type) ? lineNear(events, k, 1) : undefined;
		const from0 =
			shot !== undefined && events[shot].t !== t ? before(shot) : st;
		const then =
			board !== undefined && events[board].type === "drb"
				? after(board)
				: after(k);
		const finish = made || isBlock(type) ? finishOf(e, gid, gender) : undefined;
		const kind = kindOf(type, finish);
		// (Team 0's chance either side, as the team that made it has it.)
		const mine = (p: number) => (t === 0 ? p : 1 - p);
		const lev =
			Number.isFinite(from0.p0) && Number.isFinite(then.p0)
				? Math.max(0, mine(then.p0) - mine(from0.p0))
				: 0;
		const garbage =
			st.period >= numPeriods &&
			Number.isFinite(st.p0) &&
			Math.max(st.p0, 1 - st.p0) >= GARBAGE_P;
		let w = SHOW[kind];
		if (finish === "poster" && made) {
			w += POSTER_W;
		}
		if (type.endsWith("AndOne")) {
			w += AND_ONE_W;
		}
		if (type === "blkAtRim" || type === "blkPutBack" || type === "blkTipIn") {
			w += BLOCK_AT_RIM_W;
		}
		if (made) {
			const was = lead(st.score);
			const now = lead(then.score);
			if (was !== 0 && now === -was) {
				w += 1.5;
			} else if (now === 0) {
				w += 0.5;
			}
			if (st.clock <= 2) {
				w += BUZZER_W;
			}
		}
		w += lev * LEVERAGE_W;
		if (stars.includes(pid)) {
			w += STAR_W;
		}
		const winner = winners.get(k);
		if (winner) {
			w += WINNER_W;
		}
		if (garbage && !winner) {
			w *= GARBAGE_W;
		}
		out.set(k, {
			w,
			kind,
			pid,
			t,
			period: st.period,
			clock: st.clock,
			lev: Math.round(lev * 1000) / 1000,
			...(garbage ? { garbage: true } : {}),
			...(winner ? { winner } : {}),
		});
	});
	return out;
};

// THE SHOTS THAT DECIDED IT: the one that put the winner ahead for good, in
// the last 24 seconds of the game - and each one that tied it in the last 24
// seconds of a period that went on to overtime level. A basket and its
// and-one free throw are one play: the basket.
const LATE_S = 24;
const findWinners = (
	events: any[],
	moments: Moment[],
	{ before, after }: Standings,
	numPeriods: number,
): Map<number, "won" | "tied"> => {
	const out = new Map<number, "won" | "tied">();
	const final = moments.at(-1)!.score;
	// Each scoring line, as the basket it is part of.
	const scoring: { k: number; shot: number | undefined; period: number }[] = [];
	let lastMade: number | undefined;
	events.forEach((e, k) => {
		if (!isLine(e)) {
			return;
		}
		const changed =
			before(k).score[0] !== after(k).score[0] ||
			before(k).score[1] !== after(k).score[1];
		if (isMade(e.type)) {
			lastMade = k;
		} else if (e.type !== "ft" && e.type !== "sub") {
			lastMade = undefined;
		}
		if (changed) {
			const shot =
				isMade(e.type) ||
				(lastMade !== undefined &&
					events[lastMade].type.endsWith("AndOne") &&
					events[lastMade].t === e.t)
					? lastMade
					: undefined;
			scoring.push({ k, shot, period: before(k).period });
		}
	});
	const late = (shot: number) => before(shot).clock <= LATE_S;
	// Tied at the end of a period, and on into overtime.
	const periods = new Set(scoring.map((s) => s.period));
	for (const period of periods) {
		const last = scoring.findLast((s) => s.period === period);
		const next = scoring.find((s) => s.period > period);
		if (!last || last.shot === undefined || next === undefined) {
			continue;
		}
		const end = after(last.k).score;
		const was = before(last.shot).score;
		const t: 0 | 1 = events[last.shot].t;
		if (
			period >= numPeriods &&
			end[0] === end[1] &&
			was[t] < was[t === 0 ? 1 : 0] &&
			late(last.shot)
		) {
			out.set(last.shot, "tied");
		}
	}
	// Ahead for good.
	const winner = final[0] > final[1] ? 0 : final[1] > final[0] ? 1 : undefined;
	if (winner !== undefined) {
		const lose = winner === 0 ? 1 : 0;
		const took = scoring.findLast((s) => {
			const was = before(s.shot ?? s.k).score;
			const now = after(s.k).score;
			return was[winner] <= was[lose] && now[winner] > now[lose];
		});
		if (
			took?.shot !== undefined &&
			events[took.shot].t === winner &&
			before(took.shot).period >= numPeriods &&
			late(took.shot)
		) {
			out.set(took.shot, "won");
		}
	}
	return out;
};

// THE REEL OF A GAME just simmed: its biggest plays (REEL_TOP), the shots
// that decided it, each team's best player's best (REEL_STAR, `stars`:
// their pids) and each period's best (REEL_PER_PERIOD), cut (see cutReel).
export const makeRecapReel = (
	events: any[],
	{
		numPeriods = 4,
		spread,
		stars = [],
		gid,
		gender,
	}: {
		numPeriods?: number;
		spread?: number;
		stars?: number[];
		gid?: number;
		gender?: "male" | "female";
	} = {},
): RecapReel => {
	const weighed = weighPlays(events, {
		numPeriods,
		...(spread === undefined ? {} : { spread }),
		stars,
		...(gid === undefined ? {} : { gid }),
		...(gender === undefined ? {} : { gender }),
	});
	const ranked = [...weighed.entries()].sort(
		(a, b) => b[1].w - a[1].w || a[0] - b[0],
	);
	const chosen = new Set<number>();
	for (const [k, p] of ranked) {
		if (chosen.size < REEL_TOP || p.winner) {
			chosen.add(k);
		}
	}
	for (const pid of stars) {
		let n = 0;
		for (const [k, p] of ranked) {
			if (p.pid === pid && n < REEL_STAR) {
				chosen.add(k);
				n++;
			}
		}
	}
	const perPeriod = new Map<number, number>();
	for (const [k, p] of ranked) {
		const n = perPeriod.get(p.period) ?? 0;
		if (n < REEL_PER_PERIOD) {
			chosen.add(k);
			perPeriod.set(p.period, n + 1);
		}
	}
	const keep = new Set<number>();
	const linesOf = new Map<number, number[]>();
	for (const k of chosen) {
		const lines = clipLines(events, k);
		linesOf.set(k, lines);
		for (const i of lines) {
			keep.add(i);
		}
	}
	const cut = cutReel(events, keep);
	const plays: RecapPlay[] = [...chosen]
		.sort((a, b) => a - b)
		.map((k) => {
			const lines = linesOf.get(k)!;
			return {
				...weighed.get(k)!,
				i: cut.map.get(k)!,
				from: cut.map.get(lines[0]!)!,
				to: cut.map.get(lines.at(-1)!)! + 1,
			};
		});
	return { events: cut.events, plays };
};

// A SHORTER REEL out of a reel: only the plays asked for (by their `i`),
// cut the same way, each play's lines where they went.
export const subReel = (
	reel: RecapReel,
	pick: (play: RecapPlay) => boolean,
): RecapReel => {
	const picked = reel.plays.filter(pick);
	const keep = new Set<number>();
	for (const p of picked) {
		for (let i = p.from; i < p.to; i++) {
			if (isLine(reel.events[i])) {
				keep.add(i);
			}
		}
	}
	const cut = cutReel(reel.events, keep);
	return {
		events: cut.events,
		plays: picked.map((p) => ({
			...p,
			i: cut.map.get(p.i)!,
			from: cut.map.get(p.from)!,
			to: cut.map.get(lastLine(reel.events, p.from, p.to))! + 1,
		})),
	};
};

const lastLine = (events: any[], from: number, to: number) => {
	for (let i = to - 1; i > from; i--) {
		if (isLine(events[i])) {
			return i;
		}
	}
	return from;
};
