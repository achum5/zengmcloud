import { assert, beforeAll, describe, test } from "vitest";
import { simRealGame } from "../worker/core/GameSim.basketball/realGameForTests.ts";
import {
	buildClocks,
	shotClockAt,
} from "../ui/views/LiveGame/court3d/clock.ts";
import { compileCourt } from "../ui/views/LiveGame/court3d/director.ts";
import { finishOf } from "./liveGameWording.basketball.ts";
import {
	makeRecapReel,
	REEL_TOP,
	subReel,
	weighPlays,
	type RecapReel,
} from "./recapReel.ts";

const isLine = (e: any) =>
	typeof e?.type === "string" &&
	!["init", "stat", "timeouts"].includes(e.type) &&
	!(e.type === "sub" && e.silent);

// How the game stands at each line, as the live game and the 3D court read
// it: the score by period, who is on the floor, the team fouls this period.
const walk = (events: any[]) => {
	const at: {
		e: any;
		qtrs: number[][];
		on: [string, string];
		fouls: [number, number];
	}[] = [];
	const qtrs: number[][] = [[0, 0]];
	const on: [Set<number>, Set<number>] = [new Set(), new Set()];
	let fouls: [number, number] = [0, 0];
	for (const e of events) {
		if (e.type === "stat") {
			if (e.s === "pts") {
				qtrs.at(-1)![e.t] += e.amt;
			}
			if (e.s === "gs" && e.amt > 0) {
				on[e.t as 0 | 1].add(e.pid);
			}
			continue;
		}
		if (e.type === "sub") {
			for (const pid of e.pidsOff) {
				on[e.t as 0 | 1].delete(pid);
			}
			for (const pid of e.pids) {
				on[e.t as 0 | 1].add(pid);
			}
			if (e.silent) {
				continue;
			}
		}
		if (e.type === "period" || e.type === "overtime") {
			qtrs.push([0, 0]);
			fouls = [0, 0];
		}
		for (const f of e.clipFouls ?? []) {
			fouls[f.t as 0 | 1] += 1;
		}
		if (/^pf/.test(e.type)) {
			fouls[e.t as 0 | 1] += 1;
		}
		if (isLine(e)) {
			at.push({
				e,
				qtrs: qtrs.map((q) => [...q]),
				on: [
					[...on[0]].sort((a, b) => a - b).join(","),
					[...on[1]].sort((a, b) => a - b).join(","),
				],
				fouls: [...fouls],
			});
		}
	}
	return { at, qtrs };
};

const games: {
	events: any[];
	players: any[];
	star: number;
	reel: RecapReel;
}[] = [];
beforeAll(async () => {
	for (const seed of [22, 5, 9]) {
		const { events, players } = await simRealGame(seed);
		const pts = new Map<number, number>();
		for (const e of events) {
			if (e.type === "stat" && e.s === "pts") {
				pts.set(e.pid, (pts.get(e.pid) ?? 0) + e.amt);
			}
		}
		const star = [...pts.entries()].sort((a, b) => b[1] - a[1])[0]![0];
		games.push({
			events,
			players,
			star,
			reel: makeRecapReel(events, { stars: [star] }),
		});
	}
}, 120_000);

const sameAsTheGame = (events: any[], reel: RecapReel) => {
	const whole = walk(events);
	const cut = walk(reel.events);
	// The score, period by period, comes out the same.
	assert.deepEqual(cut.qtrs, whole.qtrs);
	// And at every line shown, the game stands as it did: the lines shown
	// are the game's, in order.
	let j = 0;
	let shown = 0;
	for (const a of cut.at) {
		const { clipStart, clipFouls, clipGap, ...e } = a.e;
		const key = JSON.stringify(e);
		while (j < whole.at.length && JSON.stringify(whole.at[j]!.e) !== key) {
			j++;
		}
		const was = whole.at[j];
		assert.ok(was, `a line of the game: ${a.e.type}`);
		assert.deepEqual(a.qtrs, was!.qtrs, `score at ${a.e.type}`);
		assert.deepEqual(a.on, was!.on, `on the floor at ${a.e.type}`);
		assert.deepEqual(a.fouls, was!.fouls, `fouls at ${a.e.type}`);
		j++;
		shown++;
	}
	assert.ok(shown > 0);
	// Each play is where it says, with its lines round it.
	for (const p of reel.plays) {
		const e = reel.events[p.i];
		assert.strictEqual(e.pid, p.pid);
		assert.ok(p.from <= p.i && p.i < p.to);
		assert.strictEqual(reel.events[p.to - 1].type === "stat", false);
	}
};

describe("recap reel", () => {
	test("keeps the biggest plays and each star's, the game as it stood at each", () => {
		for (const { events, reel, star } of games) {
			assert.ok(reel.plays.length >= REEL_TOP);
			const weighed = weighPlays(events, { stars: [star] });
			const top = [...weighed.values()]
				.map((w) => w.w)
				.sort((a, b) => b - a)
				.slice(0, REEL_TOP);
			const kept = reel.plays.map((p) => p.w);
			for (const w of top) {
				assert.ok(kept.includes(w));
			}
			sameAsTheGame(events, reel);
		}
	});

	test("a shorter reel is cut from it the same way", () => {
		for (const { events, reel } of games) {
			const best = [...reel.plays].sort((a, b) => b.w - a.w).slice(0, 3);
			const short = subReel(reel, (p) => best.includes(p));
			assert.strictEqual(short.plays.length, 3);
			sameAsTheGame(events, short);
			const one = subReel(short, (p) => p === short.plays[0]);
			sameAsTheGame(events, one);
		}
	});

	test("each break between periods after the first clip is a cut", () => {
		for (const { reel } of games) {
			const ends = subReel(
				reel,
				(p) => p === reel.plays[0] || p === reel.plays.at(-1),
			);
			for (const r of [reel, ends]) {
				const lines = r.events.filter(isLine);
				const first = lines.findIndex(
					(e) => e.type !== "period" && e.type !== "overtime",
				);
				const breaks = lines
					.slice(first)
					.filter((e) => e.type === "period" || e.type === "overtime");
				assert.isAtLeast(breaks.length, 2);
				for (const e of breaks) {
					assert.isTrue(e.clipGap, JSON.stringify(e));
				}
			}
		}
	});

	test("is a fraction of the game, and stages in 3D clip by clip", () => {
		for (const { events, players, reel } of games) {
			const size = JSON.stringify(reel).length;
			assert.ok(size < JSON.stringify(events).length / 6, `reel ${size} bytes`);
			const t0 = performance.now();
			const tl = compileCourt({ events: reel.events, players, gid: 1 });
			const ms = performance.now() - t0;
			const starts = reel.events.filter((e) => e.clipStart).length;
			assert.strictEqual(tl.clips?.length ?? 0, starts);
			// The shot clock at each cut: as the clip's possession has it, not
			// the last one's run on to zero.
			const clocks = buildClocks(tl, reel.events);
			let read = 0;
			for (const c of tl.clips ?? []) {
				const shot = shotClockAt(clocks, c + 50);
				if (shot !== undefined) {
					read += 1;
					assert.isAbove(shot, 0.5, `at ${c}`);
				}
			}
			assert.isAbove(read, starts / 2);
			console.log(
				"reel",
				size,
				"bytes",
				reel.plays.length,
				"plays",
				starts,
				"clips",
				Math.round(ms),
				"ms",
				Math.round(tl.end / 1000),
				"s",
			);
		}
	});
});

describe("the shots that decided it", () => {
	const game = (lines: any[]) => [
		{ type: "init" },
		...[0, 1].flatMap((t) =>
			[1, 2, 3, 4, 5].map((k) => ({
				type: "stat",
				t,
				pid: t * 10 + k,
				s: "gs",
				amt: 1,
			})),
		),
		{ type: "jumpBall", t: 0, pid: 1, clock: 720 },
		...lines,
		{ type: "gameOver" },
	];
	const pts = (t: number, pid: number, amt: number) => ({
		type: "stat",
		t,
		pid,
		s: "pts",
		amt,
	});
	const to4th = [2, 3, 4].map((period) => ({
		type: "period",
		period,
		clock: 720,
	}));

	test("the go-ahead three with 5 seconds left won it", () => {
		const events = game([
			{ type: "fgaMidRange", t: 0, pid: 1, clock: 700 },
			pts(0, 1, 2),
			{ type: "fgMidRange", t: 0, pid: 1, clock: 700 },
			...to4th,
			{ type: "fgaTp", t: 1, pid: 12, clock: 5 },
			pts(1, 12, 3),
			{ type: "tp", t: 1, pid: 12, clock: 5 },
		]);
		const w = weighPlays(events);
		const three = [...w.values()].find((p) => p.pid === 12)!;
		assert.strictEqual(three.winner, "won");
		assert.strictEqual(
			[...w.values()].find((p) => p.pid === 1)!.winner,
			undefined,
		);
	});

	test("a shot at the horn is cut straight from to the next period", () => {
		const events = game([
			{ type: "fgaMidRange", t: 0, pid: 1, clock: 700 },
			pts(0, 1, 2),
			{ type: "fgMidRange", t: 0, pid: 1, clock: 700 },
			{ type: "fgaTp", t: 1, pid: 12, clock: 0.4 },
			pts(1, 12, 3),
			{ type: "tp", t: 1, pid: 12, clock: 0.4 },
			{ type: "period", period: 2, clock: 720 },
			{ type: "fgaTp", t: 0, pid: 3, clock: 600 },
			pts(0, 3, 3),
			{ type: "tp", t: 0, pid: 3, clock: 600 },
		]);
		const reel = makeRecapReel(events, { numPeriods: 4, stars: [] });
		const line = reel.events.find((e) => e.type === "period");
		assert.isTrue(line.clipGap);
	});

	test("an early lead taken for good is no game-winner", () => {
		const events = game([
			{ type: "fgaTp", t: 1, pid: 12, clock: 600 },
			pts(1, 12, 3),
			{ type: "tp", t: 1, pid: 12, clock: 600 },
			...to4th,
		]);
		assert.strictEqual([...weighPlays(events).values()][0]!.winner, undefined);
	});

	test("the and-one that tied it at the horn forced overtime", () => {
		const events = game([
			{ type: "fgaTp", t: 1, pid: 12, clock: 600 },
			pts(1, 12, 3),
			{ type: "tp", t: 1, pid: 12, clock: 600 },
			...to4th,
			{ type: "fgaAtRim", t: 0, pid: 2, clock: 1 },
			pts(0, 2, 2),
			{ type: "fgAtRimAndOne", t: 0, pid: 2, clock: 1 },
			pts(0, 2, 1),
			{ type: "ft", t: 0, pid: 2, clock: 1 },
			{ type: "overtime", period: 5, clock: 300 },
			{ type: "fgaMidRange", t: 1, pid: 13, clock: 100 },
			pts(1, 13, 2),
			{ type: "fgMidRange", t: 1, pid: 13, clock: 100 },
		]);
		const w = weighPlays(events);
		assert.strictEqual(
			[...w.values()].find((p) => p.pid === 2)!.winner,
			"tied",
		);
	});

	test("the go-ahead shot at the end swung it, the first basket hardly", () => {
		const events = game([
			{ type: "fgaMidRange", t: 0, pid: 1, clock: 700 },
			pts(0, 1, 2),
			{ type: "fgMidRange", t: 0, pid: 1, clock: 700 },
			...to4th,
			{ type: "fgaTp", t: 1, pid: 12, clock: 5 },
			pts(1, 12, 3),
			{ type: "tp", t: 1, pid: 12, clock: 5 },
		]);
		const w = [...weighPlays(events).values()];
		assert.isAbove(w.find((p) => p.pid === 12)!.lev!, 0.3);
		assert.isBelow(w.find((p) => p.pid === 1)!.lev!, 0.05);
	});

	test("garbage time hardly counts", () => {
		const threes = (n: number, clock: number) =>
			Array.from({ length: n }, (_, k) => [
				{ type: "fgaTp", t: 0, pid: 3, clock: clock - k * 20 },
				pts(0, 3, 3),
				{ type: "tp", t: 0, pid: 3, clock: clock - k * 20 },
			]).flat();
		const dunk = (clock: number) => [
			{ type: "fgaAtRim", t: 0, pid: 4, clock },
			pts(0, 4, 2),
			{ type: "fgAtRim", t: 0, pid: 4, clock },
		];
		const events = game([
			...dunk(710),
			...threes(16, 690),
			...to4th,
			...dunk(300),
		]);
		const w = [...weighPlays(events).values()].filter((p) => p.pid === 4);
		assert.strictEqual(w.length, 2);
		const [early, late] = w;
		assert.isUndefined(early!.garbage);
		assert.isTrue(late!.garbage);
		assert.isBelow(late!.w, early!.w / 2);
	});
});

describe("how big a play is", () => {
	test("a dunk is the dunk the 3D court will show", () => {
		for (const { events } of games) {
			const w = weighPlays(events, { gid: 7, gender: "male" });
			let dunks = 0;
			for (const [k, p] of w) {
				const e = events[k];
				if (/^fg(AtRim|PutBack|TipIn)/.test(e.type)) {
					const finish = finishOf(e, 7, "male");
					const dunk = finish === "dunk" || finish === "poster";
					assert.strictEqual(p.kind, dunk ? "dunk" : "rim", e.type);
					dunks += dunk ? 1 : 0;
				}
			}
			assert.isAbove(dunks, 0);
		}
	});
});
