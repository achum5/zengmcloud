import { assert, test } from "vitest";
import {
	evalPlayer,
	handWorld,
	shownAt,
	type PlayerState,
} from "./evaluate.ts";
import {
	benchMoodAt,
	benchPhase,
	crowdUp,
	benchShare,
	benchStates,
	greetsFor,
	type BenchMood,
} from "./scene.ts";
import { compile } from "./testGame.ts";
import { ANIMS, bodyOf } from "./poses.ts";

// The bench idles at the pace each pose was written for, on the wall clock -
// not at a flat 1.5 cycles a second of game time, which rocked every seated
// player all night and made the bench twitch through fast-forwards.

test("a seated player shifts in his seat slowly", () => {
	const sit = ANIMS.sit;
	assert.strictEqual(sit.kind, "loop");
	const perSecond = benchPhase("sit", 1000, 3) - benchPhase("sit", 0, 3);
	assert.closeTo(perSecond, sit.kind === "loop" ? sit.fps / sit.n : 0, 1e-9);
	assert.isBelow(perSecond, 0.5);
});

test("the bench isn't all in step", () => {
	const phases = [1, 2, 3, 4, 5].map((pid) => benchPhase("sit", 0, pid) % 1);
	assert.isAbove(new Set(phases.map((x) => x.toFixed(3))).size, 3);
});

// Up off the bench for a big play and sitting back down, the whole bench got
// there in a frame - popped up and dropped back each time.
test("the bench gets up and sits back down over a moment, not in a frame", () => {
	const { tl } = compile("bench rises", 160);
	const HIGH: Record<BenchMood, number> = { sat: 0, stood: 0.6, up: 1 };
	const FRAME = 1000 / 30;
	for (const team of [0, 1] as const) {
		// (Frame by frame round each change.)
		let changes = 0;
		for (let t = 250; t < tl.end; t += 250) {
			if (benchMoodAt(tl, t, team).now === benchMoodAt(tl, t - 250, team).now) {
				continue;
			}
			changes++;
			let last: number | undefined;
			for (let u = t - 250; u < t + 750; u += FRAME) {
				const high = benchShare(benchMoodAt(tl, u, team), (x) => HIGH[x]);
				if (last !== undefined) {
					assert.isBelow(Math.abs(high - last), 0.15, `team ${team} at ${u}`);
				}
				last = high;
			}
		}
		assert.isAbove(changes, 4);
	}
}, 60_000);

// A block brings the building up: the home crowd on its feet for a home
// one (the road fans, for theirs, less so), and the blocker's bench up.
test("a block brings the crowd and the blocker's bench up", () => {
	let blocks = 0;
	for (const seed of ["a", "b"]) {
		const { tl } = compile(seed, 140);
		for (const f of tl.fx) {
			if (f.kind !== "block" || f.team === undefined) {
				continue;
			}
			// (Not another play's reaction on top of it.)
			if (tl.fx.some((g) => g.kind === "roar" && Math.abs(g.t - f.t) < 3000)) {
				continue;
			}
			blocks += 1;
			const up = crowdUp(tl, f.t + 800);
			assert.isAtLeast(up, f.team === 1 ? 0.7 : 0.3, `${seed} ${f.t}`);
			assert.strictEqual(benchMoodAt(tl, f.t + 800, f.team).now, "up");
		}
	}
	assert.isAbove(blocks, 3);
}, 240_000);

// Back at his chair after he comes off, a man is greeted: the man sat
// nearest it either side up out of his, over beside him, the two of them
// slapping hands up high - and all of them sat back down after. Not
// popped from one way of standing to another between two frames.
test("a man coming off is greeted at his chair: up, a slap of the hands, sat back down", () => {
	const { tl, players } = compile("bench greets", 160);
	const roster = players.map((p) => ({ pid: p.pid, team: p.team }));
	const body = bodyOf();
	const FRAME = 1000 / 30;
	const at = (t: number) => {
		const onFloor = new Set(
			roster.filter((p) => shownAt(tl, p.pid, t)).map((p) => p.pid),
		);
		return new Map(
			benchStates({ tl, roster, now: t, moment: { t } }, onFloor).map((x) => [
				x.st.pid,
				x.st,
			]),
		);
	};
	const hands = (st: PlayerState) => [
		handWorld(st, body, "near"),
		handWorld(st, body, "far"),
	];
	let greeted = 0;
	let slaps = 0;
	const greets = greetsFor(tl, roster);
	for (const sd of tl.sitDowns ?? []) {
		// Who stands up for him, and his own first moment at his chair.
		const g = greets.find((x) => x.pid === sd.pid && x.t === sd.t);
		const up = new Set(g?.greeters.map((x) => x.pid));
		let prev: Map<number, PlayerState> | undefined;
		for (let t = sd.t - 3000; t < sd.t + 4500; t += FRAME) {
			const now = at(t);
			for (const pid of up) {
				const st = now.get(pid);
				// (Up, he is up in front of the bench, a step out from it.)
				if (st && st.anim !== "sit") {
					assert.isAtLeast(st.y, g!.seat.y - 0.1);
					assert.isBelow(st.y, g!.seat.y + 2.5);
					// (Not out there in his way before he gets to his chair.)
					if (t < sd.t) {
						assert.isBelow(st.y, g!.seat.y + 0.3);
					}
				}
			}
			const me = now.get(sd.pid);
			if (prev && me && !prev.has(sd.pid)) {
				// (Where he got to on the floor, he is at his chair.)
				const was = evalPlayer(tl, sd.pid, t - FRAME);
				assert.isBelow(Math.hypot(was.x - me.x, was.y - me.y), 0.5);
			}
			for (const pid of [sd.pid, ...up]) {
				const a = prev?.get(pid);
				const b = now.get(pid);
				if (!a || !b) {
					continue;
				}
				assert.isBelow(Math.hypot(a.x - b.x, a.y - b.y), 0.4, `${pid} @${t}`);
				const ha = hands(a);
				const hb = hands(b);
				for (const k of [0, 1]) {
					const d = Math.hypot(
						ha[k]!.x - hb[k]!.x,
						ha[k]!.y - hb[k]!.y,
						ha[k]!.z - hb[k]!.z,
					);
					assert.isBelow(d, 0.8, `${pid}'s hand @${Math.round(t)}`);
				}
			}
			// The slap: his hand and the other man's, met, up high.
			if (me) {
				for (const pid of up) {
					const st = now.get(pid);
					if (st?.anim !== "reachFive" || Math.abs(st.phase - 0.5) > 0.025) {
						continue;
					}
					assert.strictEqual(me.anim, "reachFive");
					let close = Infinity;
					let high = 0;
					for (const p of hands(me)) {
						for (const q of hands(st)) {
							const d = Math.hypot(p.x - q.x, p.y - q.y, p.z - q.z);
							if (d < close) {
								close = d;
								high = Math.min(p.z, q.z);
							}
						}
					}
					assert.isBelow(close, 0.6, `${sd.pid} and ${pid} @${Math.round(t)}`);
					assert.isAbove(high, 5.2);
					slaps++;
				}
			}
			prev = now;
		}
		if (up.size > 0) {
			greeted++;
			// All sat back down.
			const after = at(sd.t + 4500);
			for (const pid of [sd.pid, ...up]) {
				const st = after.get(pid);
				if (st) {
					assert.strictEqual(st.anim, "sit", `${pid}`);
					assert.isUndefined(st.from);
				}
			}
		}
	}
	// (Most subs come at a break, the bench up in the huddle for it anyway.)
	assert.isAtLeast(greeted, 5);
	assert.isAtLeast(slaps, 5);
}, 120_000);
