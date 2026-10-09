import { assert, describe, test } from "vitest";
import { compileCourt } from "./director.ts";
import { evalPlayer, fastAt } from "./evaluate.ts";
import {
	callAt,
	callOrder,
	darkAt,
	INTRO_LIGHT,
	rowSpot,
	tunnelSpot,
} from "./intro.ts";
import { fakeGame, gidOf } from "./testGame.ts";

const game = (seed: string, intro?: "regular" | "playoffs") => {
	const { events, players } = fakeGame(seed, 30);
	return {
		events,
		players,
		tl: compileCourt({ events, players, gid: gidOf(seed), intro }),
	};
};

const dist = (a: { x: number; y: number }, b: { x: number; y: number }) =>
	Math.hypot(a.x - b.x, a.y - b.y);

describe("starting lineups", () => {
	test("called forwards first, then the center, then the guards", () => {
		const order = callOrder([
			{ pos: "PG" },
			{ pos: "SG" },
			{ pos: "SF" },
			{ pos: "PF" },
			{ pos: "C" },
		]).map((p) => p.pos);
		assert.deepStrictEqual(order, ["SF", "PF", "C", "PG", "SG"]);
	});

	test("only when asked for", () => {
		assert.isUndefined(game("intro-a").tl.intro);
	});

	test("the road team's five, then the home team's, before the tip", () => {
		const { tl, events } = game("intro-a", "regular");
		const intro = tl.intro!;
		assert.ok(intro);
		assert.strictEqual(intro.calls.length, 10);
		assert.deepStrictEqual(
			intro.calls.map((c) => c.team),
			[0, 0, 0, 0, 0, 1, 1, 1, 1, 1],
		);
		for (let k = 1; k < intro.calls.length; k++) {
			assert.ok(intro.calls[k]!.t0 >= intro.calls[k - 1]!.t1);
		}
		// Quick: about 15-20 seconds.
		assert.isAtLeast(intro.t1 - intro.t0, 15000);
		assert.isAtMost(intro.t1 - intro.t0, 21000);
		// The tip only once the lights are back up.
		const tip = tl.beats.find((b) => events[b.i]?.type === "jumpBall")!;
		assert.ok(tip.actionStart > intro.t1);
		// Played at real speed, never run through fast.
		for (let t = intro.t0; t < intro.t1; t += 250) {
			assert.strictEqual(fastAt(tl, t), 1);
		}
	});

	test("each man runs out from his bench to his place in the line", () => {
		const { tl } = game("intro-b", "regular");
		const intro = tl.intro!;
		const byTeam: [number[], number[]] = [[], []];
		for (const c of intro.calls) {
			byTeam[c.team].push(c.pid);
		}
		for (const c of intro.calls) {
			const k = byTeam[c.team].indexOf(c.pid);
			const before = evalPlayer(tl, c.pid, c.t0 - 1);
			assert.isBelow(dist(before, tunnelSpot(c.team, k)), 0.6);
			const lit = intro.t1 - INTRO_LIGHT;
			const after = evalPlayer(tl, c.pid, lit);
			assert.isBelow(dist(after, rowSpot(c.team, k)), 0.6);
		}
	});

	test("the lights go down and come back up", () => {
		const { tl } = game("intro-c", "playoffs");
		const intro = tl.intro!;
		assert.ok(intro.big);
		assert.strictEqual(darkAt(intro, intro.t0 - 1), 0);
		assert.strictEqual(darkAt(intro, intro.calls[0]!.t0), 1);
		assert.strictEqual(darkAt(intro, intro.t1), 0);
		assert.strictEqual(
			callAt(intro, intro.calls[3]!.t0)?.pid,
			intro.calls[3]!.pid,
		);
		assert.isUndefined(callAt(intro, intro.t1 - 10));
	});

	test("a playoff game makes more of it", () => {
		const regular = game("intro-d", "regular").tl.intro!;
		const playoffs = game("intro-d", "playoffs").tl.intro!;
		assert.isAbove(playoffs.t1 - playoffs.t0, regular.t1 - regular.t0 + 4000);
	});
});
