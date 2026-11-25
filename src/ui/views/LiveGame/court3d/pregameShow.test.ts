import { assert, describe, test } from "vitest";
import { compileCourt } from "./director.ts";
import { evalPlayer } from "./evaluate.ts";
import { ROW_Y } from "./intro.ts";
import {
	cardAt,
	planShow,
	showAim,
	showEndsAt,
	showEvents,
	showTlAt,
	type ShowClock,
	type ShowPlan,
} from "./pregameShow.ts";
import { fakeGame, gidOf } from "./testGame.ts";

const show = (seed: string, intro: "regular" | "playoffs" = "regular") => {
	const { events, players } = fakeGame(seed, 30);
	const opening = showEvents(events)!;
	const tl = compileCourt({
		events: opening,
		players,
		gid: gidOf(seed),
		intro,
	});
	return { tl, plan: planShow(tl, gidOf(seed))!, events, opening };
};

// The players where they are at t, as the cameras see them.
const momentOf = (plan: ShowPlan, t: number) =>
	({
		t,
		players: plan.intro.calls.map((c) => ({
			...evalPlayer(plan.tl, c.pid, t),
			pid: c.pid,
		})),
	}) as any;

describe("the pregame show", () => {
	test("stages only the opening tip, with the lineups called first", () => {
		const { plan, events, opening } = show("show-a");
		assert.strictEqual(opening.at(-1)?.type, "jumpBall");
		assert.isBelow(opening.length, events.length);
		assert.strictEqual(plan.intro.calls.length, 10);
		assert.isAbove(plan.first, 0);
		assert.isBelow(plan.h0, plan.h1);
		assert.isAbove(plan.h0, plan.intro.calls.at(-1)!.t1);
		// The same show for the same game, on every device.
		assert.deepStrictEqual(
			{ ...show("show-a").plan, tl: undefined },
			{ ...plan, tl: undefined },
		);
	});

	test("waits on the lines, then goes on from there once the game is in", () => {
		const { plan } = show("show-b");
		const clock: ShowClock = {};
		let max = 0;
		for (let t = 0; t < plan.h1 * 3; t += 97) {
			const tl = showTlAt(plan, clock, t, false);
			assert.isAtMost(tl, plan.h1 - 200 + 1e-6);
			if (t < plan.h0) {
				assert.strictEqual(tl, t);
			} else {
				assert.isAtLeast(tl, plan.h0 - 1e-6);
			}
			max = Math.max(max, tl);
		}
		assert.isAbove(max, plan.h1 - 400);
		assert.strictEqual(showEndsAt(plan, clock), Infinity);
		// In: on from where it had got to, without a jump, to the tip.
		const t0 = plan.h1 * 3;
		const at = showTlAt(plan, clock, t0 - 97, false);
		const now = showTlAt(plan, clock, t0, true);
		assert.isBelow(Math.abs(now - at), 200);
		const end = showEndsAt(plan, clock);
		assert.isAbove(end, t0);
		assert.strictEqual(showTlAt(plan, clock, end, true), plan.intro.t1);
	});

	test("the cards go up over the lines: the key matchup only with leaders", () => {
		const { plan } = show("show-c");
		const seen = (leaders: boolean) => {
			const kinds = new Set<string>();
			for (let t = 0; t < plan.intro.t1; t += 100) {
				const c = cardAt(plan, t, t, leaders);
				if (c) {
					assert.isAtLeast(t, plan.h0);
					assert.isBelow(t, plan.h1);
					kinds.add(c.kind);
				}
			}
			return kinds;
		};
		assert.deepStrictEqual([...seen(true)].sort(), [
			"keyMatchup",
			"lineups",
			"matchup",
		]);
		assert.deepStrictEqual([...seen(false)].sort(), ["lineups", "matchup"]);
	});

	test("each home starter in close, from in front of the line, once he is in it", () => {
		for (const seed of ["show-d", "show-e"]) {
			const { plan } = show(seed);
			for (const c of plan.intro.calls) {
				let close = 0;
				for (let t = c.t0; t < c.t1; t += 50) {
					const m = momentOf(plan, t);
					const aim = showAim(plan, t, t, m, false, true);
					if (!aim.close) {
						continue;
					}
					close += 50;
					const p = m.players.find((x: any) => x.pid === c.pid);
					assert.isAtLeast(p.y, ROW_Y - 1.6, seed);
					assert.isAbove(aim.rig.at!.y, ROW_Y + 9);
					assert.isBelow(Math.abs(aim.shot.x - p.x), 0.01);
				}
				if (c.team === 1) {
					assert.isAbove(close, (c.t1 - c.t0) * 0.3, seed);
				} else {
					assert.strictEqual(close, 0);
				}
			}
		}
	});
});
