import { assert, describe, test } from "vitest";
import { evalPlayer } from "./evaluate.ts";
import { COURT_H, rimX } from "./geometry.ts";
import { compile } from "./testGame.ts";

// BALL SCREENS THAT READ AS BALL SCREENS (see guardStep in director.ts):
// the screener's man where his coverage wants him as it is set.
describe("ball screens", () => {
	const games = ["a", "b", "c", "d"].map((seed) => compile(seed, 140).tl);
	const near = (
		tl: (typeof games)[number],
		pid: number,
		t: number,
	): { pid: number; d: number } | undefined => {
		const me = evalPlayer(tl, pid, t);
		const tr = tl.tracks.get(pid)!;
		let best: { pid: number; d: number } | undefined;
		for (const o of tl.tracks.values()) {
			if (o.pid < 0 || o.team === tr.team) {
				continue;
			}
			const q = evalPlayer(tl, o.pid, t);
			const d = Math.hypot(q.x - me.x, q.y - me.y);
			if (q.shown && (!best || d < best.d)) {
				best = { pid: o.pid, d };
			}
		}
		return best;
	};
	test("in a drop, the screener's man is sat back toward the rim as the screen is set", () => {
		let n = 0;
		let back = 0;
		for (const tl of games) {
			for (const s of tl.screens ?? []) {
				if (!s.ball || s.handoff || s.coverage !== "drop") {
					continue;
				}
				const big = near(tl, s.screeners[0]!, s.t);
				if (!big || big.d > 8) {
					continue;
				}
				const rim = { x: rimX(s.team), y: COURT_H / 2 };
				const B = evalPlayer(tl, big.pid, s.set);
				n++;
				if (
					Math.hypot(B.x - rim.x, B.y - rim.y) <=
					Math.hypot(s.at.x - rim.x, s.at.y - rim.y) - 4
				) {
					back++;
				}
			}
		}
		assert.isAbove(n, 8);
		assert.isAbove(back / n, 0.6);
	}, 480_000);
});
