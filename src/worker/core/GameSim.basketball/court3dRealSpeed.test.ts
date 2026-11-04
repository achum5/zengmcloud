import { assert, beforeAll, describe, test } from "vitest";
import {
	compileCourt,
	type CourtTimeline,
} from "../../../ui/views/LiveGame/court3d/director.ts";
import {
	evalPlayer,
	fastAt,
} from "../../../ui/views/LiveGame/court3d/evaluate.ts";
import { simRealGame } from "./realGameForTests.ts";

// A game the real sim played, as the 3D court plays it: at real speed (not
// the lead-in to a play, run through fast), where the evaluator has each man
// every tenth of a second.
const STEP = 100;
const RUNS = new Set(["run", "jog", "sprint", "walk"]);

let tl: CourtTimeline;
beforeAll(async () => {
	const { events, players } = await simRealGame(22);
	tl = compileCourt({ events, players, gid: 1 });
}, 120_000);

describe("3D court, a real game", () => {
	test("at real speed nobody jumps from one spot to another, or runs backward at a run", () => {
		const jumps: string[] = [];
		const backward: string[] = [];
		let frames = 0;
		const tracks = [...tl.tracks.values()].filter((t) => t.pid >= 0);
		const end = tl.beats.at(-1)!.end;
		const prev = new Map<number, { x: number; y: number } | undefined>();
		const since = new Map<number, number>();
		for (let t = 0; t < end; t += STEP) {
			if (fastAt(tl, t) > 1.2) {
				prev.clear();
				since.clear();
				continue;
			}
			const cut = tl.cuts.some((c) => c > t - STEP - 1 && c <= t + 1);
			for (const tr of tracks) {
				const q = evalPlayer(tl, tr.pid, t);
				const p = prev.get(tr.pid);
				prev.set(tr.pid, q.shown ? q : undefined);
				if (!q.shown || !p || cut) {
					since.delete(tr.pid);
					continue;
				}
				frames += 1;
				const vx = ((q.x - p.x) * 1000) / STEP;
				const vy = ((q.y - p.y) * 1000) / STEP;
				const v = Math.hypot(vx, vy);
				if (v > 30) {
					jumps.push(`${tr.pid} ${(v / 10).toFixed(1)} ft at ${t}`);
				}
				const back =
					v > 4 &&
					RUNS.has(q.anim) &&
					(vx * Math.cos(q.yaw) + vy * Math.sin(q.yaw)) / v < -0.5;
				if (!back) {
					since.delete(tr.pid);
					continue;
				}
				const s = since.get(tr.pid) ?? t;
				since.set(tr.pid, s);
				if (t - s === 400) {
					backward.push(`${tr.pid} (${q.anim}) at ${t}`);
				}
			}
		}
		assert.isAbove(frames, 100_000);
		assert.deepEqual(jumps, []);
		assert.deepEqual(backward, []);
	}, 900_000);
});
