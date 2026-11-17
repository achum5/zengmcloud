import { assert, describe, test } from "vitest";
import {
	bodyPoint,
	evalPlayer,
	fastAt,
	joltAt,
	plainly,
	poseOf,
	withBody,
} from "./evaluate.ts";
import { bodyOf, skeleton } from "./poses.ts";
import { compile } from "./testGame.ts";

// ALL OF HIM OVER HIS FEET (see tipAt in evaluate.ts, overFeet in
// poses.ts), and contact that shows (joltAt).
describe("over his feet", () => {
	const body = bodyOf();
	const tl = compile("a", 40).tl;
	const men = [...tl.tracks.values()].filter((tr) => tr.pid >= 0);
	// Where his hips are against the middle of his feet, the way he faces
	// (feet forward), and his feet.
	const drawn = (pid: number, t: number) => {
		const st = evalPlayer(tl, pid, t);
		const me = withBody(st, body);
		const sk = skeleton(body, poseOf(me));
		const hip = bodyPoint(me, sk.pelvis);
		const feet = [sk.legR.end, sk.legL.end].map((v) => bodyPoint(me, v));
		const mx = (feet[0]!.x + feet[1]!.x) / 2;
		const my = (feet[0]!.y + feet[1]!.y) / 2;
		return {
			st,
			ahead: (hip.x - mx) * Math.cos(st.yaw) + (hip.y - my) * Math.sin(st.yaw),
			feet,
		};
	};

	// Speeding up hard he is out ahead of his feet; pulling up hard, back
	// over them - more than drawn plain.
	test("getting going he leans out over his feet, pulling up he sits back", () => {
		let starts = 0;
		let stops = 0;
		let startLean = 0;
		let stopLean = 0;
		for (const tr of men) {
			for (let t = 30_000; t < 900_000; t += 200) {
				if (fastAt(tl, t) > 1) {
					continue;
				}
				const a = evalPlayer(tl, tr.pid, t - 200);
				const b = evalPlayer(tl, tr.pid, t);
				const c = evalPlayer(tl, tr.pid, t + 200);
				if (!a.shown || !b.shown || !c.shown || b.z > 0.3) {
					continue;
				}
				const fx = Math.cos(b.yaw);
				const fy = Math.sin(b.yaw);
				const v0 = ((b.x - a.x) * fx + (b.y - a.y) * fy) / 0.2;
				const v1 = ((c.x - b.x) * fx + (c.y - b.y) * fy) / 0.2;
				const acc = (v1 - v0) / 0.2;
				if (Math.abs(acc) < 18) {
					continue;
				}
				const lean = drawn(tr.pid, t).ahead;
				const flat = plainly(() => drawn(tr.pid, t).ahead);
				if (acc > 0) {
					starts++;
					startLean += lean - flat;
				} else {
					stops++;
					stopLean += lean - flat;
				}
			}
		}
		assert.isAbove(starts, 20);
		assert.isAbove(stops, 20);
		assert.isAbove(startLean / starts, 0.15);
		assert.isBelow(stopLean / stops, -0.1);
	}, 240_000);

	// However he leans, his feet are where they would have been.
	test("his feet stay where they were, however he leans", () => {
		let n = 0;
		let most = 0;
		for (const tr of men) {
			for (let t = 30_000; t < 600_000; t += 330) {
				const st = evalPlayer(tl, tr.pid, t);
				if (!st.shown || !st.over) {
					continue;
				}
				n++;
				const a = drawn(tr.pid, t).feet;
				const b = plainly(() => drawn(tr.pid, t).feet);
				for (let k = 0; k < 2; k++) {
					most = Math.max(
						most,
						Math.hypot(a[k]!.x - b[k]!.x, a[k]!.y - b[k]!.y, a[k]!.z - b[k]!.z),
					);
				}
			}
		}
		assert.isAbove(n, 200);
		// (Out of reach of a long lean, a foot is drawn along after him a
		// touch.)
		assert.isBelow(most, 0.35);
	}, 240_000);

	// Two of them coming together hard rock back off each other - each the
	// way away from the other.
	test("coming together hard, both rock back off it", () => {
		let hits = 0;
		let away = 0;
		// (Each man once a moment - where he is, if he is shown - and every
		// pair of them from that.)
		const STEP = 50;
		const BACK = 100 / STEP;
		const t0 = 30_000;
		const n = Math.floor((900_000 - t0) / STEP);
		const where = men.map((tr) => {
			const xs = new Float64Array(n);
			const ys = new Float64Array(n);
			for (let k = 0; k < n; k++) {
				const p = evalPlayer(tl, tr.pid, t0 + k * STEP);
				xs[k] = p.shown ? p.x : Number.NaN;
				ys[k] = p.shown ? p.y : Number.NaN;
			}
			return { xs, ys };
		});
		for (let i = 0; i < men.length; i++) {
			for (let j = 0; j < men.length; j++) {
				const a = men[i]!;
				const b = men[j]!;
				if (a.team === b.team || a.pid > b.pid) {
					continue;
				}
				const A = where[i]!;
				const B = where[j]!;
				for (let k = BACK; k < n; k++) {
					const d = Math.hypot(A.xs[k]! - B.xs[k]!, A.ys[k]! - B.ys[k]!);
					const d0 = Math.hypot(
						A.xs[k - BACK]! - B.xs[k - BACK]!,
						A.ys[k - BACK]! - B.ys[k - BACK]!,
					);
					// (NaN - either not shown - is never a hit.)
					if (!(d < 1.75 && d0 >= 1.75 && (d0 - d) / 0.1 > 5)) {
						continue;
					}
					hits++;
					// A beat later, each tipped away from the other.
					const u = t0 + k * STEP + 120;
					const pa = evalPlayer(tl, a.pid, u);
					const ja = joltAt(tl, a, u, pa.yaw);
					if (ja) {
						const wx = ja[0] * Math.cos(pa.yaw) - ja[1] * Math.sin(pa.yaw);
						const wy = ja[0] * Math.sin(pa.yaw) + ja[1] * Math.cos(pa.yaw);
						if (wx * (A.xs[k]! - B.xs[k]!) + wy * (A.ys[k]! - B.ys[k]!) > 0) {
							away++;
						}
					}
				}
			}
		}
		assert.isAbove(hits, 5);
		assert.isAbove(away / hits, 0.8);
	}, 480_000);
});
