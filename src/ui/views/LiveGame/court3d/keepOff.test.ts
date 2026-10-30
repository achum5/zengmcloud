import { assert, describe, test } from "vitest";
import {
	bodyPoint,
	evalBall,
	evalPlayer,
	floorSpotOf,
	poseOf,
	withBody,
} from "./evaluate.ts";
import { bodyOf, skeleton } from "./poses.ts";
import { compile } from "./testGame.ts";

// Wherever two men would end up in each other at real speed, one is kept off
// the other (see keepOff in director.ts): bodies meet, they don't pass
// through.

describe("keeping bodies apart", () => {
	test("hardly ever is one man drawn inside another", () => {
		const { tl } = compile("apart", 80);
		const tracks = [...tl.tracks.values()];
		const fast = (t: number) => tl.fast.some(([a, b]) => t >= a && t < b);
		let live = 0;
		let inside = 0;
		for (let t = 0; t < tl.end; t += 50) {
			if (fast(t)) {
				continue;
			}
			live += 1;
			const at = tracks
				.map((tr) => floorSpotOf(tr, t))
				.filter((p) => p !== undefined);
			for (let i = 0; i < at.length; i++) {
				for (let j = i + 1; j < at.length; j++) {
					const d = Math.hypot(at[i]!.x - at[j]!.x, at[i]!.y - at[j]!.y);
					if (d < 0.9) {
						inside += 1;
					}
				}
			}
		}
		// (Counted per pair of men, per moment of real speed: left to how
		// the runs fell, about one in twenty.)
		assert.isAbove(live, 1000);
		assert.isBelow(inside / live, 0.01);
	});

	// Face to face - up on the man with the ball, both down in it - two men
	// each lean in only as far as leaves their heads apart, never one head
	// drawn in the other.
	test("two men face to face keep their heads apart", () => {
		const { tl } = compile("apart", 80);
		const body = bodyOf();
		const fast = (t: number) => tl.fast.some(([a, b]) => t >= a && t < b);
		const pids = [...tl.tracks.keys()];
		const head = (pid: number, t: number) => {
			const st = withBody(evalPlayer(tl, pid, t), body);
			return st.shown
				? { st, at: bodyPoint(st, skeleton(body, poseOf(st)).head) }
				: undefined;
		};
		let close = 0;
		let met = 0;
		for (let t = 0; t < tl.end; t += 50) {
			if (fast(t)) {
				continue;
			}
			const heads = pids.map((pid) => head(pid, t));
			for (let i = 0; i < heads.length; i++) {
				for (let j = i + 1; j < heads.length; j++) {
					const a = heads[i];
					const b = heads[j];
					if (!a || !b || Math.hypot(a.st.x - b.st.x, a.st.y - b.st.y) > 3.5) {
						continue;
					}
					close += 1;
					const d = Math.hypot(
						a.at.x - b.at.x,
						a.at.y - b.at.y,
						a.at.z - b.at.z,
					);
					if (d < 1.6 * body.headR) {
						met += 1;
					}
				}
			}
		}
		assert.isAbove(close, 500);
		assert.isBelow(met / close, 0.005, `${met} of ${close}`);
	});

	test("a loose ball bounces round a man, not through him", () => {
		const { tl } = compile("apart", 80);
		const body = bodyOf();
		const tracks = [...tl.tracks.values()];
		const fast = (t: number) => tl.fast.some(([a, b]) => t >= a && t < b);
		let loose = 0;
		let inside = 0;
		for (let t = 0; t < tl.end; t += 50) {
			const seg = tl.ball.findLast((b) => b.t0 <= t);
			if (fast(t) || (seg?.kind !== "bounce" && seg?.kind !== "rest")) {
				continue;
			}
			const B = evalBall(tl, t, () => body);
			if (B.z > 6.3) {
				continue;
			}
			loose += 1;
			for (const tr of tracks) {
				const p = floorSpotOf(tr, t);
				if (p && Math.hypot(p.x - B.x, p.y - B.y) < 0.6) {
					inside += 1;
				}
			}
		}
		// (Left to where it fell, about one in fifty.)
		assert.isAbove(loose, 100);
		assert.isBelow(inside / loose, 0.008, `${inside} of ${loose}`);
	});

	// A charge: the man who ran him over backs off him once he is down -
	// not stood over him with the ball - and whoever comes to help him up
	// comes round that man, not through him; and takes his hands, theirs
	// gripped all the way up.
	test("down taking a charge, nobody stands on him or on the man who ran into him", () => {
		const body = bodyOf();
		const hands = (st: ReturnType<typeof evalPlayer>) => {
			const b = withBody(st, body);
			const sk = skeleton(body, poseOf(b));
			return [bodyPoint(b, sk.armR.end), bodyPoint(b, sk.armL.end)];
		};
		let charges = 0;
		let helped = 0;
		for (const seed of ["apart", "e"]) {
			const { tl } = compile(seed, 150);
			for (const tr of tl.tracks.values()) {
				for (const a of tr.acts) {
					if (a.anim !== "takeCharge" && a.anim !== "chargeDown") {
						continue;
					}
					charges += 1;
					// (Till he is up: on his own, or pulled up.)
					const pulled = tr.acts.find(
						(x) => x.anim === "pulledUp" && x.t0 >= a.t1 && x.t0 < a.t1 + 4000,
					);
					const end = pulled?.t1 ?? a.t1;
					const seg = tl.ball.findLast(
						(s) => s.kind === "hold" && s.t0 <= a.t0 + 300,
					);
					const by = seg?.kind === "hold" ? tl.tracks.get(seg.pid) : undefined;
					assert.isDefined(by);
					const helpers = [...tl.tracks.values()].filter((o) =>
						o.acts.some(
							(x) => x.anim === "helpUp" && x.t1 > a.t0 && x.t0 < end,
						),
					);
					helped += helpers.length;
					for (let t = a.t0 + 900; t < end; t += 50) {
						const F = floorSpotOf(tr, t)!;
						const H = floorSpotOf(by!, t)!;
						assert.isAbove(
							Math.hypot(F.x - H.x, F.y - H.y),
							2.4,
							`${seed} ${t}`,
						);
						for (const o of helpers) {
							const P = floorSpotOf(o, t)!;
							assert.isAbove(
								Math.hypot(P.x - H.x, P.y - H.y),
								2,
								`${seed} ${t}`,
							);
						}
					}
					// Hand in hand, from when they grip to when he is all but up.
					if (pulled) {
						const helper = helpers[0]!;
						for (const k of [0.25, 0.4, 0.55, 0.7]) {
							const t = pulled.t0 + (pulled.t1 - pulled.t0) * k;
							const [aR, aL] = hands(evalPlayer(tl, tr.pid, t));
							const [hR, hL] = hands(evalPlayer(tl, helper.pid, t));
							const d = Math.min(
								Math.hypot(aR!.x - hL!.x, aR!.y - hL!.y, aR!.z - hL!.z) +
									Math.hypot(aL!.x - hR!.x, aL!.y - hR!.y, aL!.z - hR!.z),
								Math.hypot(aR!.x - hR!.x, aR!.y - hR!.y, aR!.z - hR!.z) +
									Math.hypot(aL!.x - hL!.x, aL!.y - hL!.y, aL!.z - hL!.z),
							);
							assert.isBelow(d / 2, 0.45, `${seed} ${Math.round(t)}`);
						}
					}
				}
			}
		}
		assert.isAbove(charges, 5);
		assert.isAbove(helped, 3);
	});

	test("kept off a man, he eases over and back - no jump", () => {
		const { tl } = compile("apart", 80);
		let seen = 0;
		for (const tr of tl.tracks.values()) {
			for (const s of tr.sep ?? []) {
				seen += 1;
				assert.strictEqual(s.dx.at(-1), 0);
				assert.strictEqual(s.dy.at(-1), 0);
				assert.isBelow(Math.hypot(s.dx[0]!, s.dy[0]!), 0.05);
				for (let i = 1; i < s.dx.length; i++) {
					const step = Math.hypot(
						s.dx[i]! - s.dx[i - 1]!,
						s.dy[i]! - s.dy[i - 1]!,
					);
					// (No faster than a quick step aside: feet per 50 ms.)
					assert.isBelow(step, 0.75);
				}
			}
		}
		assert.isAbove(seen, 0);
	});
});
