import { assert, describe, test } from "vitest";
import type { CourtTimeline } from "./director.ts";
import { evalBall, evalPlayer, handWorld } from "./evaluate.ts";
import { COURT_H, COURT_W, RIM_R, RIM_Z, rimX, type Side } from "./geometry.ts";
import { bodyOf } from "./poses.ts";
import { compile } from "./testGame.ts";

const body = bodyOf();
const bodyFor = () => body;
const STEP = 20;

const outside = (b: { x: number; y: number }) =>
	b.x < 0 || b.x > COURT_W || b.y < 0 || b.y > COURT_H;

// Against the rim or its backboard: a change of course there is the iron
// or the glass, not a hand.
const atBasket = (b: { x: number; y: number; z: number }) =>
	([0, 1] as const).some((s) => {
		const x = rimX(s);
		const ring = Math.hypot(b.x - x, b.y - COURT_H / 2);
		const glass = x + (x < COURT_W / 2 ? -1.25 : 1.25);
		return (
			(Math.abs(ring - RIM_R) < 1.2 && Math.abs(b.z - RIM_Z) < 1.2) ||
			(Math.abs(b.x - glass) < 1 &&
				Math.abs(b.y - COURT_H / 2) < 3.5 &&
				b.z > RIM_Z - 1.2 &&
				b.z < RIM_Z + 4.5)
		);
	});

// Who last had a hand on the ball before it went out at or before `tw`:
// whoever was holding it, or - if it changed course in the air, off the
// floor and away from the basket - the man whose hands were nearest it
// (and, a hand in from each side, as a poke at a dribble is, anyone else
// as near as makes no difference: `also`).
const lastTouch = (
	tl: CourtTimeline,
	tw: number,
	pids: number[],
): { pid?: number; also?: number[]; why: string; t: number } => {
	let tIn: number | undefined;
	for (let t = tw; t >= tw - 4000 && t >= 0; t -= STEP) {
		if (!outside(evalBall(tl, t, bodyFor))) {
			tIn = t;
			break;
		}
	}
	if (tIn === undefined) {
		return { why: "never in play", t: tw };
	}
	for (let t = tIn; t >= Math.max(STEP, tIn - 6000); t -= STEP) {
		const b = evalBall(tl, t, bodyFor);
		if (b.holder !== undefined) {
			return { pid: b.holder, why: "held", t };
		}
		const a = evalBall(tl, t - STEP, bodyFor);
		if (a.holder !== undefined) {
			// Just out of his hands.
			return { pid: a.holder, why: "let go", t };
		}
		const c = evalBall(tl, t + STEP, bodyFor);
		const dv = Math.hypot(
			c.x - 2 * b.x + a.x,
			c.y - 2 * b.y + a.y,
			c.z - 2 * b.z + a.z,
		);
		// (Off the floor: a bounce, between samples.)
		if (dv < 0.08 || Math.min(a.z, b.z, c.z) < 1.2 || atBasket(b)) {
			continue;
		}
		let best: { pid: number; d: number } | undefined;
		const near: { pid: number; d: number }[] = [];
		for (const pid of pids) {
			const st = evalPlayer(tl, pid, t);
			if (!st.shown) {
				continue;
			}
			const d = Math.min(
				...(["near", "far", "both"] as const).map((which) => {
					const h = handWorld(st, body, which);
					return Math.hypot(h.x - b.x, h.y - b.y, h.z - b.z);
				}),
				// Off his body: anywhere from his feet to his shoulders.
				b.z < 6 ? Math.hypot(st.x - b.x, st.y - b.y) + 0.4 : Infinity,
			);
			near.push({ pid, d });
			if (!best || d < best.d) {
				best = { pid, d };
			}
		}
		if (best && best.d < 2.5) {
			const top = best;
			return {
				pid: top.pid,
				also: near
					.filter((x) => x.pid !== top.pid && x.d < top.d + 0.5)
					.map((x) => x.pid),
				why: "deflected",
				t,
			};
		}
		return {
			why: `changed course at ${Math.round(tw - t)}ms before, (${b.x.toFixed(1)}, ${b.y.toFixed(1)}, ${b.z.toFixed(1)}), nobody near (${best?.d.toFixed(1)})`,
			t,
		};
	}
	return { why: "no touch found", t: tIn };
};

describe("3D out of bounds", () => {
	test(
		"the ball always goes out off the team that loses it",
		{ timeout: 60_000 },
		() => {
			let checked = 0;
			const wrong: string[] = [];
			for (const seed of ["oob1", "oob2", "oob3", "oob4", "s8", "s12"]) {
				const { tl, players } = compile(seed, 160);
				const teamOf = new Map<number, Side>(
					players.map((p) => [p.pid, p.team]),
				);
				const pids = players.map((p) => p.pid);
				for (const f of tl.fx) {
					if (
						f.kind !== "whistle" ||
						f.call !== "out" ||
						f.team === undefined
					) {
						continue;
					}
					checked += 1;
					const touch = lastTouch(tl, f.t, pids);
					const beat = tl.beats.find((b) => b.preStart <= f.t && f.t <= b.end);
					const what = `${seed} @${Math.round(f.t)} ${beat?.type}`;
					if (touch.pid === undefined) {
						wrong.push(`${what}: ${touch.why}`);
					} else if (
						teamOf.get(touch.pid) === f.team &&
						!touch.also?.some((pid) => teamOf.get(pid) !== f.team)
					) {
						wrong.push(
							`${what}: last off ${touch.pid} (${touch.why} ${Math.round(f.t - touch.t)}ms before), but it's their ball`,
						);
					}
				}
			}
			assert.isAtLeast(checked, 20);
			assert.deepStrictEqual(wrong, []);
		},
	);
});
