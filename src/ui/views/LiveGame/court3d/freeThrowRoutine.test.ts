import { assert, describe, test } from "vitest";
import {
	bodyPoint,
	evalBall,
	evalPlayer,
	handWorld,
	poseOf,
	withBody,
} from "./evaluate.ts";
import {
	COURT_W,
	FT_LINE_DEPTH,
	FT_LINE_WIDTH,
	FT_SHOOTER_DEPTH,
	LANE_SPACES,
	type Side,
} from "./geometry.ts";
import { baseOf, bodyOf, skeleton } from "./poses.ts";
import { compile } from "./testGame.ts";

const body = bodyOf();
const bodyFor = () => body;

describe("3D free throws", () => {
	test(
		"the shooter's feet stay behind the line, and every man has a routine",
		{ timeout: 120_000 },
		() => {
			const { tl, events } = compile("ftfeet", 160);
			let trips = 0;
			let dips = 0;
			let flips = 0;
			for (const b of tl.beats) {
				const e = events[b.i]!;
				if (e.type !== "ft" && e.type !== "missFt") {
					continue;
				}
				const pid = e.pid as number;
				const team = tl.tracks.get(pid)!.team as Side;
				const depthOf = (x: number) => (team === 0 ? x : COURT_W - x);
				trips += 1;
				// Once he has walked to the line (not wherever he was fouled,
				// before it, if that happens to be near it).
				const there = Math.max(
					b.preStart,
					...tl.tracks
						.get(pid)!
						.moves.filter((m) => m.t1 <= b.actionStart)
						.map((m) => m.t1),
				);
				for (let t = there; t <= b.actionStart; t += 40) {
					const st0 = evalPlayer(tl, pid, t);
					const ball = evalBall(tl, t, bodyFor);
					// At the line, from the catch to the shot.
					// (Fouled right there, he hands the ball to the official
					// first: not his routine yet.)
					if (
						!st0.shown ||
						st0.anim === "pass" ||
						Math.abs(depthOf(st0.x) - FT_SHOOTER_DEPTH) > 1.2 ||
						Math.abs(st0.y - 25) > 2 ||
						(ball.holder !== pid &&
							baseOf(st0.anim) !== "setShot" &&
							st0.anim !== "hold" &&
							st0.anim !== "catch")
					) {
						continue;
					}
					const st = withBody(st0, body);
					const sk = skeleton(body, poseOf(st));
					for (const leg of [sk.legR, sk.legL]) {
						const toe = bodyPoint(st, leg.tip ?? leg.end);
						assert.isAtLeast(
							depthOf(toe.x),
							FT_LINE_DEPTH + FT_LINE_WIDTH + 0.05,
							`${pid} at ${Math.round(t)} (${st0.anim})`,
						);
					}
					if (baseOf(st0.anim) === "triple") {
						dips += 1;
					}
					if (ball.holder === undefined && st0.anim === "hold") {
						flips += 1;
					}
				}
			}
			assert.isAbove(trips, 20);
			assert.isAbove(dips, 0);
			assert.isAbove(flips, 0);
		},
	);

	test(
		"the men on the lane stand in their spaces, at ease till the last - then box out",
		{ timeout: 120_000 },
		() => {
			const { tl, events } = compile("ftlane", 160);
			let checked = 0;
			let boxOuts = 0;
			let easy = 0;
			let set = 0;
			let lastOnes = 0;
			for (let k = 0; k < tl.beats.length; k++) {
				const b = tl.beats[k]!;
				const e = events[b.i]!;
				if (e.type !== "ft" && e.type !== "missFt") {
					continue;
				}
				const shooter = e.pid as number;
				const team = tl.tracks.get(shooter)!.team as Side;
				const depthOf = (x: number) => (team === 0 ? x : COURT_W - x);
				const next = tl.beats[k + 1];
				const last = !(
					next &&
					(events[next.i]!.type === "ft" ||
						events[next.i]!.type === "missFt") &&
					events[next.i]!.pid === shooter
				);
				// Just before he lets it go. (The beat's moment is the ball's at
				// the rim - after a long rattle round it, well after it left
				// his hand.)
				const release =
					tl.ball
						.filter(
							(s) =>
								s.kind === "fly" &&
								"pid" in s.from &&
								s.from.pid === shooter &&
								s.t0 <= b.actionStart,
						)
						.at(-1)?.t0 ?? b.actionStart - 850;
				const t = Math.min(b.actionStart - 1500, release - 650);
				for (const [pid, tr] of tl.tracks) {
					const st0 = evalPlayer(tl, pid, t);
					const lane = Math.abs(st0.y - 25);
					const depth = depthOf(st0.x);
					if (
						!st0.shown ||
						pid === shooter ||
						lane < 8 ||
						lane > 11 ||
						depth > 17.5
					) {
						continue;
					}
					checked += 1;
					const st = withBody(st0, body);
					const sk = skeleton(body, poseOf(st));
					const space = LANE_SPACES.find(([a, z]) => depth > a && depth < z);
					assert.isDefined(space, `${pid} at depth ${depth.toFixed(2)}`);
					for (const leg of [sk.legR, sk.legL]) {
						const toe = bodyPoint(st, leg.tip ?? leg.end);
						assert.isAtLeast(
							Math.abs(toe.y - 25),
							8.05,
							`${pid}'s toes in the lane`,
						);
						const d = depthOf(toe.x);
						assert.isTrue(
							d > space![0] && d < space![1],
							`${pid}'s toes on a mark (${d.toFixed(2)})`,
						);
					}
					if (
						!last &&
						(st0.anim === "easy" ||
							st0.anim === "hips" ||
							st0.anim === "stretch" ||
							st0.anim === "shakeOut" ||
							st0.anim === "crouch")
					) {
						easy += 1;
					}
					if (last) {
						set += baseOf(st0.anim) === "laneSet" ? 1 : 0;
						lastOnes += 1;
					}
					if (last) {
						boxOuts += tr.acts.some(
							(a) =>
								(a.anim === "boxOut" || a.anim === "fight") &&
								a.t0 >= b.preStart &&
								a.t0 <= b.end + 2000,
						)
							? 1
							: 0;
					}
				}
			}
			assert.isAbove(checked, 40);
			assert.isAbove(easy, 10);
			// (On the last, set in his legs as it goes up - not stood, nor
			// down in a defensive stance.)
			assert.isAbove(lastOnes, 10);
			assert.isAbove(set / lastOnes, 0.9);
			assert.isAbove(boxOuts, 10);
		},
	);

	test(
		"teammates come in and slap the shooter low fives, palm to palm",
		{ timeout: 120_000 },
		() => {
			const { tl } = compile("ftlane", 160);
			let fives = 0;
			for (const [pid, tr] of tl.tracks) {
				for (const a of tr.acts) {
					if (a.anim !== "lowFive") {
						continue;
					}
					for (const [q, tq] of tl.tracks) {
						const b =
							q > pid &&
							tq.acts.find(
								(x) => x.anim === "lowFive" && Math.abs(x.t0 - a.t0) < 400,
							);
						if (!b) {
							continue;
						}
						const t = a.t0 + (a.t1 - a.t0) / 2;
						// (The two slapping hands: face to face - not two others
						// down a handshake line.)
						const A = evalPlayer(tl, pid, t);
						const B = evalPlayer(tl, q, t);
						if (Math.hypot(A.x - B.x, A.y - B.y) > 4) {
							continue;
						}
						fives += 1;
						const ha = handWorld(evalPlayer(tl, pid, t), body, "near");
						const hb = handWorld(evalPlayer(tl, q, t), body, "near");
						assert.isBelow(
							Math.hypot(ha.x - hb.x, ha.y - hb.y, ha.z - hb.z),
							0.6,
							`${pid} and ${q} at ${Math.round(t)}`,
						);
						assert.isBelow(ha.z, 4.5, "at the hip, not up high");
					}
				}
			}
			assert.isAbove(fives, 5);
		},
	);

	// No cutting in close on him at the line, from down under the basket:
	// the picture stays out on the floor for his free throws.
	test(
		"free throws are seen from the usual camera, not in close",
		{
			timeout: 120_000,
		},
		() => {
			const { tl } = compile("ftclose", 160);
			const at = tl.beats.filter((b) => /^(ft|missFt)$/.test(b.type));
			assert.isAtLeast(at.length, 3);
			for (const c of tl.closeUps ?? []) {
				assert.isFalse(
					at.some((b) => c.t0 < b.end && c.t1 > b.preStart),
					`close-up at ${c.t0}`,
				);
			}
		},
	);

	// Every man his own routine, the same every trip - and one man's not
	// the next man's: how many bounces, at what beat, a flip first or after,
	// a breath down in his legs.
	test(
		"each shooter has his own routine, the same every time",
		{ timeout: 120_000 },
		() => {
			const { tl, events } = compile("ftfeet", 160);
			const his = new Map<number, Set<string>>();
			for (const b of tl.beats) {
				const e = events[b.i]!;
				if (e.type !== "ft" && e.type !== "missFt") {
					continue;
				}
				const pid = e.pid as number;
				const tr = tl.tracks.get(pid)!;
				const set = tr.acts.find(
					(a) => a.anim === "setShot" && a.t0 >= b.preStart - 50,
				)!;
				// (From the official's bounce pass in to him.)
				const caught = tl.ball.find(
					(g) =>
						g.kind === "fly" &&
						g.t0 >= b.preStart &&
						g.t1 <= set.t0 &&
						"pid" in g.to &&
						g.to.pid === pid &&
						!("pid" in g.from),
				);
				if (caught?.kind !== "fly") {
					continue;
				}
				const steps: string[] = [];
				if (
					tr.acts.some(
						(a) => a.anim === "triple" && a.t0 >= caught.t1 && a.t0 < set.t0,
					)
				) {
					steps.push("sits");
				}
				const segs = tl.ball.filter(
					(g) => g.t0 >= caught.t1 + 20 && g.t0 < set.t0,
				);
				segs.forEach((g, k) => {
					if (g.kind === "hold" && g.pid === pid && g.style === "dribble") {
						const end = segs[k + 1]?.t0 ?? set.t0;
						steps.push(
							`${Math.round(((end - g.t0) * (g.rate ?? 2.1)) / 1000)} at ${(g.rate ?? 2.1).toFixed(2)}`,
						);
					} else if (
						g.kind === "fly" &&
						"pid" in g.from &&
						g.from.pid === pid
					) {
						steps.push("flip");
					}
				});
				const list = his.get(pid) ?? new Set<string>();
				list.add(steps.join(" "));
				his.set(pid, list);
			}
			assert.isAbove(his.size, 8);
			for (const [pid, ways] of his) {
				assert.strictEqual(ways.size, 1, `${pid}: ${[...ways].join(" | ")}`);
			}
			const all = new Set([...his.values()].map((w) => [...w][0]));
			assert.isAtLeast(all.size, his.size * 0.5);
		},
	);
});
