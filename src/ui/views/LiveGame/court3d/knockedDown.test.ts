import { assert, describe, test } from "vitest";
import { evalPlayer, poseOf, withBody } from "./evaluate.ts";
import { bodyOf, skeleton } from "./poses.ts";
import { compile } from "./testGame.ts";

const body = bodyOf();

describe("3D shooting fouls", () => {
	test("fouled hard going up, he is down on the floor - and stays put till he is up", () => {
		let down = 0;
		for (const seed of ["a", "b", "c", "d", "e", "f"]) {
			const { tl, events } = compile(seed, 140);
			for (const b of tl.beats) {
				const andOne = /AndOne$/.test(b.type);
				if (b.type !== "pfFG" && b.type !== "pfTP" && !andOne) {
					continue;
				}
				const sh = andOne ? events[b.i]?.pid : events[b.i]?.pidShooting;
				const tr = typeof sh === "number" ? tl.tracks.get(sh) : undefined;
				const a = tr?.acts.find(
					(x) =>
						(x.anim === "chargeDown" || x.anim === "takeCharge") &&
						Math.abs(x.t0 - b.actionStart) < 3000,
				);
				if (!tr || !a) {
					continue;
				}
				down += 1;
				const pulled = tr.acts.find(
					(x) => x.anim === "pulledUp" && x.t0 >= a.t0 && x.t0 < a.t0 + 6000,
				);
				const up = pulled?.t1 ?? a.t1;
				// (Whatever he does about it - made it through the contact - he
				// does once he is up.)
				const DOWN = new Set([
					"chargeDown",
					"takeCharge",
					"sitFloor",
					"pulledUp",
				]);
				assert.deepEqual(
					tr.acts
						.filter((x) => !DOWN.has(x.anim) && x.t1 > a.t0 && x.t0 < up)
						.map((x) => x.anim),
					[],
					`${seed} ${tr.pid}`,
				);
				// Sat on the floor a moment...
				const mid = a.anim === "takeCharge" ? a.t0 + (a.t1 - a.t0) * 0.4 : a.t1;
				const st = withBody(evalPlayer(tl, tr.pid, mid), body);
				assert.isBelow(
					skeleton(body, poseOf(st)).pelvis.u,
					1.2,
					`${seed} ${tr.pid}`,
				);
				// ... and nowhere till he is up off it.
				const P = evalPlayer(tl, tr.pid, a.t0 + 100);
				for (let t = a.t0 + 100; t < up - 100; t += 100) {
					const Q = evalPlayer(tl, tr.pid, t);
					assert.isBelow(
						Math.hypot(Q.x - P.x, Q.y - P.y),
						0.6,
						`${seed} ${tr.pid} at ${Math.round(t - a.t0)}`,
					);
				}
			}
		}
		assert.isAtLeast(down, 2);
	}, 600_000);
});
