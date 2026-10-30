import { assert, describe, test } from "vitest";
import { crewAt, crewFor } from "./crew.ts";
import { evalPlayer } from "./evaluate.ts";
import { huddleCoach } from "./geometry.ts";
import { compile } from "./testGame.ts";

// A break, the way the NBA has it: the five on the floor sat on chairs set
// out in front of their bench, the coach down on a knee in front of them -
// up off them as it ends, and the chairs cleared away.

describe("the huddle at a break", () => {
	test("the five sit on their chairs, the coach down in front of them", () => {
		const { tl } = compile("huddle", 140);
		const huddles = tl.huddles ?? [];
		assert.isAtLeast(huddles.length, 4);
		const crew = crewFor(1, undefined, undefined);
		let looked = 0;
		for (const h of huddles) {
			assert.strictEqual(h.seats.length, 5);
			const seated = new Set(h.seats.map((s) => s.pid));
			// The look at them over the break: all five sat in their own
			// chairs till it is nearly over - then up, all together.
			const shot = tl.shots.find((s) => s.t0 > h.t0 && s.t0 < h.t1);
			assert.isDefined(shot);
			for (let t = shot!.t0; t < shot!.t1 - 700; t += 100) {
				looked += 1;
				for (const { pid, at } of h.seats) {
					const st = evalPlayer(tl, pid, t);
					assert.strictEqual(st.anim, "chairSit", `${pid} at ${t}`);
					assert.isBelow(Math.hypot(st.x - at.x, st.y - at.y), 0.05);
				}
				const coach = crewAt(tl, t, crew).states.find(
					(s) => s.pid === (h.team === 0 ? -11 : -12),
				)!;
				const C = huddleCoach(h.team);
				assert.strictEqual(coach.anim, "coachKneel", `${t}`);
				assert.isBelow(Math.hypot(coach.x - C.x, coach.y - C.y), 0.05);
			}
			for (const { pid } of h.seats) {
				assert.strictEqual(evalPlayer(tl, pid, shot!.t1 - 300).anim, "chairUp");
			}
			// Nobody else in a chair, nor through them, while they are out.
			for (let t = h.t0; t < h.t1; t += 100) {
				for (const tr of tl.tracks.values()) {
					if (seated.has(tr.pid)) {
						continue;
					}
					const st = evalPlayer(tl, tr.pid, t);
					if (!st.shown) {
						continue;
					}
					for (const { at } of h.seats) {
						assert.isAbove(
							Math.hypot(st.x - at.x, st.y - at.y),
							1.2,
							`${tr.pid} at ${t}`,
						);
					}
				}
			}
		}
		assert.isAbove(looked, 40);
	}, 240_000);
});
