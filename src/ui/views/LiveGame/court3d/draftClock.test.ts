import { assert, describe, test } from "vitest";
import { makeCamera, project } from "./camera.ts";
import { CLOCK_LOOP, clockShotAt, stageOnTheClock } from "./draftClock.ts";
import { placesOf, takeOf, TABLE_R, TABLES } from "./draftStage.ts";
import { bodyOf } from "./poses.ts";

const body = bodyOf();
const bodyFor = () => body;

describe("the room between picks", () => {
	test("the best left are each at their own table - the one they get up from when their name is read", () => {
		for (let first = 0; first < 60; first += 3) {
			const pids = [first, first + 1, first + 2];
			const scene = stageOnTheClock(pids.map((pid) => ({ pid })));
			assert.isAtLeast(scene.seated.length, 1);
			assert.isAtMost(scene.seated.length, 2);
			const tables = scene.room.families.map(
				(f) => `${f.table.x},${f.table.y}`,
			);
			assert.strictEqual(new Set(tables).size, tables.length);
			const m = scene.moment(5000, bodyFor);
			scene.seated.forEach((pid, i) => {
				const st = m.players.find((p) => p.pid === pid)!;
				const seat = placesOf(takeOf(pid)).seat;
				assert.strictEqual(st.anim, "chairSit");
				assert.closeTo(st.x, seat.x, 1e-9);
				assert.closeTo(st.y, seat.y, 1e-9);
				assert.strictEqual(
					scene.room.families[i]!.table.x,
					placesOf(takeOf(pid)).table.x,
				);
			});
			// Their families sat round them, nobody in a table.
			for (const c of scene.castAt!(5000)) {
				assert.strictEqual(c.anim, "chairSit");
				for (const tb of TABLES) {
					assert.isAbove(Math.hypot(c.x - tb.x, c.y - tb.y), TABLE_R);
				}
			}
			for (const m2 of scene.cast!) {
				assert.isBelow(m2.pid, -200);
			}
		}
	});

	test("the cameras go round the room, and in on each of them - him in the picture, on a phone and a wide screen", () => {
		const scene = stageOnTheClock([{ pid: 3 }, { pid: 4 }, { pid: 5 }]);
		const n = scene.seated.length;
		const kinds = new Set<string>();
		for (let t = 0; t < CLOCK_LOOP * 2; t += 250) {
			const { shot } = clockShotAt(t, n);
			kinds.add(shot.kind);
			if (shot.kind !== "prospect") {
				continue;
			}
			const pid = scene.seated[shot.i]!;
			for (const [narrow, w, h] of [
				[false, 640, 360],
				[true, 360, 270],
			] as const) {
				const m = scene.moment(t, bodyFor);
				const { shot: aimed, rig } = scene.aim(t, narrow, m);
				const cam = makeCamera(aimed, w, h, rig);
				const st = m.players.find((p) => p.pid === pid)!;
				const head = project(cam, { x: st.x, y: st.y, z: 4.2 });
				const where = `${narrow ? "phone" : "wide"} at ${t}`;
				assert.isAbove(head.x, w * 0.1, where);
				assert.isBelow(head.x, w * 0.9, where);
				assert.isAbove(head.y, 0, where);
				assert.isBelow(head.y, h * 0.74, where);
			}
		}
		assert.deepStrictEqual([...kinds].sort(), ["prospect", "room", "wall"]);
		// Nobody left: the podium instead.
		assert.strictEqual(clockShotAt(14_000, 0).shot.kind, "podium");
	});
});
