import { assert, describe, test } from "vitest";
import { bowlMatrix, bowlProject } from "./bowlData.ts";
import { MAIN_RIG, makeCamera, project, REPLAY_RIG } from "./camera.ts";

describe("the bowl's camera", () => {
	test("puts the floor where the game does, upright or not", () => {
		for (const rig of [MAIN_RIG, REPLAY_RIG]) {
			for (const [w, h] of [
				[536, 302],
				[561, 422],
			] as const) {
				const cam = makeCamera({ x: 30, width: 70, y: 22 }, w, h, rig);
				const m = bowlMatrix(cam);
				const flat = bowlMatrix({ ...cam, upright: false });
				for (const p of [
					{ x: -13, y: -12, z: 0 },
					{ x: 107, y: 62, z: 0 },
					{ x: 47, y: 25, z: 0 },
					{ x: -13, y: 40, z: 0 },
				]) {
					const want = project(cam, p);
					for (const mm of [m, flat]) {
						const got = bowlProject(mm, cam, p);
						assert.closeTo(got.x, want.x, 1e-3);
						assert.closeTo(got.y, want.y, 1e-3);
						assert.closeTo(got.depth, want.depth, 1e-3);
					}
				}
				// (Up off the floor, as the camera has it.)
				const up = { x: 20, y: -30, z: 25 };
				const got = bowlProject(m, cam, up);
				assert.closeTo(got.x, project(cam, up).x, 1e-3);
				assert.closeTo(got.y, project(cam, up).y, 1e-3);
			}
		}
	});
});
