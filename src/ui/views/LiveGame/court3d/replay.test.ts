import { assert, describe, test } from "vitest";
import { makeCamera, project, REPLAY_RIG } from "./camera.ts";
import { COURT_W } from "./geometry.ts";
import { replayed, replayOf } from "./playback.ts";
import { bodyOf } from "./poses.ts";
import { momentAt, replayAim } from "./scene.ts";
import { compile } from "./testGame.ts";

const body = bodyOf();

describe("3D replays", () => {
	// The slow-motion replay of a big dunk is of him and the rim: the ball
	// in the picture the whole way, the rim near the top of it, and - as he
	// throws it down - all of him, his feet on the floor in the picture, not
	// cut off along its foot under half a picture of the crowd.
	test("a dunk shown again stays on him and the rim, all of him in it", () => {
		let dunks = 0;
		for (const seed of ["b", "d"]) {
			const { tl, players } = compile(seed, 150);
			for (const [w, h] of [
				[1280, 720],
				[800, 600],
			] as const) {
				for (const f of tl.fx) {
					if (!replayed(f) || f.kind !== "dunk") {
						continue;
					}
					dunks += 1;
					const r = replayOf(tl, f);
					assert.isDefined(r.pid, `${seed} @${f.t}`);
					const rimX = r.rim === 0 ? 5.25 : COURT_W - 5.25;
					const inside = (p: { x: number; y: number }, what: string) => {
						assert.isAbove(p.x, 0, what);
						assert.isBelow(p.x, w, what);
						assert.isAbove(p.y, 0, what);
						assert.isBelow(p.y, h, what);
					};
					for (let t = r.from; t <= r.to; t += 50) {
						const m = momentAt(tl, t, players, () => body);
						const cam = makeCamera(replayAim(m, r, w / h), w, h, REPLAY_RIG);
						const at = `${seed} ${w}x${h} @${Math.round(t)}`;
						inside(project(cam, m.ball), `the ball ${at}`);
						const iron = project(cam, { x: rimX, y: 25, z: 10 });
						inside(iron, `the rim ${at}`);
						assert.isBelow(iron.y, h * 0.36, `the rim up top ${at}`);
						const under = project(cam, { x: rimX, y: 25, z: 0 });
						assert.isBelow(under.y, h * 0.97, `the floor under it ${at}`);
					}
					// Throwing it down: all of him.
					const m = momentAt(tl, f.t - 40, players, () => body);
					const him = m.players.find((p) => p.pid === r.pid)!;
					const cam = makeCamera(replayAim(m, r, w / h), w, h, REPLAY_RIG);
					const at = `${seed} ${w}x${h} @${Math.round(f.t)}`;
					inside(project(cam, { ...him, z: 0 }), `his feet ${at}`);
					inside(project(cam, { ...him, z: him.z + 6.5 }), `his head ${at}`);
				}
			}
		}
		assert.isAtLeast(dunks, 8);
	}, 60_000);
});
