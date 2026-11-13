import { assert, describe, test } from "vitest";
import { makeCamera, project } from "./camera.ts";
import type { CrewRole } from "./crew.ts";
import { DRAFT_WALK, stageDraftWalk } from "./draftWalk.ts";
import { bodyPoint, poseOf } from "./evaluate.ts";
import { COURT_H, COURT_W } from "./geometry.ts";
import { callAt, darkAt } from "./intro.ts";
import { bodyOf, skeleton } from "./poses.ts";

const PID = 41;
const body = bodyOf();
const bodyFor = () => body;

describe("a draft pick's walk-out", () => {
	test("it is a scene, not a wait: nine or ten seconds, his whole walk in it", () => {
		const scene = stageDraftWalk(PID);
		assert.isAbove(scene.ms, 8000);
		assert.isBelow(scene.ms, 11_000);
		assert.strictEqual(scene.ms, DRAFT_WALK.ms);
		assert.isBelow(DRAFT_WALK.arrive, DRAFT_WALK.wave);
		assert.isBelow(DRAFT_WALK.wave, DRAFT_WALK.lightsUp);
		assert.isBelow(DRAFT_WALK.lightsUp, DRAFT_WALK.ms);
	});

	test("he comes out from the far sideline at a walk, to the logo at center court, and turns to the camera", () => {
		const scene = stageDraftWalk(PID);
		const at = (t: number) => scene.moment(t, bodyFor).players[0]!;
		const start = at(0);
		assert.strictEqual(start.pid, PID);
		// In front of his new team's bench: off the floor, past the far
		// sideline, on the home side.
		assert.isBelow(start.y, 0);
		assert.isAbove(start.x, COURT_W / 2);
		// Never quicker than a man walks.
		for (let t = 100; t < scene.ms; t += 100) {
			const a = at(t - 100);
			const b = at(t);
			const ftps = Math.hypot(b.x - a.x, b.y - a.y) / 0.1;
			assert.isBelow(ftps, 7.5, `${t}`);
			if (t > DRAFT_WALK.arrive + 100) {
				assert.isBelow(ftps, 0.2, `still at ${t}`);
			}
		}
		const end = at(scene.ms);
		assert.closeTo(end.x, COURT_W / 2, 1.5);
		assert.closeTo(end.y, COURT_H / 2, 1.5);
		// Turned to the near side, where the camera is: his face, not the back
		// of his head.
		const cam = scene.aim(scene.ms, false, scene.moment(scene.ms, bodyFor)).rig
			.at!;
		const toCam = Math.atan2(cam.y - end.y, cam.x - end.x);
		const off = Math.abs(
			((end.yaw - toCam + Math.PI * 3) % (Math.PI * 2)) - Math.PI,
		);
		assert.isBelow(off, 0.35);
		assert.strictEqual(end.anim, "easy");
	});

	test("his feet step him there: the one on the floor stays where it is", () => {
		const scene = stageDraftWalk(PID);
		const STEP = 32;
		let travel = 0;
		let slide = 0;
		let prev:
			| { x: number; y: number; feet: { x: number; y: number }[] }
			| undefined;
		// (Under way: not setting off, nor pulling up.)
		for (let t = 1500; t < DRAFT_WALK.arrive - 600; t += STEP) {
			const st = scene.moment(t, bodyFor).players[0]!;
			const sk = skeleton(body, poseOf(st));
			const feet = [bodyPoint(st, sk.legR.end), bodyPoint(st, sk.legL.end)];
			if (prev) {
				travel += Math.hypot(st.x - prev.x, st.y - prev.y);
				slide += Math.min(
					...feet.map((f, i) =>
						Math.hypot(f.x - prev!.feet[i]!.x, f.y - prev!.feet[i]!.y),
					),
				);
			}
			prev = { x: st.x, y: st.y, feet };
		}
		assert.isAbove(travel, 10);
		assert.isBelow(slide / travel, 0.12, `slid ${slide} of ${travel}`);
	});

	test("the lights are down on him as he comes, and back up by the end - his new crowd on its feet for him", () => {
		const { tl } = stageDraftWalk(PID).show;
		assert.strictEqual(darkAt(tl.intro, 0), 0);
		for (let t = 1500; t < DRAFT_WALK.lightsUp; t += 250) {
			assert.isAbove(darkAt(tl.intro, t), 0.98, `${t}`);
			// (Called: the spotlight is his, and the home crowd is up - see
			// lights.ts and crowdAt.)
			const call = callAt(tl.intro, t);
			assert.strictEqual(call?.pid, PID);
			assert.strictEqual(call?.team, 1);
		}
		assert.strictEqual(darkAt(tl.intro, DRAFT_WALK.ms), 0);
	});

	test("the camera has him head to foot in the picture the whole way, on a phone and a wide screen", () => {
		const scene = stageDraftWalk(PID);
		for (const [narrow, w, h] of [
			[false, 640, 360],
			[true, 360, 270],
		] as const) {
			for (let t = 0; t <= scene.ms; t += 200) {
				const m = scene.moment(t, bodyFor);
				const { shot, rig } = scene.aim(t, narrow, m);
				const cam = makeCamera(shot, w, h, rig);
				const st = m.players[0]!;
				const feet = project(cam, { x: st.x, y: st.y, z: 0 });
				// (His cap on his head, and his hand up waving.)
				const top = project(cam, { x: st.x, y: st.y, z: body.H + 1.6 });
				const where = `${narrow ? "phone" : "wide"} at ${t}`;
				assert.isAbove(top.y, 0, where);
				assert.isBelow(feet.y, h, where);
				assert.isAbove(feet.x, w * 0.35, where);
				assert.isBelow(feet.x, w * 0.8, where);
				// And big enough to see: a quarter of the picture's height at the
				// least, most of it once he is on his mark.
				const tall = (feet.y - top.y) / h;
				assert.isAbove(tall, 0.25, where);
				if (t > DRAFT_WALK.arrive + 1200) {
					assert.isAbove(tall, 0.55, where);
				}
			}
		}
	});

	test("nobody else has any business on the floor, and there is no ball to be seen", () => {
		const scene = stageDraftWalk(PID);
		const roles: CrewRole[] = [
			"ref",
			"coach",
			"photo",
			"table",
			"media",
			"mop",
			"staff",
			"dance",
			"trainer",
		];
		assert.deepStrictEqual(
			roles.filter((r) => scene.crew!(r)),
			["photo", "table", "staff"],
		);
		for (let t = 0; t <= scene.ms; t += 500) {
			const m = scene.moment(t, bodyFor);
			assert.lengthOf(m.players, 1);
			assert.isUndefined(m.warm);
			const { shot, rig } = scene.aim(t, false, m);
			const cam = makeCamera(shot, 640, 360, rig);
			const ball = project(cam, m.ball);
			assert.isTrue(
				ball.x < -100 || ball.x > 740 || ball.y < -100 || ball.y > 460,
				`ball in the picture at ${t}`,
			);
		}
	});

	test("cameras go off in the stands across from him - more once he is on his mark, none once the lights are up", () => {
		const scene = stageDraftWalk(PID);
		const count = (from: number, to: number) => {
			let n = 0;
			for (let t = from; t < to; t += 40) {
				for (const f of scene.flashes!(t)) {
					n += 1;
					// Up in the far stands.
					assert.isBelow(f.y, -12);
					assert.isAbove(f.z, 3.6);
				}
			}
			return n / (to - from);
		};
		const coming = count(1000, DRAFT_WALK.arrive);
		const there = count(DRAFT_WALK.arrive, DRAFT_WALK.lightsUp);
		assert.isAbove(coming, 0);
		assert.isAbove(there, coming * 2);
		assert.strictEqual(count(DRAFT_WALK.lightsUp, DRAFT_WALK.ms), 0);
	});
});
