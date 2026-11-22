import { assert, describe, test } from "vitest";
import { makeCamera, project } from "./camera.ts";
import type { CrewRole } from "./crew.ts";
import {
	COMMISSIONER,
	DRAFT_NIGHT,
	FATHER,
	HIS_TABLE,
	MARK,
	MOTHER,
	PODIUM,
	STAGE,
	STEPS,
	TABLE_R,
	TABLES,
	draftFlashesAt,
	groundAt,
	stageDraftNight,
} from "./draftStage.ts";
import { bodyPoint, poseOf } from "./evaluate.ts";
import { bodyOf, skeleton } from "./poses.ts";

const PID = 41;
const body = bodyOf();
const bodyFor = () => body;
const N = DRAFT_NIGHT;

describe("a pick made on draft night", () => {
	test("it goes in the order the broadcast has it", () => {
		const order = [
			N.podium,
			N.call,
			N.stand,
			N.stood,
			N.hug,
			N.leave,
			N.atSteps,
			N.onStage,
			N.arrive,
			N.meet,
			N.photo,
			N.wave,
			N.ms,
		];
		for (let i = 1; i < order.length; i++) {
			assert.isBelow(order[i - 1]!, order[i]!, `${i}`);
		}
		// Long enough to be the moment, short enough to watch every pick.
		assert.isAbove(N.ms, 15_000);
		assert.isBelow(N.ms, 25_000);
		// The commissioner is there to meet him before he gets there.
		assert.isBelow(N.commGo, N.arrive);
	});

	test("he is sat at his family's table till his name is read, and ends up on the stage", () => {
		const scene = stageDraftNight(PID);
		const at = (t: number) => scene.moment(t, bodyFor).players[0]!;
		const sat = at(N.call - 100);
		assert.strictEqual(sat.pid, PID);
		assert.strictEqual(sat.anim, "chairSit");
		assert.isBelow(
			Math.hypot(sat.x - HIS_TABLE.x, sat.y - HIS_TABLE.y),
			TABLE_R + 1.5,
		);
		assert.strictEqual(sat.z, 0);
		const end = at(N.ms);
		assert.closeTo(end.x, MARK.x, 0.3);
		assert.closeTo(end.y, MARK.y, 0.3);
		assert.strictEqual(end.z, STAGE.z);
		// (Stood on it, not hanging in the air over it.)
		assert.strictEqual(end.aloft, 0);
	});

	test("he walks it - never quicker than a man walks - and up the steps, not through them", () => {
		const scene = stageDraftNight(PID);
		const at = (t: number) => scene.moment(t, bodyFor).players[0]!;
		for (let t = 100; t <= N.ms; t += 100) {
			const a = at(t - 100);
			const b = at(t);
			const ftps = Math.hypot(b.x - a.x, b.y - a.y) / 0.1;
			assert.isBelow(ftps, 7, `${t}`);
			// His feet where the floor is under him (his step's own little
			// lift off it aside).
			const lift = b.z - groundAt(b.x, b.y);
			assert.isAtLeast(lift, -1e-9, `${t}`);
			assert.isBelow(lift, 0.3, `${t}`);
			// Never through a table.
			for (const c of TABLES) {
				assert.isAbove(Math.hypot(b.x - c.x, b.y - c.y), TABLE_R, `${t}`);
			}
		}
		// On the stage only by its steps: whenever he is over its front edge,
		// he is between their sides.
		for (let t = N.leave; t <= N.arrive; t += 50) {
			const st = at(t);
			if (st.y < STEPS.y1 && st.y > STAGE.y1 - 0.5) {
				assert.isAbove(st.x, STEPS.x0, `${t}`);
				assert.isBelow(st.x, STEPS.x1, `${t}`);
			}
		}
	});

	test("his feet step him up the aisle: the one on the floor stays where it is", () => {
		const scene = stageDraftNight(PID);
		const STEP = 32;
		let travel = 0;
		let slide = 0;
		let prev:
			| { x: number; y: number; feet: { x: number; y: number }[] }
			| undefined;
		for (let t = N.leave + 700; t < N.atSteps - 500; t += STEP) {
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
		assert.isAbove(travel, 4);
		assert.isBelow(slide / travel, 0.12, `slid ${slide} of ${travel}`);
	});

	test("the commissioner is at the podium for the call, and meets him at center stage", () => {
		const scene = stageDraftNight(PID);
		const who = (t: number, pid: number) =>
			scene.castAt!(t).find((s) => s.pid === pid)!;
		const reading = who(N.call, COMMISSIONER);
		assert.closeTo(reading.x, PODIUM.x, 0.5);
		assert.isBelow(reading.y, PODIUM.y);
		assert.strictEqual(reading.z, STAGE.z);
		const meeting = who(N.meet + 400, COMMISSIONER);
		const him = scene.moment(N.meet + 400, bodyFor).players[0]!;
		assert.strictEqual(meeting.anim, "dapHug");
		assert.strictEqual(him.anim, "dapHug");
		assert.isBelow(Math.hypot(meeting.x - him.x, meeting.y - him.y), 2);
		// His mother hugs him at the table; his father is up clapping.
		const mom = who(N.hug + 400, MOTHER);
		const himThen = scene.moment(N.hug + 400, bodyFor).players[0]!;
		assert.strictEqual(mom.anim, "dapHug");
		assert.isBelow(Math.hypot(mom.x - himThen.x, mom.y - himThen.y), 2);
		assert.strictEqual(who(N.hug, FATHER).anim, "clap");
		// All three their own people, below any pid of the crew's.
		for (const m of scene.cast!) {
			assert.isBelow(m.pid, -200);
		}
	});

	test("the cameras have him in the picture from his name on - on a phone and a wide screen", () => {
		const scene = stageDraftNight(PID);
		for (const [narrow, w, h] of [
			[false, 640, 360],
			[true, 360, 270],
		] as const) {
			for (let t = N.call; t <= N.ms; t += 150) {
				// (Just as the cut comes, a frame or so either side.)
				const m = scene.moment(t, bodyFor);
				const { shot, rig } = scene.aim(t, narrow, m);
				const cam = makeCamera(shot, w, h, rig);
				const st = m.players[0]!;
				const head = project(cam, { x: st.x, y: st.y, z: st.z + body.H });
				const where = `${narrow ? "phone" : "wide"} at ${t}`;
				assert.isAbove(head.x, w * 0.1, where);
				assert.isBelow(head.x, w * 0.9, where);
				// His head clear of the bar along the bottom (a quarter of it).
				assert.isAbove(head.y, 0, where);
				assert.isBelow(head.y, h * 0.74, where);
			}
		}
	});

	test("nobody working a game is on the floor, and there is no ball to be seen", () => {
		const scene = stageDraftNight(PID);
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
			[],
		);
		for (let t = 0; t <= scene.ms; t += 500) {
			const m = scene.moment(t, bodyFor);
			assert.lengthOf(m.players, 1);
			const { shot, rig } = scene.aim(t, false, m);
			const cam = makeCamera(shot, 640, 360, rig);
			const ball = project(cam, m.ball);
			assert.isTrue(
				ball.x < -100 || ball.x > 740 || ball.y < -100 || ball.y > 460,
				`ball in the picture at ${t}`,
			);
		}
	});

	test("cameras go off round his table as he gets up, and all along the front of the stage once he is on it", () => {
		const count = (from: number, to: number) => {
			let n = 0;
			for (let t = from; t < to; t += 40) {
				n += draftFlashesAt(t).length;
			}
			return n;
		};
		assert.strictEqual(count(0, N.call), 0);
		assert.isAbove(count(N.call + 300, N.leave), 0);
		assert.strictEqual(count(N.leave + 900, N.meet), 0);
		assert.isAbove(count(N.meet, N.ms - 400), count(N.call + 300, N.leave));
	});
});
