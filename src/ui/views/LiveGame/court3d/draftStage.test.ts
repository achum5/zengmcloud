import { assert, describe, test } from "vitest";
import { makeCamera, project } from "./camera.ts";
import type { CrewRole } from "./crew.ts";
import {
	COMMISSIONER,
	DRAFT_CALL,
	MARK,
	PODIUM,
	POSES,
	STAGE,
	STEPS,
	TABLE_R,
	TABLES,
	groundAt,
	stageDraftNight,
	takeOf,
	type DraftKin,
	type DraftReact,
	type DraftTake,
} from "./draftStage.ts";
import { HUG_MS } from "./poses.ts";
import { bodyPoint, poseOf } from "./evaluate.ts";
import { bodyOf, skeleton } from "./poses.ts";

const body = bodyOf();
const bodyFor = () => body;

// Every table, every way of taking it, every family - the rest of a take
// cycled through as they go.
const REACTS: DraftReact[] = ["up", "beat", "hands", "sky", "pump"];
const FAMILIES: ["mom" | "dad", DraftKin][] = [
	["mom", "dad"],
	["mom", "brother"],
	["mom", "none"],
	["dad", "mom"],
	["dad", "none"],
];
const TAKES: DraftTake[] = [];
for (const side of [1, -1] as const) {
	for (const row of [-1, 0, 1] as const) {
		for (const react of REACTS) {
			for (const [hugger, other] of FAMILIES) {
				const i = TAKES.length;
				TAKES.push({
					side,
					row,
					react,
					hugger,
					other,
					pose: POSES[i % POSES.length]!,
					pace: 4.7 + (i % 4) * 0.3,
					open: ([-1, 0, 1] as const)[i % 3]!,
					podiumCam: i % 2 === 0 ? 1 : -1,
					stageCam: ([-1, 0, 1] as const)[(i >> 1) % 3]!,
				});
			}
		}
	}
}
const PID = 41;
const scenes = TAKES.map((take, i) => ({
	i,
	take,
	scene: stageDraftNight(PID + i, undefined, take),
}));
const name = (take: DraftTake) =>
	`${take.side}/${take.row}/${take.react}/${take.hugger}+${take.other}`;

describe("a pick made on draft night", () => {
	test("each player's pick is his own, the same every time, and they are not all alike", () => {
		const seen = {
			table: new Set<string>(),
			react: new Set<string>(),
			family: new Set<string>(),
			pose: new Set<string>(),
		};
		for (let pid = 0; pid < 400; pid++) {
			const take = takeOf(pid);
			assert.deepStrictEqual(takeOf(pid), take);
			seen.table.add(`${take.side}/${take.row}`);
			seen.react.add(take.react);
			seen.family.add(`${take.hugger}+${take.other}`);
			seen.pose.add(take.pose);
		}
		assert.strictEqual(seen.table.size, 6);
		assert.strictEqual(seen.react.size, 5);
		assert.strictEqual(seen.family.size, 5);
		assert.strictEqual(seen.pose.size, POSES.length);
	});

	test("it goes in the order the broadcast has it", () => {
		for (const { take, scene } of scenes) {
			const N = scene.night;
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
				N.pose,
				N.ms,
			];
			for (let i = 1; i < order.length; i++) {
				assert.isBelow(order[i - 1]!, order[i]!, `${name(take)} ${i}`);
			}
			// The broadcast's graphics go by these.
			assert.strictEqual(N.podium, DRAFT_CALL.podium);
			assert.strictEqual(N.call, DRAFT_CALL.call);
			// Long enough to be the moment, short enough to watch every pick.
			assert.isAbove(N.ms, 15_000, name(take));
			assert.isBelow(N.ms, 26_000, name(take));
			// The commissioner is there to meet him before he gets there.
			assert.isBelow(N.commGo, N.arrive);
		}
	});

	test("he is sat at his family's table till his name is read, and ends up on the stage", () => {
		for (const { take, scene } of scenes) {
			const at = (t: number) => scene.moment(t, bodyFor).players[0]!;
			const sat = at(scene.night.call - 100);
			const table = scene.places.table;
			assert.strictEqual(sat.anim, "chairSit", name(take));
			assert.isBelow(
				Math.hypot(sat.x - table.x, sat.y - table.y),
				TABLE_R + 1.5,
			);
			assert.strictEqual(sat.z, 0);
			const end = at(scene.ms);
			assert.closeTo(end.x, MARK.x, 0.3);
			assert.closeTo(end.y, MARK.y, 0.3);
			assert.strictEqual(end.z, STAGE.z);
			// (Stood on it, not hanging in the air over it.)
			assert.strictEqual(end.aloft, 0);
		}
	});

	test("he walks it - never quicker than a man walks - and up the steps, not through them or a table", () => {
		for (const { take, scene } of scenes) {
			const N = scene.night;
			const at = (t: number) => scene.moment(t, bodyFor).players[0]!;
			for (let t = 100; t <= scene.ms; t += 100) {
				const a = at(t - 100);
				const b = at(t);
				const ftps = Math.hypot(b.x - a.x, b.y - a.y) / 0.1;
				const where = `${name(take)} at ${t}`;
				assert.isBelow(ftps, 7, where);
				// His feet where the floor is under him (his step's own little
				// lift off it aside).
				const lift = b.z - groundAt(b.x, b.y);
				assert.isAtLeast(lift, -1e-9, where);
				assert.isBelow(lift, 0.3, where);
				for (const c of TABLES) {
					assert.isAbove(Math.hypot(b.x - c.x, b.y - c.y), TABLE_R, where);
				}
			}
			// On the stage only by its steps: whenever he is over its front
			// edge, he is between their sides.
			for (let t = N.leave; t <= N.arrive; t += 50) {
				const st = at(t);
				if (st.y < STEPS.y1 && st.y > STAGE.y1 - 0.5) {
					assert.isAbove(st.x, STEPS.x0, name(take));
					assert.isBelow(st.x, STEPS.x1, name(take));
				}
			}
		}
	}, 60_000);

	test("nobody walks through him, or through a table", () => {
		for (const { take, scene } of scenes) {
			const N = scene.night;
			for (let t = 0; t <= scene.ms; t += 100) {
				const him = scene.moment(t, bodyFor).players[0]!;
				for (const c of scene.castAt!(t)) {
					const where = `${name(take)}: ${c.pid} at ${t}`;
					for (const tb of TABLES) {
						assert.isAbove(Math.hypot(c.x - tb.x, c.y - tb.y), TABLE_R, where);
					}
					// (Hugging him, as close as a hug is.)
					const hugging =
						c.pid === COMMISSIONER
							? t >= N.arrive - 300 && t < N.photo + 300
							: t >= N.hug - 300 && t < N.hug + HUG_MS + 300;
					if (!hugging) {
						assert.isAbove(Math.hypot(c.x - him.x, c.y - him.y), 1.3, where);
					}
				}
			}
		}
	}, 60_000);

	test("his feet step him up the aisle: the one on the floor stays where it is", () => {
		for (const { take, scene } of scenes) {
			const N = scene.night;
			if (N.atSteps - N.leave < 2500) {
				// (From the front row it is a step or two.)
				continue;
			}
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
			assert.isAbove(travel, 4, name(take));
			assert.isBelow(
				slide / travel,
				0.12,
				`${name(take)} slid ${slide} of ${travel}`,
			);
		}
	}, 60_000);

	test("the commissioner is at the podium for the call, and meets him at center stage; family hug him at the table", () => {
		for (const { take, scene } of scenes) {
			const N = scene.night;
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
			// One of his family hugs him at the table; whoever else is with
			// them is up on their feet.
			const family = scene.castAt!(N.hug + 400).filter(
				(c) => c.pid !== COMMISSIONER,
			);
			assert.lengthOf(family, take.other === "none" ? 1 : 2, name(take));
			const himThen = scene.moment(N.hug + 400, bodyFor).players[0]!;
			assert.strictEqual(himThen.anim, "dapHug", name(take));
			assert.strictEqual(family[0]!.anim, "dapHug", name(take));
			assert.isBelow(
				Math.hypot(family[0]!.x - himThen.x, family[0]!.y - himThen.y),
				2,
			);
			if (family[1]) {
				assert.include(["clap", "cheer"], family[1].anim, name(take));
			}
			// All of them their own people, below any pid of the crew's.
			for (const m of scene.cast!) {
				assert.isBelow(m.pid, -200);
			}
		}
	});

	test("the cameras have him in the picture from his name on - on a phone and a wide screen", () => {
		for (const { take, scene } of scenes) {
			for (const [narrow, w, h] of [
				[false, 640, 360],
				[true, 360, 270],
			] as const) {
				for (let t = scene.night.call; t <= scene.ms; t += 150) {
					const m = scene.moment(t, bodyFor);
					const { shot, rig } = scene.aim(t, narrow, m);
					const cam = makeCamera(shot, w, h, rig);
					const st = m.players[0]!;
					const head = project(cam, { x: st.x, y: st.y, z: st.z + body.H });
					const where = `${name(take)} ${narrow ? "phone" : "wide"} at ${t}`;
					assert.isAbove(head.x, w * 0.1, where);
					assert.isBelow(head.x, w * 0.9, where);
					// His head clear of the bar along the bottom (a quarter of it).
					assert.isAbove(head.y, 0, where);
					assert.isBelow(head.y, h * 0.74, where);
				}
			}
		}
	}, 60_000);

	test("nobody working a game is on the floor, and there is no ball to be seen", () => {
		const { scene } = scenes[0]!;
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
		for (const { take, scene } of scenes) {
			const N = scene.night;
			const count = (from: number, to: number) => {
				let n = 0;
				for (let t = from; t < to; t += 40) {
					n += scene.flashes!(t).length;
				}
				return n;
			};
			assert.strictEqual(count(0, N.call), 0);
			assert.isAbove(count(N.call + 300, N.leave), 0, name(take));
			assert.strictEqual(count(N.leave + 900, N.meet), 0);
			assert.isAbove(count(N.meet, N.ms - 400), 0);
		}
	});
});
