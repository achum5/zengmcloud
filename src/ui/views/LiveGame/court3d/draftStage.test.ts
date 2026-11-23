import { assert, describe, test } from "vitest";
import { makeCamera, project } from "./camera.ts";
import type { CrewRole } from "./crew.ts";
import {
	boardCam,
	BROTHER,
	COMMISSIONER,
	DRAFT_CALL,
	FATHER,
	MARK,
	MOTHER,
	PODIUM,
	SEAT_R,
	STAFFER,
	POSES,
	SITS,
	STAGE,
	STEPS,
	TABLE_R,
	TABLES,
	UPS,
	groundAt,
	stageDraftNight,
	takeOf,
	type DraftKin,
	type DraftMood,
	type DraftTake,
} from "./draftStage.ts";
import { bodyPoint, poseOf } from "./evaluate.ts";
import { bodyOf, poseAt, skeleton, type AnimName } from "./poses.ts";

const body = bodyOf();
const bodyFor = () => body;

// Every table, every family, and every way of taking it - in his chair and
// up on his feet - the rest of a take cycled through as they go.
const REACTIONS = (Object.keys(UPS) as DraftMood[]).flatMap((mood) => {
	const sits = SITS[mood];
	const ups = UPS[mood];
	return Array.from({ length: Math.max(sits.length, ups.length) }, (_, i) => ({
		mood,
		sit: sits[i % sits.length]!,
		up: ups[i % ups.length]!,
	}));
});
const PER_TABLE = 5;
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
		for (let k = 0; k < PER_TABLE; k++) {
			for (const [hugger, other] of FAMILIES) {
				const i = TAKES.length;
				TAKES.push({
					side,
					row,
					...REACTIONS[i % REACTIONS.length]!,
					hugger,
					other,
					pose: POSES[i % POSES.length]!,
					pace: 4.7 + (i % 4) * 0.3,
					open: ([-1, 0, 1] as const)[i % 3]!,
					podiumCam: i % 2 === 0 ? 1 : -1,
					stageCam: ([-1, 0, 1] as const)[(i >> 1) % 3]!,
					footCam: (i >> 2) % 2 === 0 ? 1 : -1,
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
const KIN: Record<DraftKin, number | undefined> = {
	mom: MOTHER,
	dad: FATHER,
	brother: BROTHER,
	none: undefined,
};
const name = (take: DraftTake) =>
	`${take.side}/${take.row}/${take.sit}+${take.up}/${take.hugger}+${take.other}`;

describe("a pick made on draft night", () => {
	test("each player's pick is his own, the same every time, and they are not all alike", () => {
		const seen = {
			table: new Set<string>(),
			mood: new Set<string>(),
			sit: new Set<string>(),
			up: new Set<string>(),
			family: new Set<string>(),
			pose: new Set<string>(),
		};
		for (let pid = 0; pid < 400; pid++) {
			const take = takeOf(pid);
			assert.deepStrictEqual(takeOf(pid), take);
			seen.table.add(`${take.side}/${take.row}`);
			seen.mood.add(take.mood);
			seen.sit.add(take.sit);
			seen.up.add(take.up);
			assert.include(SITS[take.mood], take.sit);
			assert.include(UPS[take.mood], take.up);
			seen.family.add(`${take.hugger}+${take.other}`);
			seen.pose.add(take.pose);
		}
		assert.strictEqual(seen.table.size, 6);
		assert.strictEqual(seen.mood.size, Object.keys(UPS).length);
		assert.strictEqual(seen.sit.size, new Set(Object.values(SITS).flat()).size);
		assert.strictEqual(seen.up.size, new Set(Object.values(UPS).flat()).size);
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
				N.hugEnd,
				N.leave,
				N.atFoot,
				N.handed,
				N.capOn,
				N.atSteps,
				N.onStage,
				N.arrive,
				N.meet,
				N.photo,
				N.pose,
				N.off,
				N.offVia,
				N.ms,
			];
			for (let i = 1; i < order.length; i++) {
				assert.isBelow(order[i - 1]!, order[i]!, `${name(take)} ${i}`);
			}
			// The broadcast's graphics go by these.
			assert.strictEqual(N.podium, DRAFT_CALL.podium);
			assert.strictEqual(N.call, DRAFT_CALL.call);
			// Long enough to be the moment the broadcast makes of it, short
			// enough to watch every pick.
			assert.isAbove(N.ms, 24_000, name(take));
			assert.isBelow(N.ms, 40_000, name(take));
			// Each of his family's hugs held a while.
			assert.isAtLeast(N.hugEnd - N.hug, 2500);
			if (take.other !== "none") {
				assert.isAtLeast(N.hug2, N.hugEnd);
				assert.isAtLeast(N.hug2End - N.hug2, 2000);
			}
			// The commissioner is there to meet him before he gets there.
			assert.isBelow(N.commGo, N.arrive);
		}
	});

	test("before his name is read, the cameras are down among the tables, looking up at the board, the wall or across at the stage", () => {
		for (const side of [-1, 0, 1] as const) {
			for (const u of [0, 0.5, 1]) {
				const c = boardCam(side, u, 1);
				assert.isBelow(c.at.z, 5);
				assert.isAbove(c.look.z, c.at.z + 10);
				for (const tb of TABLES) {
					assert.isAbove(
						Math.hypot(c.at.x - tb.x, c.at.y - tb.y),
						SEAT_R + 0.5,
						`${side} ${u}`,
					);
				}
			}
		}
		for (const { take, scene } of scenes) {
			const m = scene.moment(1200, bodyFor);
			const { rig } = scene.aim(1200, false, m);
			assert.isBelow(rig.at!.z, 5, name(take));
		}
	});

	test("he is sat at his family's table till his name is read, and ends up on the stage", () => {
		for (const { take, scene } of scenes) {
			const at = (t: number) => scene.moment(t, bodyFor).players[0]!;
			const sat = at(scene.night.call - 100);
			const table = scene.places.table;
			assert.strictEqual(sat.anim, "tableSit", name(take));
			assert.isBelow(
				Math.hypot(sat.x - table.x, sat.y - table.y),
				TABLE_R + 1.5,
			);
			assert.strictEqual(sat.z, 0);
			// The pictures taken at center stage, and him off at the back of
			// it.
			const posed = at(scene.night.pose + 500);
			assert.closeTo(posed.x, MARK.x, 0.3);
			assert.closeTo(posed.y, MARK.y, 0.3);
			const end = at(scene.ms);
			assert.isBelow(end.y, MARK.y - 3);
			assert.isAbove(end.x, STAGE.x0);
			assert.isBelow(end.x, STAGE.x1);
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
					const apart = Math.hypot(b.x - c.x, b.y - c.y);
					if (apart <= TABLE_R) {
						assert.isAbove(apart, TABLE_R, where);
					}
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
					// (The nearest table: asked once, not of every one.)
					let nearest = Infinity;
					for (const tb of TABLES) {
						nearest = Math.min(nearest, Math.hypot(c.x - tb.x, c.y - tb.y));
					}
					if (nearest <= TABLE_R) {
						assert.isAbove(nearest, TABLE_R, where);
					}
					// (Hugging him, as close as a hug is.)
					const hugging =
						c.pid === COMMISSIONER
							? t >= N.arrive - 300 && t < N.photo + 300
							: c.pid === KIN[take.hugger]
								? t >= N.hug - 300 && t < N.hugEnd + 300
								: c.pid === KIN[take.other]
									? t >= N.hug2 - 300 && t < N.hug2End + 300
									: false;
					const apart = Math.hypot(c.x - him.x, c.y - him.y);
					if (!hugging && apart <= 1.3) {
						assert.isAbove(apart, 1.3, where);
					}
				}
			}
		}
	}, 60_000);

	test("his feet step him up the aisle: the one on the floor stays where it is", () => {
		for (const { take, scene } of scenes) {
			const N = scene.night;
			if (N.atFoot - N.leave < 2500) {
				// (From the front row it is a step or two.)
				continue;
			}
			const STEP = 32;
			let travel = 0;
			let slide = 0;
			let prev:
				| { x: number; y: number; feet: { x: number; y: number }[] }
				| undefined;
			for (let t = N.leave + 700; t < N.atFoot - 500; t += STEP) {
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

	test("the commissioner is at the podium for the call, meets him at center stage and goes back; family hug him at the table", () => {
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
			// And back to the podium for the next, once he is off.
			const back = who(N.ms + 3000, COMMISSIONER);
			assert.closeTo(back.x, reading.x, 0.05);
			assert.closeTo(back.y, reading.y, 0.05);
			// One of his family hugs him at the table, and then whoever else
			// is with them.
			const family = (t: number) =>
				scene.castAt!(t).filter((c) =>
					[MOTHER, FATHER, BROTHER].includes(c.pid),
				);
			assert.lengthOf(family(N.hug), take.other === "none" ? 1 : 2, name(take));
			const hugs: [number, number][] = [[N.hug + 600, KIN[take.hugger]!]];
			if (take.other !== "none") {
				hugs.push([N.hug2 + 600, KIN[take.other]!]);
			}
			for (const [t, pid] of hugs) {
				const himThen = scene.moment(t, bodyFor).players[0]!;
				const them = family(t).find((c) => c.pid === pid)!;
				// (Bent over his mother; chest to chest with his father or his
				// brother.)
				const man = pid !== MOTHER;
				// (Grinning, or not, as it hit him.)
				assert.strictEqual(
					himThen.anim.replace(/Grin$/, ""),
					man ? "embraceMan" : "embrace",
					name(take),
				);
				assert.strictEqual(
					them.anim,
					man ? "embraceManUp" : "embraceUp",
					name(take),
				);
				assert.isBelow(Math.hypot(them.x - himThen.x, them.y - himThen.y), 2);
			}
			// All of them their own people, below any pid of the crew's.
			for (const m of scene.cast!) {
				assert.isBelow(m.pid, -200);
			}
		}
	});

	test("in each hug his arms go right round them, and his head comes down beside theirs - however tall he is", () => {
		// (His family as they are drawn: see kinOf.)
		const theirs: Record<number, ReturnType<typeof bodyOf>> = {
			[MOTHER]: bodyOf(65, 140),
			[FATHER]: bodyOf(74, 215),
			[BROTHER]: bodyOf(75, 190),
		};
		for (const hgt of [73, 78, 85]) {
			const his = bodyOf(hgt, 215);
			for (const { take, scene } of scenes.filter((_, i) => i % 7 === 0)) {
				const N = scene.night;
				const hugs: [number, number][] = [
					[(N.hug + N.hugEnd) / 2, KIN[take.hugger]!],
				];
				if (take.other !== "none") {
					hugs.push([(N.hug2 + N.hug2End) / 2, KIN[take.other]!]);
				}
				for (const [t, pid] of hugs) {
					const where = `${name(take)} ${hgt}in, ${pid} at ${t}`;
					const him = scene.moment(t, () => his).players[0]!;
					const them = scene.castAt!(t).find((c) => c.pid === pid)!;
					const body = theirs[pid]!;
					const me = skeleton(his, poseOf(him));
					const you = skeleton(body, poseOf(them));
					// (Across from him: how far out in front of him, and over to
					// his left.)
					const c = Math.cos(him.yaw);
					const sn = Math.sin(him.yaw);
					const mine = (p: { x: number; y: number }) => ({
						f: (p.x - him.x) * c + (p.y - him.y) * sn,
						s: (p.x - him.x) * sn - (p.y - him.y) * c,
					});
					const center = mine(them);
					for (const arm of [me.armR, me.armL]) {
						const wrist = mine(bodyPoint(him, arm.end));
						const elbow = mine(bodyPoint(him, arm.mid));
						// His wrists on their back, past the middle of them...
						assert.isAbove(wrist.f, center.f, where);
						// ...and his elbows out round their sides, not through them.
						assert.isAbove(
							Math.abs(elbow.s - center.s),
							body.shoulderW * 0.9,
							where,
						);
					}
					const a = bodyPoint(him, me.head);
					const b = bodyPoint(them, you.head);
					assert.isAbove(
						Math.hypot(a.x - b.x, a.y - b.y, a.z - b.z),
						(his.headR + body.headR) * 0.8,
						where,
					);
				}
			}
		}
	});

	test("his cap is handed him at the foot of the steps, and on before he goes up them", () => {
		for (const { take, scene } of scenes) {
			const N = scene.night;
			const t = (N.handFrom + N.handed) / 2;
			const him = scene.moment(t, bodyFor).players[0]!;
			const staff = scene.castAt!(t).find((c) => c.pid === STAFFER)!;
			assert.strictEqual(him.anim, "dap", name(take));
			assert.strictEqual(staff.anim, "dap", name(take));
			assert.isBelow(Math.hypot(staff.x - him.x, staff.y - him.y), 2.6);
			assert.isAbove(him.y, STEPS.y1, name(take));
			const on = scene.moment(N.capOn, bodyFor).players[0]!;
			assert.strictEqual(on.anim, "capOn", name(take));
			assert.isAbove(N.capOn, N.handed);
			assert.isBelow(N.capOn, N.atSteps);
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
					// (His head where his pose has it: sat, it is down at the
					// table.)
					const at = bodyPoint(st, skeleton(body, poseOf(st)).head);
					const head = project(cam, { ...at, z: at.z + 0.4 });
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

describe("how he takes it", () => {
	const dist = (
		a: { f: number; s: number; u: number },
		b: { f: number; s: number; u: number },
	) => Math.hypot(a.f - b.f, a.s - b.s, a.u - b.u);
	const sits = [...new Set(Object.values(SITS).flat())];
	const ups = [...new Set([...Object.values(UPS).flat(), ...POSES])];
	const bodies = [bodyOf(72, 185), bodyOf(78, 215), bodyOf(86, 260)];
	const each = (
		anims: AnimName[],
		check: (anim: AnimName, k: number, b: ReturnType<typeof bodyOf>) => void,
	) => {
		for (const b of bodies) {
			for (const anim of anims) {
				for (let k = 0; k <= 20; k++) {
					check(anim, k, b);
				}
			}
		}
	};

	test("in his chair he stays sat in it; up, he stands up straight in his suit - not down in a stance", () => {
		each(sits, (anim, k, b) => {
			const sk = skeleton(b, poseAt(anim, k / 20));
			const seat = skeleton(b, poseAt("tableSit", 0)).pelvis.u;
			assert.closeTo(sk.pelvis.u, seat, 0.12, `${anim} ${k}`);
		});
		each(ups, (anim, k, b) => {
			const q = poseAt(anim, k / 20);
			const sk = skeleton(b, q);
			const tall = skeleton(b, poseAt("easy", 0)).pelvis.u;
			assert.isAbove(sk.pelvis.u, tall - 0.25, `${anim} ${k}`);
			assert.isAtMost(q.lean, 22, `${anim} ${k}`);
		});
	});

	test("a hand at his face is at it, in front; one on his heart is on his chest; hands together meet", () => {
		let face = 0;
		let heart = 0;
		let clasp = 0;
		each([...sits, ...ups], (anim, k, b) => {
			const q = poseAt(anim, k / 20);
			const sk = skeleton(b, q);
			for (const [arm, on, over] of [
				[sk.armR, q.faceN, q.heartN],
				[sk.armL, q.faceF, q.heartF],
			] as const) {
				if (on >= 0.99) {
					face += 1;
					assert.isBelow(dist(arm.end, sk.head), b.headR * 2.2, `${anim} ${k}`);
					assert.isAbove(arm.tip!.f, sk.head.f, `${anim} ${k}`);
				}
				if (over >= 0.99) {
					heart += 1;
					assert.isBelow(
						dist(arm.end, sk.chest),
						b.torso * 0.65,
						`${anim} ${k}`,
					);
					assert.isAbove(arm.end.f, sk.chest.f, `${anim} ${k}`);
				}
			}
			if (q.clasp >= 0.99 && q.claspApart < 0.01) {
				clasp += 1;
				assert.isBelow(dist(sk.armR.end, sk.armL.end), 0.35, `${anim} ${k}`);
			}
			// (His head never down past his shoulders, nor off his neck.)
			assert.isAbove(sk.head.u, sk.chest.u, `${anim} ${k}`);
		});
		assert.isAbove(face, 0);
		assert.isAbove(heart, 0);
		assert.isAbove(clasp, 0);
	});

	test("there are plenty of ways to take it, and every one of them is used", () => {
		assert.isAtLeast(sits.length, 12);
		assert.isAtLeast(new Set(Object.values(UPS).flat()).size, 15);
		assert.isAtLeast(POSES.length, 8);
	});
});
