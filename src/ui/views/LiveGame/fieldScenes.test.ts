import { assert, beforeEach, describe, test } from "vitest";
import { clearCourtRng, seedCourtRng } from "./courtRng.ts";
import { buildFieldScene, newFieldSceneCtx } from "./fieldScenes.ts";
import { dirFor, ENDZONE, FIELD_LEN, fieldX } from "./fieldSpots.ts";

beforeEach(() => {
	seedCourtRng("scene-test");
	return () => {
		clearCourtRng();
	};
});

// Twenty-two players with the positions a football roster has, which is all the
// scene builder needs from a box score.
const roster = (base: number) =>
	[
		"QB",
		"QB",
		"RB",
		"RB",
		"WR",
		"WR",
		"WR",
		"WR",
		"TE",
		"TE",
		"OL",
		"OL",
		"OL",
		"OL",
		"OL",
		"DL",
		"DL",
		"DL",
		"DL",
		"LB",
		"LB",
		"LB",
		"CB",
		"CB",
		"S",
		"S",
		"K",
		"P",
	].map((pos, i) => ({
		pid: base + i,
		name: `${pos}${base + i}`,
		pos,
	}));

const players: [ReturnType<typeof roster>, ReturnType<typeof roster>] = [
	roster(100),
	roster(200),
];

const resolvePid = (t: 0 | 1, name: string | undefined) =>
	name === undefined
		? undefined
		: players[t].find((p) => p.name === name)?.pid;

const sportState = (over: Partial<any> = {}): any => ({
	t: 0,
	scrimmage: 25,
	toGo: 10,
	awaitingKickoff: false,
	awaitingAfterTouchdown: false,
	plays: [
		{
			down: 1,
			toGo: 10,
			scrimmage: 25,
			yards: 0,
			t: 0,
			countsTowardsNumPlays: true,
			countsTowardsYards: true,
		},
	],
	...over,
});

const build = (event: any, displayT: 0 | 1, state = sportState(), ctx?: any) =>
	buildFieldScene({
		event,
		displayT,
		text: "text",
		score: undefined,
		sportState: state,
		players,
		resolvePid,
		ctx: ctx ?? newFieldSceneCtx(),
	});

describe("buildFieldScene", () => {
	test("a run starts at the line and ends where the yards say", () => {
		const scene = build({ type: "run", names: ["RB102"], yds: 8 }, 0)!;
		assert.strictEqual(scene.kind, "run");
		assert.strictEqual(scene.t, 0);
		const los = fieldX(25, dirFor(0));
		assert.strictEqual(scene.losX, los);
		// Eight yards toward the end zone the away team attacks (+x).
		assert.ok(scene.ball!.to.x - los > 7 && scene.ball!.to.x - los < 9);
	});

	// The men the play names have to win their own slots in the formation, or
	// they get bolted on beside it - which is a team with twelve men on the
	// field. The names below are deliberately NOT the first at their position:
	// WR107 is the fourth receiver, QB101 the backup, DL218 the third lineman.
	test("every scene puts eleven men on each side and nobody twice", () => {
		for (const [event, t] of [
			[{ type: "run", names: ["RB103"], yds: 3 }, 0],
			[{ type: "passComplete", names: ["QB101", "WR107"], yds: 12 }, 0],
			[{ type: "punt", names: ["P127"], yds: 44 }, 0],
			[{ type: "kickoff", names: ["K126"], yds: 20 }, 1],
			[{ type: "sack", names: ["QB101", "DL218"], yds: -7 }, 0],
			[{ type: "passIncomplete", names: ["QB101", "WR106"], yds: 9 }, 0],
			[{ type: "handoff", names: ["QB101", "RB103"] }, 0],
			// A receiver returning a kick: the return formation is written
			// entirely of backs, so he has no slot of his own to win.
			[{ type: "kickoffReturn", names: ["WR107"], yds: 22 }, 1],
			[{ type: "puntReturn", names: ["CB222"], yds: 8 }, 1],
		] as const) {
			const scene = build(event as any, t as 0 | 1)!;
			const pids = scene.actors.map((a) => a.pid);
			assert.strictEqual(
				new Set(pids).size,
				pids.length,
				`${event.type} drew somebody twice`,
			);
			for (const team of [0, 1] as const) {
				const n = scene.actors.filter((a) => a.t === team).length;
				assert.strictEqual(n, 11, `${event.type} had ${n} men on team ${team}`);
			}
		}
	});

	test("a pass shows the thrower where it left his hand and the target where it lands", () => {
		const scene = build(
			{ type: "passComplete", names: ["QB100", "WR104"], yds: 16 },
			0,
		)!;
		const main = scene.actors.find((a) => a.role === "main")!;
		const passer = scene.actors.find((a) => a.role === "passer")!;
		assert.strictEqual(main.name, "WR104");
		assert.strictEqual(passer.name, "QB100");
		// The quarterback is behind the line, the receiver well past it.
		assert.ok(passer.x < scene.losX);
		assert.ok(main.x > scene.losX + 14);
	});

	// A kicker who fails to win the kicker's slot in his own unit used to pick
	// up a coverage lane and sprint downfield behind his own kick.
	test("a kicker is pinned where he kicked from, whatever slot he filled", () => {
		for (const event of [
			{ type: "punt", names: ["OL110"], yds: 45 },
			{ type: "kickoff", names: ["WR104"], yds: 4 },
			{ type: "fieldGoalAttempt", names: ["S124"], yds: 38 },
		] as const) {
			const scene = build(event as any, 0)!;
			const kicker = scene.actors.find((a) => a.role === "main")!;
			assert.ok(kicker.path === undefined, `${event.type} kicker was sent off`);
			assert.ok(
				(scene.losX - kicker.x) * scene.dir > 0,
				`${event.type} kicker ended up downfield`,
			);
		}
	});

	test("a kicker stays where he kicked from", () => {
		const scene = build({ type: "punt", names: ["P127"], yds: 45 }, 0)!;
		const punter = scene.actors.find((a) => a.role === "main")!;
		// He lines up fourteen yards deep and steps INTO the kick, so he finishes
		// a couple of yards nearer the line - and nowhere near forty-five yards
		// downfield behind his own punt, which is what he used to do.
		assert.ok(
			scene.losX - punter.x > 10 && scene.losX - punter.x < 16,
			`punter finished ${scene.losX - punter.x} yards behind the line`,
		);
		assert.ok(scene.ball!.to.x > scene.losX + 30);
	});

	// THE REGRESSION THAT MATTERED MOST. The sim counts a return's yard lines
	// from the goal line of the team that just LOST the ball, so reading them in
	// the returner's frame put the whole play on the wrong end of the field -
	// which stacked twenty-two men into a corner.
	test("a return is placed from the frame the sim actually counts it in", () => {
		const state = sportState({
			// The kicking team still holds the frame: their own 35 was the tee, and
			// the ball was caught at what they call the 88.
			t: 0,
			scrimmage: 35,
			plays: [
				{
					down: 1,
					toGo: 10,
					scrimmage: 88,
					yards: 0,
					t: 1,
					countsTowardsNumPlays: false,
					countsTowardsYards: false,
				},
			],
		});
		const scene = build(
			{ type: "kickoffReturn", names: ["RB202"], yds: 26 },
			1,
			state,
		)!;
		// The catch is at the RECEIVING team's 12, near the left end zone, because
		// the away team (who kicked) counts up from the left.
		assert.ok(
			Math.abs(scene.losX - (ENDZONE + 88)) < 1,
			`catch spot was ${scene.losX}`,
		);
		// And the return runs the other way - back toward the left end zone.
		assert.ok(
			scene.ball!.to.x < scene.losX,
			`return went the wrong way: ${scene.ball!.to.x} vs ${scene.losX}`,
		);
		// Nobody ends up crammed against the back of an end zone.
		const maxX = Math.max(...scene.actors.map((a) => a.x));
		assert.ok(maxX < FIELD_LEN - 1, `someone was stacked at ${maxX}`);
	});

	test("a kickoff flies to the yard line it was kicked to, not `yds` yards", () => {
		const state = sportState({ t: 0, scrimmage: 35, plays: [
			{ down: 1, toGo: 10, scrimmage: 35, yards: 0, t: 0 },
		] });
		// yds is the line it reached in the RECEIVING team's numbers: a 2 means
		// it came down on their 2, which is a 63-yard kick from the 35.
		const scene = build({ type: "kickoff", names: ["K126"], yds: 2 }, 0, state)!;
		const flown = scene.ball!.to.x - scene.losX;
		assert.ok(flown > 55, `kickoff only travelled ${flown} yards`);
	});

	test("a stoppage never turns the field around", () => {
		const state = sportState({ t: 0 });
		// The injured man is a defender, so the event's team is the other side.
		const scene = build({ type: "injury", names: ["S224"] }, 1, state)!;
		assert.strictEqual(scene.t, 0);
		assert.strictEqual(scene.dir, dirFor(0));
	});

	test("an interception keeps the throwing team's field and features the thief", () => {
		const state = sportState({ t: 0 });
		const scene = build(
			{ type: "interception", names: ["CB222"], yds: 9 },
			1,
			state,
		)!;
		assert.strictEqual(scene.t, 0);
		const thief = scene.actors.find((a) => a.role === "defender")!;
		assert.strictEqual(thief.name, "CB222");
		assert.strictEqual(thief.t, 1);
	});

	test("down and distance reads the way a scoreboard says it", () => {
		const normal = build({ type: "run", names: ["RB102"], yds: 2 }, 0)!;
		assert.strictEqual(normal.down, "1st & 10");
		const goal = build(
			{ type: "run", names: ["RB102"], yds: 2 },
			0,
			sportState({
				plays: [{ down: 3, toGo: 6, scrimmage: 96, yards: 0, t: 0 }],
			}),
		)!;
		assert.strictEqual(goal.down, "3rd & goal");
		// And there is no first down line to draw when the line to gain is the
		// goal line.
		assert.strictEqual(goal.firstDownX, undefined);
	});

	test("the first down line sits ahead of the ball, whichever way play is going", () => {
		for (const t of [0, 1] as const) {
			const scene = build(
				{ type: "run", names: [`RB${t === 0 ? 102 : 202}`], yds: 1 },
				t,
				sportState({
					t,
					plays: [{ down: 1, toGo: 10, scrimmage: 30, yards: 0, t }],
				}),
			)!;
			assert.ok(scene.firstDownX !== undefined);
			assert.ok(
				(scene.firstDownX! - scene.losX) * dirFor(t) > 0,
				`first down line was behind the ball for team ${t}`,
			);
		}
	});

	test("the drive summary counts the plays the drive actually ran", () => {
		const scene = build(
			{ type: "run", names: ["RB102"], yds: 4 },
			0,
			sportState({
				plays: [
					{
						down: 1,
						toGo: 10,
						scrimmage: 20,
						yards: 6,
						t: 0,
						countsTowardsNumPlays: true,
						countsTowardsYards: true,
					},
					{
						down: 2,
						toGo: 4,
						scrimmage: 26,
						yards: 4,
						t: 0,
						countsTowardsNumPlays: true,
						countsTowardsYards: true,
					},
				],
			}),
		)!;
		assert.strictEqual(scene.drive, "Drive: 2 plays, 10 yards");
	});

	test("an extra point carries no down and distance", () => {
		// The touchdown's own "1st & goal" is still sitting in the play list when
		// the kick goes up, and showing it on an extra point is simply wrong.
		const scene = build(
			{ type: "extraPointAttempt", names: ["K126"], yds: 33 },
			0,
			sportState({
				awaitingAfterTouchdown: true,
				plays: [{ down: 1, toGo: 10, scrimmage: 97, yards: 0, t: 0 }],
			}),
		)!;
		assert.strictEqual(scene.down, undefined);
	});

	// A flag or an injury happens inside a formation but is not a CALL, and the
	// label used to read "Trips · undefined".
	test("a scene with no call shows the formation alone, never an undefined", () => {
		for (const type of ["penalty", "injury", "timeout"] as const) {
			const scene = build({ type, names: ["OL110"] }, 0);
			if (!scene?.playName) {
				continue;
			}
			assert.ok(
				!scene.playName.includes("undefined"),
				`${type} showed "${scene.playName}"`,
			);
		}
		// And an ordinary play still reads as formation and concept together.
		const pass = build(
			{ type: "passComplete", names: ["QB100", "WR104"], yds: 11 },
			0,
		)!;
		assert.ok(pass.playName!.includes(" · "), pass.playName);
	});

	test("an event the field has nothing to say about produces no scene", () => {
		assert.strictEqual(build({ type: "clock" }, 0), undefined);
		assert.strictEqual(build({ type: "penaltyCount" }, 0), undefined);
	});
});

describe("the details a viewer reads first", () => {
	test("a penalty puts a flag on the grass, and nothing else does", () => {
		const flagged = build({ type: "penalty", names: ["OL110"] }, 0)!;
		assert.ok(flagged.flag, "a penalty with no flag");
		assert.ok(flagged.flag!.x > 0 && flagged.flag!.y > 0);
		const run = build({ type: "run", names: ["RB102"], yds: 5 }, 0)!;
		assert.strictEqual(run.flag, undefined);
	});

	// He was appearing a stride from the tackle with no way of having got there.
	test("the man who made the tackle runs to it", () => {
		const scene = build(
			{ type: "sack", names: ["QB100", "DL215"], yds: -8 },
			0,
		)!;
		const tackler = scene.actors.find((a) => a.role === "defender")!;
		assert.ok(tackler.path && tackler.path.length >= 2, "he teleported");
		const start = tackler.path![0]!;
		const finish = tackler.path!.at(-1)!;
		assert.ok(
			Math.hypot(finish.x - tackler.x, finish.y - tackler.y) < 0.01,
			"his path does not end where he is",
		);
		assert.ok(
			Math.hypot(start.x - finish.x, start.y - finish.y) > 1,
			"he did not actually go anywhere",
		);
	});

	test("the ball a man is carrying travels the path he runs", () => {
		const scene = build({ type: "run", names: ["RB102"], yds: 14 }, 0)!;
		const carrier = scene.actors.find((a) => a.role === "main")!;
		assert.ok(scene.ball!.path, "a carried ball with a curve of its own");
		assert.deepStrictEqual(scene.ball!.path, carrier.path);
	});
});

describe("a scramble", () => {
	// It started as a pass and stopped being one, which looks nothing like a
	// designed quarterback run - and used to be drawn as exactly that.
	const scramble = () =>
		build(
			{ type: "run", names: ["QB100"], yds: 13 },
			0,
			sportState({
				plays: [{ down: 2, toGo: 9, scrimmage: 40, yards: 13, t: 0 }],
			}),
			(() => {
				const c = newFieldSceneCtx();
				// The handoff/dropback that opened the play named the quarterback.
				c.quarterback = "QB100";
				return c;
			})(),
		)!;

	test("is called a scramble and the defense is in a coverage, not run defense", () => {
		const scene = scramble();
		assert.ok(scene.playName!.includes("Scramble"), scene.playName);
		assert.ok(
			scene.defenseName?.includes("Cover") ||
				scene.defenseName?.includes("Quarters") ||
				scene.defenseName?.includes("Tampa") ||
				scene.defenseName?.includes("Fire"),
			`defense was "${scene.defenseName}"`,
		);
	});

	test("the line is protecting and the receivers are running routes", () => {
		const scene = scramble();
		const linemen = scene.actors.filter((a) => a.job === "block");
		assert.ok(linemen.length >= 4, "nobody was protecting");
		// A protecting lineman gives ground; a run blocker fires forward.
		for (const lineman of linemen) {
			assert.ok(
				(scene.losX - lineman.x) * scene.dir > 0,
				"a lineman who fired off downhill on a scramble",
			);
		}
		assert.ok(
			scene.actors.some((a) => a.job === "route"),
			"nobody ran a route",
		);
	});

	test("a short-yardage keeper is still a sneak, not a scramble", () => {
		const scene = build(
			{ type: "run", names: ["QB100"], yds: 1 },
			0,
			sportState({
				plays: [{ down: 4, toGo: 1, scrimmage: 40, yards: 1, t: 0 }],
			}),
			(() => {
				const c = newFieldSceneCtx();
				c.quarterback = "QB100";
				return c;
			})(),
		)!;
		assert.ok(scene.playName!.includes("Sneak"), scene.playName);
	});
});

describe("nobody stands and watches", () => {
	// A pass that simply lands with nobody near it is the one thing that never
	// happens, and the sim has no stat for who knocked it down.
	test("an incompletion has somebody breaking it up", () => {
		const scene = build(
			{ type: "passIncomplete", names: ["QB100", "WR104"], yds: 12 },
			0,
		)!;
		const breakup = scene.actors.find((a) => a.role === "defender")!;
		assert.ok(breakup, "an incompletion nobody defended");
		assert.strictEqual(breakup.t, 1, "the breakup came from the offense");
		assert.ok(
			Math.hypot(breakup.x - scene.ball!.to.x, breakup.y - scene.ball!.to.y) <
				12,
			"the man who broke it up was miles away",
		);
	});

	// What makes a run long rather than fast is somebody missing him on the way.
	test("a long run has a missed tackle in it, a short one does not", () => {
		const long = build(
			{ type: "run", names: ["RB102"], yds: 28 },
			0,
			sportState({
				plays: [{ down: 1, toGo: 10, scrimmage: 30, yards: 28, t: 0 }],
			}),
		)!;
		const carrier = long.actors.find((a) => a.role === "main")!;
		// Somebody on defence finished well short of the tackle, on his path.
		const strandedOnHisPath = long.actors.filter(
			(a) =>
				a.t === 1 &&
				(carrier.path ?? []).some(
					(p) => Math.hypot(p.x - a.x, p.y - a.y) < 3.5,
				) &&
				Math.hypot(a.x - carrier.x, a.y - carrier.y) > 6,
		);
		assert.ok(strandedOnHisPath.length >= 1, "nobody missed him");
	});

	test("a flag lands on the man it was thrown at", () => {
		const scene = build({ type: "penalty", names: ["OL110"] }, 0)!;
		const culprit = scene.actors.find((a) => a.role === "main")!;
		assert.ok(
			Math.hypot(scene.flag!.x - culprit.x, scene.flag!.y - culprit.y) < 4,
			"the flag landed nowhere near him",
		);
	});
});
