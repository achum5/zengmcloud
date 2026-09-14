import { assert, beforeEach, describe, test } from "vitest";
import { clearCourtRng, seedCourtRng } from "./courtRng.ts";
import {
	assignCoverage,
	chooseCoverage,
	COVERAGES,
	defenseLabel,
	groupDefenders,
} from "./coverages.ts";
import {
	DEFENSE_FRONTS,
	OFFENSE_FORMATIONS,
	defenseSlots,
} from "./formations.ts";
import {
	dirFor,
	FIELD_LEN,
	FIELD_W,
	fieldX,
	MID_Y,
	placeFormation,
	type FieldActor,
	type Slot,
} from "./fieldSpots.ts";
import {
	assignRoutes,
	PASS_CONCEPTS,
	SLOT_WR_L,
	SLOT_WR_R,
} from "./playbook.ts";

beforeEach(() => {
	seedCourtRng("coverage-test");
	return () => {
		clearCourtRng();
	};
});

const geom = { losX: fieldX(30, dirFor(0)), dir: dirFor(0), ballAcross: MID_Y };
const qbSpot = { x: geom.losX - 5.5, y: MID_Y };

const lineUp = (slots: Slot[], t: 0 | 1, base: number): FieldActor[] =>
	placeFormation(slots, geom.losX, geom.dir, geom.ballAcross).map((p, i) => ({
		pid: base + i,
		name: `P${base + i}`,
		x: p.x,
		y: p.y,
		role: "onField" as const,
		slotIndex: i,
		t,
	}));

const offenseWithRoutes = (conceptName = "Four Verticals") =>
	assignRoutes({
		actors: lineUp(OFFENSE_FORMATIONS.shotgun.slots, 0, 100),
		slots: OFFENSE_FORMATIONS.shotgun.slots,
		concept: PASS_CONCEPTS.find((c) => c.name === conceptName)!,
		geom,
		protectDepth: 5.5,
	});

const play = ({
	coverage,
	defSlots = defenseSlots("pass"),
	reachTarget = false,
	ballTo,
}: {
	coverage: (typeof COVERAGES)[keyof typeof COVERAGES];
	defSlots?: Slot[];
	reachTarget?: boolean;
	ballTo?: { x: number; y: number };
}) =>
	assignCoverage({
		defenders: lineUp(defSlots, 1, 200),
		defSlots,
		receivers: offenseWithRoutes(),
		coverage,
		target: qbSpot,
		geom,
		reachTarget,
		ballTo,
	});

const downfield = (a: FieldActor) => (a.x - geom.losX) * geom.dir;

describe("grouping the defense", () => {
	test("every front sorts into groups, ordered across the field", () => {
		for (const front of Object.values(DEFENSE_FRONTS)) {
			const groups = groupDefenders(front.slots);
			const total =
				groups.dl.length +
				groups.lb.length +
				groups.nb.length +
				groups.cb.length +
				groups.s.length;
			assert.strictEqual(total, 11, `${front.name} lost somebody`);
			for (const list of [groups.dl, groups.lb, groups.cb, groups.s]) {
				const across = list.map((i) => front.slots[i]!.across);
				assert.deepStrictEqual(
					across,
					[...across].sort((a, b) => a - b),
					`${front.name} group out of order`,
				);
			}
		}
	});

	test("nickel has a fifth back and dime a sixth", () => {
		const nickel = groupDefenders(DEFENSE_FRONTS.nickel.slots);
		assert.strictEqual(nickel.nb.length + nickel.cb.length + nickel.s.length, 5);
		const dime = groupDefenders(DEFENSE_FRONTS.dime.slots);
		assert.strictEqual(dime.nb.length + dime.cb.length + dime.s.length, 6);
	});
});

describe("every coverage", () => {
	test("gives all eleven defenders a job, and nobody stands still", () => {
		for (const [key, coverage] of Object.entries(COVERAGES)) {
			for (const front of Object.values(DEFENSE_FRONTS)) {
				const after = assignCoverage({
					defenders: lineUp(front.slots, 1, 200),
					defSlots: front.slots,
					receivers: offenseWithRoutes(),
					coverage,
					target: qbSpot,
					geom,
					reachTarget: false,
					ballTo: undefined,
				});
				assert.strictEqual(after.length, 11);
				const idle = after.filter((a) => !a.path || a.path.length < 2);
				assert.ok(
					idle.length <= 1,
					`${key} vs ${front.name} left ${idle.length} men standing`,
				);
			}
		}
	});

	test("keeps everybody on the field", () => {
		for (const coverage of Object.values(COVERAGES)) {
			for (const a of play({ coverage })) {
				for (const p of a.path ?? []) {
					assert.ok(p.x >= 0 && p.x <= FIELD_LEN, `${coverage.name} x ${p.x}`);
					assert.ok(p.y >= 0 && p.y <= FIELD_W, `${coverage.name} y ${p.y}`);
				}
			}
		}
	});
});

describe("the shells are actually different", () => {
	const deepMen = (coverage: (typeof COVERAGES)[keyof typeof COVERAGES]) =>
		play({ coverage }).filter((a) => downfield(a) > 11).length;

	test("two deep in Cover 2, three in Cover 3, four in quarters", () => {
		assert.strictEqual(deepMen(COVERAGES.cover2), 2);
		assert.strictEqual(deepMen(COVERAGES.cover3), 3);
		assert.strictEqual(deepMen(COVERAGES.cover4), 4);
	});

	test("Tampa 2 sends a linebacker up the middle that Cover 2 does not", () => {
		const middleRunner = (coverage: (typeof COVERAGES)[keyof typeof COVERAGES]) =>
			play({ coverage }).some(
				(a) => downfield(a) > 15 && Math.abs(a.y - MID_Y) < 4,
			);
		assert.ok(!middleRunner(COVERAGES.cover2));
		assert.ok(middleRunner(COVERAGES.tampa2));
	});

	// In man coverage the defenders finish deep because their RECEIVERS did, so
	// counting deep bodies says nothing. What makes Cover 1 Cover 1 is that
	// every eligible man is covered and exactly one defender is deep in the
	// middle with nobody to cover - the free safety.
	test("Cover 1 covers everybody and leaves one free safety in the middle", () => {
		const offense = offenseWithRoutes();
		const after = assignCoverage({
			defenders: lineUp(defenseSlots("pass"), 1, 200),
			defSlots: defenseSlots("pass"),
			receivers: offense,
			coverage: COVERAGES.cover1,
			target: qbSpot,
			geom,
			reachTarget: false,
			ballTo: undefined,
		});
		const eligibles = offense.filter(
			(a) => (a.slotIndex ?? 0) >= 5 && a.slotIndex !== 6 && a.path,
		);
		for (const wr of eligibles) {
			const nearest = Math.min(
				...after.map((d) => Math.hypot(d.x - wr.x, d.y - wr.y)),
			);
			assert.ok(nearest < 5, `slot ${wr.slotIndex} was uncovered (${nearest})`);
		}
		const free = after.filter(
			(a) =>
				downfield(a) > 12 &&
				Math.abs(a.y - MID_Y) < 5 &&
				!eligibles.some((wr) => Math.hypot(wr.x - a.x, wr.y - a.y) < 5),
		);
		assert.strictEqual(free.length, 1, "no free safety, or more than one");
	});

	test("a blitz sends more than four and a base coverage sends four", () => {
		const rushing = (coverage: (typeof COVERAGES)[keyof typeof COVERAGES]) =>
			play({ coverage }).filter(
				(a) => Math.hypot(a.x - qbSpot.x, a.y - qbSpot.y) < 5,
			).length;
		assert.ok(rushing(COVERAGES.cover0) > rushing(COVERAGES.cover3));
		assert.ok(rushing(COVERAGES.fireZone) > rushing(COVERAGES.cover3));
	});
});

describe("man coverage", () => {
	test("a corner goes where the receiver he has goes", () => {
		const offense = offenseWithRoutes();
		const after = assignCoverage({
			defenders: lineUp(defenseSlots("pass"), 1, 200),
			defSlots: defenseSlots("pass"),
			receivers: offense,
			coverage: COVERAGES.cover1,
			target: qbSpot,
			geom,
			reachTarget: false,
			ballTo: undefined,
		});
		for (const slot of [SLOT_WR_L, SLOT_WR_R]) {
			const wr = offense.find((a) => a.slotIndex === slot)!;
			const nearest = Math.min(
				...after.map((d) => Math.hypot(d.x - wr.x, d.y - wr.y)),
			);
			assert.ok(nearest < 4, `nobody covered slot ${slot} (${nearest})`);
		}
	});
});

describe("the rush", () => {
	test("stops short of the quarterback unless he actually went down", () => {
		const nearest = (reachTarget: boolean) =>
			Math.min(
				...play({ coverage: COVERAGES.cover3, reachTarget }).map((a) =>
					Math.hypot(a.x - qbSpot.x, a.y - qbSpot.y),
				),
			);
		assert.ok(nearest(false) > 1.5);
		assert.ok(nearest(true) < 1.5);
	});
});

describe("breaking on the ball", () => {
	// A zone that never reacts is seven men standing on spots. Exactly one of
	// them closes, because a zone where everybody converges is just man coverage
	// drawn badly.
	test("one zone defender - and only one - closes on the catch", () => {
		const ballTo = { x: geom.losX + 14, y: MID_Y + 9 };
		const before = play({ coverage: COVERAGES.cover3 });
		const after = play({ coverage: COVERAGES.cover3, ballTo });
		const near = (list: FieldActor[]) =>
			list.filter((a) => Math.hypot(a.x - ballTo.x, a.y - ballTo.y) < 3).length;
		assert.ok(near(after) > near(before), "nobody broke on the ball");
		assert.ok(near(after) <= 1, "the whole zone converged");
	});

	test("nobody breaks before the ball is thrown", () => {
		const after = play({ coverage: COVERAGES.cover2, ballTo: undefined });
		for (const a of after) {
			assert.ok((a.path?.length ?? 0) <= 3);
		}
	});
});

describe("calling the coverage", () => {
	test("a defense on its own goal line plays man, because there is no deep", () => {
		for (let i = 0; i < 40; i += 1) {
			const call = chooseCoverage({
				down: 1,
				toGo: 3,
				scrimmage: 97,
				sacked: false,
			});
			assert.ok(call.man, `called ${call.name} on the goal line`);
		}
	});

	test("third and long gets somebody deep, never an all-out blitz", () => {
		for (let i = 0; i < 60; i += 1) {
			const call = chooseCoverage({
				down: 3,
				toGo: 14,
				scrimmage: 40,
				sacked: false,
			});
			assert.ok(!call.man, `called ${call.name} on third and fourteen`);
		}
	});

	test("a sack is more often pressure than not", () => {
		let blitzes = 0;
		for (let i = 0; i < 200; i += 1) {
			if (chooseCoverage({ down: 2, toGo: 8, scrimmage: 40, sacked: true }).rushers > 4) {
				blitzes += 1;
			}
		}
		assert.ok(blitzes > 80, `only ${blitzes} of 200 sacks came from pressure`);
	});

	test("ordinary downs get variety rather than one call", () => {
		const seen = new Set<string>();
		for (let i = 0; i < 200; i += 1) {
			seen.add(
				chooseCoverage({ down: 1, toGo: 10, scrimmage: 45, sacked: false }).name,
			);
		}
		assert.ok(seen.size >= 3, `only saw ${[...seen].join(", ")}`);
	});
});

describe("defenseLabel", () => {
	test("reads like a broadcast", () => {
		assert.strictEqual(defenseLabel("Nickel", "Cover 3"), "Nickel · Cover 3");
		assert.strictEqual(defenseLabel("Goal Line", "Goal Line"), "Goal Line");
	});
});

describe("disguise", () => {
	// A defense that lines up in what it is about to play has told the
	// quarterback everything.
	test("the safeties start somewhere other than where they end up", () => {
		for (const coverage of [COVERAGES.cover3, COVERAGES.cover2, COVERAGES.cover1]) {
			const after = play({ coverage });
			const safeties = after.filter(
				(a) => defenseSlots("pass")[a.slotIndex!]!.pos === "S",
			);
			assert.strictEqual(safeties.length, 2, coverage.name);
			for (const s of safeties) {
				assert.ok(s.path && s.path.length >= 2, `${coverage.name} safety idle`);
				const from = s.path![0]!;
				const to = s.path!.at(-1)!;
				assert.ok(
					Math.hypot(to.x - from.x, to.y - from.y) > 2,
					`${coverage.name} safety never rotated`,
				);
			}
		}
	});

	test("a two-deep coverage shows one deep, and a one-deep coverage shows two", () => {
		const shownDepths = (coverage: (typeof COVERAGES)[keyof typeof COVERAGES]) =>
			play({ coverage })
				.filter((a) => defenseSlots("pass")[a.slotIndex!]!.pos === "S")
				.map((a) => Math.round((a.path![0]!.x - geom.losX) * geom.dir));
		// Cover 2 is two deep, so it shows one man deep and one down.
		const two = shownDepths(COVERAGES.cover2).sort((a, b) => a - b);
		assert.ok(two[0]! < 10 && two[1]! > 10, `Cover 2 showed ${two}`);
		// Cover 3 is one deep, so it shows two at a matching depth.
		const three = shownDepths(COVERAGES.cover3);
		assert.ok(
			Math.abs(three[0]! - three[1]!) <= 1,
			`Cover 3 showed ${three}`,
		);
	});
});
