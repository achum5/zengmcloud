import { assert, beforeEach, describe, test } from "vitest";
import { clearCourtRng, seedCourtRng } from "./courtRng.ts";
import {
	assignPassDefense,
	assignRoutes,
	assignRunBlocking,
	assignRunPursuit,
	callPass,
	callRun,
	ELIGIBLE,
	PASS_CONCEPTS,
	ROUTES,
	routePath,
	RUN_SCHEMES,
	runPath,
	SLOT_C,
	SLOT_QB,
	SLOT_RT,
	SLOT_WR_L,
	SLOT_WR_R,
} from "./playbook.ts";
import {
	defenseSlots,
	dirFor,
	FIELD_LEN,
	FIELD_W,
	fieldX,
	MID_Y,
	offenseSlots,
	placeFormation,
	type FieldActor,
} from "./fieldSpots.ts";

beforeEach(() => {
	seedCourtRng("playbook-test");
	return () => {
		clearCourtRng();
	};
});

const geomFor = (t: 0 | 1) => ({
	losX: fieldX(30, dirFor(t)),
	dir: dirFor(t),
	ballAcross: MID_Y,
});

// The eleven, lined up, the way the scene builder hands them over.
const lineUp = (t: 0 | 1, kind: "pass" | "run" = "pass"): FieldActor[] => {
	const slots = offenseSlots(kind);
	const geom = geomFor(t);
	return placeFormation(slots, geom.losX, geom.dir, geom.ballAcross).map(
		(p, i) => ({
			pid: 100 + i,
			name: `O${i}`,
			x: p.x,
			y: p.y,
			role: "onField" as const,
			slotIndex: i,
			t,
		}),
	);
};

const lineUpDefense = (t: 0 | 1): FieldActor[] => {
	const slots = defenseSlots("pass");
	const geom = geomFor(t === 0 ? 1 : 0);
	return placeFormation(slots, geom.losX, geom.dir, geom.ballAcross).map(
		(p, i) => ({
			pid: 200 + i,
			name: `D${i}`,
			x: p.x,
			y: p.y,
			role: "onField" as const,
			slotIndex: i,
			t,
		}),
	);
};

describe("the route tree", () => {
	test("every route runs somewhere, and a blocker doesn't", () => {
		for (const [name, points] of Object.entries(ROUTES)) {
			if (name === "block") {
				assert.strictEqual(points.length, 0);
				continue;
			}
			assert.ok(points.length >= 3, `${name} is barely a route`);
		}
	});

	test("a go is deep, a slant is short and inside, a flat is short and outside", () => {
		const slots = offenseSlots("pass");
		const geom = geomFor(0);
		const wr = slots[SLOT_WR_R]!;
		const at = (route: Parameters<typeof routePath>[0]["route"]) =>
			routePath({ slot: wr, route, ...geom }).at(-1)!;
		const start = placeFormation([wr], geom.losX, geom.dir, geom.ballAcross)[0]!;

		const go = at("go");
		assert.ok((go.x - geom.losX) * geom.dir > 30, "a go should get deep");

		const slant = at("slant");
		assert.ok((slant.x - geom.losX) * geom.dir < 12, "a slant is not deep");
		// He lines up outside, so breaking in means moving toward the middle.
		assert.ok(
			Math.abs(slant.y - MID_Y) < Math.abs(start.y - MID_Y),
			"a slant should break toward the middle",
		);

		const flat = at("flat");
		assert.ok((flat.x - geom.losX) * geom.dir < 6, "a flat is not deep");
		assert.ok(
			Math.abs(flat.y - MID_Y) > Math.abs(start.y - MID_Y),
			"a flat should work toward the sideline",
		);
	});

	test("a route is mirrored for the other side of the formation", () => {
		const slots = offenseSlots("pass");
		const geom = geomFor(0);
		const left = routePath({ slot: slots[SLOT_WR_L]!, route: "slant", ...geom });
		const right = routePath({
			slot: slots[SLOT_WR_R]!,
			route: "slant",
			...geom,
		});
		// Both break toward the middle - from opposite sides, so in opposite
		// directions across the field.
		assert.ok(left.at(-1)!.y > left[0]!.y);
		assert.ok(right.at(-1)!.y < right[0]!.y);
	});

	test("a route is mirrored for the other direction of play too", () => {
		for (const route of ["go", "post", "out", "drag"] as const) {
			for (const t of [0, 1] as const) {
				const geom = geomFor(t);
				const slot = offenseSlots("pass")[SLOT_WR_R]!;
				const end = routePath({ slot, route, ...geom }).at(-1)!;
				// Downfield is downfield whichever way the offense is going.
				assert.ok(
					(end.x - geom.losX) * geom.dir > 0,
					`${route} ran backwards for team ${t}`,
				);
			}
		}
	});

	test("nobody runs a route into the parking lot", () => {
		for (const t of [0, 1] as const) {
			const geom = { ...geomFor(t), ballAcross: 6 };
			for (const slot of offenseSlots("pass")) {
				for (const route of Object.keys(ROUTES) as (keyof typeof ROUTES)[]) {
					for (const p of routePath({ slot, route, ...geom })) {
						assert.ok(p.x >= 0 && p.x <= FIELD_LEN, `${route} x ${p.x}`);
						assert.ok(p.y >= 0 && p.y <= FIELD_W, `${route} y ${p.y}`);
					}
				}
			}
		}
	});
});

describe("the concepts", () => {
	test("every concept gives all five eligible men a job", () => {
		for (const concept of PASS_CONCEPTS) {
			for (const slot of ELIGIBLE) {
				assert.ok(
					concept.routes[slot] !== undefined,
					`${concept.name} left slot ${slot} without an assignment`,
				);
			}
		}
	});

	// A short concept is allowed a vertical - Stick has a clear-out and so does
	// half the quick game. What makes it short is that somebody is open NOW, and
	// what makes a deep one deep is that somebody is running past everybody.
	test("a short concept always has somebody open underneath; a deep one goes deep", () => {
		const slots = offenseSlots("pass");
		const geom = geomFor(0);
		const depths = (concept: (typeof PASS_CONCEPTS)[number]) =>
			ELIGIBLE.map((i) => {
				const route = concept.routes[i];
				if (!route || route === "block") {
					return undefined;
				}
				const end = routePath({ slot: slots[i]!, route, ...geom }).at(-1);
				return end ? (end.x - geom.losX) * geom.dir : undefined;
			}).filter((d): d is number => d !== undefined);
		for (const concept of PASS_CONCEPTS) {
			const d = depths(concept);
			assert.ok(d.length >= 3, `${concept.name} has nobody running anything`);
			if (concept.depth === "deep") {
				assert.ok(
					Math.max(...d) > 24,
					`${concept.name} is not deep (${Math.max(...d)})`,
				);
			}
			if (concept.depth === "short") {
				assert.ok(
					Math.min(...d) < 8,
					`${concept.name} has nothing quick (${Math.min(...d)})`,
				);
			}
		}
	});
});

describe("calling it", () => {
	test("the depth of the throw picks the depth of the concept", () => {
		for (let i = 0; i < 30; i += 1) {
			assert.strictEqual(
				callPass({ airYards: 30, toGo: 10, sacked: false }).depth,
				"deep",
			);
			assert.strictEqual(
				callPass({ airYards: 10, toGo: 10, sacked: false }).depth,
				"medium",
			);
			assert.strictEqual(
				callPass({ airYards: 3, toGo: 10, sacked: false }).depth,
				"short",
			);
		}
	});

	// The bug this pins: a dropback has no throw to measure, and passing the
	// quarterback's five-yard DROP in place of the air yards called a screen on
	// every third down.
	test("with no throw yet, the call comes from the distance, never a screen", () => {
		for (let i = 0; i < 30; i += 1) {
			const call = callPass({ airYards: undefined, toGo: 4, sacked: false });
			assert.ok(!call.name.includes("Screen"), `called ${call.name}`);
			assert.strictEqual(call.depth, "short");
			assert.strictEqual(
				callPass({ airYards: undefined, toGo: 14, sacked: false }).depth,
				"deep",
			);
		}
	});

	test("a ball thrown at the line is the screen it looks like", () => {
		assert.ok(
			callPass({ airYards: 0, toGo: 7, sacked: false }).name.includes("Screen"),
		);
	});

	test("a sack is called from the down, not from the yards it lost", () => {
		const call = callPass({ airYards: -8, toGo: 15, sacked: true });
		assert.strictEqual(call.depth, "deep");
	});

	test("third and long that was run is a draw; short yardage is downhill", () => {
		const draw = callRun({
			yards: 4,
			down: 3,
			toGo: 11,
			byQuarterback: false,
			kneel: false,
		});
		assert.strictEqual(draw.name, "Draw");
		for (let i = 0; i < 20; i += 1) {
			const short = callRun({
				yards: 1,
				down: 3,
				toGo: 1,
				byQuarterback: false,
				kneel: false,
			});
			assert.ok(
				["Inside Zone", "Power", "Trap"].includes(short.name),
				`called ${short.name} on third and one`,
			);
		}
	});

	test("a quarterback keeper is a sneak in short yardage and a scramble otherwise", () => {
		assert.strictEqual(
			callRun({
				yards: 1,
				down: 4,
				toGo: 1,
				byQuarterback: true,
				kneel: false,
			}).name,
			"QB Sneak",
		);
		assert.strictEqual(
			callRun({
				yards: 14,
				down: 2,
				toGo: 9,
				byQuarterback: true,
				kneel: false,
			}).name,
			"Scramble",
		);
	});

	test("taking a knee is never mistaken for a running play", () => {
		assert.strictEqual(
			callRun({
				yards: -1,
				down: 2,
				toGo: 10,
				byQuarterback: true,
				kneel: true,
			}).name,
			"Victory",
		);
	});
});

describe("the run", () => {
	test("the back starts where he was, crosses the line, and ends where the sim says", () => {
		for (const t of [0, 1] as const) {
			const geom = geomFor(t);
			const start = { x: geom.losX - geom.dir * 7, y: MID_Y };
			const end = { x: geom.losX + geom.dir * 12, y: MID_Y + 4 };
			for (const scheme of RUN_SCHEMES) {
				const path = runPath({ start, end, scheme, ...geom });
				assert.deepStrictEqual(path[0], start);
				assert.deepStrictEqual(path.at(-1), end);
				// He was behind the line and finished in front of it, so somewhere in
				// between he crossed it.
				const crossed = path.some((p) => (p.x - geom.losX) * geom.dir > 0);
				assert.ok(crossed, `${scheme.name} never reached the line`);
			}
		}
	});

	test("a play that lost yardage never shows him crossing the line first", () => {
		const geom = geomFor(0);
		const start = { x: geom.losX - 7, y: MID_Y };
		const end = { x: geom.losX - 3, y: MID_Y };
		const path = runPath({
			start,
			end,
			scheme: RUN_SCHEMES.find((s) => s.name === "Power")!,
			...geom,
		});
		assert.ok(path.every((p) => p.x <= geom.losX + 0.001));
	});
});

describe("handing out the jobs", () => {
	test("the line sets into a pocket and the quarterback gets behind it", () => {
		const geom = geomFor(0);
		const actors = assignRoutes({
			actors: lineUp(0),
			slots: offenseSlots("pass"),
			concept: PASS_CONCEPTS[0]!,
			geom,
			protectDepth: 5.5,
		});
		for (let i = SLOT_C; i <= SLOT_RT; i += 1) {
			const lineman = actors.find((a) => a.slotIndex === i)!;
			assert.ok(lineman.path, "a lineman with nothing to do");
			// He gave ground: he is deeper than the line now.
			assert.ok((geom.losX - lineman.x) * geom.dir > 0.5);
		}
		const qb = actors.find((a) => a.slotIndex === SLOT_QB)!;
		assert.ok((geom.losX - qb.x) * geom.dir > 4);
	});

	test("the eligible men get paths and the blockers stay put", () => {
		const concept = PASS_CONCEPTS.find((c) => c.name === "Slant–Flat")!;
		const before = lineUp(0);
		const after = assignRoutes({
			actors: before,
			slots: offenseSlots("pass"),
			concept,
			geom: geomFor(0),
			protectDepth: 5.5,
		});
		const wr = after.find((a) => a.slotIndex === SLOT_WR_L)!;
		assert.ok(wr.path && wr.path.length > 2);
		// The tight end is blocking on this one, so he has not moved.
		const te = after.find((a) => a.slotIndex === 5)!;
		const teBefore = before.find((a) => a.slotIndex === 5)!;
		assert.strictEqual(te.x, teBefore.x);
		assert.strictEqual(te.y, teBefore.y);
	});

	test("four men rush, and only a sack gets one of them home", () => {
		const geom = geomFor(0);
		const target = { x: geom.losX - geom.dir * 5.5, y: MID_Y };
		const offense = assignRoutes({
			actors: lineUp(0),
			slots: offenseSlots("pass"),
			concept: PASS_CONCEPTS[0]!,
			geom,
			protectDepth: 5.5,
		});
		const near = (reachTarget: boolean) => {
			const d = assignPassDefense({
				defenders: lineUpDefense(1),
				defSlots: defenseSlots("pass"),
				receivers: offense,
				target,
				geom,
				reachTarget,
			});
			return Math.min(
				...d
					.filter((a) => (a.slotIndex ?? 99) <= 3)
					.map((a) => Math.hypot(a.x - target.x, a.y - target.y)),
			);
		};
		assert.ok(near(false) > 1.5, "the rush got home without a sack");
		assert.ok(near(true) < 1.5, "the sack never reached the quarterback");
	});

	test("a cover man goes where his receiver goes", () => {
		const geom = geomFor(0);
		const offense = assignRoutes({
			actors: lineUp(0),
			slots: offenseSlots("pass"),
			concept: PASS_CONCEPTS.find((c) => c.name === "Four Verticals")!,
			geom,
			protectDepth: 5.5,
		});
		const defense = assignPassDefense({
			defenders: lineUpDefense(1),
			defSlots: defenseSlots("pass"),
			receivers: offense,
			target: { x: geom.losX - geom.dir * 5.5, y: MID_Y },
			geom,
			reachTarget: false,
		});
		// The corner over the split end followed him down the field.
		const cb = defense.find((a) => a.slotIndex === 7)!;
		const wr = offense.find((a) => a.slotIndex === SLOT_WR_L)!;
		assert.ok(Math.hypot(cb.x - wr.x, cb.y - wr.y) < 4);
	});

	test("on a run the line fires forward instead of setting back", () => {
		const geom = geomFor(0);
		const after = assignRunBlocking({
			actors: lineUp(0, "run"),
			slots: offenseSlots("run"),
			scheme: RUN_SCHEMES[0]!,
			geom,
		});
		for (let i = SLOT_C; i <= SLOT_RT; i += 1) {
			const lineman = after.find((a) => a.slotIndex === i)!;
			assert.ok(
				(lineman.x - geom.losX) * geom.dir > 0,
				"a run blocker who went backwards",
			);
		}
	});

	test("on a run everybody on defence flows to the ball, the front seven hardest", () => {
		const geom = geomFor(0);
		const ballEnd = { x: geom.losX + geom.dir * 9, y: MID_Y + 6 };
		const before = lineUpDefense(1);
		const after = assignRunPursuit({ defenders: before, ballEnd, geom });
		const closed = (list: FieldActor[], i: number) => {
			const a = list.find((x) => x.slotIndex === i)!;
			return Math.hypot(a.x - ballEnd.x, a.y - ballEnd.y);
		};
		for (const i of [0, 5, 9]) {
			assert.ok(
				closed(after, i) < closed(before, i),
				`defender ${i} did not pursue`,
			);
		}
		// A safety is still closing when the front seven have arrived.
		assert.ok(closed(after, 9) > closed(after, 0));
	});
});
