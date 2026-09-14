import { assert, beforeEach, describe, test } from "vitest";
import { clearCourtRng, seedCourtRng } from "./courtRng.ts";
import {
	adjustRoute,
	assignRoutes,
	assignRunBlocking,
	assignRunPursuit,
	assignRunSupport,
	callPass,
	callRun,
	ELIGIBLE,
	PASS_CONCEPTS,
	ROUTES,
	routePath,
	RUN_SCHEMES,
	runPath,
	engageLine,
	SLOT_C,
	SLOT_QB,
	SLOT_RB,
	SLOT_RT,
	SLOT_TE,
	SLOT_WR_L,
	SLOT_WR_SLOT,
	SLOT_WR_R,
} from "./playbook.ts";
import {
	dirFor,
	FIELD_LEN,
	FIELD_W,
	fieldX,
	MID_Y,
	placeFormation,
	type FieldActor,
} from "./fieldSpots.ts";
import { defenseSlots, offenseSlots } from "./formations.ts";

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
		const start = placeFormation(
			[wr],
			geom.losX,
			geom.dir,
			geom.ballAcross,
		)[0]!;

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
		const left = routePath({
			slot: slots[SLOT_WR_L]!,
			route: "slant",
			...geom,
		});
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

describe("the mesh point", () => {
	test("two men crossing pass one over and one under", () => {
		const geom = geomFor(0);
		const slots = offenseSlots("pass");
		const mesh = PASS_CONCEPTS.find((c) => c.name === "Mesh")!;
		const after = assignRoutes({
			actors: lineUp(0, "pass"),
			slots,
			concept: mesh,
			geom,
			protectDepth: 5.5,
		});
		const crossers = after.filter((a) => {
			const i = a.slotIndex;
			return i !== undefined && mesh.routes[i] === "drag";
		});
		assert.strictEqual(crossers.length, 2, "the mesh needs two crossers");

		// They still cross - the whole point is that they rub off each other -
		// but they are never at the same place at the same time.
		const depth = (a: FieldActor) => (a.x - geom.losX) * geom.dir;
		assert.ok(
			Math.abs(depth(crossers[0]!) - depth(crossers[1]!)) > 0.8,
			"the two crossers ran at the same depth",
		);
		// And the one giving way is the one running UNDER, not off the field.
		for (const a of crossers) {
			assert.ok(depth(a) > 0, "a crosser ended behind the line");
		}
	});

	test("a lone crosser runs his route as written", () => {
		const geom = geomFor(0);
		const slots = offenseSlots("pass");
		// One drag and nothing else crossing: nobody has to give way.
		const single = {
			name: "One Drag",
			depth: "short" as const,
			routes: {
				[SLOT_TE]: "drag" as const,
				[SLOT_WR_L]: "go" as const,
				[SLOT_WR_R]: "out" as const,
				[SLOT_WR_SLOT]: "curl" as const,
				[SLOT_RB]: "checkdown" as const,
			},
		};
		const withOne = assignRoutes({
			actors: lineUp(0, "pass"),
			slots,
			concept: single,
			geom,
			protectDepth: 5.5,
		}).find((a) => a.slotIndex === SLOT_TE)!;
		const plain = routePath({ slot: slots[SLOT_TE]!, route: "drag", ...geom });
		assert.ok(Math.abs(withOne.x - plain.at(-1)!.x) < 0.001);
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

	test("on a run nobody on offence just stands there", () => {
		const geom = geomFor(0);
		const slots = offenseSlots("run");
		const before = lineUp(0, "run");
		const carrier = before.find((a) => a.slotIndex === SLOT_RB)!;
		const blocked = assignRunBlocking({
			actors: before,
			slots,
			scheme: RUN_SCHEMES[0]!,
			geom,
		});
		const after = assignRunSupport({
			actors: blocked,
			slots,
			scheme: RUN_SCHEMES[0]!,
			geom,
			carrierPid: carrier.pid,
		});

		for (const actor of after) {
			if (actor.pid === carrier.pid) {
				continue;
			}
			const from = before.find((a) => a.pid === actor.pid)!;
			assert.ok(
				Math.hypot(actor.x - from.x, actor.y - from.y) > 0.5,
				`${actor.slotIndex} never moved`,
			);
			assert.ok(actor.job !== undefined, `${actor.slotIndex} has no job`);
		}

		// The man with the ball is left entirely alone - a carrier labelled a
		// blocker would be dragged into the line pairing.
		const stillCarrier = after.find((a) => a.pid === carrier.pid)!;
		assert.strictEqual(stillCarrier.job, undefined);
	});

	test("the quarterback's fake goes away from the run", () => {
		const geom = geomFor(0);
		const slots = offenseSlots("run");
		// A scheme aimed to one side; the fake has to break the other way.
		const scheme = RUN_SCHEMES.find((s) => Math.abs(s.aim) > 2)!;
		const before = lineUp(0, "run");
		const after = assignRunSupport({
			actors: before,
			slots,
			scheme,
			geom,
			carrierPid: before.find((a) => a.slotIndex === SLOT_RB)!.pid,
		});
		const qb = after.find((a) => a.slotIndex === SLOT_QB)!;
		const qbBefore = before.find((a) => a.slotIndex === SLOT_QB)!;
		assert.strictEqual(qb.job, "fake");
		const wentToward = (qb.y - qbBefore.y) * Math.sign(scheme.aim);
		// Across is mirrored for the direction of play, so compare in the same
		// terms the scheme is written in.
		assert.ok(
			wentToward * (geom.dir === 1 ? 1 : -1) < 0,
			"the fake followed the run instead of holding the backside",
		);
	});

	test("a receiver's run block is not mistaken for a man in the trenches", () => {
		const geom = geomFor(0);
		const slots = offenseSlots("run");
		const after = assignRunSupport({
			actors: lineUp(0, "run"),
			slots,
			scheme: RUN_SCHEMES[0]!,
			geom,
			carrierPid: undefined,
		});
		for (const actor of after) {
			const slot = slots[actor.slotIndex!]!;
			if (slot.pos === "WR") {
				assert.strictEqual(
					actor.job,
					"stalk",
					"a receiver was given a lineman's job",
				);
			}
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

		// And they do not form a picket fence. The old model nudged each man
		// sideways by his slot number, which left eleven defenders evenly
		// spaced on one vertical line however they had started.
		const xs = after.map((a) => a.x);
		const spread = Math.max(...xs) - Math.min(...xs);
		assert.ok(spread > 2, `pursuit ended on one line (spread ${spread})`);
	});

	test("nobody covers more ground than the play was long", () => {
		const geom = geomFor(0);
		// A two-yard gain: the corner on the far numbers cannot be in on it.
		const ballEnd = { x: geom.losX + geom.dir * 2, y: MID_Y };
		const before = lineUpDefense(1);
		const after = assignRunPursuit({ defenders: before, ballEnd, geom });
		for (const actor of after) {
			const from = before.find((a) => a.pid === actor.pid)!;
			const travelled = Math.hypot(actor.x - from.x, actor.y - from.y);
			assert.ok(
				travelled < 10,
				`defender ${actor.slotIndex} ran ${travelled.toFixed(1)} yards on a two-yard run`,
			);
		}
	});

	test("a long run is chased down by everybody", () => {
		const geom = geomFor(0);
		const ballEnd = { x: geom.losX + geom.dir * 34, y: MID_Y + 9 };
		const before = lineUpDefense(1);
		const after = assignRunPursuit({ defenders: before, ballEnd, geom });
		for (const actor of after) {
			const from = before.find((a) => a.pid === actor.pid)!;
			assert.ok(
				Math.hypot(actor.x - ballEnd.x, actor.y - ballEnd.y) <
					Math.hypot(from.x - ballEnd.x, from.y - ballEnd.y),
				`defender ${actor.slotIndex} gave up on a long run`,
			);
		}
	});
});

describe("the trenches", () => {
	const geom = geomFor(0);

	test("each blocker pairs with a rusher and they lock up in between", () => {
		const blockers = [
			{
				pid: 1,
				name: "B1",
				x: geom.losX - 1,
				y: MID_Y - 3,
				role: "onField" as const,
				t: 0 as const,
			},
			{
				pid: 2,
				name: "B2",
				x: geom.losX - 1,
				y: MID_Y + 3,
				role: "onField" as const,
				t: 0 as const,
			},
		];
		const rushers = [
			{
				pid: 11,
				name: "R1",
				x: geom.losX + 2,
				y: MID_Y - 3.4,
				role: "onField" as const,
				t: 1 as const,
			},
			{
				pid: 12,
				name: "R2",
				x: geom.losX + 2,
				y: MID_Y + 3.4,
				role: "onField" as const,
				t: 1 as const,
			},
		];
		const after = engageLine({ blockers, rushers });
		for (const [i, b] of after.blockers.entries()) {
			const r = after.rushers[i]!;
			const apart = Math.hypot(b.x - r.x, b.y - r.y);
			// They are fighting over one piece of ground - but they are two men,
			// and drawing them at the same point made a one-on-one look like a
			// lone player. Close enough to read as engaged, far enough apart to
			// read as two.
			assert.ok(apart < 1.6, `pair ${i} never met (${apart})`);
			assert.ok(apart > 0.4, `pair ${i} drawn on top of each other`);
			// And they met between where they started, not on top of either one.
			assert.ok(b.x > geom.losX - 1 && b.x < geom.losX + 2);
		}
	});

	test("a rusher nobody blocks keeps going", () => {
		const free = {
			pid: 99,
			name: "Free",
			x: geom.losX + 1,
			y: MID_Y + 22,
			role: "onField" as const,
			t: 1 as const,
			path: [
				{ x: geom.losX + 1, y: MID_Y + 22 },
				{ x: geom.losX - 5, y: MID_Y },
			],
		};
		const after = engageLine({
			blockers: [
				{
					pid: 1,
					name: "B1",
					x: geom.losX - 1,
					y: MID_Y,
					role: "onField" as const,
					t: 0 as const,
				},
			],
			rushers: [
				{
					pid: 11,
					name: "R1",
					x: geom.losX + 1,
					y: MID_Y,
					role: "onField" as const,
					t: 1 as const,
				},
				free,
			],
		});
		const stillFree = after.rushers.find((a) => a.pid === 99)!;
		assert.deepStrictEqual(stillFree.path, free.path);
	});

	test("nobody is paired with a man on the other side of the formation", () => {
		const after = engageLine({
			blockers: [
				{
					pid: 1,
					name: "B1",
					x: geom.losX - 1,
					y: MID_Y - 20,
					role: "onField" as const,
					t: 0 as const,
				},
			],
			rushers: [
				{
					pid: 11,
					name: "R1",
					x: geom.losX + 1,
					y: MID_Y + 20,
					role: "onField" as const,
					t: 1 as const,
				},
			],
		});
		// Forty yards apart is not a block.
		assert.strictEqual(after.blockers[0]!.path, undefined);
	});
});

describe("reading the coverage", () => {
	test("you cannot run past two deep safeties, so a go comes back", () => {
		assert.strictEqual(adjustRoute("go", "twoHigh"), "comeback");
		assert.strictEqual(adjustRoute("wheel", "twoHigh"), "out");
		// The intermediate stuff is what two-high gives you, so it is untouched.
		assert.strictEqual(adjustRoute("dig", "twoHigh"), "dig");
		assert.strictEqual(adjustRoute("slant", "twoHigh"), "slant");
	});

	test("against one deep man the routes break away from him", () => {
		assert.strictEqual(adjustRoute("post", "singleHigh"), "corner");
		assert.strictEqual(adjustRoute("seam", "singleHigh"), "corner");
		// And with nobody over the top outside, a comeback is a waste of a go.
		assert.strictEqual(adjustRoute("comeback", "singleHigh"), "go");
	});

	test("against man you break away from the man trailing you", () => {
		assert.strictEqual(adjustRoute("curl", "man"), "comeback");
		assert.strictEqual(adjustRoute("go", "man"), "go");
	});

	// The hot adjustment: everybody is coming, so there is no time for any of it.
	test("against an all-out blitz everything gets hot", () => {
		for (const deep of ["go", "seam", "post"] as const) {
			assert.strictEqual(adjustRoute(deep, "blitz"), "slant");
		}
		assert.strictEqual(adjustRoute("dig", "blitz"), "drag");
		assert.strictEqual(adjustRoute("corner", "blitz"), "out");
		// A route that is already hot stays as it is.
		assert.strictEqual(adjustRoute("slant", "blitz"), "slant");
		assert.strictEqual(adjustRoute("flat", "blitz"), "flat");
	});

	test("a blocker never becomes a receiver and a screen is still a screen", () => {
		for (const shell of ["man", "blitz", "singleHigh", "twoHigh"] as const) {
			assert.strictEqual(adjustRoute("block", shell), "block");
			assert.strictEqual(adjustRoute("screen", shell), "screen");
		}
	});

	test("with nothing known about the defense, the call is the call", () => {
		for (const route of Object.keys(ROUTES) as (keyof typeof ROUTES)[]) {
			assert.strictEqual(adjustRoute(route, undefined), route);
		}
	});

	// The point of all of it: the same concept against different coverages is a
	// different set of routes.
	test("one concept against two shells is two different plays", () => {
		const geom = geomFor(0);
		const endsFor = (shell: "twoHigh" | "blitz") =>
			assignRoutes({
				actors: lineUp(0),
				slots: offenseSlots("pass"),
				concept: PASS_CONCEPTS.find((c) => c.name === "Four Verticals")!,
				geom,
				protectDepth: 5.5,
				shell,
			})
				.filter((a) => a.job === "route")
				.map((a) => Math.round((a.x - geom.losX) * geom.dir));
		const deepAgainstTwo = endsFor("twoHigh");
		const hotAgainstBlitz = endsFor("blitz");
		assert.ok(
			Math.max(...hotAgainstBlitz) < Math.max(...deepAgainstTwo),
			`blitz ${hotAgainstBlitz} vs two-high ${deepAgainstTwo}`,
		);
	});
});
