import { assert, beforeEach, describe, test } from "vitest";
import { clearCourtRng, seedCourtRng } from "./courtRng.ts";
import {
	assignHuddle,
	assignScramble,
	assignSpecialTeams,
	carrierPath,
	type SpecialTeamsKind,
} from "./specialTeams.ts";
import { specialTeamsDefense, specialTeamsOffense } from "./formations.ts";
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

beforeEach(() => {
	seedCourtRng("special-teams-test");
	return () => {
		clearCourtRng();
	};
});

const geom = { losX: fieldX(30, dirFor(0)), dir: dirFor(0), ballAcross: MID_Y };

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

const stage = (kind: SpecialTeamsKind, unit: "punt" | "kick" | "kickoff" | "kickoffReturn") => {
	const kickingSlots = specialTeamsOffense(unit)!;
	const receivingSlots = specialTeamsDefense(unit)!;
	const launch = { x: geom.losX - 14, y: MID_Y };
	const landing = { x: geom.losX + 42, y: MID_Y + 4 };
	return assignSpecialTeams({
		kind,
		kicking: lineUp(kickingSlots, 0, 100),
		kickingSlots,
		receiving: lineUp(receivingSlots, 1, 200),
		receivingSlots,
		geom,
		launch,
		landing,
		carrier: kind === "return" ? carrierPath(launch, landing) : undefined,
	});
};

const moved = (before: FieldActor[], after: FieldActor[]) =>
	after.filter((a, i) => {
		const b = before[i]!;
		return Math.hypot(a.x - b.x, a.y - b.y) > 1;
	}).length;

describe("every special teams unit", () => {
	test("gives nearly everybody something to do", () => {
		for (const [kind, unit] of [
			["punt", "punt"],
			["kick", "kick"],
			["kickoff", "kickoff"],
			["return", "kickoffReturn"],
		] as const) {
			const staged = stage(kind, unit);
			for (const side of [staged.kicking, staged.receiving]) {
				assert.strictEqual(side.length, 11, kind);
				const idle = side.filter((a) => !a.path || a.path.length < 2);
				assert.ok(idle.length <= 2, `${kind} left ${idle.length} standing`);
			}
		}
	});

	test("keeps everybody inside the stadium", () => {
		for (const [kind, unit] of [
			["punt", "punt"],
			["kick", "kick"],
			["kickoff", "kickoff"],
			["return", "kickoffReturn"],
		] as const) {
			const staged = stage(kind, unit);
			for (const side of [staged.kicking, staged.receiving]) {
				for (const a of side) {
					for (const p of a.path ?? []) {
						assert.ok(p.x >= 0 && p.x <= FIELD_LEN, `${kind} x ${p.x}`);
						assert.ok(p.y >= 0 && p.y <= FIELD_W, `${kind} y ${p.y}`);
					}
				}
			}
		}
	});
});

describe("the punt", () => {
	const staged = () => stage("punt", "punt");

	test("the punter stays back and everybody else goes down the field", () => {
		const slots = specialTeamsOffense("punt")!;
		const before = lineUp(slots, 0, 100);
		const after = staged().kicking;
		const punter = after.find((a) => slots[a.slotIndex!]!.pos === "P")!;
		// He is still behind the line, not covering his own punt.
		assert.ok((geom.losX - punter.x) * geom.dir > 8);
		// And the coverage is genuinely downfield.
		const downfield = after.filter((a) => (a.x - geom.losX) * geom.dir > 20);
		assert.ok(downfield.length >= 8, `only ${downfield.length} covered`);
		assert.ok(moved(before, after) >= 10);
	});

	test("the gunners are gone before anybody else", () => {
		const slots = specialTeamsOffense("punt")!;
		const after = staged().kicking;
		const gunners = after.filter((a) => Math.abs(slots[a.slotIndex!]!.across) > 20);
		assert.strictEqual(gunners.length, 2);
		for (const g of gunners) {
			assert.ok(
				(g.delay ?? 0) === 0,
				"a gunner who waited for the rest of the line",
			);
		}
		const interior = after.filter(
			(a) => Math.abs(slots[a.slotIndex!]!.across) <= 8 && a.delay,
		);
		assert.ok(interior.length >= 4, "nobody held up the rush");
	});

	test("the return team gets a man under the ball and a wall in front of him", () => {
		const landing = { x: geom.losX + 42, y: MID_Y + 4 };
		const after = staged().receiving;
		const catcher = after.filter(
			(a) => Math.hypot(a.x - landing.x, a.y - landing.y) < 2,
		);
		assert.strictEqual(catcher.length, 1, "nobody, or everybody, caught it");
		const wall = after.filter(
			(a) => (a.x - landing.x) * geom.dir > 2 && (a.x - landing.x) * geom.dir < 20,
		);
		assert.ok(wall.length >= 3, `only ${wall.length} men set up`);
	});
});

describe("the kickoff", () => {
	test("the cover team goes down in a wave, keeping its lanes", () => {
		const after = stage("kickoff", "kickoff").kicking;
		const delays = after.map((a) => a.delay ?? 0);
		assert.ok(Math.max(...delays) > Math.min(...delays), "everybody left at once");
		// They stay spread across the field rather than converging into a lump.
		const cover = after.filter((a) => (a.x - geom.losX) * geom.dir > 20);
		const spread = Math.max(...cover.map((a) => a.y)) - Math.min(...cover.map((a) => a.y));
		assert.ok(spread > 15, `the cover team bunched into ${spread} yards`);
	});

	test("the return team turns and builds in front of the returner", () => {
		const landing = { x: geom.losX + 42, y: MID_Y + 4 };
		const after = stage("kickoff", "kickoff").receiving;
		const ahead = after.filter((a) => (a.x - landing.x) * geom.dir > 4);
		assert.ok(ahead.length >= 6, `only ${ahead.length} got in front`);
	});
});

describe("the place kick", () => {
	test("the line barely moves and the edge rushers come round it", () => {
		const staged = stage("kick", "kick");
		const slots = specialTeamsDefense("kick")!;
		const before = lineUp(specialTeamsOffense("kick")!, 0, 100);
		// Protection holds: nobody on the kicking team travels far.
		for (const [i, a] of staged.kicking.entries()) {
			const b = before[i]!;
			assert.ok(
				Math.hypot(a.x - b.x, a.y - b.y) < 4,
				"the field goal line went for a walk",
			);
		}
		// And the men on the edge are the ones who actually get somewhere.
		const launch = { x: geom.losX - 14, y: MID_Y };
		const edge = staged.receiving.filter(
			(a) => Math.abs(slots[a.slotIndex!]!.across) > 9,
		);
		const inside = staged.receiving.filter(
			(a) => Math.abs(slots[a.slotIndex!]!.across) <= 9,
		);
		const near = (list: FieldActor[]) =>
			Math.min(...list.map((a) => Math.hypot(a.x - launch.x, a.y - launch.y)));
		assert.ok(near(edge) < near(inside), "the interior beat the edge round");
	});
});

describe("the return", () => {
	test("blockers get in front of the ball and chasers close from behind", () => {
		const landing = { x: geom.losX + 42, y: MID_Y + 4 };
		const staged = stage("return", "kickoffReturn");
		const ahead = staged.kicking.filter(
			(a) => (a.x - landing.x) * geom.dir > 0,
		);
		assert.ok(ahead.length >= 6, `only ${ahead.length} blockers led the way`);
		const before = lineUp(specialTeamsDefense("kickoffReturn")!, 1, 200);
		for (const [i, a] of staged.receiving.entries()) {
			const b = before[i]!;
			const closedIn =
				Math.hypot(a.x - landing.x, a.y - landing.y) <
				Math.hypot(b.x - landing.x, b.y - landing.y);
			assert.ok(closedIn, "a chaser who ran away from the ball");
		}
	});
});

describe("carrierPath", () => {
	test("starts where he caught it, ends where he was brought down, and weaves", () => {
		const from = { x: 20, y: MID_Y };
		const to = { x: 70, y: MID_Y + 8 };
		const path = carrierPath(from, to);
		assert.deepStrictEqual(path[0], from);
		assert.ok(Math.abs(path.at(-1)!.x - to.x) < 1e-9);
		assert.ok(Math.abs(path.at(-1)!.y - to.y) < 1e-9);
		// Somewhere in the middle he is off the straight line between them.
		const straightAt = (x: number) =>
			from.y + ((to.y - from.y) * (x - from.x)) / (to.x - from.x);
		const off = Math.max(...path.map((p) => Math.abs(p.y - straightAt(p.x))));
		assert.ok(off > 0.5, `the return was a straight line (${off})`);
	});
});

describe("a loose ball", () => {
	const actors = (): FieldActor[] =>
		Array.from({ length: 11 }, (_, i) => ({
			pid: 300 + i,
			name: `X${i}`,
			x: geom.losX + (i - 5) * 2,
			y: MID_Y + ((i % 3) - 1) * 6,
			role: "onField" as const,
			slotIndex: i,
			t: 0 as const,
		}));

	test("the men near it go after it and the men far from it do not", () => {
		const ball = { x: geom.losX + 1, y: MID_Y };
		const before = actors();
		const after = assignScramble({ actors: before, ball, count: 4 });
		const chasing = after.filter((a) => a.path);
		assert.strictEqual(chasing.length, 4, "everybody piled on, or nobody did");
		// And the four are the four nearest.
		const nearestFour = before
			.map((a, i) => ({ i, d: Math.hypot(a.x - ball.x, a.y - ball.y) }))
			.sort((a, b) => a.d - b.d)
			.slice(0, 4)
			.map(({ i }) => before[i]!.pid);
		for (const a of chasing) {
			assert.ok(nearestFour.includes(a.pid), `${a.pid} came from miles away`);
		}
	});

	test("they end up on the ball but not on top of each other", () => {
		const ball = { x: geom.losX + 1, y: MID_Y };
		const after = assignScramble({ actors: actors(), ball, count: 5 }).filter(
			(a) => a.path,
		);
		for (const a of after) {
			assert.ok(Math.hypot(a.x - ball.x, a.y - ball.y) < 3.5, "missed the ball");
		}
		for (const [i, a] of after.entries()) {
			for (const b of after.slice(i + 1)) {
				assert.ok(
					Math.hypot(a.x - b.x, a.y - b.y) > 0.01,
					"two men in the same place",
				);
			}
		}
	});

	test("the nearest man is on it first", () => {
		const ball = { x: geom.losX, y: MID_Y };
		const after = assignScramble({ actors: actors(), ball, count: 5 }).filter(
			(a) => a.path,
		);
		const delays = after.map((a) => a.delay ?? 0);
		assert.ok(Math.max(...delays) > Math.min(...delays), "everybody arrived together");
	});
});

describe("a stoppage", () => {
	const eleven = (): FieldActor[] =>
		Array.from({ length: 11 }, (_, i) => ({
			pid: 400 + i,
			name: `H${i}`,
			x: geom.losX + (i - 5) * 1.5,
			y: MID_Y + ((i % 5) - 2) * 5,
			role: "onField" as const,
			slotIndex: i,
			t: 0 as const,
		}));

	test("everybody goes to the huddle and nobody stands where the whistle caught him", () => {
		const before = eleven();
		const after = assignHuddle({ actors: before, geom, depth: 9, across: -13 });
		assert.strictEqual(after.length, 11);
		for (const [i, a] of after.entries()) {
			assert.ok(a.path && a.path.length === 2, "a man who did not move");
			assert.deepStrictEqual(a.path![0], {
				x: before[i]!.x,
				y: before[i]!.y,
			});
		}
	});

	test("the huddle is a ring, not a pile", () => {
		const after = assignHuddle({
			actors: eleven(),
			geom,
			depth: 9,
			across: -13,
		});
		for (const [i, a] of after.entries()) {
			for (const b of after.slice(i + 1)) {
				assert.ok(
					Math.hypot(a.x - b.x, a.y - b.y) > 0.5,
					"two men in the same spot",
				);
			}
		}
		// And it is behind the ball on the side it was told to form.
		const centreX = after.reduce((sum, a) => sum + a.x, 0) / after.length;
		assert.ok((geom.losX - centreX) * geom.dir > 5);
	});

	test("the huddle stays on the field", () => {
		for (const across of [-25, 0, 25]) {
			for (const ballAcross of [4, MID_Y, FIELD_W - 4]) {
				const after = assignHuddle({
					actors: eleven(),
					geom: { ...geom, ballAcross },
					depth: 9,
					across,
				});
				for (const a of after) {
					assert.ok(a.y >= 0 && a.y <= FIELD_W, `huddle off the field: ${a.y}`);
				}
			}
		}
	});
});
