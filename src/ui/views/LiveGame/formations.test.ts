import { assert, beforeEach, describe, test } from "vitest";
import { clearCourtRng, seedCourtRng } from "./courtRng.ts";
import {
	chooseDefenseFront,
	chooseOffenseFormation,
	DEFENSE_FRONTS,
	defenseSlots,
	OFFENSE_FORMATIONS,
	offenseSlots,
	specialTeamsDefense,
	specialTeamsOffense,
} from "./formations.ts";
import {
	dirFor,
	FIELD_LEN,
	FIELD_W,
	fieldX,
	MID_Y,
	placeFormation,
} from "./fieldSpots.ts";
import {
	SLOT_C,
	SLOT_QB,
	SLOT_RB,
	SLOT_RT,
	SLOT_TE,
	SLOT_WR_L,
	SLOT_WR_R,
	SLOT_WR_SLOT,
} from "./playbook.ts";

beforeEach(() => {
	seedCourtRng("formation-test");
	return () => {
		clearCourtRng();
	};
});

describe("every offensive formation", () => {
	// THE INVARIANT THE WHOLE PLAYBOOK RESTS ON. A concept names a job by its
	// slot, so if one formation put the quarterback somewhere other than slot 6
	// the tight end would start running the quarterback's drop.
	test("is eleven men in the same slot order", () => {
		for (const [key, formation] of Object.entries(OFFENSE_FORMATIONS)) {
			assert.strictEqual(formation.slots.length, 11, key);
			for (let i = SLOT_C; i <= SLOT_RT; i += 1) {
				assert.strictEqual(formation.slots[i]!.pos, "OL", `${key} slot ${i}`);
			}
			assert.strictEqual(formation.slots[SLOT_QB]!.pos, "QB", key);
			for (const i of [SLOT_TE, SLOT_RB, SLOT_WR_SLOT, SLOT_WR_L, SLOT_WR_R]) {
				assert.ok(
					["TE", "RB", "WR"].includes(formation.slots[i]!.pos),
					`${key} slot ${i} is a ${formation.slots[i]!.pos}`,
				);
			}
		}
	});

	test("puts nobody downfield before the snap and nobody off the field", () => {
		for (const [key, formation] of Object.entries(OFFENSE_FORMATIONS)) {
			for (const t of [0, 1] as const) {
				const dir = dirFor(t);
				const losX = fieldX(35, dir);
				for (const p of placeFormation(formation.slots, losX, dir, MID_Y)) {
					assert.ok((losX - p.x) * dir >= -0.001, `${key} lined up downfield`);
					assert.ok(p.x >= 0 && p.x <= FIELD_LEN, `${key} x`);
					assert.ok(p.y >= 0 && p.y <= FIELD_W, `${key} y`);
				}
			}
		}
	});

	test("empty really is empty and heavy really is heavy", () => {
		// Nobody in the backfield but the quarterback.
		const empty = OFFENSE_FORMATIONS.empty;
		assert.ok(empty.empty);
		const backfield = empty.slots.filter(
			(slot, i) => i !== SLOT_QB && slot.depth > 3,
		);
		assert.strictEqual(backfield.length, 0);
		// And a heavy set has more blockers than receivers outside the line.
		const heavy = OFFENSE_FORMATIONS.heavy;
		assert.ok(heavy.heavy);
		const wide = heavy.slots.filter((slot) => Math.abs(slot.across) > 16);
		assert.ok(wide.length <= 1, "a heavy set with the field spread");
	});

	test("trips puts three men to one side", () => {
		const eligible = OFFENSE_FORMATIONS.trips.slots.filter(
			(slot) => slot.pos === "WR" || slot.pos === "TE",
		);
		const oneSide = eligible.filter((slot) => slot.across > 8);
		assert.ok(oneSide.length >= 3, `only ${oneSide.length} to the trips side`);
	});
});

describe("calling the formation", () => {
	test("short yardage is heavy, never empty", () => {
		for (let i = 0; i < 60; i += 1) {
			const f = chooseOffenseFormation({
				running: false,
				down: 3,
				toGo: 1,
				scrimmage: 45,
			});
			assert.ok(!f.empty, `called ${f.name} on third and one`);
		}
	});

	test("the goal line is heavy whatever the distance says", () => {
		for (let i = 0; i < 40; i += 1) {
			const f = chooseOffenseFormation({
				running: false,
				down: 1,
				toGo: 10,
				scrimmage: 98,
			});
			assert.ok(f.heavy || f.name === "I-Form", `called ${f.name} on the two`);
		}
	});

	test("third and long spreads the field", () => {
		let spread = 0;
		for (let i = 0; i < 100; i += 1) {
			const f = chooseOffenseFormation({
				running: false,
				down: 3,
				toGo: 12,
				scrimmage: 40,
			});
			assert.ok(!f.heavy, `called ${f.name} on third and twelve`);
			if (f.empty || f.name === "Trips") {
				spread += 1;
			}
		}
		assert.ok(spread > 50, `only ${spread} of 100 spread out`);
	});

	test("a drive does not look like the same snap every time", () => {
		const seen = new Set<string>();
		for (let i = 0; i < 200; i += 1) {
			seen.add(
				chooseOffenseFormation({
					running: false,
					down: 1,
					toGo: 10,
					scrimmage: 40,
				}).name,
			);
		}
		assert.ok(seen.size >= 3, `only saw ${[...seen].join(", ")}`);
	});
});

describe("the defensive answer", () => {
	test("every front is eleven men, none of them behind the offense", () => {
		for (const [key, front] of Object.entries(DEFENSE_FRONTS)) {
			assert.strictEqual(front.slots.length, 11, key);
			for (const t of [0, 1] as const) {
				const dir = dirFor(t);
				const losX = fieldX(35, dir);
				for (const p of placeFormation(front.slots, losX, dir, MID_Y)) {
					assert.ok((p.x - losX) * dir >= -0.001, `${key} lined up offside`);
				}
			}
		}
	});

	test("three receivers get nickel, empty gets dime, the goal line gets the box", () => {
		const at = (offense: keyof typeof OFFENSE_FORMATIONS, o: any) =>
			chooseDefenseFront({
				offense: OFFENSE_FORMATIONS[offense],
				down: 1,
				toGo: 10,
				scrimmage: 40,
				...o,
			}).name;
		assert.strictEqual(at("shotgun", {}), "Nickel");
		assert.strictEqual(at("empty", {}), "Dime");
		assert.strictEqual(at("heavy", { scrimmage: 98, toGo: 1 }), "Goal Line");
		assert.strictEqual(at("heavy", {}), "4-3");
	});

	test("a heavy set is never answered with dime", () => {
		for (let toGo = 1; toGo <= 15; toGo += 1) {
			const front = chooseDefenseFront({
				offense: OFFENSE_FORMATIONS.heavy,
				down: 3,
				toGo,
				scrimmage: 40,
			});
			assert.notStrictEqual(front.name, "Dime");
		}
	});
});

describe("the special teams units", () => {
	test("each unit is eleven men", () => {
		for (const kind of ["punt", "kick", "kickoff", "kickoffReturn"] as const) {
			assert.strictEqual(specialTeamsOffense(kind)!.length, 11, kind);
			assert.strictEqual(specialTeamsDefense(kind)!.length, 11, kind);
		}
		// And a play from scrimmage is not a special teams unit.
		assert.strictEqual(specialTeamsOffense("pass"), undefined);
		assert.strictEqual(specialTeamsDefense("run"), undefined);
	});

	test("the default alignments fall back to scrimmage sets", () => {
		assert.strictEqual(offenseSlots("pass").length, 11);
		assert.strictEqual(offenseSlots("run").length, 11);
		assert.strictEqual(defenseSlots("pass").length, 11);
		// A punt asks for the punt unit, not a shotgun set.
		assert.ok(offenseSlots("punt").some((slot) => slot.pos === "P"));
	});
});
