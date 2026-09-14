import { assert, beforeEach, describe, test } from "vitest";
import { clearCourtRng, seedCourtRng } from "./courtRng.ts";
import {
	bezierAt,
	clampX,
	clampY,
	defenseSlots,
	dirFor,
	ENDZONE,
	FIELD_LEN,
	FIELD_W,
	fieldX,
	HASH_FAR,
	HASH_NEAR,
	MID_Y,
	offenseSlots,
	placeFormation,
	runControlPoints,
	snapAcross,
	synthEndPoint,
	synthLooseBall,
	toField,
} from "./fieldSpots.ts";

beforeEach(() => {
	// Every spot on the field is invented, so pin the stream or these assert
	// against a different fiction each run.
	seedCourtRng("field-test");
	return () => {
		clearCourtRng();
	};
});

describe("fieldX", () => {
	test("a team's own goal line is its 0 and the other end zone is its 100", () => {
		// Away (display team 0) attacks right: its own goal line is the left one.
		assert.strictEqual(fieldX(0, dirFor(0)), ENDZONE);
		assert.strictEqual(fieldX(100, dirFor(0)), FIELD_LEN - ENDZONE);
		// Home attacks left, so the same numbers land at the opposite ends.
		assert.strictEqual(fieldX(0, dirFor(1)), FIELD_LEN - ENDZONE);
		assert.strictEqual(fieldX(100, dirFor(1)), ENDZONE);
	});

	test("midfield is midfield whichever way you're going", () => {
		assert.strictEqual(fieldX(50, dirFor(0)), FIELD_LEN / 2);
		assert.strictEqual(fieldX(50, dirFor(1)), FIELD_LEN / 2);
	});
});

describe("toField", () => {
	test("depth is measured BEHIND the line, into the offense's own territory", () => {
		const los = fieldX(50, dirFor(0));
		// A quarterback five yards deep is five yards back toward his own goal.
		assert.strictEqual(toField(los, dirFor(0), 5, MID_Y).x, los - 5);
		// The same five yards for a team going the other way is the other side.
		assert.strictEqual(toField(los, dirFor(1), 5, MID_Y).x, los + 5);
	});

	test("negative depth crosses the line, which is where a defense stands", () => {
		const los = fieldX(50, dirFor(0));
		assert.ok(toField(los, dirFor(0), -6, MID_Y).x > los);
	});

	test("nothing is ever placed off the field", () => {
		const los = fieldX(98, dirFor(0));
		const deep = toField(los, dirFor(0), -40, 400);
		assert.ok(deep.x <= FIELD_LEN);
		assert.ok(deep.y <= FIELD_W);
		assert.strictEqual(clampX(-50), 0.6);
		assert.strictEqual(clampY(-50), 0.8);
	});
});

describe("snapAcross", () => {
	test("the ball is always spotted between the hashes, often on one", () => {
		let onAHash = 0;
		for (let i = 0; i < 200; i += 1) {
			const y = snapAcross();
			assert.ok(y >= HASH_NEAR - 0.001 && y <= HASH_FAR + 0.001);
			if (Math.abs(y - HASH_NEAR) < 0.001 || Math.abs(y - HASH_FAR) < 0.001) {
				onAHash += 1;
			}
		}
		// Roughly two plays in five start on a hash - enough that the field does
		// not always look like it's being played down the middle.
		assert.ok(onAHash > 40, `only ${onAHash} of 200 on a hash`);
	});
});

describe("placeFormation", () => {
	test("eleven men, all of them on the field", () => {
		for (const kind of ["pass", "run", "punt", "kick", "kickoff"] as const) {
			for (const t of [0, 1] as const) {
				const dir = dirFor(t);
				const los = fieldX(25, dir);
				for (const slots of [offenseSlots(kind), defenseSlots(kind)]) {
					assert.strictEqual(slots.length, 11, kind);
					for (const p of placeFormation(slots, los, dir, MID_Y)) {
						assert.ok(p.x >= 0 && p.x <= FIELD_LEN, `${kind} x ${p.x}`);
						assert.ok(p.y >= 0 && p.y <= FIELD_W, `${kind} y ${p.y}`);
					}
				}
			}
		}
	});

	test("the offense is behind the ball and the defense in front of it", () => {
		const dir = dirFor(0);
		const los = fieldX(40, dir);
		const off = placeFormation(offenseSlots("pass"), los, dir, MID_Y);
		const def = placeFormation(defenseSlots("pass"), los, dir, MID_Y);
		// Nobody on offense is downfield of the line before the snap.
		assert.ok(Math.max(...off.map((p) => p.x)) <= los + 0.001);
		// Every defender is on the other side of it.
		assert.ok(Math.min(...def.map((p) => p.x)) >= los - 0.001);
	});

	test("a formation slides across to wherever the ball was spotted", () => {
		const dir = dirFor(0);
		const los = fieldX(40, dir);
		const middle = placeFormation(offenseSlots("pass"), los, dir, MID_Y);
		const hash = placeFormation(offenseSlots("pass"), los, dir, HASH_NEAR);
		assert.ok(hash[0]!.y < middle[0]!.y);
	});
});

describe("runControlPoints", () => {
	test("a carrier weaves rather than sliding down the hypotenuse", () => {
		const from = { x: 30, y: MID_Y };
		const to = { x: 62, y: MID_Y + 9 };
		const [c1, c2] = runControlPoints(from, to);
		// At least one control point is meaningfully off the straight line.
		const straightAt = (x: number) =>
			from.y + ((to.y - from.y) * (x - from.x)) / (to.x - from.x);
		const off1 = Math.abs(c1.y - straightAt(c1.x));
		const off2 = Math.abs(c2.y - straightAt(c2.x));
		assert.ok(Math.max(off1, off2) > 0.5, `${off1} / ${off2}`);
	});

	test("a two-yard plunge barely wiggles", () => {
		const from = { x: 60, y: MID_Y };
		const to = { x: 62, y: MID_Y };
		const [c1, c2] = runControlPoints(from, to);
		assert.ok(Math.abs(c1.y - MID_Y) < 1);
		assert.ok(Math.abs(c2.y - MID_Y) < 1);
	});
});

describe("bezierAt", () => {
	test("the curve starts where the play starts and ends where it ends", () => {
		const from = { x: 20, y: 10 };
		const to = { x: 80, y: 40 };
		const [c1, c2] = runControlPoints(from, to);
		assert.deepStrictEqual(bezierAt(from, c1, c2, to, 0), from);
		const end = bezierAt(from, c1, c2, to, 1);
		assert.ok(Math.abs(end.x - to.x) < 1e-9);
		assert.ok(Math.abs(end.y - to.y) < 1e-9);
	});
});

describe("synthEndPoint", () => {
	test("yards gained move the ball toward the end zone being attacked", () => {
		for (const t of [0, 1] as const) {
			const dir = dirFor(t);
			const los = fieldX(40, dir);
			const gain = synthEndPoint(los, dir, 12, MID_Y, 3);
			const loss = synthEndPoint(los, dir, -6, MID_Y, 3);
			// "Toward the end zone" is +x for the away team and -x for the home one.
			assert.ok((gain.x - los) * dir > 0);
			assert.ok((loss.x - los) * dir < 0);
		}
	});

	test("a gain that would run off the back of the end zone stops at the field", () => {
		const dir = dirFor(0);
		const los = fieldX(95, dir);
		const end = synthEndPoint(los, dir, 40, MID_Y, 3);
		assert.ok(end.x <= FIELD_LEN);
	});
});

describe("synthLooseBall", () => {
	test("a loose ball lands somewhere nobody meant it to, still in the stadium", () => {
		const near = { x: 60, y: MID_Y };
		for (let i = 0; i < 50; i += 1) {
			const p = synthLooseBall(near, 1, 5);
			assert.ok(p.x !== near.x || p.y !== near.y);
			assert.ok(p.x >= 0 && p.x <= FIELD_LEN);
			assert.ok(p.y >= 0 && p.y <= FIELD_W);
		}
	});
});
