import { assert, describe, test } from "vitest";
import {
	afterTypingShift,
	KEY_LAYERS,
	nextShiftState,
	shiftedKey,
	type ShiftState,
} from "./OnScreenKeyboard.tsx";

describe("nextShiftState", () => {
	test("one tap arms it for a single letter", () => {
		assert.strictEqual(nextShiftState("off", false), "once");
	});

	test("tapping again puts it away", () => {
		assert.strictEqual(nextShiftState("once", false), "off");
	});

	test("two quick taps lock it", () => {
		assert.strictEqual(nextShiftState("once", true), "lock");
	});

	// The one asymmetry, and it is deliberate: a locked shift can only be
	// released by tapping it, so a double tap must not be able to land on
	// "once" - there would be no way back to lock without a third tap.
	test("a locked shift releases on any tap", () => {
		assert.strictEqual(nextShiftState("lock", false), "off");
		assert.strictEqual(nextShiftState("lock", true), "off");
	});
});

describe("afterTypingShift", () => {
	test("a one-shot shift is spent by the letter it capitalized", () => {
		assert.strictEqual(afterTypingShift("once"), "off");
	});

	test("caps lock survives typing", () => {
		assert.strictEqual(afterTypingShift("lock"), "lock");
	});

	test("no shift stays no shift", () => {
		assert.strictEqual(afterTypingShift("off"), "off");
	});
});

describe("shiftedKey", () => {
	test("unshifted letters are lowercase", () => {
		assert.strictEqual(shiftedKey("q", "off"), "q");
	});

	test("both shift states capitalize", () => {
		for (const shift of ["once", "lock"] as ShiftState[]) {
			assert.strictEqual(shiftedKey("q", shift), "Q");
		}
	});
});

describe("KEY_LAYERS", () => {
	test("the letter layer is a qwerty keyboard, every letter once", () => {
		const letters = KEY_LAYERS.letters.flat();
		assert.strictEqual(letters.length, 26);
		assert.deepStrictEqual(
			[...letters].sort(),
			"abcdefghijklmnopqrstuvwxyz".split(""),
		);
	});

	test("every layer has the three rows the layout code expects", () => {
		for (const rows of Object.values(KEY_LAYERS)) {
			assert.strictEqual(rows.length, 3);
			for (const row of rows) {
				assert.ok(row.length > 0);
			}
		}
	});

	// A row wider than the letter layer's widest would overflow a phone, since
	// every key in a row takes an equal share of it.
	test("no row is wider than the widest letter row", () => {
		const widest = Math.max(...KEY_LAYERS.letters.map((row) => row.length));
		for (const rows of Object.values(KEY_LAYERS)) {
			for (const row of rows) {
				assert.ok(row.length <= widest, row.join(""));
			}
		}
	});

	test("the digits are all reachable", () => {
		const numbers = KEY_LAYERS.numbers.flat();
		for (const digit of "0123456789") {
			assert.ok(numbers.includes(digit), digit);
		}
	});
});
