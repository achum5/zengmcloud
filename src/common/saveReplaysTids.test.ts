import { assert, describe, test } from "vitest";
import {
	isValidSaveReplaysTid,
	SAVE_REPLAYS_ALL_PLAYOFFS,
	SAVE_REPLAYS_ALL_STAR,
	SAVE_REPLAYS_DRAMATIC,
	SAVE_REPLAYS_SENTINELS,
} from "./constants.ts";

describe("isValidSaveReplaysTid", () => {
	test("real team IDs are fine", () => {
		for (const tid of [0, 1, 29, 400]) {
			assert.ok(isValidSaveReplaysTid(tid), String(tid));
		}
	});

	// THE BUG THIS CLOSES. The settings validator hardcoded a floor of -2, so
	// the -3 written by the form's own "Feats & game winners" button was
	// rejected on save - "Must contain only team ID numbers" - even though the
	// sim honored it. Every option the form can produce has to survive its own
	// validator.
	test("every sentinel the form can write is accepted", () => {
		for (const sentinel of SAVE_REPLAYS_SENTINELS) {
			assert.ok(isValidSaveReplaysTid(sentinel), String(sentinel));
		}
	});

	test("the three sentinels are the three the code names", () => {
		assert.deepStrictEqual(
			[...SAVE_REPLAYS_SENTINELS].sort((a, b) => b - a),
			[SAVE_REPLAYS_ALL_STAR, SAVE_REPLAYS_ALL_PLAYOFFS, SAVE_REPLAYS_DRAMATIC],
		);
	});

	// A sentinel means something specific to the sim; an unrecognized negative
	// number means nothing, so it must not be storable.
	test("a negative that is not a sentinel is rejected", () => {
		assert.ok(!isValidSaveReplaysTid(-4));
		assert.ok(!isValidSaveReplaysTid(-99));
	});

	test("non-integers are rejected", () => {
		for (const bad of [1.5, "0", null, undefined, Number.NaN, [], {}]) {
			assert.ok(!isValidSaveReplaysTid(bad), JSON.stringify(bad));
		}
	});

	test("sentinels never collide with a real team ID", () => {
		for (const sentinel of SAVE_REPLAYS_SENTINELS) {
			assert.ok(sentinel < 0, String(sentinel));
		}
		assert.strictEqual(
			new Set(SAVE_REPLAYS_SENTINELS).size,
			SAVE_REPLAYS_SENTINELS.length,
		);
	});
});
