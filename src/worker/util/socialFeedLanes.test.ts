import { assert, describe, test } from "vitest";
import { LANE_SHARE, laneCaps, laneOf } from "./socialFeed.ts";

// WHO THE TIMELINE IS MADE OF. Left to interest alone a day was a quarter
// local radio and a seventh official team accounts, with players at one post
// in twenty. The lanes are what make it mostly fans and the beat, with the
// news breakers and the players themselves in it too.
describe("feed lanes", () => {
	test("every account lands in exactly one lane", () => {
		assert.strictEqual(
			laneOf({ kind: "player", archetypeId: "player" }),
			"player",
		);
		assert.strictEqual(
			laneOf({ kind: "team", archetypeId: "teamOfficial" }),
			"team",
		);
		for (const fan of ["homerFan", "doomerFan", "casualFan", "troll"]) {
			assert.strictEqual(
				laneOf({ kind: "media", archetypeId: fan, tid: 1 }),
				"fan",
			);
		}
		assert.strictEqual(
			laneOf({ kind: "media", archetypeId: "beatWriter", tid: 1 }),
			"beat",
		);
		// The local film room covers one team for a living too.
		assert.strictEqual(
			laneOf({ kind: "media", archetypeId: "analytics", tid: 1 }),
			"beat",
		);
		assert.strictEqual(
			laneOf({ kind: "media", archetypeId: "localRadio", tid: 1 }),
			"radio",
		);
		// The national lane is the accounts with no team at all.
		for (const nat of [
			"insider",
			"aggregator",
			"nationalPundit",
			"analytics",
		]) {
			assert.strictEqual(
				laneOf({ kind: "media", archetypeId: nat }),
				"national",
			);
		}
	});

	test("fans and the beat are most of the day; franchises and radio are not", () => {
		assert.isAbove(LANE_SHARE.fan, LANE_SHARE.beat);
		assert.isAbove(LANE_SHARE.beat, LANE_SHARE.team);
		assert.isAbove(LANE_SHARE.player, LANE_SHARE.team);
		assert.isAbove(LANE_SHARE.national, LANE_SHARE.team);
		assert.isAbove(LANE_SHARE.fan + LANE_SHARE.beat + LANE_SHARE.national, 0.6);
		const total = Object.values(LANE_SHARE).reduce((a, b) => a + b, 0);
		assert.closeTo(total, 1, 0.01);
	});

	test("caps cover the day and never starve a lane", () => {
		const caps = laneCaps(45);
		const sum = Object.values(caps).reduce((a, b) => a + b, 0);
		assert.isAtLeast(sum, 45);
		for (const cap of Object.values(caps)) {
			assert.isAtLeast(cap, 1);
		}
		for (const cap of Object.values(laneCaps(0))) {
			assert.strictEqual(cap, 1);
		}
	});
});
