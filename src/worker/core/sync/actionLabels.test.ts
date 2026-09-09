import { assert, describe, test } from "vitest";
import {
	isSimAuthorityLockedCall,
	isSingleGameSimLabel,
	isTimelineAdvanceLabel,
} from "./actionLabels.ts";

describe("isTimelineAdvanceLabel", () => {
	test("a whole day is an advance", () => {
		assert.isTrue(isTimelineAdvanceLabel("playMenu.sim"));
		assert.isTrue(isTimelineAdvanceLabel("playMenu.day"));
		assert.isTrue(isTimelineAdvanceLabel("playMenu.week"));
		assert.isTrue(isTimelineAdvanceLabel("toolsMenu.skipToPlayoffs"));
	});

	// THE REPLAYED GAME. A league-mate's own-game sim published under a label
	// the engine read as a whole-day advance, so when it lost the
	// compare-and-swap to somebody setting a lineup it was thrown away and the
	// device rolled back - and the fence slice it had claimed, never completed,
	// lapsed into crash recovery for the room's next scheduled sim to replay.
	// One game with the rest of the day still to play is a disjoint slice the
	// fence protects; a lost race for it rebases like any edit.
	test("a single game is NOT an advance, however it was drained", () => {
		assert.isFalse(isTimelineAdvanceLabel("actions.liveGame"));
		assert.isFalse(isTimelineAdvanceLabel("actions.simGame"));
		assert.isFalse(isTimelineAdvanceLabel("playMenu.simGame"));
	});

	test("a single game still needs authority to be CALLED", () => {
		// The pre-action guard routes these through the own-game carve-out; the
		// carve-out only exists because the call is locked in the first place.
		assert.isTrue(isSimAuthorityLockedCall("actions", "liveGame"));
		assert.isTrue(isSimAuthorityLockedCall("actions", "simGame"));
		assert.isTrue(isSingleGameSimLabel("playMenu.simGame"));
		assert.isFalse(isSingleGameSimLabel("playMenu.sim"));
	});

	test("an ordinary edit is not an advance", () => {
		assert.isFalse(isTimelineAdvanceLabel("main.updatePlayingTime"));
		assert.isFalse(isTimelineAdvanceLabel("nodot"));
	});

	// A league-mate changed a setting on the one device that was not in charge
	// of simming, got a toast, and went looking for the change on another
	// device. The settings page is every device's to use.
	test("league settings need no sim authority, and are not an advance", () => {
		assert.isFalse(isSimAuthorityLockedCall("main", "updateGameAttributes"));
		assert.isFalse(
			isSimAuthorityLockedCall("main", "updateGameAttributesGodMode"),
		);
		assert.isFalse(isTimelineAdvanceLabel("main.updateGameAttributesGodMode"));
		// What still is: the things that restructure the league or move time.
		assert.isTrue(isSimAuthorityLockedCall("main", "draftLottery"));
		assert.isTrue(isSimAuthorityLockedCall("main", "updateConfsDivs"));
	});
});
