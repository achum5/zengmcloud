import { assert, beforeEach, describe, test } from "vitest";
import api from "./index.ts";
import { local } from "../util/index.ts";

// ---------------------------------------------------------------------------
// Only the page playing the CURRENT live sim may declare it over.
//
// Every LiveGame page reports "over" - on unmount and on reaching its final
// play - and that includes finished games parked in other tabs and REPLAYS of
// old games. The report used to clear liveGameInProgress unconditionally, so
// any of those landing after play.ts set the flag un-hid everything the flag
// exists to hide mid-playback: the phase text, the ready-up control, the score
// ticker. That is how a season-ending live sim had "2005 draft lottery" and
// the ready-up button on screen at Q1 of Game 4 of the finals.
//
// The clear travels with liveSimGid: accepting a report clears it, ignoring
// one leaves it in place. Asserting on liveSimGid therefore pins exactly the
// accept/ignore decision.
// ---------------------------------------------------------------------------

const onLiveSimOver = (api as any).main.onLiveSimOver as (
	gid?: number,
) => Promise<void>;

describe("onLiveSimOver is scoped to the live sim actually in progress", () => {
	beforeEach(() => {
		local.liveSimGid = undefined;
	});

	test("a different game's page cannot end the current live sim", async () => {
		// Game 55 is mid-playback; a replay of old game 99 hits its final play in
		// another tab (or this one, moments earlier).
		local.liveSimGid = 55;
		await onLiveSimOver(99);
		assert.strictEqual(
			local.liveSimGid,
			55,
			"a replay/stale page's 'over' report ended a live sim it knows nothing about",
		);
	});

	test("the current game's page ends it normally", async () => {
		local.liveSimGid = 55;
		await onLiveSimOver(55);
		assert.strictEqual(local.liveSimGid, undefined);
	});

	test("a report with no gid still clears, so the flag can never wedge on", async () => {
		// The only no-gid reporter is a page that never received its game data
		// (the user bailed on a pending live sim). Swallowing that would leave
		// "Live game in progress" stuck in the header forever.
		local.liveSimGid = 55;
		await onLiveSimOver(undefined);
		assert.strictEqual(local.liveSimGid, undefined);
	});

	test("with nothing in progress, any report is accepted as before", async () => {
		// Followers never set liveSimGid (play.ts does not run there), and their
		// pages must still be able to release the flag their own sync path set.
		local.liveSimGid = undefined;
		await onLiveSimOver(99);
		assert.strictEqual(local.liveSimGid, undefined);
	});
});
