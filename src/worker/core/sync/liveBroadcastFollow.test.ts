import { assert, describe, test } from "vitest";
import { decideFollowAction } from "./liveBroadcastFollow.ts";

const live = { startedAt: 1000, gameOver: false };
const finished = { startedAt: 1000, gameOver: true };

describe("decideFollowAction", () => {
	test("a broadcast this device has never seen pulls it in", () => {
		assert.strictEqual(decideFollowAction(live, undefined), "join");
	});

	test("who ran the sim makes no difference - there is only one kind now", () => {
		// The whole point of the normalization: the decision cannot see, and does
		// not ask, whether the simmer was the device in charge of simming.
		assert.strictEqual(decideFollowAction(live, undefined), "join");
		assert.strictEqual(decideFollowAction(finished, undefined), "join");
	});

	test("the next broadcast pulls it in again, even after leaving the last", () => {
		assert.strictEqual(
			decideFollowAction(
				{ startedAt: 2000, gameOver: false },
				{ startedAt: 1000, left: true },
			),
			"join",
		);
	});

	test("while watching, heartbeats only move the cursor - never re-navigate", () => {
		assert.strictEqual(decideFollowAction(live, { startedAt: 1000 }), "cursor");
		assert.strictEqual(
			decideFollowAction(finished, { startedAt: 1000 }),
			"cursor",
		);
	});

	test("after leaving, heartbeats offer the way back instead of taking it", () => {
		// This is what makes Leave stick: the broadcaster is still heartbeating
		// several times a second, and every one of those used to be a chance to
		// drag the viewer back onto the live game page.
		assert.strictEqual(
			decideFollowAction(live, { startedAt: 1000, left: true }),
			"pill",
		);
	});

	test("no invitation back into a game that has already gone final", () => {
		assert.strictEqual(
			decideFollowAction(finished, { startedAt: 1000, left: true }),
			"ignore",
		);
	});
});
