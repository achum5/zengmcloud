import { assert, test } from "vitest";
import { broadcastRole } from "./broadcastRole.ts";

const broadcast = (isBroadcaster: boolean) => ({
	active: true,
	gid: 7,
	byName: "Alex",
	isBroadcaster,
	startedAt: 1,
	cursor: 40,
	paused: false,
	gameOver: false,
});

test("a league-mate's live sim makes this page a follower", () => {
	assert.deepStrictEqual(broadcastRole(broadcast(false), false), {
		isFollower: true,
		isBroadcaster: false,
	});
	assert.deepStrictEqual(broadcastRole(broadcast(true), false), {
		isFollower: false,
		isBroadcaster: true,
	});
	assert.deepStrictEqual(broadcastRole(undefined, false), {
		isFollower: false,
		isBroadcaster: false,
	});
});

test("a replay is this device's own, whatever is being broadcast", () => {
	for (const mp of [broadcast(false), broadcast(true), undefined]) {
		assert.deepStrictEqual(broadcastRole(mp, true), {
			isFollower: false,
			isBroadcaster: false,
		});
	}
});
