import { assert, test } from "vitest";
import { MAX_LIVE_GAME_CHUNKS, validLiveGameRef } from "./liveGameRef.ts";

test("only a whole gid and a sane chunk count make doc ids", () => {
	assert.isTrue(validLiveGameRef(123, 1));
	assert.isTrue(validLiveGameRef(0, MAX_LIVE_GAME_CHUNKS));
	assert.isFalse(validLiveGameRef(123, 0));
	assert.isFalse(validLiveGameRef(123, MAX_LIVE_GAME_CHUNKS + 1));
	assert.isFalse(validLiveGameRef(123, 1e9));
	assert.isFalse(validLiveGameRef(123, 1.5));
	assert.isFalse(validLiveGameRef(Number.NaN, 1));
	assert.isFalse(validLiveGameRef(1.5, 1));
	assert.isFalse(validLiveGameRef(Infinity, 1));
	assert.isFalse(validLiveGameRef("v2state", 1));
	assert.isFalse(validLiveGameRef(undefined, 1));
	assert.isFalse(validLiveGameRef(123, "1"));
});
