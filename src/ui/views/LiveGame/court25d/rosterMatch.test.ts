import { assert, test } from "vitest";
import { eventsMatchRoster } from "./rosterMatch.ts";

const roster = [{ pid: 1 }, { pid: 2 }, { pid: 3 }];

test("events from this game match", () => {
	assert.isTrue(
		eventsMatchRoster(
			[{ type: "fga", pid: 1 }, { type: "clock" }, { type: "drb", pid: 3 }],
			roster,
		),
	);
});

test("one player from another game is enough to reject", () => {
	assert.isFalse(
		eventsMatchRoster(
			[
				{ type: "fga", pid: 1 },
				{ type: "ft", pid: 99 },
			],
			roster,
		),
	);
});

test("no roster yet never matches", () => {
	assert.isFalse(eventsMatchRoster([{ type: "fga", pid: 1 }], []));
});
