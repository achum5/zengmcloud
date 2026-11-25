import { assert, describe, test } from "vitest";
import { pairKey, rankRivalries, type RivalryTeam } from "./rivalries.ts";

// Six teams, two divisions of three in one conference: 0-2 in one, 3-5 in
// the other. 0 and 1 share a city; the rest are far apart.
const teams: RivalryTeam[] = [0, 1, 2, 3, 4, 5].map((tid) => ({
	tid,
	cid: 0,
	did: tid < 3 ? 0 : 1,
}));
const locations = new Map([
	[0, { latitude: 34.05, longitude: -118.24 }],
	[1, { latitude: 34.0, longitude: -118.3 }],
	[2, { latitude: 47.6, longitude: -122.33 }],
	[3, { latitude: 42.36, longitude: -71.06 }],
	[4, { latitude: 25.76, longitude: -80.19 }],
	[5, { latitude: 41.88, longitude: -87.63 }],
]);
const draw = new Map(teams.map((t) => [t.tid, 0.5]));
const key = ([a, b]: [number, number]) => pairKey(a, b);

describe("rankRivalries", () => {
	test("the same city, then the same division", () => {
		const ranked = rankRivalries({ teams, locations, draw }).map(key);
		assert.strictEqual(ranked[0], "0-1");
		// Every division matchup before any other.
		const division = ["0-1", "0-2", "1-2", "3-4", "3-5", "4-5"];
		assert.sameMembers(ranked.slice(0, 6), division);
	});

	test("playoff history makes a rivalry", () => {
		const ranked = rankRivalries({
			teams,
			locations,
			draw,
			playoffs: new Map([["2-4", 1.25]]),
		}).map(key);
		assert.strictEqual(ranked[0], "2-4");
	});

	test("user teams against each other come first, with more than one", () => {
		const ranked = rankRivalries({
			teams,
			locations,
			draw,
			users: [2, 3, 5],
		}).map(key);
		assert.sameMembers(ranked.slice(0, 3), ["2-3", "2-5", "3-5"]);
		// One user team: nothing special.
		const alone = rankRivalries({ teams, locations, draw, users: [2] });
		assert.strictEqual(key(alone[0]!), "0-1");
	});

	test("between equals, the better teams", () => {
		const strong = new Map(draw);
		strong.set(3, 1);
		strong.set(4, 1);
		const ranked = rankRivalries({ teams, locations, draw: strong }).map(key);
		assert.isBelow(ranked.indexOf("3-4"), ranked.indexOf("4-5"));
		assert.isBelow(ranked.indexOf("3-4"), ranked.indexOf("0-2"));
	});
});
