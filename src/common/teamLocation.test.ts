import { assert, describe, test } from "vitest";
import { findCity, teamLocation } from "./teamLocation.ts";
import teamInfos from "./teamInfos.ts";

describe("teamLocation", () => {
	test("every built-in team region is on the map", () => {
		for (const { region } of Object.values(teamInfos)) {
			assert.isDefined(teamLocation({ region }), region);
		}
	});

	test("names that stand for a city", () => {
		assert.strictEqual(findCity("Golden State"), "San Francisco");
		assert.strictEqual(findCity("Utah"), "Salt Lake City");
		assert.strictEqual(findCity("saint louis"), "St. Louis");
		assert.strictEqual(findCity("Montréal"), "Montreal");
		assert.strictEqual(findCity("Portland, Oregon"), "Portland");
		assert.strictEqual(findCity("Portland, Maine"), "Portland, Maine");
		assert.strictEqual(findCity("Greater Boston"), "Boston");
		assert.strictEqual(findCity("Springfield"), "Springfield");
		assert.isUndefined(findCity("Gotham"));
		assert.isUndefined(findCity(""));
	});

	test("a home city counts only for the region it was set for", () => {
		const homeCity = { region: "Gotham", city: "Newark" };
		assert.strictEqual(
			teamLocation({ region: "Gotham", homeCity })?.city,
			"Newark",
		);
		assert.strictEqual(
			teamLocation({ region: "Chicago", homeCity })?.city,
			"Chicago",
		);
		assert.isUndefined(teamLocation({ region: "Gotham" }));
	});

	test("coordinates are where the city is", () => {
		const boston = teamLocation({ region: "Boston" })!;
		assert.closeTo(boston.latitude, 42.36, 0.2);
		assert.closeTo(boston.longitude, -71.06, 0.2);
		const springfield = teamLocation({ region: "Springfield" })!;
		// Massachusetts', where the game was invented.
		assert.closeTo(springfield.longitude, -72.6, 0.2);
	});
});
