import { assert, test } from "vitest";
import { venueFor } from "./venue.ts";

const teams = Array.from({ length: 400 }, (_, i) => ({
	region: `City ${i}`,
	name: `Team ${i}`,
	abbrev: `T${i}`,
}));

test("a team plays at the same place every home game", () => {
	for (const t of teams.slice(0, 20)) {
		assert.deepEqual(venueFor({ ...t }), venueFor({ ...t }));
	}
});

test("about a third of teams under a roof, on turf; outdoors, mostly grass", () => {
	const venues = teams.map(venueFor);
	const domes = venues.filter((v) => v.roof === "dome");
	assert.isAbove(domes.length / venues.length, 0.2);
	assert.isBelow(domes.length / venues.length, 0.4);
	assert.isTrue(domes.every((v) => v.turf === "artificial"));
	const open = venues.filter((v) => v.roof === "open");
	assert.isAbove(
		open.filter((v) => v.turf === "grass").length / open.length,
		0.5,
	);
});
