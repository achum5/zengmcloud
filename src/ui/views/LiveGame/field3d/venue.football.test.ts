import { assert, test } from "vitest";
import { inShadow, venueFor } from "./venue.ts";

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
	const venues = teams.map((t) => venueFor(t));
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

test("what a team has set for its stadium is how it is, and what it has not stays as it was", () => {
	for (const t of teams.slice(0, 40)) {
		const picked = venueFor(t, { gid: 7 });
		const zones = venueFor(
			{ ...t, stadium: { endZones: "striped" } },
			{ gid: 7 },
		);
		assert.strictEqual(zones.endZones, "striped");
		assert.strictEqual(zones.roof, picked.roof);
		assert.strictEqual(zones.turf, picked.turf);
		assert.strictEqual(zones.sky, picked.sky);
		const dome = venueFor({ ...t, stadium: { roof: "dome" } }, { gid: 7 });
		assert.strictEqual(dome.roof, "dome");
		assert.strictEqual(dome.endZones, picked.endZones);
		assert.isUndefined(dome.shadow);
		const grass = venueFor(
			{ ...t, stadium: { roof: "dome", turf: "grass", sky: "night" } },
			{ gid: 7 },
		);
		assert.strictEqual(grass.turf, "grass");
		assert.strictEqual(grass.sky, "night");
	}
});

test("prime time under the lights, the rest in the afternoon - or all one way, if the team says - and the shadow only in the sun", () => {
	const t = teams.find((x) => venueFor(x).roof === "open")!;
	const days = Array.from({ length: 400 }, (_, gid) => venueFor(t, { gid }));
	assert.isTrue(days.every((v) => v.sky === "day"));
	const nights = Array.from({ length: 40 }, (_, gid) =>
		venueFor(t, { gid, primeTime: true }),
	);
	assert.isTrue(nights.every((v) => v.sky === "night" && !v.shadow));
	const shaded = days.filter((v) => v.shadow).length / days.length;
	assert.isAbove(shaded, 0.4);
	assert.isBelow(shaded, 0.9);
	for (const v of days) {
		if (v.shadow) {
			// (The near side in it, the far side in the sun.)
			assert.isTrue(inShadow(v.shadow, { x: 180, y: 200 }));
			assert.isFalse(inShadow(v.shadow, { x: 180, y: -10 }));
		}
	}
	for (const sky of ["day", "night"] as const) {
		for (const primeTime of [false, true]) {
			assert.strictEqual(
				venueFor({ ...t, stadium: { sky } }, { gid: 3, primeTime }).sky,
				sky,
			);
		}
	}
});
