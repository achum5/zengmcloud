import { assert, test } from "vitest";
import { cleanHelmet, cleanStadium } from "./stadiumLook.ts";

test("a stadium's settings kept to the ones there are", () => {
	assert.deepEqual(
		cleanStadium({
			roof: "dome",
			turf: "grass",
			endZones: "green",
			sky: "night",
		}),
		{ roof: "dome", turf: "grass", endZones: "green", sky: "night" },
	);
	assert.deepEqual(cleanStadium({ roof: "retractable", sky: "day", x: 1 }), {
		sky: "day",
	});
	assert.deepEqual(
		cleanStadium({
			endZoneColor: "#203731",
			endZoneText: "  GREEN BAY  ",
			endZoneTextColor: "gold",
			midLogo: false,
		}),
		{ endZoneColor: "#203731", endZoneText: "GREEN BAY", midLogo: false },
	);
	assert.deepEqual(cleanStadium({ endZoneText: "", midLogo: true }), {
		endZoneText: "",
	});
	assert.strictEqual(
		cleanStadium({ endZoneText: "X".repeat(40) })?.endZoneText?.length,
		24,
	);
	assert.isUndefined(cleanStadium({}));
	assert.isUndefined(cleanStadium("dome"));
	assert.isUndefined(cleanStadium(undefined));
});

test("a helmet's colors kept to colors, a stripe to a color or none", () => {
	assert.deepEqual(
		cleanHelmet({
			shell: "#B3995D",
			stripe: "",
			stripeEdge: "#ffffff",
			facemask: "gray",
			logo: false,
		}),
		{ shell: "#B3995D", stripe: "", stripeEdge: "#ffffff", logo: false },
	);
	assert.deepEqual(
		cleanHelmet({
			homeJersey: "#002244",
			homePants: "#ffffff",
			awayNumber: "navy",
			awayPants: "",
		}),
		{ homeJersey: "#002244", homePants: "#ffffff" },
	);
	assert.isUndefined(cleanHelmet({ shell: "", logo: true }));
	assert.isUndefined(cleanHelmet(null));
});
