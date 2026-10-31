import { assert, test } from "vitest";
import type { KitArt } from "../court3d/kitArt.ts";
import { dressFieldKit, fieldKit } from "./kit.ts";

const CHIEFS = ["#c8102e", "#ffb612", "#ffffff"] as [string, string, string];

test("a team's helmet in its colors, the same home and away", () => {
	const home = fieldKit(CHIEFS, "home");
	const away = fieldKit(CHIEFS, "away");
	assert.strictEqual(home.helmet, CHIEFS[0]);
	assert.strictEqual(away.helmet, home.helmet);
	assert.strictEqual(away.helmetStripe, home.helmetStripe);
	assert.strictEqual(home.facemask, away.facemask);
});

test("a helmet as the team has set it: its shell, stripe and mask - or no stripe at all", () => {
	const own = fieldKit(CHIEFS, "home", {
		shell: "#b3995d",
		stripe: "#000000",
		stripeEdge: "#ffffff",
		facemask: "#101010",
	});
	assert.strictEqual(own.helmet, "#b3995d");
	assert.strictEqual(own.helmetStripe, "#000000");
	assert.strictEqual(own.helmetStripeEdge, "#ffffff");
	assert.strictEqual(own.facemask, "#101010");
	// (The jersey still the team's.)
	assert.strictEqual(own.jersey, fieldKit(CHIEFS, "home").jersey);
	const bare = fieldKit(CHIEFS, "home", { stripe: "" });
	assert.isUndefined(bare.helmetStripe);
	assert.isUndefined(bare.helmetStripeEdge);
	// Just the shell: a stripe picked to show on it.
	const shell = fieldKit(CHIEFS, "away", { shell: "#ffffff" });
	assert.strictEqual(shell.helmet, "#ffffff");
	assert.notStrictEqual(shell.helmetStripe, "#ffffff");
});

test("a uniform as the team has set it - and what it has not, picked to go with it", () => {
	for (const edition of ["home", "away"] as const) {
		const plain = fieldKit(CHIEFS, edition);
		const own = fieldKit(CHIEFS, edition, {
			[`${edition}Jersey`]: "#101820",
			[`${edition}Number`]: "#ffb612",
			[`${edition}Pants`]: "#a5acaf",
		});
		assert.strictEqual(own.jersey, "#101820", edition);
		assert.strictEqual(own.number, "#ffb612", edition);
		assert.strictEqual(own.pants, "#a5acaf", edition);
		// (The helmet as it was.)
		assert.strictEqual(own.helmet, plain.helmet, edition);
		// Just the jersey: numbers and a collar that show on it.
		const jersey = fieldKit(CHIEFS, edition, {
			[`${edition}Jersey`]: "#101820",
		});
		assert.notStrictEqual(jersey.number, "#101820", edition);
		assert.notStrictEqual(jersey.collar, "#101820", edition);
		// The other edition as it was.
		const other = edition === "home" ? "away" : "home";
		assert.deepEqual(
			fieldKit(CHIEFS, other, { [`${edition}Jersey`]: "#101820" }),
			fieldKit(CHIEFS, other),
		);
	}
});

test("a uniform drawn from a picture: its lettering and trim the picture's, no bands or stripes of the game's", () => {
	const kit = fieldKit(CHIEFS, "home");
	const art: KitArt = {
		id: "pic",
		data: new Uint8ClampedArray(4),
		scale: 1,
		number: "#101010",
		trim: "#00ff00",
	};
	const dressed = dressFieldKit(kit, art);
	assert.strictEqual(dressed.number, "#101010");
	// (The name in the number's color, where the picture has none for it.)
	assert.strictEqual(dressed.name, "#101010");
	assert.strictEqual(dressed.collar, "#00ff00");
	assert.strictEqual(dressed.numberEdge, kit.numberEdge);
	assert.deepEqual(dressed.sleeveStripes, []);
	assert.deepEqual(dressed.pantsStripes, []);
	// The plain colors, where the picture is clear, as they were.
	assert.strictEqual(dressed.jersey, kit.jersey);
	assert.strictEqual(dressed.pants, kit.pants);
	assert.strictEqual(dressFieldKit(kit, undefined), kit);
});
