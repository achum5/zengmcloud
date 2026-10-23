import { assert, test } from "vitest";
import { fieldKit } from "./kit.ts";

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
