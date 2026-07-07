import { assert, describe, test } from "vitest";
import { linkifyRecap, type RecapLink } from "./linkifyRecap.ts";

const P = (name: string, pid: number): RecapLink => ({
	name,
	href: `/l/1/player/${pid}`,
});

describe("linkifyRecap", () => {
	test("links a plain name", () => {
		const out = linkifyRecap("Zion Williamson scored 30.", [
			P("Zion Williamson", 5),
		]);
		assert.strictEqual(out, "[Zion Williamson](/l/1/player/5) scored 30.");
	});

	test("keeps bold on a bolded name (bold link)", () => {
		const out = linkifyRecap("**Zion Williamson** was great.", [
			P("Zion Williamson", 5),
		]);
		assert.strictEqual(out, "**[Zion Williamson](/l/1/player/5)** was great.");
	});

	test("longer name wins; shorter name doesn't double-link inside it", () => {
		const out = linkifyRecap("Trey Murphy III struggled.", [
			P("Trey Murphy III", 7),
			P("Murphy", 9),
		]);
		assert.strictEqual(out, "[Trey Murphy III](/l/1/player/7) struggled.");
	});

	test("does not corrupt plain numbers in the text", () => {
		const out = linkifyRecap("combining for 27 efficient points", [
			P("Nobody Here", 1),
		]);
		assert.strictEqual(out, "combining for 27 efficient points");
	});

	test("links team names too", () => {
		const out = linkifyRecap("the Bayou faded late", [
			{ name: "Bayou", href: "/l/1/roster/NOL_3" },
		]);
		assert.strictEqual(out, "the [Bayou](/l/1/roster/NOL_3) faded late");
	});

	test("only whole-word matches (no substring links)", () => {
		// "Cam" must not link inside "Camden".
		const out = linkifyRecap("Camden and Cam played.", [P("Cam", 4)]);
		assert.strictEqual(out, "Camden and [Cam](/l/1/player/4) played.");
	});
});
