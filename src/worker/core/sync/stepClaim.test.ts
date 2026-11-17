import { afterEach, assert, describe, test } from "vitest";
import { clearStepClaim, foreignStepClaim, setStepClaim } from "./stepClaim.ts";

// A ready-up step run on a league mate's phone can't stamp the room busy
// (only the device in charge can), so its claim is the busy signal.
describe("stepClaim", () => {
	afterEach(() => {
		clearStepClaim();
	});

	const claim = (holderId: string, at: number, completed = false) => ({
		holderId,
		draftKey: "2026-5",
		pick: 3,
		at,
		completed,
	});

	test("another device's running step counts", () => {
		setStepClaim(claim("B", 1000), 600_000);
		assert.isDefined(foreignStepClaim("A", 2000));
	});

	test("our own, a completed one, or one past its lease does not", () => {
		setStepClaim(claim("A", 1000), 600_000);
		assert.isUndefined(foreignStepClaim("A", 2000));
		setStepClaim(claim("B", 1000, true), 600_000);
		assert.isUndefined(foreignStepClaim("A", 2000));
		setStepClaim(claim("B", 1000), 600_000);
		assert.isUndefined(foreignStepClaim("A", 1000 + 600_001));
	});

	test("nothing known, nothing claimed", () => {
		assert.isUndefined(foreignStepClaim("A"));
	});
});
