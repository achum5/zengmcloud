import { assert, describe, test } from "vitest";
import { classifyPreflightError } from "./preflightProblem.ts";

// The four things a user has to do by hand in the Firebase console, and the
// error each one throws when it hasn't been done. Getting this table wrong
// sends someone to the wrong console page, or tells them to turn on a setting
// that is already on - which is worse than saying nothing.
//
// A live Firebase project is exactly what a test cannot have, so these are the
// error shapes the SDK really throws, classified without one.
const firebaseError = (code: string, message = "") => {
	const error = new Error(message || code) as Error & { code: string };
	error.code = code;
	return error;
};

describe("classifyPreflightError", () => {
	test("the provider being off points at Authentication", () => {
		const { problem, link } = classifyPreflightError(
			"auth",
			firebaseError(
				"auth/operation-not-allowed",
				"Firebase: Error (auth/operation-not-allowed).",
			),
		);
		assert.strictEqual(problem.title, "Anonymous sign-in is off");
		assert.strictEqual(link?.url, "auth");
		assert.strictEqual(link?.rules, undefined);
	});

	// A project where Authentication was never opened at all.
	test("auth never having been set up is the same fix", () => {
		const { link } = classifyPreflightError(
			"auth",
			firebaseError("auth/configuration-not-found"),
		);
		assert.strictEqual(link?.url, "auth");
	});

	test("a bad API key says so instead of blaming the provider", () => {
		const { problem, link } = classifyPreflightError(
			"auth",
			firebaseError("auth/api-key-not-valid"),
		);
		assert.ok(problem.title.includes("API key"));
		assert.strictEqual(link, undefined);
	});

	// The one that matters most: a dead connection looks like every other
	// failure, and "turn on Anonymous sign-in" is useless advice when it is on.
	test("a network failure is never mistaken for a misconfigured project", () => {
		for (const step of ["auth", "write", "read"] as const) {
			const { problem, link } = classifyPreflightError(
				step,
				firebaseError("auth/network-request-failed"),
			);
			assert.strictEqual(problem.title, "Couldn't reach Firebase", step);
			assert.strictEqual(link, undefined, step);
		}
		assert.strictEqual(
			classifyPreflightError("write", firebaseError("unavailable")).problem
				.title,
			"Couldn't reach Firebase",
		);
	});

	test("unpublished rules point at the Rules tab and offer them", () => {
		const { problem, link } = classifyPreflightError(
			"write",
			firebaseError(
				"permission-denied",
				"Missing or insufficient permissions.",
			),
		);
		assert.ok(problem.title.includes("rules"));
		assert.strictEqual(link?.url, "rules");
		assert.strictEqual(link?.rules, true);
	});

	// Rules that let the host write but nobody read: the league would look fine
	// to whoever set it up and be dead for everyone else.
	test("write-but-not-read is called out as its own problem", () => {
		const { problem, link } = classifyPreflightError(
			"read",
			firebaseError("permission-denied"),
		);
		assert.ok(problem.title.includes("writing but not reading"));
		assert.strictEqual(link?.rules, true);
	});

	// A project with no database is a completely different fix from bad rules,
	// and is only distinguishable from the message prose.
	test("a project with no Firestore database points at Firestore, not Rules", () => {
		const { problem, link } = classifyPreflightError(
			"write",
			firebaseError(
				"not-found",
				"The database (default) does not exist for project my-league",
			),
		);
		assert.ok(problem.title.includes("no Firestore database"));
		assert.strictEqual(link?.url, "firestore");
	});

	test("an error with no code at all still produces a fix", () => {
		const { problem } = classifyPreflightError("write", "something odd");
		assert.ok(problem.title.length > 0);
		assert.ok(problem.fix.length > 0);
	});
});
