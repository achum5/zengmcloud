import { assert, describe, test } from "vitest";

// firestore.rules is the only thing standing between a league's data and anyone
// who can sign in - and sign-in is anonymous, so that is everyone. It ships as
// text a user pastes into a console, which means nothing type-checks it, no
// import graph reaches it, and a bad edit is invisible until someone else's
// league is readable. These are the properties worth pinning.
//
// The specific hole this closes: the rules used to say `allow read, write` on
// the room registry. Firestore's `read` covers `list`, so a signed-in stranger
// could query the whole `leagues` collection, learn every room code, and from
// there read and delete every league in the project.
const rules = (
	import.meta as unknown as {
		glob: (
			pattern: string,
			options: { query: string; import: string; eager: true },
		) => Record<string, string>;
	}
).glob("../../public/firestore.rules", {
	query: "?raw",
	import: "default",
	eager: true,
});

const raw = Object.values(rules)[0];

// The rules file explains itself at length, and the prose says things like
// "allow list" while explaining why nothing does. Scan the rules, not the
// commentary.
const stripComments = (text: string): string =>
	text
		.split("\n")
		.map((line) => {
			const i = line.indexOf("//");
			return i === -1 ? line : line.slice(0, i);
		})
		.join("\n");

const source = raw === undefined ? undefined : stripComments(raw);

// The body of one `match` block, without any nested match blocks - so the room
// document's own grants can be read apart from its subcollections'.
//
// Note the brace counting starts at the block's OWN opening brace, not at the
// first one after the path: a rules path is full of braces ("/leagues/{code}")
// and starting from those reads one path segment as the whole block.
const ownGrantsOf = (matchPath: string): string => {
	assert.ok(source, "public/firestore.rules was not found");
	const header = `match ${matchPath} {`;
	const start = source!.indexOf(header);
	assert.notStrictEqual(start, -1, `no match block for ${matchPath}`);

	let depth = 0;
	let nested = 0;
	let out = "";
	for (let i = start + header.length - 1; i < source!.length; i++) {
		const ch = source![i]!;
		if (ch === "{") {
			depth += 1;
			if (depth > 1) {
				nested += 1;
			}
		} else if (ch === "}") {
			if (depth > 1) {
				nested -= 1;
			}
			depth -= 1;
			if (depth === 0) {
				break;
			}
		}
		// Everything at the block's own level, skipping whatever a nested match
		// (or a nested path's braces) encloses.
		if (depth === 1 && nested === 0) {
			out += ch;
		}
	}
	return out;
};

describe("firestore.rules", () => {
	test("the file is there and is a rules file", () => {
		assert.ok(raw?.includes("rules_version = '2'"), "not a rules file");
		assert.ok(source!.includes("service cloud.firestore"));
	});

	// THE ONE THAT MATTERS. `get` is "open the document at this path", which
	// needs the code. `list` (and the `read` that implies it) is "hand me the
	// documents I could not have named", which is enumeration.
	test("the room registry can be opened by code but never enumerated", () => {
		const grants = ownGrantsOf("/leagues/{code}");
		assert.ok(grants.includes("allow get:"), "rooms must be openable by code");
		assert.notMatch(
			grants,
			/allow[^;]*\blist\b/,
			"granting list lets a stranger enumerate every league in the project",
		);
		assert.notMatch(
			grants,
			/allow[^;]*\bread\b/,
			"`read` implies `list` - say `get` when you mean one document",
		);
	});

	test("no rule anywhere grants list", () => {
		assert.notMatch(
			source!,
			/allow[^;]*\blist\b/,
			"nothing in a room needs enumerating",
		);
	});

	// A control-doc write carries the uid that made it, so a change can never be
	// attributed to another device.
	test("control writes must be stamped by their author", () => {
		const grants = ownGrantsOf("/control/{docId}");
		for (const verb of ["allow create:", "allow update:"]) {
			const line = grants.split("\n").find((l) => l.includes(verb));
			assert.ok(line, `no ${verb} rule for control docs`);
		}
		assert.ok(
			grants.includes("stampedByMe()"),
			"control writes must require holderId == request.auth.uid",
		);
	});

	// The version pointer is the protocol. A write that lowers it rewinds the
	// league for every device in the room.
	test("the version pointer can never move backwards", () => {
		assert.ok(
			source!.includes(
				"request.resource.data.version >= resource.data.version",
			),
			"the v2state pointer needs a monotonic-version guard",
		);
		assert.ok(
			ownGrantsOf("/control/{docId}").includes("versionNeverGoesBackwards()"),
			"the monotonic guard must actually be applied to the control docs",
		);
	});

	// Append-only: a delivered notification or a recorded change cannot be
	// rewritten after the fact.
	test("the change log and the notification queue are append-only", () => {
		for (const path of [
			"/changes/{changeId}",
			"/notifications/{notificationId}",
		]) {
			const grants = ownGrantsOf(path);
			assert.ok(
				grants.includes("allow update: if false;"),
				`${path} must refuse updates`,
			);
			assert.ok(
				grants.includes("request.resource.data.authorId == request.auth.uid"),
				`${path} must require the author to be the writer`,
			);
		}
	});

	// A device's push token is its own.
	test("a device can only write its own member entry", () => {
		assert.ok(
			ownGrantsOf("/members/{uid}").includes("request.auth.uid == uid"),
		);
	});
});
