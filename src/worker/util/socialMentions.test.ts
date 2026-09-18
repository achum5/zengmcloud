// WHAT COUNTS AS SOMEBODY TALKING ABOUT YOU.
//
// The walk over the timeline needs a league behind it; the decision does not,
// so it is tested here against hand-written posts. The view types for this
// page do not catch a renamed field, so this is the thing standing between a
// Mentions tab and a tab that is always empty.

import { assert, describe, test } from "vitest";
import { isMentionOf } from "./socialFeed.ts";

const post = (over: Partial<any> = {}): any => ({
	accountId: "m:writer",
	text: "Rough night for the bench.",
	pids: [],
	tids: [],
	replies: [],
	...over,
});

const PLAYER = { id: "p:12", handle: "TyreseGreen", kind: "player", pid: 12 } as const;
const TEAM = { id: "t:3", handle: "Bucks", kind: "team", tid: 3 } as const;
const WRITER = { id: "m:beat", handle: "AmyOnTheBeat", kind: "media" } as const;
// A fan of the Bucks. Carries the same tid the franchise does.
const FAN = {
	id: "m:fan9",
	handle: "BucksGuy",
	kind: "media",
	tid: 3,
} as const;

describe("isMentionOf", () => {
	test("a post about the player is a mention of him", () => {
		assert.isTrue(isMentionOf(post({ pids: [12] }), PLAYER));
		assert.isTrue(isMentionOf(post({ pid: 12 }), PLAYER));
		assert.isFalse(isMentionOf(post({ pids: [13] }), PLAYER));
	});

	test("a post about the team is a mention of the team", () => {
		assert.isTrue(isMentionOf(post({ tids: [3] }), TEAM));
		assert.isFalse(isMentionOf(post({ tids: [4] }), TEAM));
	});

	test("supporting a team is not being one", () => {
		// A fan and a beat writer carry the tid of the club they follow. Before
		// this, every post about the Bucks turned up in the fan's mentions and
		// the tab was an exact copy of the team's feed.
		assert.isFalse(isMentionOf(post({ tids: [3] }), FAN));
		// They are still mentioned by name.
		assert.isTrue(isMentionOf(post({ text: "@BucksGuy called it" }), FAN));
		// And the franchise's own account still matches on the club.
		assert.isTrue(isMentionOf(post({ tids: [3] }), TEAM));
	});

	test("a player account matches on the player, not on his club", () => {
		// pids/tids both travel on a post; a player's account must not collect
		// every post about his team.
		assert.isFalse(
			isMentionOf(post({ tids: [3] }), {
				...PLAYER,
				tid: 3,
			}),
		);
		assert.isTrue(isMentionOf(post({ pids: [12] }), { ...PLAYER, tid: 3 }));
	});

	test("free agency is not a mention of every team at once", () => {
		// tid -1 is the league's "no team", and an account carrying it must not
		// match every post that mentions a free agent.
		assert.isFalse(
			isMentionOf(post({ tids: [-1] }), {
				id: "t:x",
				handle: "FA",
				kind: "team",
				tid: -1,
			}),
		);
	});

	test("being quoted is being mentioned", () => {
		assert.isTrue(
			isMentionOf(
				post({ quoted: { accountId: "m:beat", text: "Called it." } }),
				WRITER,
			),
		);
		assert.isFalse(
			isMentionOf(
				post({ quoted: { accountId: "m:other", text: "Called it." } }),
				WRITER,
			),
		);
	});

	test("by name, and not by a name that merely starts the same way", () => {
		assert.isTrue(
			isMentionOf(post({ text: "@AmyOnTheBeat has this wrong." }), WRITER),
		);
		assert.isFalse(
			isMentionOf(post({ text: "@AmyOnTheBeatles has this wrong." }), WRITER),
		);
		// The bare name is not a handle.
		assert.isFalse(
			isMentionOf(post({ text: "AmyOnTheBeat has this wrong." }), WRITER),
		);
	});

	test("a reply under somebody else's post still points here", () => {
		assert.isTrue(
			isMentionOf(
				post({
					replies: [{ accountId: "m:fan", text: "@AmyOnTheBeat said the same" }],
				}),
				WRITER,
			),
		);
	});

	test("an account does not mention itself", () => {
		assert.isFalse(
			isMentionOf(post({ accountId: "m:beat", text: "Rough night." }), WRITER),
		);
		// Not even by talking about the player it is named for.
		assert.isFalse(isMentionOf(post({ accountId: "p:12", pids: [12] }), PLAYER));
	});

	test("but its own post comes along when somebody answered it", () => {
		// The reply is the mention; the post is only here to carry it, because
		// a reply without the thing it answers is a one-liner with no context.
		assert.isTrue(
			isMentionOf(
				post({
					accountId: "m:beat",
					replies: [{ accountId: "m:fan", text: "No chance." }],
				}),
				WRITER,
			),
		);
		// Its own replies under its own post are not other people talking.
		assert.isFalse(
			isMentionOf(
				post({
					accountId: "m:beat",
					replies: [{ accountId: "m:beat", text: "To be clear:" }],
				}),
				WRITER,
			),
		);
	});

	test("a handle with a dot in it is matched literally", () => {
		const dotted = { id: "m:d", handle: "A.J.Wire", kind: "media" } as const;
		assert.isTrue(isMentionOf(post({ text: "per @A.J.Wire" }), dotted));
		assert.isFalse(isMentionOf(post({ text: "per @AxJxWire" }), dotted));
	});
});
