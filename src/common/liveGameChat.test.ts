import { assert, describe, test } from "vitest";
import {
	mergeChatMessages,
	sanitizeChatText,
	visibleChatMessages,
	type LiveGameChatMessage,
} from "./liveGameChat.ts";

const msg = (
	over: Partial<LiveGameChatMessage> & { id: string; cursor: number },
): LiveGameChatMessage => ({
	at: 0,
	tid: 0,
	abbrev: "BOS",
	text: "hi",
	...over,
});

describe("visibleChatMessages", () => {
	// The whole reason the anchor is a play index: a viewer must never see a
	// reaction to a play they have not watched yet. Someone who joins a
	// broadcast late, or sits paused while the simmer plays on, would otherwise
	// have the game spoiled by the chat.
	test("a message from later in the game is not shown to someone behind it", () => {
		const messages = [
			msg({ id: "a", cursor: 10, text: "close game" }),
			msg({ id: "b", cursor: 400, text: "WHAT A SHOT" }),
		];
		assert.deepStrictEqual(
			visibleChatMessages(messages, 100).map((m) => m.text),
			["close game"],
		);
		assert.deepStrictEqual(
			visibleChatMessages(messages, 400).map((m) => m.text),
			["close game", "WHAT A SHOT"],
		);
	});

	test("messages sharing an anchor keep the order they were sent", () => {
		const messages = [
			msg({ id: "b", cursor: 5, at: 200, text: "second" }),
			msg({ id: "a", cursor: 5, at: 100, text: "first" }),
		];
		assert.deepStrictEqual(
			visibleChatMessages(messages, 5).map((m) => m.text),
			["first", "second"],
		);
	});
});

describe("mergeChatMessages", () => {
	test("a re-delivered message is deduped, not doubled", () => {
		const a = msg({ id: "a", cursor: 1 });
		assert.strictEqual(mergeChatMessages([a], [a]).length, 1);
	});

	test("two people typing at once both survive the merge", () => {
		const merged = mergeChatMessages(
			[msg({ id: "a", cursor: 1, at: 1 })],
			[msg({ id: "b", cursor: 1, at: 2 })],
		);
		assert.deepStrictEqual(
			merged.map((m) => m.id),
			["a", "b"],
		);
	});

	test("junk in the payload is dropped rather than rendered", () => {
		const merged = mergeChatMessages(
			[msg({ id: "a", cursor: 1 })],
			[undefined as any, { text: "no id" } as any],
		);
		assert.deepStrictEqual(
			merged.map((m) => m.id),
			["a"],
		);
	});
});

describe("sanitizeChatText", () => {
	test("collapses whitespace and drops empties", () => {
		assert.strictEqual(sanitizeChatText("  hey   there \n"), "hey there");
		assert.strictEqual(sanitizeChatText("   "), undefined);
		assert.strictEqual(sanitizeChatText(""), undefined);
	});

	test("bounds the length so one message cannot bloat the room doc", () => {
		assert.strictEqual(sanitizeChatText("x".repeat(500))!.length, 280);
	});
});
