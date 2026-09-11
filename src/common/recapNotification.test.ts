import { assert, describe, test } from "vitest";
import { recapNotificationParts, trimToSentence } from "./recapNotification.ts";

const DAY_RECAP = `**Amen Brooks' 42 and Vince Lowry's 43 light up the night**

*Vince Lowry pours in 43 · 76ers stun the Hawks 106-98 · Anthony Foster goes for 38*

The Magic held off the Timberwolves 119-116 (OT) behind Scoot Jackson's 25 points and 18 rebounds. Naji Mathis had 32 points in the Jazz's 106-99 win over the Heat.

Elsewhere, the Warriors edged the Spurs 113-110 and the Hornets edged the Bucks 98-95.`;

describe("a recap as notification text", () => {
	test("the headline is the title and the first line of the story is the body", () => {
		assert.deepStrictEqual(recapNotificationParts(DAY_RECAP), {
			title: "Amen Brooks' 42 and Vince Lowry's 43 light up the night",
			body: "The Magic held off the Timberwolves 119-116 (OT) behind Scoot Jackson's 25 points and 18 rebounds. Naji Mathis had 32 points in the Jazz's 106-99 win over the Heat.",
		});
	});

	test("the deck is dropped - it is the same night said shorter", () => {
		const { title, body } = recapNotificationParts(DAY_RECAP);
		const both = `${title} ${body}`;
		assert.ok(!both.includes("pours in 43"));
		assert.ok(!both.includes("·"));
	});

	test("no markdown survives", () => {
		const { title, body } = recapNotificationParts(
			`**A **big** night**\n\nThe [Boston Celtics](/l/1/roster/BOS_0) won *comfortably*.`,
		);
		const both = `${title} ${body}`;
		assert.ok(!both.includes("*"), both);
		assert.ok(!both.includes("["), both);
		assert.ok(body!.includes("The Boston Celtics won comfortably."), both);
	});

	test("a long first line is cut at a sentence, not mid-word", () => {
		const long = `**Headline**\n\n${"A".repeat(40)}. ${"B".repeat(40)}. ${"C".repeat(400)}.`;
		const { body } = recapNotificationParts(long, 120);
		assert.ok(body!.endsWith("."), body);
		assert.ok(!body!.includes("C"), body);
		assert.ok(body!.includes("B"), body);
	});

	test("a first sentence longer than the budget still ends cleanly", () => {
		const { body } = recapNotificationParts(
			`**H**\n\n${"word ".repeat(200)}end.`,
			50,
		);
		assert.ok(body!.endsWith("…"), body);
		assert.ok(!body!.includes("wor…"), body);
	});

	test("a headline too long for a title is cut rather than left to run on", () => {
		const { title } = recapNotificationParts(`**${"long ".repeat(60)}end**`);
		assert.ok(title!.length <= 91, String(title!.length));
		assert.ok(title!.endsWith("…"), title);
	});

	test("a filed one-line note is the title, said once", () => {
		assert.deepStrictEqual(recapNotificationParts("**Celtics roll**"), {
			title: "Celtics roll",
			body: undefined,
		});
	});

	test("a note with no headline leads with its first sentence", () => {
		assert.deepStrictEqual(
			recapNotificationParts(
				"Big night in Boston. Nobody else close.\n\nMore.",
			),
			{
				title: "Big night in Boston.",
				body: "Nobody else close.",
			},
		);
	});

	test("a one-sentence note with no headline is not repeated", () => {
		assert.deepStrictEqual(
			recapNotificationParts("Big night in Boston.\n\nAnd elsewhere."),
			{ title: "Big night in Boston.", body: undefined },
		);
	});

	test("nothing in, nothing out", () => {
		assert.deepStrictEqual(recapNotificationParts(undefined), {});
		assert.deepStrictEqual(recapNotificationParts(""), {});
		assert.deepStrictEqual(recapNotificationParts("   \n\n  "), {});
	});

	test("trimToSentence leaves a short string alone", () => {
		assert.strictEqual(trimToSentence("Short.", 100), "Short.");
	});
});
