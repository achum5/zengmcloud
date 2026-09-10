import { assert, describe, test } from "vitest";
import { recapNotificationBody, trimToSentence } from "./recapNotification.ts";

const DAY_RECAP = `**Amen Brooks' 42 and Vince Lowry's 43 light up the night**

*Vince Lowry pours in 43 · 76ers stun the Hawks 106-98 · Anthony Foster goes for 38*

The Magic held off the Timberwolves 119-116 (OT) behind Scoot Jackson's 25 points and 18 rebounds. Naji Mathis had 32 points in the Jazz's 106-99 win over the Heat.

Elsewhere, the Warriors edged the Spurs 113-110 and the Hornets edged the Bucks 98-95.`;

describe("a recap as notification text", () => {
	test("the headline and the first line of the story, and nothing else", () => {
		assert.strictEqual(
			recapNotificationBody(DAY_RECAP),
			"Amen Brooks' 42 and Vince Lowry's 43 light up the night\nThe Magic held off the Timberwolves 119-116 (OT) behind Scoot Jackson's 25 points and 18 rebounds. Naji Mathis had 32 points in the Jazz's 106-99 win over the Heat.",
		);
	});

	test("the deck is dropped - it is the same night said shorter", () => {
		const body = recapNotificationBody(DAY_RECAP)!;
		assert.ok(!body.includes("pours in 43"));
		assert.ok(!body.includes("·"));
	});

	test("no markdown survives", () => {
		const body = recapNotificationBody(
			`**A **big** night**\n\nThe [Boston Celtics](/l/1/roster/BOS_0) won *comfortably*.`,
		)!;
		assert.ok(!body.includes("*"), body);
		assert.ok(!body.includes("["), body);
		assert.ok(body.includes("The Boston Celtics won comfortably."), body);
	});

	test("a long first line is cut at a sentence, not mid-word", () => {
		const long = `**Headline**\n\n${"A".repeat(40)}. ${"B".repeat(40)}. ${"C".repeat(400)}.`;
		const body = recapNotificationBody(long, 120)!;
		assert.ok(body.endsWith("."), body);
		assert.ok(!body.includes("C"), body);
		assert.ok(body.includes("B"), body);
	});

	test("a first sentence longer than the budget still ends cleanly", () => {
		const body = recapNotificationBody(
			`**H**\n\n${"word ".repeat(200)}end.`,
			50,
		)!;
		assert.ok(body.endsWith("…"), body);
		assert.ok(!body.includes("wor…"), body);
	});

	test("a filed one-line note is not repeated", () => {
		assert.strictEqual(
			recapNotificationBody("**Celtics roll**"),
			"Celtics roll",
		);
	});

	test("a note with no headline is just its first line", () => {
		assert.strictEqual(
			recapNotificationBody("Big night in Boston.\n\nAnd elsewhere."),
			"Big night in Boston.",
		);
	});

	test("nothing in, nothing out", () => {
		assert.strictEqual(recapNotificationBody(undefined), undefined);
		assert.strictEqual(recapNotificationBody(""), undefined);
		assert.strictEqual(recapNotificationBody("   \n\n  "), undefined);
	});

	test("trimToSentence leaves a short string alone", () => {
		assert.strictEqual(trimToSentence("Short.", 100), "Short.");
	});
});
