import { assert, describe, test } from "vitest";
import { NO_QUIRKS, quirksFor } from "./socialQuirks.ts";

const team = { name: "Cyclones", abbrev: "MIA" };

describe("quirksFor", () => {
	test("an account's habits never change", () => {
		const a = quirksFor({ id: "m:a", kind: "media", tone: "hype", team });
		const b = quirksFor({ id: "m:a", kind: "media", tone: "hype", team });
		assert.deepStrictEqual(a, b);
	});

	test("different accounts get different habits", () => {
		const all = new Set<string>();
		for (let i = 0; i < 60; i++) {
			all.add(
				JSON.stringify(
					quirksFor({ id: `m:${i}`, kind: "media", tone: "hype", team }),
				),
			);
		}
		assert.ok(all.size > 20, `only ${all.size} distinct habit sets`);
	});

	test("most accounts have no hashtag at all", () => {
		// A feed where everybody has a gimmick is as fake as one where nobody
		// does, so this is a ceiling and not a floor.
		let tagged = 0;
		for (let i = 0; i < 200; i++) {
			if (
				quirksFor({ id: `m:${i}`, kind: "media", tone: "beat", team })
					.hashtag !== undefined
			) {
				tagged += 1;
			}
		}
		assert.ok(tagged < 80, `${tagged} of 200 beat writers use a hashtag`);
	});

	test("a doomer never posts a rah-rah hashtag", () => {
		for (let i = 0; i < 200; i++) {
			for (const tone of ["doom", "snark", "wonk"] as const) {
				const { hashtag } = quirksFor({
					id: `m:${i}`,
					kind: "media",
					tone,
					team,
				});
				assert.ok(
					hashtag === undefined || !/^#(Go|.*Nation$)/.test(hashtag),
					`${tone} account posting ${hashtag}`,
				);
			}
		}
	});

	test("an account with no team has no team hashtag", () => {
		for (let i = 0; i < 40; i++) {
			assert.strictEqual(
				quirksFor({ id: `m:${i}`, kind: "media", tone: "hype" }).hashtag,
				undefined,
			);
		}
	});

	test("the blank set decorates nothing", () => {
		assert.strictEqual(NO_QUIRKS.openerRate, 0);
		assert.strictEqual(NO_QUIRKS.closerRate, 0);
		assert.strictEqual(NO_QUIRKS.hashtag, undefined);
		assert.strictEqual(NO_QUIRKS.emojiBoost, 0);
	});
});
