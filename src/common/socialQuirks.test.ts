import { assert, describe, test } from "vitest";
import { quirksFor } from "./socialQuirks.ts";
import type { SocialTone } from "./socialPersonality.ts";

const TEAM = { name: "Boston Celtics", abbrev: "BOS" };
const TONES: SocialTone[] = [
	"wire",
	"beat",
	"hype",
	"snark",
	"doom",
	"wonk",
	"corporate",
	"unhinged",
];

describe("quirksFor", () => {
	test("the same account always gets the same quirks", () => {
		const a = quirksFor({
			id: "p:12",
			kind: "player",
			tone: "hype",
			team: TEAM,
		});
		const b = quirksFor({
			id: "p:12",
			kind: "player",
			tone: "hype",
			team: TEAM,
		});
		assert.deepStrictEqual(a, b);
	});

	test("neighbouring ids do not share a quirk sheet", () => {
		// The point of burning the generator's first draw: ids that hash close
		// together must not come out as the same person.
		const sheets = new Set<string>();
		for (let pid = 0; pid < 200; pid++) {
			sheets.add(
				JSON.stringify(
					quirksFor({
						id: `p:${pid}`,
						kind: "player",
						tone: "hype",
						team: TEAM,
					}),
				),
			);
		}
		assert.ok(sheets.size > 60, `only ${sheets.size} distinct sheets in 200`);
	});

	test("quirks are sparse: most accounts have no gimmick at all", () => {
		let gimmicks = 0;
		let total = 0;
		for (const tone of TONES) {
			for (let i = 0; i < 100; i++) {
				const q = quirksFor({
					id: `m:cast:${tone}:${i}`,
					kind: "media",
					tone,
					team: TEAM,
				});
				total += 1;
				if (
					q.hashtag !== undefined ||
					q.ellipses ||
					q.exclaims ||
					q.emojiBoost > 0
				) {
					gimmicks += 1;
				}
			}
		}
		assert.ok(
			gimmicks / total < 0.5,
			`${gimmicks} of ${total} accounts have a gimmick`,
		);
	});

	test("a hashtag is one token with no spaces and no digits", () => {
		for (let i = 0; i < 300; i++) {
			const q = quirksFor({
				id: `m:cast:homer:${i}`,
				kind: "media",
				tone: "hype",
				team: { name: "Golden State Warriors", abbrev: "GSW" },
			});
			if (q.hashtag !== undefined) {
				assert.match(q.hashtag, /^#[A-Za-z]+$/, q.hashtag);
			}
		}
	});

	test("an account with no team never gets a hashtag", () => {
		for (let i = 0; i < 200; i++) {
			const q = quirksFor({ id: `m:${i}`, kind: "media", tone: "hype" });
			assert.strictEqual(q.hashtag, undefined);
		}
	});

	test("the dry voices never become emoji people", () => {
		for (const tone of ["wire", "wonk"] as const) {
			for (let i = 0; i < 200; i++) {
				const q = quirksFor({
					id: `m:${tone}:${i}`,
					kind: "media",
					tone,
					team: TEAM,
				});
				assert.strictEqual(q.emojiBoost, 0);
			}
		}
	});
});
