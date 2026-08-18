import { assert, describe, test } from "vitest";
import {
	appearanceForSeason,
	appearancesDiffer,
	recordAppearance,
	type PlayerAppearance,
} from "./playerAppearance.ts";

// Faces are compared by value, so a stand-in with one field is enough to
// exercise every rule here.
const f = (id: string) => ({ hair: { id } }) as any;

describe("appearanceForSeason", () => {
	const p = {
		face: f("bald"),
		appearances: [
			{ season: 2010, face: f("afro") },
			{ season: 2016, face: f("short") },
		] as PlayerAppearance[],
	};

	test("a season resolves to the look in effect then, not the newest one", () => {
		assert.strictEqual(appearanceForSeason(p, 2010).face!.hair.id, "afro");
		assert.strictEqual(appearanceForSeason(p, 2015).face!.hair.id, "afro");
		assert.strictEqual(appearanceForSeason(p, 2016).face!.hair.id, "short");
		assert.strictEqual(appearanceForSeason(p, 2030).face!.hair.id, "short");
	});

	test("a season before the record falls back to the earliest look", () => {
		// Closer to the truth than today's face, which is the whole point.
		assert.strictEqual(appearanceForSeason(p, 2004).face!.hair.id, "afro");
	});

	test("no history means the player always looked like he does now", () => {
		// The normal state: every existing player, and every league that never
		// turned face aging on.
		const plain = { face: f("curly"), imgURL: "" };
		assert.strictEqual(appearanceForSeason(plain, 2011).face!.hair.id, "curly");
		assert.strictEqual(
			appearanceForSeason(plain, undefined).face!.hair.id,
			"curly",
		);
	});

	test("no season asked means the current look", () => {
		assert.strictEqual(appearanceForSeason(p, undefined).face!.hair.id, "bald");
	});

	test("a photo is carried per season just like a face", () => {
		const withPhoto = {
			imgURL: "now.png",
			appearances: [{ season: 2010, imgURL: "then.png" }],
		};
		assert.strictEqual(appearanceForSeason(withPhoto, 2012).imgURL, "then.png");
	});
});

describe("appearancesDiffer", () => {
	test("identical looks are not a change", () => {
		assert.isFalse(appearancesDiffer({ face: f("afro") }, { face: f("afro") }));
		assert.isFalse(appearancesDiffer({}, {}));
	});

	test("either half counts", () => {
		assert.isTrue(appearancesDiffer({ face: f("afro") }, { face: f("bald") }));
		assert.isTrue(appearancesDiffer({ imgURL: "a" }, { imgURL: "b" }));
	});
});

describe("recordAppearance", () => {
	test("an unchanged look writes nothing at all", () => {
		// This is what keeps the history a couple of entries instead of one per
		// season - and what keeps a synced league from re-uploading identical
		// faces every preseason.
		assert.isUndefined(
			recordAppearance({
				appearances: undefined,
				season: 2015,
				firstSeason: 2010,
				look: { face: f("afro") },
				previous: { face: f("afro") },
			}),
		);
	});

	test("the first change seeds the era that came before it", () => {
		// Without this the record would claim he always looked the way he does
		// after the change, which is the bug being fixed.
		const history = recordAppearance({
			appearances: undefined,
			season: 2015,
			firstSeason: 2010,
			look: { face: f("bald") },
			previous: { face: f("afro") },
		});
		assert.deepStrictEqual(
			history!.map((e) => [e.season, e.face!.hair.id]),
			[
				[2010, "afro"],
				[2015, "bald"],
			],
		);
	});

	test("later changes append, and stay in season order", () => {
		const first = recordAppearance({
			appearances: undefined,
			season: 2015,
			firstSeason: 2010,
			look: { face: f("bald") },
			previous: { face: f("afro") },
		});
		const second = recordAppearance({
			appearances: first,
			season: 2019,
			look: { face: f("short-bald") },
			firstSeason: 2010,
		});
		assert.deepStrictEqual(
			second!.map((e) => e.season),
			[2010, 2015, 2019],
		);
	});

	test("re-recording a season replaces it rather than duplicating", () => {
		// Editing the same season twice must not stack entries.
		const history = recordAppearance({
			appearances: [
				{ season: 2010, face: f("afro") },
				{ season: 2015, face: f("bald") },
			],
			season: 2015,
			firstSeason: 2010,
			look: { face: f("dreads") },
		});
		assert.strictEqual(history!.length, 2);
		assert.strictEqual(
			appearanceForSeason({ appearances: history }, 2015).face!.hair.id,
			"dreads",
		);
	});

	test("writing what the history already says is skipped", () => {
		assert.isUndefined(
			recordAppearance({
				appearances: [{ season: 2010, face: f("afro") }],
				season: 2014,
				firstSeason: 2010,
				look: { face: f("afro") },
			}),
		);
	});
});
