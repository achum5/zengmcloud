import { assert, describe, test } from "vitest";
import { ADS_SHOWN, adsFor } from "./arenaAds.ts";
import {
	arenaAdPictureIds,
	withArenaAdPictures,
} from "../../../../common/arenaAdPictures.ts";

describe("the building's sponsors", () => {
	test("made up for a building, the same every game in it, none twice", () => {
		const a = adsFor("BOS", ["#007a33", "#ffffff", "#000000"], undefined);
		const b = adsFor("BOS", ["#007a33", "#ffffff", "#000000"], undefined);
		assert.strictEqual(a.length, ADS_SHOWN);
		assert.deepEqual(
			a.map((x) => x.text),
			b.map((x) => x.text),
		);
		assert.strictEqual(new Set(a.map((x) => x.text)).size, ADS_SHOWN);
	});

	test("the team's own first, in its colors where it gave none", () => {
		const ads = adsFor(
			"BOS",
			["#007a33", "#ffffff", "#000000"],
			[
				{ text: "Super Mart", sub: "groceries" },
				{},
				{ text: "Acme", bg: "#112233", fg: "#ffcc00" },
			],
		);
		assert.strictEqual(ads.length, ADS_SHOWN);
		assert.strictEqual(ads[0]!.text, "SUPER MART");
		assert.strictEqual(ads[0]!.sub, "GROCERIES");
		assert.strictEqual(ads[0]!.bg, "#007a33");
		assert.strictEqual(ads[0]!.fg, "#ffffff");
		// (The blank one skipped.)
		assert.strictEqual(ads[1]!.text, "ACME");
		assert.strictEqual(ads[1]!.bg, "#112233");
		assert.strictEqual(ads[1]!.fg, "#ffcc00");
	});

	test("an ad's uploaded picture is kept with the court and filled in for the game", () => {
		const court = {
			arenaAds: [
				{ text: "A", imageURL: "pic:0123456789abcdef0123" },
				{ text: "B", imageURL: "https://example.com/b.png" },
				{ text: "C", imageURL: "pic:ffffffffffffffffffff" },
			],
		};
		assert.deepEqual(arenaAdPictureIds(court), [
			"0123456789abcdef0123",
			"ffffffffffffffffffff",
		]);
		const filled = withArenaAdPictures(court.arenaAds, {
			"0123456789abcdef0123": "data:image/png;base64,AAAA",
		});
		assert.strictEqual(filled[0]!.imageURL, "data:image/png;base64,AAAA");
		assert.strictEqual(filled[1]!.imageURL, "https://example.com/b.png");
		// (Gone from the league: no picture, the words.)
		assert.isUndefined(filled[2]!.imageURL);
		assert.strictEqual(filled[2]!.text, "C");
	});
});
