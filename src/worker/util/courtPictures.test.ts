import { assert, beforeEach, describe, test } from "vitest";
import { mockIDBLeague, resetCache, resetG } from "../../test/helpers.ts";
import { idb } from "../db/index.ts";
import { g } from "./index.ts";
import { team } from "../core/index.ts";
import { changeTracker } from "../db/changeTracker.ts";
import {
	PIC,
	courtPictureIds,
	decalsForGame,
	prunePictures,
	resolveCourt,
	storePicture,
} from "./courtPictures.ts";

// A picture uploaded for a court is kept once in the league and named from
// the court by id; whatever draws the court gets it filled back in, and a
// picture nothing uses any more is let go.

const PNG = (n: number) => `data:image/png;base64,${"A".repeat(40 + n)}=`;

const setup = async () => {
	resetG();
	g.setWithoutSavingToDB("numTeams", 2);
	g.setWithoutSavingToDB("numActiveTeams", 2);
	const teams = [0, 1].map((tid) =>
		team.generate({
			tid,
			cid: 0,
			did: 0,
			region: `R${tid}`,
			name: `T${tid}`,
			abbrev: `T${tid}`,
			pop: 3,
			popRank: tid + 1,
		}),
	);
	await resetCache({ teams });
	idb.league = { ...mockIDBLeague(), get: async () => undefined };
};

describe("court pictures", () => {
	beforeEach(() => {
		changeTracker.disable();
		changeTracker.reset();
	});

	test("stored once, named by id, filled back in to draw", async () => {
		await setup();
		const id = await storePicture(PNG(1));
		assert.strictEqual(await storePicture(PNG(1)), id);
		const court = {
			floor: "#335577",
			logoURL: `${PIC}${id}`,
			cornerLogoURL: "https://example.com/x.png",
			railImageURL: `${PIC}gone`,
		};
		assert.deepStrictEqual(courtPictureIds(court), [id, "gone"]);
		assert.deepStrictEqual(await resolveCourt(court), {
			floor: "#335577",
			logoURL: PNG(1),
			cornerLogoURL: "https://example.com/x.png",
		});
	});

	test("only pictures are kept", async () => {
		await setup();
		for (const bad of [
			"https://example.com/x.png",
			"data:text/html;base64,AAAA",
			`data:image/png;base64,${"A".repeat(800_000)}`,
		]) {
			let threw = false;
			try {
				await storePicture(bad);
			} catch {
				threw = true;
			}
			assert.ok(threw, bad.slice(0, 30));
		}
	});

	test("a picture nothing uses is let go; one a court or uniform uses stays", async () => {
		await setup();
		const kept = await storePicture(PNG(2));
		const worn = await storePicture(PNG(3));
		const loose = await storePicture(PNG(4));
		const t0 = (await idb.cache.teams.get(0))!;
		t0.court = { logoURL: `${PIC}${kept}` };
		await idb.cache.teams.put(t0);
		const t1 = (await idb.cache.teams.get(1))!;
		t1.jerseySkins = { home: worn };
		await idb.cache.teams.put(t1);
		await prunePictures([kept, worn, loose]);
		assert.ok(await idb.cache.jerseySkins.get(kept));
		assert.ok(await idb.cache.jerseySkins.get(worn));
		assert.strictEqual(await idb.cache.jerseySkins.get(loose), undefined);
	});

	test("a game gets the decals for its occasion and season", async () => {
		await setup();
		const id = await storePicture(PNG(5));
		g.setWithoutSavingToDB("courtDecals", [
			{ image: "https://example.com/always.png", when: "always" },
			{ image: `${PIC}${id}`, when: "openingNight", from: 2025, to: 2026 },
			{
				image: "https://example.com/playoffs.png",
				when: "playoffs",
				pair: true,
			},
			{ image: "https://example.com/finals.png", when: "finals", from: 2030 },
		]);
		const hrefs = async (game: Parameters<typeof decalsForGame>[0]) =>
			(await decalsForGame(game)).map((d) => d.href);
		assert.deepStrictEqual(await hrefs({ season: 2025, day: 1 }), [
			"https://example.com/always.png",
			PNG(5),
		]);
		assert.deepStrictEqual(await hrefs({ season: 2027, day: 1 }), [
			"https://example.com/always.png",
		]);
		assert.deepStrictEqual(await hrefs({ season: 2025, day: 40 }), [
			"https://example.com/always.png",
		]);
		assert.deepStrictEqual(
			await hrefs({ season: 2031, day: 1, playoffs: true, finals: true }),
			[
				"https://example.com/always.png",
				"https://example.com/playoffs.png",
				"https://example.com/finals.png",
			],
		);
		// And a picture a decal uses is kept.
		await prunePictures([id]);
		assert.ok(await idb.cache.jerseySkins.get(id));
	});
});
