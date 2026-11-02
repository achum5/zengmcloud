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
	homeCourtForGame,
	isHomeOpener,
	prunePictures,
	resolveCourt,
	resolveScoreBug,
	resolveStadium,
	stadiumPictureIds,
	storePicture,
	withDecals,
} from "./courtPictures.ts";
import type {
	CourtDecalPlaced,
	CourtStyle,
	Game,
	StadiumLook,
} from "../../common/types.ts";

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

	test("a whole-floor picture (a JPEG) is kept like any other", async () => {
		await setup();
		const jpeg = `data:image/jpeg;base64,${"B".repeat(300_000)}`;
		const id = await storePicture(jpeg);
		const court = {
			floorImageURL: `${PIC}${id}`,
			floorLines: "picture" as const,
		};
		assert.deepStrictEqual(courtPictureIds(court), [id]);
		assert.deepStrictEqual(await resolveCourt(court), {
			floorImageURL: jpeg,
			floorLines: "picture",
		});
		const t0 = (await idb.cache.teams.get(0))!;
		t0.court = court;
		await idb.cache.teams.put(t0);
		await prunePictures([id]);
		assert.ok(await idb.cache.jerseySkins.get(id));
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

	test("a football stadium's pictures: filled back in to draw, kept while it uses them", async () => {
		await setup();
		const mid = await storePicture(PNG(5));
		const cup = await storePicture(PNG(6));
		const stadium: StadiumLook = {
			turfColor: "#0b3d91",
			midLogoURL: `${PIC}${mid}`,
			trophyURL: `${PIC}gone`,
		};
		assert.deepStrictEqual(stadiumPictureIds(stadium), [mid, "gone"]);
		assert.deepStrictEqual(await resolveStadium(stadium), {
			turfColor: "#0b3d91",
			midLogoURL: PNG(5),
		});
		const t0 = (await idb.cache.teams.get(0))!;
		t0.stadium = { midLogoURL: `${PIC}${mid}` };
		await idb.cache.teams.put(t0);
		await prunePictures([mid, cup]);
		assert.ok(await idb.cache.jerseySkins.get(mid));
		assert.strictEqual(await idb.cache.jerseySkins.get(cup), undefined);
		// (And the league's decals for a game laid on the field, with or
		// without a stadium of its own.)
		const decals: CourtDecalPlaced[] = [{ href: PNG(7), pair: true }];
		assert.deepStrictEqual(withDecals(stadium, decals), {
			...stadium,
			decals,
		});
		assert.deepStrictEqual(withDecals<StadiumLook>(undefined, decals), {
			decals,
		});
		assert.strictEqual(withDecals(stadium, []), stadium);
	});

	test("a jersey linked from elsewhere is left alone: nothing to keep, nothing to remove", async () => {
		await setup();
		const link = "https://cdn.example.com/kits/BOS-home.png";
		const t0 = (await idb.cache.teams.get(0))!;
		t0.jerseySkins = { home: link };
		await idb.cache.teams.put(t0);
		let deleted = 0;
		const del = idb.cache.jerseySkins.delete.bind(idb.cache.jerseySkins);
		idb.cache.jerseySkins.delete = async (id: string) => {
			deleted += 1;
			return del(id);
		};
		await prunePictures([link, "https://cdn.example.com/kits/old.png"]);
		idb.cache.jerseySkins.delete = del;
		assert.strictEqual(deleted, 0);
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

	test("a court's versions: their pictures kept and filled in, the game's laid on it", async () => {
		await setup();
		const id = await storePicture(PNG(8));
		const court: CourtStyle = {
			floor: "#335577",
			versions: {
				playoffs: {
					pictures: [
						{ image: `${PIC}${id}`, adjust: { dx: -38 }, pair: "diagonal" },
					],
				},
				homeOpener: {
					pictures: [{ image: "https://example.com/opener.png" }],
				},
			},
		};
		assert.deepStrictEqual(courtPictureIds(court), [id]);
		const shown = await resolveCourt(court);
		assert.strictEqual(shown?.versions?.playoffs?.pictures?.[0]?.image, PNG(8));
		g.setWithoutSavingToDB("courtDecals", [
			{ image: "https://example.com/league.png", when: "playoffs" },
		]);
		const season = g.get("season");
		const laid = async (game: Partial<Game>) =>
			(
				(
					await homeCourtForGame(shown, {
						gid: 10,
						season,
						day: 50,
						teams: [{ tid: 0 }, { tid: 1 }],
						...game,
					} as Game)
				)?.decals ?? []
			).map((d) => [d.href, d.pair]);
		const playoffs = [
			[PNG(8), "diagonal"],
			["https://example.com/league.png", undefined],
		];
		assert.deepStrictEqual(await laid({ playoffs: true }), playoffs);
		// (The finals, with none of their own, the playoffs'.)
		assert.deepStrictEqual(
			await laid({ playoffs: true, finals: true }),
			playoffs,
		);
		// Its first home game of the season - and not its second.
		const game = (gid: number, day: number, home: number) =>
			({
				gid,
				season,
				day,
				playoffs: false,
				teams: [{ tid: home }, { tid: 1 - home }],
			}) as Game;
		await idb.cache.games.put(game(1, 3, 1));
		await idb.cache.games.put(game(2, 5, 0));
		await idb.cache.games.put(game(3, 9, 0));
		assert.deepStrictEqual(await laid({ gid: 2, day: 5 }), [
			["https://example.com/opener.png", undefined],
		]);
		assert.deepStrictEqual(await laid({ gid: 3, day: 9 }), []);
		assert.isTrue(await isHomeOpener(game(1, 3, 1)));
		assert.isFalse(await isHomeOpener({ ...game(2, 5, 0), playoffs: true }));
		// And a picture a version uses is kept.
		const t0 = (await idb.cache.teams.get(0))!;
		t0.court = court;
		await idb.cache.teams.put(t0);
		await prunePictures([id]);
		assert.ok(await idb.cache.jerseySkins.get(id));
	});

	test("the league's score bug gets its pictures, and keeps them", async () => {
		await setup();
		const id = await storePicture(PNG(6));
		const bug = {
			width: 100,
			height: 20,
			image: `${PIC}${id}`,
			pieces: [
				{ show: "image" as const, x: 0, y: 0, w: 5, h: 5, image: `${PIC}gone` },
				{ show: "clock" as const, x: 5, y: 0, w: 5, h: 5 },
			],
		};
		g.setWithoutSavingToDB("scoreBug", bug);
		const shown = await resolveScoreBug(bug);
		assert.strictEqual(shown?.image, PNG(6));
		assert.strictEqual(shown?.pieces[0]!.image, undefined);
		assert.strictEqual(await resolveScoreBug(null), null);
		await prunePictures([id]);
		assert.ok(await idb.cache.jerseySkins.get(id));
	});
});
