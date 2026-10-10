import { assert, beforeEach, describe, test } from "vitest";
import { mockIDBLeague, resetCache, resetG } from "../../../test/helpers.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { team } from "../index.ts";
import { takeArenaLooks } from "./replayLooks.ts";
import { changeTracker } from "../../db/changeTracker.ts";

// The banners in the 3D arena's rafters are drawn like the ones on the team
// history page: each title's in the colors and logo the team had the season
// it won it, each retired number in the colors of the jersey it is shown in.

const NOW: [string, string, string] = ["#0e2240", "#fec524", "#8b2131"];
const THEN: [string, string, string] = ["#4d90cd", "#fdb927", "#0e2240"];

const setup = async () => {
	resetG();
	g.setWithoutSavingToDB("numTeams", 2);
	g.setWithoutSavingToDB("numActiveTeams", 2);
	g.setWithoutSavingToDB("season", 2030);
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
	teams[0]!.colors = NOW;
	teams[0]!.imgURL = "/img/now.svg";
	teams[0]!.retiredJerseyNumbers = [
		{ number: "44", seasonRetired: 2025, seasonTeamInfo: 2012, text: "Legend" },
	];
	await resetCache({ teams });
	idb.league = mockIDBLeague();
	const rounds = g.get("numGamesPlayoffSeries", 2030).length;
	const t = (await idb.cache.teams.get(0))!;
	for (const [season, won, colors, imgURL] of [
		[2012, rounds, THEN, "/img/then.svg"],
		[2020, rounds, undefined, undefined],
		[2021, 1, NOW, undefined],
		[2030, -1, NOW, undefined],
	] as const) {
		const row: any = team.genSeasonRow(t);
		row.season = season;
		row.tid = 0;
		row.playoffRoundsWon = won;
		if (colors) {
			row.colors = colors;
		} else {
			delete row.colors;
		}
		if (imgURL) {
			row.imgURL = imgURL;
		} else {
			delete row.imgURL;
			delete row.imgURLSmall;
		}
		await idb.cache.teamSeasons.add(row);
	}
};

describe("the arena's banners", () => {
	beforeEach(() => {
		changeTracker.disable();
		changeTracker.reset();
	});

	test("each title in the team's colors and logo that season", async () => {
		await setup();
		const looks = (await takeArenaLooks(0, 2030))!;
		assert.deepStrictEqual(looks.titles, [2012, 2020]);
		assert.deepStrictEqual(looks.titleLooks, [
			{ colors: THEN, imgURL: "/img/then.svg" },
			// (A season that kept no colors or logo of its own: the team's.)
			{ colors: NOW, imgURL: "/img/now.svg" },
		]);
	});

	test("a retired number in the colors of the jersey it is shown in", async () => {
		await setup();
		const looks = (await takeArenaLooks(0, 2030))!;
		assert.deepStrictEqual(looks.retired, [
			{ number: "44", name: "Legend", colors: THEN },
		]);
	});
});
