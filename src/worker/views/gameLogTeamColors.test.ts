import { assert, beforeEach, describe, test } from "vitest";
import { resetCache, resetG } from "../../test/helpers.ts";
import { idb } from "../db/index.ts";
import { g } from "../util/index.ts";
import { team } from "../core/index.ts";
import { setTeamInfo } from "./gameLog.ts";
import { DEFAULT_TEAM_COLORS } from "../../common/constants.ts";
import { changeTracker } from "../db/changeTracker.ts";

// ---------------------------------------------------------------------------
// Team colours for a game played in a past season.
//
// setTeamInfo resolves every field of a historical box score the same way -
// take it from that season's teamSeason row, and fall back to teamInfoCache
// when the row does not carry it. Every field except COLOURS, which was
// assigned raw and so fell straight through to the neutral default whenever a
// season row lacked it. Rewatching an old game therefore drew both clubs in
// grey, which is what this covers.
//
// The gap is easy to reintroduce because teamInfoCache genuinely has no colors
// field - the obvious sibling fallback does not compile, so the honest-looking
// fix is to leave the line bare.
// ---------------------------------------------------------------------------

const LAKERS: [string, string, string] = ["#552583", "#fdb927", "#000000"];
const PAST_SEASON = 2002;

const setup = async ({
	seasonRowHasColors,
}: {
	seasonRowHasColors: boolean;
}) => {
	resetG();
	g.setWithoutSavingToDB("numTeams", 2);
	g.setWithoutSavingToDB("numActiveTeams", 2);
	g.setWithoutSavingToDB("season", PAST_SEASON + 3);

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
	teams[0]!.colors = LAKERS;

	await resetCache({ teams });

	// A season row for the old game, of the shape an imported real-players
	// history produces: it knows the name and record but carries no colours.
	const row: any = team.genSeasonRow((await idb.cache.teams.get(0))!);
	row.season = PAST_SEASON;
	row.tid = 0;
	row.won = 59;
	row.lost = 23;
	if (!seasonRowHasColors) {
		delete row.colors;
	}
	await idb.cache.teamSeasons.add(row);
};

const runSetTeamInfo = async () => {
	const t: any = { tid: 0, players: [] };
	await setTeamInfo(t, 0, undefined, {
		season: PAST_SEASON,
		teams: [
			{ tid: 0, abbrev: "T0" },
			{ tid: 1, abbrev: "T1" },
		],
	});
	return t;
};

describe("team colours on a past-season box score", () => {
	beforeEach(() => {
		changeTracker.disable();
		changeTracker.reset();
	});

	test("a season row without colours falls back to the club's own, not grey", async () => {
		await setup({ seasonRowHasColors: false });
		const t = await runSetTeamInfo();

		assert.ok(
			JSON.stringify(t.colors) !== JSON.stringify(DEFAULT_TEAM_COLORS),
			"a rewatched old game drew the team in the neutral default instead of its own colours",
		);
		assert.deepStrictEqual(t.colors, LAKERS);
	});

	test("a season row WITH colours still wins - that is the point of storing them", async () => {
		// A club that changed colours must be drawn in the ones it wore THAT year,
		// so the fallback above must never override a row that has them.
		await setup({ seasonRowHasColors: true });
		const throwback: [string, string, string] = [
			"#111111",
			"#222222",
			"#333333",
		];
		const row = (await idb.cache.teamSeasons.indexGet(
			"teamSeasonsByTidSeason",
			[0, PAST_SEASON],
		))!;
		(row as any).colors = throwback;
		await idb.cache.teamSeasons.put(row);

		const t = await runSetTeamInfo();
		assert.deepStrictEqual(t.colors, throwback);
	});

	test("a team that no longer exists still gets something renderable", async () => {
		// All-star and placeholder sides have no team record to read, and must not
		// throw on the way to the default.
		await setup({ seasonRowHasColors: false });
		const t: any = { tid: -1, players: [] };
		await setTeamInfo(t, 0, undefined, {
			season: PAST_SEASON,
			teams: [
				{ tid: -1, abbrev: "AS1" },
				{ tid: -2, abbrev: "AS2" },
			],
		});
		assert.deepStrictEqual(t.colors, DEFAULT_TEAM_COLORS);
	});
});
