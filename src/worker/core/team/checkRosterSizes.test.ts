import { assert, beforeAll, describe, test } from "vitest";
import { PHASE, PLAYER } from "../../../common/constants.ts";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { player, team } from "../index.ts";
import { idb } from "../../db/index.ts";
import { g, helpers } from "../../util/index.ts";
import type { PlayerWithoutKey } from "../../../common/types.ts";
import { DEFAULT_LEVEL } from "../../../common/budgetLevels.ts";

beforeAll(() => {
	resetG(); // Two teams: user and AI

	g.setWithoutSavingToDB("numTeams", 2);
	g.setWithoutSavingToDB("numActiveTeams", 2);
});

// resetCacheWithPlayers({0: 10, 1: 9, [PLAYER.FREE_AGENT]: 1}) will make 10 players on team 0, 9 on team 1, and	// 1 free agent with a minimum contract.
const resetCacheWithPlayers = async (info: Map<number, number>) => {
	const players: PlayerWithoutKey[] = [];

	for (const [tid, numPlayers] of info) {
		for (let i = 0; i < numPlayers; i++) {
			const p = player.generate(tid, 30, 2017, true, DEFAULT_LEVEL);

			if (tid === PLAYER.FREE_AGENT) {
				p.contract.amount = g.get("minContract");
			}

			players.push(p);
		}
	}

	const numTeams = Array.from(info.keys()).filter((tid) => tid >= 0).length;
	const teamsDefault = helpers.getTeamsDefault();
	const teams = teamsDefault.slice(0, numTeams).map(team.generate);

	await resetCache({
		players,
		teams,
	});
};

test("add players to AI team under roster limit without returning error message", async () => {
	await resetCacheWithPlayers(
		new Map([
			[0, 10],
			[1, 9],
			[PLAYER.FREE_AGENT, 1],
		]),
	);

	// Confirm roster size under limit
	let players = await idb.cache.players.indexGetAll("playersByTid", 1);
	assert.strictEqual(players.length, 9);
	const userTeamSizeError = await team.checkRosterSizes("user");
	await team.checkRosterSizes("other");
	assert.strictEqual(userTeamSizeError, undefined);

	// Confirm players added up to limit
	players = await idb.cache.players.indexGetAll("playersByTid", 1);
	assert.strictEqual(players.length, g.get("minRosterSize"));
});

test("automatically create a scrub when AI team needs to add a player but there is none", async () => {
	await resetCacheWithPlayers(
		new Map([
			[0, 10],
			[1, 9],
		]),
	);

	// Confirm roster size under limit
	const userTeamSizeError = await team.checkRosterSizes("user");
	await team.checkRosterSizes("other");
	assert.strictEqual(userTeamSizeError, undefined);

	const players = await idb.cache.players.indexGetAll("playersByTid", 1);
	assert.strictEqual(players.length, g.get("minRosterSize"));
});

test("remove players to AI team over roster limit without returning error message", async () => {
	await resetCacheWithPlayers(
		new Map([
			[0, 10],
			[1, 24],
		]),
	);

	// Confirm roster size over limit
	let players = await idb.cache.players.indexGetAll("playersByTid", 1);
	assert.strictEqual(players.length, 24); // Confirm no error message and roster size pruned to limit

	const userTeamSizeError = await team.checkRosterSizes("user");
	await team.checkRosterSizes("other");
	assert.strictEqual(userTeamSizeError, undefined);
	players = await idb.cache.players.indexGetAll("playersByTid", 1);
	assert.strictEqual(players.length, 15);
});

test("return error message when user team is under roster limit", async () => {
	await resetCacheWithPlayers(
		new Map([
			[0, 9],
			[1, 10],
			[PLAYER.FREE_AGENT, 1],
		]),
	);

	// Confirm roster size under limit
	let players = await idb.cache.players.indexGetAll(
		"playersByTid",
		g.get("userTid"),
	);
	assert.strictEqual(players.length, 9); // Confirm roster size error and no auto-signing of players

	const userTeamSizeError = await team.checkRosterSizes("user");
	assert.strictEqual(typeof userTeamSizeError, "string");
	if (userTeamSizeError) {
		assert(userTeamSizeError.includes("less"));
		assert(userTeamSizeError.includes("minimum"));
	}
	players = await idb.cache.players.indexGetAll(
		"playersByTid",
		g.get("userTid"),
	);
	assert.strictEqual(players.length, 9);
});

test("return error message when user team is over roster limit", async () => {
	await resetCacheWithPlayers(
		new Map([
			[0, 24],
			[1, 10],
		]),
	);

	// Confirm roster size over limit
	let players = await idb.cache.players.indexGetAll(
		"playersByTid",
		g.get("userTid"),
	);
	assert.strictEqual(players.length, 24); // Confirm roster size error and no auto-release of players

	const userTeamSizeError = await team.checkRosterSizes("user");
	assert.strictEqual(typeof userTeamSizeError, "string");
	if (userTeamSizeError) {
		assert(userTeamSizeError.includes("more"));
		assert(userTeamSizeError.includes("maximum"));
	}
	players = await idb.cache.players.indexGetAll(
		"playersByTid",
		g.get("userTid"),
	);
	assert.strictEqual(players.length, 24);
});

// ---------------------------------------------------------------------------
// A ROOKIE CUT BEFORE HE PLAYS COSTS NOTHING, FOR AN AI TEAM TOO.
//
// player.release takes a justDrafted flag and skips the dead money when it is
// set - the user's release button computes it with helpers.justDrafted. This
// path passed false unconditionally, so an AI team cutting its own second
// rounder booked the whole rookie deal as money paid to nobody, for a release
// the rules charge nothing for.
// ---------------------------------------------------------------------------
describe("releasing a just-drafted rookie", () => {
	const build = async (rookie: boolean) => {
		resetG();
		g.setWithoutSavingToDB("numTeams", 2);
		g.setWithoutSavingToDB("numActiveTeams", 2);
		g.setWithoutSavingToDB("phase", PHASE.FREE_AGENCY);
		g.setWithoutSavingToDB("userTids", [999]);
		g.setWithoutSavingToDB("maxRosterSize", 3);
		g.setWithoutSavingToDB("minRosterSize", 1);

		const season = g.get("season");
		const players: any[] = [];
		for (let i = 0; i < 4; i++) {
			const p: any = player.generate(0, 22, season - 22, true, DEFAULT_LEVEL);
			p.pid = i;
			p.born.year = season - 22;
			p.ratings.at(-1).ovr = 60 - i * 10;
			p.value = 60 - i * 10;
			// ONE POSITION FOR ALL FOUR, so the man this fixture is about is the
			// man cutOrder actually lets go. player.generate draws a position at
			// random, and cutOrder protects the last player at a thin one
			// (SCARCITY_PROTECTION) - so on the draws where the rookie was
			// somebody's only centre he was spared and a different player was
			// released, which is a different test passing or failing by luck.
			p.ratings.at(-1).pos = "SF";
			p.injury = { type: "Healthy", gamesRemaining: 0 };
			// The worst of them is the one cutOrder will let go.
			p.contract = { amount: g.get("minContract"), exp: season + 2 };
			if (i === 3) {
				p.draft = { ...p.draft, year: season, round: 2, pick: 1, tid: 0 };
				p.contract.rookie = rookie;
			}
			players.push(p);
		}
		await resetCache({
			players,
			teams: [0, 1].map((tid) =>
				team.generate({
					tid,
					cid: 0,
					did: 0,
					region: `R${tid}`,
					name: `N${tid}`,
					abbrev: `T${tid}`,
					pop: 1,
					imgURL: "",
				} as any),
			),
		});
	};

	test("costs the team nothing", async () => {
		await build(true);
		await team.checkRosterSizes("other");
		assert.lengthOf(
			await idb.cache.releasedPlayers.getAll(),
			0,
			"an AI team paid dead money for a cut the rules make free",
		);
	});

	// The control: the same cut, on a man whose deal is not a rookie contract,
	// still books the money. Without this the test above passes if releases
	// stop being recorded at all.
	test("but any other contract still does", async () => {
		await build(false);
		await team.checkRosterSizes("other");
		assert.isAbove((await idb.cache.releasedPlayers.getAll()).length, 0);
	});
});
