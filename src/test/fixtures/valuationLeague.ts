import { resetCache, resetG } from "../helpers.ts";
import { idb } from "../../worker/db/index.ts";
import { g, local } from "../../worker/util/index.ts";
import { PHASE, PLAYER } from "../../common/constants.ts";
import { player, team } from "../../worker/core/index.ts";
import { DEFAULT_LEVEL } from "../../common/budgetLevels.ts";

// ---------------------------------------------------------------------------
// A LEAGUE THE TRADE VALUATION CAN BE ASKED QUESTIONS ABOUT.
//
// Shared by the exploit tests (what a person can get away with) and the
// property tests (what the arithmetic must never do), because both learned the
// same two lessons the hard way and neither should have to learn them again.
//
// 1. THE LEAGUE NEEDS A REAL SPREAD OF ABILITY. Value is a z-score against the
//    league, so a fixture where everyone sits in a narrow band turns an
//    ordinary starter into a four-sigma freak and prices him accordingly. On a
//    flat 44-57 band a 62 ovr came out immune to his own contract - a finding
//    that was entirely an artefact of the fixture.
//
// 2. RATINGS GO THROUGH THE REAL VALUE FUNCTION, once the whole league is in
//    the cache. Pinning p.value by hand - as the older trade tests reasonably
//    do, because they are testing guards rather than arithmetic - assumes age
//    and potential out of the answer before the calculator ever sees them.
//
// And one thing worth knowing before writing a test against it: a buyer
// protects EVERY player above coreValue outright (selectBuildingBlocks), so
// asking one for a good player is answered by that guard before a single
// number is compared. Anything meant to measure valuation has to ask for
// somebody modest.
// ---------------------------------------------------------------------------

export const USER_TID = 0;
export const AI_TID = 1;
export const NUM_TEAMS = 8;

// A real league's ovr distribution - see (1) above.
const SPREAD = [
	28, 33, 36, 38, 40, 42, 43, 44, 45, 46, 47, 48, 48, 49, 50, 50, 51, 52, 52,
	53, 54, 55, 56, 57, 58, 60, 62, 64, 67, 71,
];

let nextPid = 1;

// A player as a test writes him: the team is supplied by build().
export type Spec = {
	ovr: number;
	pot?: number;
	age?: number;
	amount?: number;
	exp?: number;
	injuredGames?: number;
};

const makePlayer = ({
	tid,
	ovr,
	pot = ovr,
	age = 27,
	amount,
	exp,
	injuredGames = 0,
}: {
	tid: number;
	ovr: number;
	pot?: number;
	age?: number;
	amount?: number;
	exp?: number;
	injuredGames?: number;
}) => {
	const season = g.get("season");
	const p: any = player.generate(tid, age, season - age, true, DEFAULT_LEVEL);
	p.pid = nextPid++;
	p.born.year = season - age;
	const r = p.ratings.at(-1);
	r.ovr = ovr;
	r.pot = Math.max(ovr, pot);
	p.contract = {
		amount: amount ?? g.get("minContract") * 3,
		exp: exp ?? season + 2,
	};
	p.injury =
		injuredGames > 0
			? { type: "Sprained Ankle", gamesRemaining: injuredGames }
			: { type: "Healthy", gamesRemaining: 0 };
	return p;
};

export const stubLeagueDb = () => {
	const store = {
		index: () => store,
		getAll: async () => [],
		get: async () => undefined,
		async *iterate() {},
	};
	(idb as any).league = {
		getAll: async () => [],
		get: async () => undefined,
		transaction: () => ({
			store,
			objectStore: () => store,
			done: Promise.resolve(),
		}),
	};
};

// A plausible league: the AI team is a mid-table buyer with a normal roster,
// the user has one too, and there are enough other teams for the league-wide
// bars (star, starter, core) to mean something.
export const buildValuationLeague = async (extra: {
	user?: Spec[];
	ai?: Spec[];
	// Draft picks the AI team owns, as round numbers. All in the same future
	// season, so a pile of them is a pile of near-identical assets and any
	// difference in what they are worth is the pile itself.
	aiPicks?: number[];
	// Wins for the AI team, which is what decides its tier. The default puts it
	// mid-table, where it reads as a buyer; drop it and it becomes a seller,
	// which matters because the tiers do not share an age table - a buyer's
	// stops at 24 and does not extend, so an ageing player gets no tier-level
	// penalty from it at all.
	aiWon?: number;
}) => {
	resetG();
	nextPid = 1;
	g.setWithoutSavingToDB("numTeams", NUM_TEAMS);
	g.setWithoutSavingToDB("numActiveTeams", NUM_TEAMS);
	g.setWithoutSavingToDB("phase", PHASE.REGULAR_SEASON);
	g.setWithoutSavingToDB("userTid", USER_TID);
	g.setWithoutSavingToDB("userTids", [USER_TID]);
	g.setWithoutSavingToDB("smartAiFrontOffice", true);
	g.setWithoutSavingToDB("realisticFaces", false);
	g.setWithoutSavingToDB("faceAging", false);
	g.setWithoutSavingToDB("sonRate", 0);
	g.setWithoutSavingToDB("brotherRate", 0);

	const season = g.get("season");
	const teams: any[] = [];
	for (let tid = 0; tid < NUM_TEAMS; tid++) {
		teams.push(
			team.generate({
				tid,
				cid: tid % 2,
				did: tid % 2,
				region: `R${tid}`,
				name: `N${tid}`,
				abbrev: `T${tid}`,
				pop: 3,
				imgURL: "",
			} as any),
		);
	}

	const players: any[] = [];
	// A spread of ordinary players everywhere, so nobody's roster is a special
	// case and the league bars land where they would in a real league.
	for (let tid = 0; tid < NUM_TEAMS; tid++) {
		for (let i = 0; i < 11; i++) {
			players.push(
				makePlayer({
					tid,
					ovr: SPREAD[(tid * 11 + i) % SPREAD.length]!,
					age: 24 + (i % 7),
				}),
			);
		}
	}
	const userExtra: any[] = [];
	for (const spec of extra.user ?? []) {
		const p = makePlayer({ ...spec, tid: USER_TID });
		players.push(p);
		userExtra.push(p);
	}
	const aiExtra: any[] = [];
	for (const spec of extra.ai ?? []) {
		const p = makePlayer({ ...spec, tid: AI_TID });
		players.push(p);
		aiExtra.push(p);
	}

	// A draft class for the picks to be worth something against. Without one
	// every pick prices at null and the whole valuation comes out NaN - the trap
	// offseasonSim.test.ts warns about. Steep, so slots are not interchangeable.
	if ((extra.aiPicks ?? []).length > 0) {
		for (let i = 0; i < NUM_TEAMS * 3; i++) {
			const prospect = makePlayer({
				tid: PLAYER.UNDRAFTED,
				ovr: 66 - i,
				pot: 74 - i,
				age: 19,
			});
			prospect.draft.year = season + 1;
			players.push(prospect);
		}
	}

	const draftPicks = (extra.aiPicks ?? []).map((round, i) => ({
		dpid: 100 + i,
		tid: AI_TID,
		originalTid: AI_TID,
		round,
		pick: 0,
		season: season + 1,
	}));

	await resetCache({ players, teams, draftPicks: draftPicks as any });
	stubLeagueDb();

	// The REAL value function, run once the whole league is in the cache -
	// value is relative to the league's ovr mean and standard deviation, so it
	// cannot be computed before the league exists. Pinning p.value by hand
	// instead, as the other trade tests reasonably do for testing the guards,
	// would make most of this file vacuous: age and potential would have been
	// assumed out of the answer before the AI ever saw it.
	local.playerOvrMeanStdStale = true;
	for (const p of await idb.cache.players.getAll()) {
		await player.updateValues(p);
		await idb.cache.players.put(p);
	}
	for (let tid = 0; tid < NUM_TEAMS; tid++) {
		const row: any = team.genSeasonRow((await idb.cache.teams.get(tid))!);
		row.season = season;
		// Middling records all round, so nobody is pushed to an extreme tier.
		row.won =
			tid === AI_TID && extra.aiWon !== undefined
				? extra.aiWon
				: 38 + (tid % 3) * 3;
		row.lost = 82 - row.won;
		row.gp = 82;
		await idb.cache.teamSeasons.add(row);
	}
	return { userExtra, aiExtra, dpids: draftPicks.map((dp) => dp.dpid) };
};
