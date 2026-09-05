import { assert, beforeAll, describe, test } from "vitest";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { idb } from "../../db/index.ts";
import { g, helpers } from "../../util/index.ts";
import { PHASE } from "../../../common/constants.ts";
import { player, team } from "../index.ts";
import GameSim from "../GameSim.ts";
import { processTeam } from "../game/loadTeams.ts";
import createRandomPlayers from "../league/create/createRandomPlayers.ts";
import { DEFAULT_LEVEL } from "../../../common/budgetLevels.ts";
import {
	plannedMinutes,
	type RotationStint,
	type TeamRotation,
} from "../../../common/rotation.ts";
import { generateRotation } from "../team/generateRotation.ts";

// The worker's process.env type is a closed set; the diagnostic flag reaches
// it the way the other harnesses' do.
const nodeEnv: Record<string, string | undefined> =
	(globalThis as any).process?.env ?? {};

// DOES THE SIM ACTUALLY FOLLOW A PLAN?
//
// Two real teams and a realistic plan - the coach's own rotation, drawn up for
// a roster missing its best player - and the minutes everybody ends up with.
// The plan is a guide, so nothing is exact: foul trouble, blowouts and a close
// finish all hand the floor back to the coach, by design, and dead balls
// decide when a change can happen. But a plan followed puts the best player
// on the bench for most of the night, and lands everybody else within a few
// minutes of what was drawn for them. That is what is asserted.

const NUM_TEAMS = 2;

const stubLeagueDb = () => {
	const store = {
		index: () => store,
		getAll: async () => [],
		get: async () => undefined,
		async *iterate() {},
	};
	(idb as any).league = {
		transaction: () => ({
			store,
			objectStore: () => store,
			done: Promise.resolve(),
		}),
		getAll: async () => [],
		get: async () => undefined,
	};
};

type Side = Awaited<ReturnType<typeof processTeam>>;

let sides: [Side, Side];
let ordered: number[];

const fullGame = (pids: number[]): RotationStint[] =>
	pids.flatMap((pid) =>
		[0, 1, 2, 3].map((period) => ({ pid, period, start: 0, end: 1 })),
	);

const play = (rotation: TeamRotation | undefined, games = 6) => {
	const minutes = new Map<number, number>();
	for (let i = 0; i < games; i++) {
		const teams = helpers.deepCopy(sides) as any;
		teams[0].rotation = rotation;
		const result: any = new GameSim({
			gid: i,
			day: 1,
			teams,
			doPlayByPlay: false,
			homeCourtFactor: 1,
			neutralSite: false,
			allStarGame: false,
			baseInjuryRate: 0,
		} as any).run();
		for (const p of result.team[0].player) {
			minutes.set(p.id, (minutes.get(p.id) ?? 0) + p.stat.min / games);
		}
		if (nodeEnv.ROTATION_DIAG) {
			console.log(
				`game ${i} ot=${result.overtimes} ` +
					result.team[0].player
						.map((p: any) => `${p.id}:${p.stat.min.toFixed(0)}m/${p.stat.pf}pf`)
						.join(" "),
			);
		}
	}
	return minutes;
};

beforeAll(async () => {
	resetG();
	g.setWithoutSavingToDB("numActiveTeams", NUM_TEAMS);
	g.setWithoutSavingToDB("numTeams", NUM_TEAMS);
	g.setWithoutSavingToDB("userTids", [0]);
	g.setWithoutSavingToDB("userTid", 0);
	g.setWithoutSavingToDB("phase", PHASE.REGULAR_SEASON);
	g.setWithoutSavingToDB("rotationPlans", true);

	const teams: any[] = [];
	for (let tid = 0; tid < NUM_TEAMS; tid++) {
		teams.push(
			team.generate({
				tid,
				cid: 0,
				did: 0,
				region: `Region${tid}`,
				name: `Name${tid}`,
				abbrev: `T${tid}`,
				pop: 1,
				imgURL: "",
			} as any),
		);
	}
	const players = await createRandomPlayers({
		activeTids: teams.map((t) => t.tid),
		onlyFreeAgents: false,
		scoutingLevel: DEFAULT_LEVEL,
		teams,
	});
	await resetCache({ players, teams, draftPicks: [] });
	stubLeagueDb();
	for (let tid = 0; tid < NUM_TEAMS; tid++) {
		const t = (await idb.cache.teams.get(tid))!;
		await idb.cache.teamSeasons.add(team.genSeasonRow(t) as any);
	}
	for (const p of await idb.cache.players.indexGetAll("playersByTid", [
		0,
		Infinity,
	])) {
		await player.updateValues(p);
		p.injury = { type: "Healthy", gamesRemaining: 0 };
		await idb.cache.players.put(p);
	}

	const load = async (tid: number) => {
		const [t, teamSeason, ps] = await Promise.all([
			idb.cache.teams.get(tid),
			idb.cache.teamSeasons.indexGet("teamSeasonsBySeasonTid", [
				g.get("season"),
				tid,
			]),
			idb.getCopies.players({ tid }, "noCopyCache"),
		]);
		return processTeam(t!, teamSeason!, ps);
	};
	sides = [await load(0), await load(1)];

	// Best to worst, the way the coach sees them.
	ordered = [...sides[0].player]
		.sort((a: any, b: any) => b.valueNoPot - a.valueNoPot)
		.map((p: any) => p.id);
});

describe("a rotation plan in the sim", () => {
	// The coach's rotation for the roster without its best player, which is
	// the shape a real plan has: starters, a bench, nobody all night.
	const planWithoutStar = (): TeamRotation => {
		const star = ordered[0]!;
		const candidates = sides[0].player
			.filter((p: any) => p.id !== star)
			.map((p: any) => ({
				pid: p.id,
				value: p.valueNoPot,
				ptModifier: 1,
				injured: false,
			}));
		return {
			auto: false,
			stints: generateRotation(candidates, {
				numPeriods: 4,
				periodLength: 12,
				numPlayersOnCourt: 5,
			}),
		};
	};

	test("a plan is followed", () => {
		const star = ordered[0]!;
		const plan = planWithoutStar();

		const withPlan = play(plan, 10);
		const without = play(undefined, 10);

		// The best player is not in the plan, so he only sees the floor when
		// the coach takes over: foul trouble, a blowout, a close finish.
		assert.isBelow(withPlan.get(star)!, 12, "star with plan");
		assert.isAbove(without.get(star)!, 25, "star without plan");

		// Everybody else lands near what was drawn for him. This is the
		// fidelity a planner cares about, and it is the number to watch if the
		// substitution rules ever change.
		let totalError = 0;
		let n = 0;
		for (const p of sides[0].player as any[]) {
			if (p.id === star) {
				continue;
			}
			const planned = plannedMinutes(plan.stints, p.id, 12);
			totalError += Math.abs(withPlan.get(p.id)! - planned);
			n += 1;
		}
		assert.isBelow(totalError / n, 4, "mean minutes off plan");
	});

	test("a team on auto is left to the coach", () => {
		const star = ordered[0]!;
		const minutes = play({ ...planWithoutStar(), auto: true });
		assert.isAbove(minutes.get(star)!, 25);
	});

	test("nothing changes while the league setting is off", () => {
		g.setWithoutSavingToDB("rotationPlans", false);
		try {
			const star = ordered[0]!;
			const minutes = play(planWithoutStar());
			assert.isAbove(minutes.get(star)!, 25);
		} finally {
			g.setWithoutSavingToDB("rotationPlans", true);
		}
	});

	// Fidelity at the level a planner cares about: a six minute stint in the
	// plan is a stint of about six minutes on the floor, not two and not
	// twelve. Dead balls and the late-game window keep it from being exact.
	test("a planned stint lands close to its length", () => {
		const tenthMan = ordered[9]!;
		const starters = ordered.slice(0, 5);
		const stints: RotationStint[] = fullGame(starters.slice(0, 4));
		// The fifth starter sits for the second half of the first quarter, and
		// the tenth man takes exactly that stretch.
		const fifth = starters[4]!;
		stints.push(
			{ pid: fifth, period: 0, start: 0, end: 0.5 },
			{ pid: tenthMan, period: 0, start: 0.5, end: 1 },
			...[1, 2, 3].map((period) => ({ pid: fifth, period, start: 0, end: 1 })),
		);

		const minutes = play({ auto: false, stints }, 10);
		assert.closeTo(minutes.get(tenthMan)!, 6, 3, "tenth man's six minutes");
	});

	test("a player who cannot play is not forced on", () => {
		const tenthMan = ordered[9]!;
		const teams = helpers.deepCopy(sides) as any;
		teams[0].rotation = {
			auto: false,
			stints: fullGame([
				ordered[1]!,
				ordered[2]!,
				ordered[3]!,
				ordered[4]!,
				tenthMan,
			]),
		};
		const injured = teams[0].player.find((p: any) => p.id === tenthMan);
		injured.injured = true;
		const result: any = new GameSim({
			gid: 0,
			day: 1,
			teams,
			doPlayByPlay: false,
			homeCourtFactor: 1,
			neutralSite: false,
			allStarGame: false,
			baseInjuryRate: 0,
		} as any).run();
		const line = result.team[0].player.find((p: any) => p.id === tenthMan);
		assert.strictEqual(line.stat.min, 0);
	});
});
