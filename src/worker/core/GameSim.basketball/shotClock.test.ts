import { afterAll, assert, beforeAll, test } from "vitest";
import { resetCache, resetG } from "../../../test/helpers.ts";
import { idb } from "../../db/index.ts";
import { g, helpers } from "../../util/index.ts";
import { PHASE } from "../../../common/constants.ts";
import { player, team } from "../index.ts";
import GameSim from "../GameSim.ts";
import { processTeam } from "../game/loadTeams.ts";
import createRandomPlayers from "../league/create/createRandomPlayers.ts";
import { DEFAULT_LEVEL } from "../../../common/budgetLevels.ts";
import { compileCourt } from "../../../ui/views/LiveGame/court25d/director.ts";
import {
	buildClocks,
	gameClockAt,
	shotClockAt,
} from "../../../ui/views/LiveGame/court25d/clock.ts";

// THE SHOT CLOCK ON THE 3D COURT IS THE SIM'S.
//
// The sim runs a shot clock (no possession outlasts it) and now writes it on
// every play-by-play line. The court used to guess it from who had the ball,
// and guessed wrong whenever the sim handed out a fresh one the court didn't
// know about - an offensive rebound, a foul, a timeout - so the one on screen
// sat at 0 with play going on. A real game, played and then staged, has to
// read what the sim read, and never run out while the ball is still alive.

const seededRandom = () => {
	let a = 0x1234567;
	return () => {
		a |= 0;
		a = (a + 0x6d2b79f5) | 0;
		let t = Math.imul(a ^ (a >>> 15), 1 | a);
		t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
		return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
	};
};
const realRandom = Math.random;

let events: any[] = [];
let roster: { pid: number; team: 0 | 1 }[] = [];

afterAll(() => {
	Math.random = realRandom;
});

beforeAll(async () => {
	Math.random = seededRandom();
	resetG();
	g.setWithoutSavingToDB("numActiveTeams", 2);
	g.setWithoutSavingToDB("numTeams", 2);
	g.setWithoutSavingToDB("phase", PHASE.REGULAR_SEASON);

	const teams: any[] = [];
	for (let tid = 0; tid < 2; tid++) {
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
		activeTids: [0, 1],
		onlyFreeAgents: false,
		scoutingLevel: DEFAULT_LEVEL,
		teams,
	});
	await resetCache({ players, teams, draftPicks: [] });
	for (let tid = 0; tid < 2; tid++) {
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
		const t = await idb.cache.teams.get(tid);
		const teamSeason = await idb.cache.teamSeasons.indexGet(
			"teamSeasonsBySeasonTid",
			[g.get("season"), tid],
		);
		const ps = await idb.cache.players.indexGetAll("playersByTid", tid);
		return processTeam(t!, teamSeason!, ps);
	};
	const sides = [await load(0), await load(1)];
	const result: any = new GameSim({
		gid: 1,
		day: 1,
		teams: helpers.deepCopy(sides) as any,
		doPlayByPlay: true,
		homeCourtFactor: 1,
		neutralSite: false,
		allStarGame: false,
		baseInjuryRate: 0,
	} as any).run();
	events = result.playByPlay;
	// Display side: raw team 0 is home, drawn on the right.
	roster = sides.flatMap((side: any, raw) =>
		side.player.map((p: any) => ({ pid: p.id, team: raw === 0 ? 1 : 0 })),
	) as any;
}, 60_000);

test("every line with a game clock carries the sim's shot clock", () => {
	const withClock = events.filter((e) => typeof e.clock === "number");
	assert.isAbove(withClock.length, 200);
	for (const e of withClock) {
		assert.isNumber(e.shotClock, e.type);
		assert.isAtLeast(e.shotClock, 0);
		assert.isAtMost(e.shotClock, 24);
	}
});

test("the court shows the sim's shot clock, and never 0 with the ball alive", () => {
	const tl = compileCourt({ events, players: roster as any, gid: 1 });
	const clocks = buildClocks(tl, events);

	// On each line, exactly what the sim had. The exceptions are all lines
	// where the clock restarts: a make (a fresh 24, held until the inbound),
	// a line that hands the ball over (the new possession starts at 24), and
	// one after which the sim's clock reads higher than it could have run down
	// to (an offensive rebound, a foul, a timeout) - there it shows at least
	// what the sim had.
	const possStarts = new Set(tl.poss.map(([t]) => t));
	const lines = tl.beats
		.map((b) => ({ t: b.actionStart, e: events[b.i] }))
		.filter(
			(x) =>
				typeof x.e?.shotClock === "number" && typeof x.e.clock === "number",
		);
	let checked = 0;
	for (const [k, { t, e }] of lines.entries()) {
		const shown = shotClockAt(clocks, t);
		const game = gameClockAt(clocks, t);
		if (shown === undefined || game === undefined || e.shotClock > game) {
			// Off: the period will end first.
			continue;
		}
		const next = lines[k + 1]?.e;
		const made =
			/^fg(?!a)/.test(e.type) || /^tp/.test(e.type) || e.type === "ft";
		const resetHere =
			made ||
			possStarts.has(t) ||
			(next !== undefined &&
				next.shotClock > e.shotClock - (e.clock - next.clock) + 0.5);
		if (resetHere) {
			assert.isAtLeast(shown, e.shotClock - 0.25, `${e.type} at ${e.clock}`);
		} else {
			assert.closeTo(shown, e.shotClock, 0.25, `${e.type} at ${e.clock}`);
			checked += 1;
		}
	}
	assert.isAbove(checked, 100);

	// Between lines it runs with the game clock, and a line arrives before it
	// would run out: at no point does it read 0 while the next line, which
	// comes at the sim's shot clock, is still to come with time on it.
	let zeroWhileAlive = 0;
	for (let k = 0; k + 1 < lines.length; k++) {
		const a = lines[k]!;
		const b = lines[k + 1]!;
		if (b.e.shotClock < 1) {
			continue;
		}
		for (let t = a.t + 100; t < b.t; t += 250) {
			const shown = shotClockAt(clocks, t);
			if (shown !== undefined && shown <= 0) {
				zeroWhileAlive += 1;
				break;
			}
		}
	}
	assert.strictEqual(zeroWhileAlive, 0);
}, 60_000);
