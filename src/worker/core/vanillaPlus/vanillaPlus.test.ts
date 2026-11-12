import { afterAll, assert, beforeAll, test } from "vitest";
import { PHASE, PLAYER } from "../../../common/constants.ts";
import { makeSeededRandom } from "../../../common/random.ts";
import { idb } from "../../db/index.ts";
import { g, local } from "../../util/index.ts";
import { createArenaLeague } from "../../../test/arena/runLeague.ts";
import { vanillaPlusSettings } from "./settings.ts";
import { ensureLanes, type Lane } from "./lanes.ts";
import { tweaksFor } from "./tweaks.ts";
import { playerWorth, windowFor, worth, worthSettings } from "./worth.ts";
import { calendar, calendarSettings, thisSeason } from "./calendar.ts";
import { tradeValueCalculator } from "./index.ts";
import makeItWork from "../trade/makeItWork.ts";
import { contractOf } from "../realisticFrontOffice/contractOf.ts";
import { marketStats, tradeDay, tradeWindow } from "./market.ts";
import { partnerTakes, roomOnValues } from "./clearRoom.ts";
import type { RoomPlan } from "../realisticFrontOffice/clearRoom.ts";

const importNode = (name: string) => import(/* @vite-ignore */ name);
const realRandom = Math.random;
const wasOn = vanillaPlusSettings.on;
const calendarWas = vanillaPlusSettings.calendar;
const patienceWas = vanillaPlusSettings.patience;
const auctionsWas = vanillaPlusSettings.auctions;

beforeAll(async () => {
	await importNode("fake-indexeddb/auto");
	Math.random = makeSeededRandom(31);
	(idb as any).league = undefined;
	await createArenaLeague(9131);
	g.setWithoutSavingToDB("spectator", false);
	g.setWithoutSavingToDB("userTid", 0);
	g.setWithoutSavingToDB("userTids", [0]);
	g.setWithoutSavingToDB("realisticFrontOffices", true);
	g.setWithoutSavingToDB("phase", PHASE.PRESEASON);
	vanillaPlusSettings.on = true;
}, 120_000);

afterAll(() => {
	vanillaPlusSettings.on = wasOn;
	vanillaPlusSettings.calendar = calendarWas;
	vanillaPlusSettings.patience = patienceWas;
	vanillaPlusSettings.auctions = auctionsWas;
	Math.random = realRandom;
});

test("every team has a lane: a few all-in, the bottom rebuilding or retooling", async () => {
	const lanes = await ensureLanes();
	const n = g.get("numActiveTeams");
	assert.strictEqual(lanes.size, n);
	const count = (lane: Lane) =>
		[...lanes.values()].filter((x) => x === lane).length;
	assert.strictEqual(count("allIn"), Math.ceil(0.2 * n));
	// The top half all-in or playoff teams; the bottom quarter rebuilding.
	assert.isAtLeast(count("allIn") + count("playoff"), Math.ceil(0.5 * n));
	assert.isAtLeast(count("rebuild"), Math.ceil(0.25 * n));
	// Stock's strategy kept in step.
	for (const t of await idb.cache.teams.getAll()) {
		assert.strictEqual(
			t.strategy,
			t.lane === "allIn" || t.lane === "playoff" ? "contending" : "rebuilding",
		);
	}
});

test("a contender with no big men values a center more than a guard as good", async () => {
	const lanes = await ensureLanes();
	const allIn = [...lanes].find(([, lane]) => lane === "allIn")![0];
	// Its centers and power forwards off the roster.
	for (const p of await idb.cache.players.indexGetAll("playersByTid", allIn)) {
		const pos = p.ratings.at(-1)!.pos;
		if (pos.includes("C") || pos === "PF") {
			p.tid = PLAYER.FREE_AGENT;
			await idb.cache.players.put(p);
		}
	}
	const others = (
		await idb.cache.players.indexGetAll("playersByTid", [0, Infinity])
	).filter((p) => p.tid !== allIn);
	const centers = others.filter((p) => p.ratings.at(-1)!.pos === "C");
	const guards = others.filter((p) =>
		["PG", "SG", "G"].includes(p.ratings.at(-1)!.pos),
	);
	// A center and a guard of about the same value.
	let pair: [(typeof others)[number], (typeof others)[number]] | undefined;
	let gap = Infinity;
	for (const c of centers.filter((p) => p.valueNoPot >= 55)) {
		for (const gd of guards) {
			const d = Math.abs(c.valueNoPot - gd.valueNoPot);
			if (d < gap) {
				gap = d;
				pair = [c, gd];
			}
		}
	}
	assert.isDefined(pair);
	const tweaks = await tweaksFor(lanes);
	const asset = (p: (typeof others)[number]) =>
		({
			type: "player",
			pid: p.pid,
			value: 0,
			contractValue: 0,
			injury: p.injury,
			age: 25,
			justDrafted: false,
		}) as const;
	const center = tweaks(allIn, asset(pair![0]), "add");
	const guard = tweaks(allIn, asset(pair![1]), "add");
	assert.isAbove(center, guard);
	assert.isAbove(center, 1);
	assert.isAtMost(center, 1.25);
});

test(
	"a rebuilding team won't give its best player away",
	{ timeout: 120_000 },
	async () => {
		const lanes = await ensureLanes();
		const calc = await tradeValueCalculator();
		for (const [tid, lane] of lanes) {
			if (lane !== "rebuild") {
				continue;
			}
			const best = (
				await idb.cache.players.indexGetAll("playersByTid", tid)
			).sort((a, b) => b.value - a.value)[0]!;
			const dv = await calc.evaluate({
				tid,
				pidsAdd: [],
				pidsRemove: [best.pid],
				dpidsAdd: [],
				dpidsRemove: [],
				tradingPartnerTid: 0,
			});
			assert.isBelow(dv, 0);
		}
	},
);

test(
	"what would make it work never leaves a roster past the limit",
	{ timeout: 120_000 },
	async () => {
		const lanes = await ensureLanes();
		const calc = await tradeValueCalculator();
		const maxRosterSize = g.get("maxRosterSize");
		const best = (await idb.cache.players.indexGetAll("playersByTid", 0)).sort(
			(a, b) => b.value - a.value,
		)[0]!;
		for (const tid of [...lanes.keys()].filter((tid) => tid !== 0)) {
			const teams = await makeItWork(
				[
					{
						tid: 0,
						pids: [best.pid],
						dpids: [],
						pidsExcluded: [],
						dpidsExcluded: [],
					},
					{ tid, pids: [], dpids: [], pidsExcluded: [], dpidsExcluded: [] },
				],
				{ holdUserConstant: false, valueChangeCalculator: calc },
			);
			if (!teams) {
				continue;
			}
			for (const i of [0, 1] as const) {
				const now = (
					await idb.cache.players.indexGetAll("playersByTid", teams[i].tid)
				).length;
				const after = now - teams[i].pids.length + teams[1 - i]!.pids.length;
				assert.isAtMost(after, Math.max(maxRosterSize, now));
			}
		}
	},
);

test("contenders weigh now, rebuilders later: teenagers and veterans priced by when they help", async () => {
	const players = await idb.cache.players.indexGetAll("playersByTid", [
		0,
		Infinity,
	]);
	const season = g.get("season");
	const age = (p: (typeof players)[number]) => season - p.born.year;
	const best = (list: typeof players) =>
		list.sort((a, b) => b.value - a.value)[0]!;
	const teen = best(players.filter((p) => age(p) <= 20));
	const vet = best(players.filter((p) => age(p) >= 30));
	const t = (lane?: "allIn" | "rebuild") => playerWorth(teen, lane);
	const v = (lane?: "allIn" | "rebuild") => playerWorth(vet, lane);
	assert.isBelow(t("allIn").own, t().market);
	assert.isAbove(t("rebuild").own, t().market);
	assert.isAbove(v("allIn").own, v().market);
	assert.isBelow(v("rebuild").own, v().market);
	// In order across the lanes.
	const lanes = ["allIn", "playoff", "retool", "rebuild"] as const;
	for (let i = 1; i < lanes.length; i++) {
		assert.isAbove(
			playerWorth(teen, lanes[i]).own,
			playerWorth(teen, lanes[i - 1]).own,
		);
		assert.isBelow(
			playerWorth(vet, lanes[i]).own,
			playerWorth(vet, lanes[i - 1]).own,
		);
	}
});

// The market's horizon is set so that players in their prime cost about
// what vanilla says: the scale everything else is measured on.
test("players in their prime are worth about vanilla's price", async () => {
	const season = g.get("season");
	const tv = (value: number) => {
		const z = (value - local.playerOvrMean) / local.playerOvrStd;
		return z > 1 ? z ** 7 : z;
	};
	const ratios = (
		await idb.cache.players.indexGetAll("playersByTid", [0, Infinity])
	)
		.filter((p) => {
			const a = season - p.born.year;
			return a >= 25 && a <= 28 && tv(p.value) > 1;
		})
		.map((p) => worth(p, worthSettings.market) / tv(p.value))
		.sort((a, b) => a - b);
	assert.isAbove(ratios.length, 10);
	const median = ratios[Math.floor(ratios.length / 2)]!;
	assert.isAbove(median, 0.7);
	assert.isBelow(median, 1.4);
});

test("no team counts a player it gives away at less than his market value", async () => {
	const lanes = await ensureLanes();
	const tweaks = await tweaksFor(lanes);
	for (const p of await idb.cache.players.indexGetAll("playersByTid", [
		0,
		Infinity,
	])) {
		const asset = {
			type: "player",
			pid: p.pid,
			value: 0,
			contractValue: 0,
			injury: p.injury,
			age: g.get("season") - p.born.year,
			justDrafted: false,
		} as const;
		assert.isAtLeast(tweaks(p.tid, asset, "remove"), 1);
	}
});

test("this season counts most to a team in place for the playoffs", async () => {
	vanillaPlusSettings.calendar = true;
	const c = await calendar();
	const lanes = await ensureLanes();
	const [best] = [...lanes].find(([, lane]) => lane === "allIn")!;
	const [worst] = [...lanes].find(([, lane]) => lane === "rebuild")!;
	assert.isAbove(c.playoffs.get(best)!, 1);
	assert.isBelow(c.playoffs.get(worst)!, 1);
	assert.isAbove(thisSeason(c, c.playoffs.get(best)!), thisSeason(c, 1));
	assert.isBelow(thisSeason(c, c.playoffs.get(worst)!), thisSeason(c, 1));
	// The market's, before the season: a full season, as vanilla's.
	assert.closeTo(thisSeason(c, 1), 1, 1e-9);
});

test("a player's seasons past his contract count by the chance he re-signs", async () => {
	const p = (
		await idb.cache.players.indexGetAll("playersByTid", [0, Infinity])
	).sort((a, b) => b.value - a.value)[0]!;
	const full = worth(p, worthSettings.market, 0, {
		thisSeason: 1,
		nextSeason: 1,
		exp: Infinity,
		retention: 1,
	});
	const expiring = worth(p, worthSettings.market, 0, {
		thisSeason: 1,
		nextSeason: 1,
		exp: g.get("season"),
		retention: 0.5,
	});
	assert.isBelow(expiring, full);
	// This season is all his; only what's after is at risk.
	assert.isAbove(expiring, full * 0.3);
});

test(
	"selling its best player makes a bad team's own pick better",
	{ timeout: 120_000 },
	async () => {
		const lanes = await ensureLanes();
		const [worst] = [...lanes].find(([, lane]) => lane === "rebuild")!;
		const best = (
			await idb.cache.players.indexGetAll("playersByTid", worst)
		).sort((a, b) => b.value - a.value)[0]!;
		const other = [...lanes.keys()].find((tid) => tid !== worst && tid !== 0)!;
		const deal = {
			tid: worst,
			pidsAdd: [],
			pidsRemove: [best.pid],
			dpidsAdd: [],
			dpidsRemove: [],
			tradingPartnerTid: other,
		};
		const withTank = await (await tradeValueCalculator()).evaluate(deal);
		const calc = await tradeValueCalculator();
		(calc as any).ownPicks = false;
		const without = await calc.evaluate(deal);
		assert.isAbove(withTank, without);
		// To a human, never cheaper for it.
		const human = await (
			await tradeValueCalculator()
		).evaluate({ ...deal, tradingPartnerTid: 0 });
		assert.isAtMost(human, without + 1e-9);
	},
);

// Owner: "If a contender suffers an awful injury, other contenders should be
// far more willing to go all in if it puts them over the top."
test("a contender's star out for the year raises the other contenders' stake in this season", async () => {
	vanillaPlusSettings.calendar = true;
	const before = await calendar();
	const ranked = [...before.playoffs].sort((a, b) => b[1] - a[1]);
	const [favorite] = ranked[0]!;
	const [nextBest] = ranked[1]!;
	const star = (
		await idb.cache.players.indexGetAll("playersByTid", favorite)
	).sort((a, b) => b.valueNoPot - a.valueNoPot)[0]!;
	const was = star.injury;
	try {
		star.injury = { type: "Torn ACL", gamesRemaining: 200 };
		await idb.cache.players.put(star);
		const after = await calendar();
		assert.isBelow(
			after.playoffs.get(favorite)!,
			before.playoffs.get(favorite)!,
		);
		assert.isAbove(
			after.playoffs.get(nextBest)!,
			before.playoffs.get(nextBest)!,
		);
		// And he's worth this season to nobody.
		assert.strictEqual(thisSeason(after, 1, 200), 0);
	} finally {
		star.injury = was;
		await idb.cache.players.put(star);
	}
});

// Owner: "teams are just constantly shopping around. And the deadline is just
// when push comes to shove... their patience grows smaller as the deadline
// approaches." Early in the season a team turns down deals it comes out only
// a little ahead on - there's time to find better; at the deadline it takes
// them.
test("teams want to come out well ahead early in the season, barely at the deadline", async () => {
	vanillaPlusSettings.calendar = true;
	vanillaPlusSettings.patience = true;
	const season = g.get("season");
	const phase = g.get("phase");
	const teamSeasons = await idb.cache.teamSeasons.indexGetAll(
		"teamSeasonsBySeasonTid",
		[[season], [season, "Z"]],
	);
	const saved = teamSeasons.map((ts) => [ts.won, ts.lost] as const);
	const play = async (games: number) => {
		for (const ts of teamSeasons) {
			ts.won = Math.floor(games / 2);
			ts.lost = games - ts.won;
			await idb.cache.teamSeasons.put(ts);
		}
	};
	// Every one-for-one between one CPU team's top six and others': how many
	// the others would take on worth alone that they turn down for patience.
	const refused = async () => {
		const calc = await tradeValueCalculator();
		const lanes = await ensureLanes();
		const [partner, ...others] = [...lanes.keys()].filter((x) => x !== 0);
		const mine = (await idb.cache.players.indexGetAll("playersByTid", partner!))
			.sort((a, b) => b.value - a.value)
			.slice(0, 6);
		let worthIt = 0;
		let turnedDown = 0;
		for (const tid of others.slice(0, 10)) {
			const theirs = (await idb.cache.players.indexGetAll("playersByTid", tid))
				.sort((a, b) => b.value - a.value)
				.slice(0, 6);
			for (const a of mine) {
				for (const b of theirs) {
					const deal = {
						tid,
						pidsAdd: [a.pid],
						pidsRemove: [b.pid],
						dpidsAdd: [],
						dpidsRemove: [],
						tradingPartnerTid: partner,
					};
					const raw = await calc.evaluate({ ...deal, raw: true });
					const patient = await calc.evaluate(deal);
					// Patience only ever asks for more.
					assert.isAtMost(patient, raw + 1e-9);
					if (raw > 0) {
						worthIt += 1;
						if (patient <= 0) {
							turnedDown += 1;
						}
					}
				}
			}
		}
		return { worthIt, turnedDown };
	};
	try {
		g.setWithoutSavingToDB("phase", PHASE.REGULAR_SEASON);
		await play(0);
		const start = await calendar();
		const early = await refused();
		await play(Math.ceil(g.get("numGames") * g.get("tradeDeadline")));
		const deadline = await calendar();
		const late = await refused();
		assert.isAbove(start.patience, 0.15);
		assert.isBelow(deadline.patience, 0.02);
		assert.isAbove(early.turnedDown, 0);
		assert.isBelow(
			late.turnedDown / Math.max(1, late.worthIt),
			early.turnedDown / early.worthIt,
		);
	} finally {
		teamSeasons.forEach((ts, i) => {
			[ts.won, ts.lost] = saved[i]!;
		});
		for (const ts of teamSeasons) {
			await idb.cache.teamSeasons.put(ts);
		}
		g.setWithoutSavingToDB("phase", phase);
		vanillaPlusSettings.patience = patienceWas;
	}
});

// Owner: "the value of getting rid of good guys so the team can tank for a
// pick early in the season or getting a good guy early so the team can push
// for a high seed... not necessarily never worth it early." A team sure of
// where it stands is less patient than one on the bubble.
test("a team sure of its place is less patient than one on the bubble", async () => {
	vanillaPlusSettings.calendar = true;
	const was = calendarSettings.patienceSure;
	try {
		calendarSettings.patienceSure = 0.25;
		const lanes = await ensureLanes();
		const [contender] = [...lanes].find(([, lane]) => lane === "allIn")!;
		const c = await calendar();
		const each = [...c.patienceOf.values()];
		assert.isBelow(c.patienceOf.get(contender)!, Math.max(...each));
		for (const x of each) {
			assert.isAtLeast(x, calendarSettings.patienceEnd - 1e-9);
			assert.isAtMost(x, c.patience + 1e-9);
		}
	} finally {
		calendarSettings.patienceSure = was;
	}
});

// Found in the arena: a 16-66 team gave a 24-year-old 60/65 on a cheap deal
// for three nobodies - its better pick paid for more than his games this
// season. A bad team's pick pays for a player's games this season only: no
// young player goes for nothing to tank.
test(
	"a bad team doesn't give away a young player for nothing to tank",
	{ timeout: 120_000 },
	async () => {
		vanillaPlusSettings.calendar = true;
		const season = g.get("season");
		const phase = g.get("phase");
		const lanes = await ensureLanes();
		const teamSeasons = await idb.cache.teamSeasons.indexGetAll(
			"teamSeasonsBySeasonTid",
			[[season], [season, "Z"]],
		);
		const saved = teamSeasons.map((ts) => [ts.won, ts.lost] as const);
		try {
			g.setWithoutSavingToDB("phase", PHASE.REGULAR_SEASON);
			// A third of the season gone, the rebuilders losing.
			for (const ts of teamSeasons) {
				const lane = lanes.get(ts.tid);
				ts.won = lane === "rebuild" ? 8 : lane === "retool" ? 11 : 16;
				ts.lost = 27 - ts.won;
				await idb.cache.teamSeasons.put(ts);
			}
			const calc = await tradeValueCalculator();
			let checked = 0;
			for (const [tid, lane] of await ensureLanes()) {
				if (lane !== "rebuild" || tid === 0) {
					continue;
				}
				const other = [...lanes.keys()].find((x) => x !== tid && x !== 0)!;
				const young = (await idb.cache.players.indexGetAll("playersByTid", tid))
					.filter((p) => season - p.born.year <= 25 && p.valueNoPot >= 55)
					.slice(0, 3);
				for (const p of young) {
					const dv = await calc.evaluate({
						tid,
						pidsAdd: [],
						pidsRemove: [p.pid],
						dpidsAdd: [],
						dpidsRemove: [],
						tradingPartnerTid: other,
						raw: true,
					});
					assert.isAtMost(dv, 1e-9, `team ${tid} gives away pid ${p.pid}`);
					checked += 1;
				}
			}
			assert.isAbove(checked, 0);
		} finally {
			teamSeasons.forEach((ts, i) => {
				[ts.won, ts.lost] = saved[i]!;
			});
			for (const ts of teamSeasons) {
				await idb.cache.teamSeasons.put(ts);
			}
			g.setWithoutSavingToDB("phase", phase);
		}
	},
);

// Found in the owner's league: losing teams rated in the top half stayed
// "playoff" all season and bought aging stars. A team out of the race for
// the postseason - well behind the last play-in place in its conference -
// stops buying for this season, before the deadline.
test("a team out of the race stops buying for this season", async () => {
	const season = g.get("season");
	const phase = g.get("phase");
	const before = await ensureLanes();
	// Three of the preseason's playoff teams lose, the rest of the league
	// wins: every play-in place goes to a winning team.
	const losers = new Set(
		[...before]
			.filter(([, lane]) => lane === "playoff")
			.map(([tid]) => tid)
			.slice(0, 3),
	);
	const teamSeasons = await idb.cache.teamSeasons.indexGetAll(
		"teamSeasonsBySeasonTid",
		[[season], [season, "Z"]],
	);
	const saved = teamSeasons.map((ts) => [ts.won, ts.lost] as const);
	try {
		g.setWithoutSavingToDB("phase", PHASE.REGULAR_SEASON);
		// Half the season gone.
		const played = Math.round(g.get("numGames") / 2);
		for (const ts of teamSeasons) {
			ts.won = Math.round(played * (losers.has(ts.tid) ? 0.15 : 0.6));
			ts.lost = played - ts.won;
			await idb.cache.teamSeasons.put(ts);
		}
		const lanes = await ensureLanes();
		assert.isAbove(losers.size, 0);
		for (const tid of losers) {
			assert.include(["retool", "rebuild"], lanes.get(tid), `team ${tid}`);
		}
		// The teams in the race keep theirs.
		assert.isAbove(
			[...lanes].filter(([tid, lane]) => !losers.has(tid) && lane === "allIn")
				.length,
			0,
		);
	} finally {
		teamSeasons.forEach((ts, i) => {
			[ts.won, ts.lost] = saved[i]!;
		});
		for (const ts of teamSeasons) {
			await idb.cache.teamSeasons.put(ts);
		}
		g.setWithoutSavingToDB("phase", phase);
		await ensureLanes();
	}
});

// Found in the owner's league: a rebuilder valued a 2030 second at a third
// of a first, and a 2033 second took part of the price of a 25-year-old
// starter. Future drafts are priced by the coming class, on draft day,
// waiting: a second is worth little in any draft, and a first a few years
// off nearly what one now is.
test("a future second is worth little, a future first nearly a first now", async () => {
	const lanes = await ensureLanes();
	const [tid] = [...lanes].find(([, lane]) => lane === "rebuild")!;
	const other = [...lanes.keys()].find((x) => x !== tid)!;
	const calc = await tradeValueCalculator();
	const season = g.get("season");
	const median = async (year: number, round: number) => {
		const values = [];
		for (const dp of await idb.cache.draftPicks.getAll()) {
			if (dp.tid !== tid && dp.season === year && dp.round === round) {
				values.push(
					await calc.evaluate({
						tid,
						pidsAdd: [],
						pidsRemove: [],
						dpidsAdd: [dp.dpid],
						dpidsRemove: [],
						tradingPartnerTid: other,
						raw: true,
					}),
				);
			}
		}
		values.sort((a, b) => a - b);
		return values[Math.floor(values.length / 2)]!;
	};
	const firstNow = await median(season, 1);
	for (const year of [season + 2, season + 3]) {
		const first = await median(year, 1);
		const second = await median(year, 2);
		assert.isBelow(
			second,
			first * 0.15,
			`${year}: second ${second}, first ${first}`,
		);
		assert.isAbove(
			first,
			firstNow * 0.5,
			`${year}: first ${first}, now ${firstNow}`,
		);
	}
});

// The owner, 2026-11-12: CPU teams valued seconds - and with careers capped
// at potential + 3, firsts from the 5th on too - at about nothing, so they
// threw them in to move up a spot (#9 for #18, #21, #32 and two seconds).
// A pick's player who doesn't make it is let go: a pick is a long shot,
// never a cost, and every one is worth something.
test("every pick is worth something: a drafted player who busts is let go", async () => {
	const lanes = await ensureLanes();
	const [tid] = [...lanes].find(([, lane]) => lane === "rebuild")!;
	const other = [...lanes.keys()].find((x) => x !== tid)!;
	const calc = await tradeValueCalculator();
	const season = g.get("season");
	const { pickValues } = await import("./worth.ts");
	const table = await pickValues();
	for (const [key, values] of Object.entries(table)) {
		for (const v of values as number[]) {
			assert.isAtLeast(v, local.playerOvrMean - 1e-9, `table ${key}`);
		}
	}
	for (const dp of await idb.cache.draftPicks.getAll()) {
		if (dp.tid === tid || dp.season !== season + 1) {
			continue;
		}
		const dv = await calc.evaluate({
			tid,
			pidsAdd: [],
			pidsRemove: [],
			dpidsAdd: [dp.dpid],
			dpidsRemove: [],
			tradingPartnerTid: other,
			raw: true,
		});
		assert.isAbove(dv, 0, `${dp.season} R${dp.round} (${dp.originalTid})`);
	}
});

// Found in the owner's league: Memphis put its 18-year-old 49/77's odds of
// re-signing at 13% - the free agency model's for an unrestricted free agent
// on a bad team - wrote off his best seasons and would trade him for a worse
// prospect. Coming off a rookie deal he's restricted: his team can match.
test("a player on a rookie deal is counted on to stay: his team can match", async () => {
	vanillaPlusSettings.calendar = true;
	const c = await calendar();
	let checked = 0;
	for (const p of await idb.cache.players.indexGetAll("playersByTid", [
		0,
		Infinity,
	])) {
		const retention = c.retention.get(p.pid);
		if (retention !== undefined && contractOf(p).rookie) {
			assert.isAtLeast(retention, calendarSettings.restrictedStays - 1e-9);
			checked += 1;
		}
	}
	assert.isAbove(checked, 0);
});

// Last: it trades. The whole market on - calendar, patience, every team
// shopping, auctions - runs its days, trades, and leaves every roster legal.
test(
	"the market that never stops runs, trades and keeps rosters legal",
	{ timeout: 600_000 },
	async () => {
		vanillaPlusSettings.calendar = true;
		vanillaPlusSettings.patience = true;
		vanillaPlusSettings.auctions = true;
		const phase = g.get("phase");
		const looks = marketStats.looks;
		const trades = marketStats.trades;
		try {
			g.setWithoutSavingToDB("phase", PHASE.REGULAR_SEASON);
			for (let day = 0; day < 6; day++) {
				await tradeDay();
			}
			assert.isAbove(marketStats.looks, looks);
			assert.isAbove(marketStats.trades, trades);
			const maxRosterSize = g.get("maxRosterSize");
			for (const tid of (await ensureLanes()).keys()) {
				const n = (await idb.cache.players.indexGetAll("playersByTid", tid))
					.length;
				assert.isAtMost(n, maxRosterSize + 2, `team ${tid}`);
			}
		} finally {
			g.setWithoutSavingToDB("phase", phase);
			vanillaPlusSettings.patience = patienceWas;
			vanillaPlusSettings.auctions = auctionsWas;
		}
	},
);

// Owner, 2026-11-12: "teams without a real chance for a title even if a
// playoff team should not be going all in to be good this year for like a
// really good 37 year old... you don't want teams mortgaging their potential
// future really good years in the current year if they don't have a real
// shot at it." A team with a real shot keeps its lane's window; one without
// counts its coming seasons more - a young one's next few most - so an old
// star is worth less to it, a young player more.
test("a team without a real shot at the title doesn't spend its future on this season", async () => {
	vanillaPlusSettings.calendar = true;
	const lanes = await ensureLanes();
	const c = await calendar();
	const shot = (tid: number) => c.titleShot.get(tid) ?? 0;
	const [contender] = [...lanes]
		.filter(([, lane]) => lane === "allIn")
		.sort((a, b) => shot(b[0]) - shot(a[0]))[0]!;
	assert.isAtLeast(shot(contender), worthSettings.realShot);
	assert.deepEqual(
		windowFor("allIn", shot(contender), c.coreAge.get(contender)),
		worthSettings.weights.allIn,
	);
	const [hopeful] = [...lanes].find(
		([tid, lane]) =>
			lane === "playoff" && shot(tid) < worthSettings.realShot / 2,
	)!;
	// A young team's window rises to the seasons it means to strike in.
	const young = windowFor("playoff", 0, worthSettings.youngCore);
	assert.isAbove(young[1]!, young[0]!);
	assert.isAbove(young[2]!, young[0]!);
	const season = g.get("season");
	const players = await idb.cache.players.indexGetAll("playersByTid", [
		0,
		Infinity,
	]);
	const vet = players
		.filter((p) => season - p.born.year >= 33)
		.sort((a, b) => b.valueNoPot - a.valueNoPot)[0]!;
	const kid = players
		.filter((p) => season - p.born.year <= 21)
		.sort((a, b) => b.value - a.value)[0]!;
	const to = (tid: number, p: (typeof players)[number]) =>
		playerWorth(p, lanes.get(tid), c, c.playoffs.get(tid), tid).own;
	const asLane = (tid: number, p: (typeof players)[number]) =>
		playerWorth(p, lanes.get(tid), c, c.playoffs.get(tid)).own;
	assert.isBelow(to(hopeful, vet), asLane(hopeful, vet));
	assert.isAbove(to(hopeful, kid), asLane(hopeful, kid));
	assert.isBelow(
		to(hopeful, vet) / to(hopeful, kid),
		to(contender, vet) / to(contender, kid),
	);
});

// Found in the arena: a 51-31 team gave two firsts and two young players to
// clear $5.5M for a free agent - room trades ran on the old engine's
// values. On vanilla+'s: the team taking the players gets no more picks than
// it takes them with, and a team won't give real value for room to sign a
// player worth less.
test("a team clearing cap room pays no more than it must, and only for a player worth it", async () => {
	const calc = await tradeValueCalculator();
	const lanes = await ensureLanes();
	const [tid, partner] = [...lanes.keys()].filter((x) => x !== 0);
	const roster = (await idb.cache.players.indexGetAll("playersByTid", tid!))
		.sort((a, b) => b.value - a.value)
		.slice(4, 6);
	const picks = (
		await idb.cache.draftPicks.indexGetAll("draftPicksByTid", tid!)
	)
		.filter((dp) => dp.round === 1)
		.slice(0, 2);
	const give = [
		...roster.map((p) => ({ kind: "player" as const, p })),
		...picks.map((dp) => ({ kind: "pick" as const, dp })),
	];
	const plan = { tid: tid!, partner: partner!, give } as unknown as RoomPlan;
	const others = (
		await idb.cache.players.indexGetAll("playersByTid", [0, Infinity])
	)
		.filter((p) => p.tid !== tid && p.tid !== partner)
		.sort((a, b) => b.value - a.value);
	const star = others[0]!;
	const scrub = others.at(-1)!;
	const forStar = await roomOnValues(calc, plan, star.pid);
	assert.isDefined(forStar);
	assert.isTrue(await partnerTakes(calc, plan, forStar!));
	const sent = forStar!.filter((a) => a.kind === "pick");
	// No fewer of the picks would do.
	if (sent.length > 0) {
		const players = forStar!.filter((a) => a.kind === "player");
		assert.isFalse(await partnerTakes(calc, plan, players));
	}
	assert.isUndefined(await roomOnValues(calc, plan, scrub.pid));
});

// The offseason's trading windows: as the draft, the re-signing period and
// the preseason open, every CPU team gets a look - and only then.
test(
	"CPU teams trade as an offseason trading window opens",
	{ timeout: 600_000 },
	async () => {
		vanillaPlusSettings.calendar = true;
		vanillaPlusSettings.patience = true;
		vanillaPlusSettings.auctions = true;
		const phase = g.get("phase");
		try {
			g.setWithoutSavingToDB("phase", PHASE.REGULAR_SEASON);
			const looks = marketStats.looks;
			await tradeWindow();
			assert.strictEqual(marketStats.looks, looks);
			g.setWithoutSavingToDB("phase", PHASE.PRESEASON);
			const trades = marketStats.trades;
			for (let window = 0; window < 4; window++) {
				await tradeWindow();
			}
			assert.isAbove(marketStats.looks, looks);
			assert.isAbove(marketStats.trades, trades);
		} finally {
			g.setWithoutSavingToDB("phase", phase);
			vanillaPlusSettings.patience = patienceWas;
			vanillaPlusSettings.auctions = auctionsWas;
		}
	},
);
