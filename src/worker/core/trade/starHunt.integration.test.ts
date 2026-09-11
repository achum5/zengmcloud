import { assert, beforeEach, describe, test } from "vitest";
import { idb } from "../../db/index.ts";
import { g, local } from "../../util/index.ts";
import { player } from "../index.ts";
import { changeTracker } from "../../db/changeTracker.ts";
import { DEFAULT_LEVEL } from "../../../common/budgetLevels.ts";
import { ValueChangeCalculator } from "../team/ValueChangeCalculator.ts";
import { huntStar, type AttemptContext } from "./betweenAiTeams.ts";
import { huntDiagnostics } from "./starHunt.ts";
import {
	getLeagueTradeContext,
	getTradePosture,
	type TradePosture,
} from "./tradePosture.ts";
import {
	AI_TID,
	buildValuationLeague as build,
	NUM_TEAMS,
} from "../../../test/fixtures/valuationLeague.ts";

// A star hunt, end to end, in the valuation league: the contender names the
// seller's star, sends the money the cap rule demands, pays in the young
// players and picks it would otherwise sit on - and keeps its own star home.

// Team 3 draws the weak end of the fixture spread, so it reads as a seller.
const SELLER_TID = 3;

const addPlayer = async (
	tid: number,
	{
		ovr,
		age,
		amount,
		exp,
	}: { ovr: number; age: number; amount: number; exp: number },
) => {
	const season = g.get("season");
	const p: any = player.generate(tid, age, season - age, true, DEFAULT_LEVEL);
	p.pid = 900 + ovr + age;
	p.born.year = season - age;
	const r = p.ratings.at(-1);
	r.ovr = ovr;
	r.pot = ovr;
	p.contract = { amount, exp };
	p.injury = { type: "Healthy", gamesRemaining: 0 };
	p.gamesUntilTradable = 0;
	await idb.cache.players.add(p);
	return p;
};

describe("a contender in striking distance hunts a star", () => {
	beforeEach(() => {
		changeTracker.disable();
		changeTracker.reset();
	});

	test("lands him, pays with youth and picks, and keeps its own star", async () => {
		// The hunter: a winning, ageing team with two good young players and
		// two firsts to spend, and a genuine star of its own.
		await build({
			ai: [
				{ ovr: 66, age: 28, amount: 25_000 },
				{ ovr: 57, age: 24, amount: 6_000 },
				{ ovr: 56, age: 23, amount: 5_000 },
			],
			aiPicks: [1, 1],
			aiWon: 60,
		});
		const season = g.get("season");
		// The seller: a bad team with a star past its window, on a big deal.
		const star = await addPlayer(SELLER_TID, {
			ovr: 64,
			age: 30,
			amount: 24_000,
			exp: season + 2,
		});
		const sellerRow = await idb.cache.teamSeasons.indexGet(
			"teamSeasonsBySeasonTid",
			[season, SELLER_TID],
		);
		sellerRow!.won = 18;
		sellerRow!.lost = 64;
		await idb.cache.teamSeasons.put(sellerRow!);
		local.playerOvrMeanStdStale = true;
		for (const p of await idb.cache.players.getAll()) {
			await player.updateValues(p);
			await idb.cache.players.put(p);
		}

		const context = await getLeagueTradeContext();
		const postures = new Map<number, TradePosture>();
		for (let tid = 0; tid < NUM_TEAMS; tid++) {
			postures.set(tid, await getTradePosture(tid, context));
		}
		const hunter = postures.get(AI_TID)!;
		const seller = postures.get(SELLER_TID)!;
		assert.isTrue(hunter.strikingDistance, `hunter is ${hunter.tier}`);
		assert.isTrue(seller.shoppableStar, `seller is ${seller.tier}`);

		const ctx: AttemptContext = {
			postures,
			valueChangeCalculator: new ValueChangeCalculator(),
			aiTids: [...postures.keys()].filter((tid) => tid !== 0),
			season,
			starOvr: context.starOvr,
			starValue: context.starValue,
		};
		const players = await idb.cache.players.indexGetAll("playersByTid", AI_TID);
		const offer = await huntStar({
			initiator: AI_TID,
			initPosture: hunter,
			players,
			candidates: [SELLER_TID],
			ctx,
		});
		assert.isNotNull(
			offer,
			`no package came together: ${JSON.stringify([...huntDiagnostics])}`,
		);
		const [hunterSide, sellerSide] = offer!.teams;
		assert.include(sellerSide.pids, star.pid, "the star did not come back");
		assert.isTrue(offer!.landsStar);
		// The seller gives the star and nothing else.
		assert.deepStrictEqual(sellerSide.pids, [star.pid]);
		assert.deepStrictEqual(sellerSide.dpids, []);
		// The hunter's own star stays home.
		for (const pid of hunterSide.pids) {
			const p = (await idb.cache.players.get(pid))!;
			assert.isBelow(
				p.value,
				context.starValue,
				`the hunter gave up its own star (${p.value.toFixed(1)})`,
			);
		}
		// And it paid with something real: players or picks beyond salary.
		assert.isAbove(hunterSide.pids.length + hunterSide.dpids.length, 1);
	});
});
