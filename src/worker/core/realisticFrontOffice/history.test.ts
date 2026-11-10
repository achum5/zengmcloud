import { afterAll, assert, beforeAll, beforeEach, test } from "vitest";
import { PHASE, PLAYER } from "../../../common/constants.ts";
import { getCurrentContract } from "../../../common/contracts.ts";
import { makeSeededRandom } from "../../../common/random.ts";
import { howMethod, howPhrases } from "../../../common/signingHow.ts";
import type { EventBBGM } from "../../../common/types.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { formatEventText } from "../../util/formatEventText.ts";
import { createArenaLeague } from "../../../test/arena/runLeague.ts";
import { setTestContract } from "../../../test/helpers.ts";
import { cbaSettings, maxFor } from "./cba.ts";
import {
	capStatusOf,
	contractCard,
	contractRules,
	exceptionOf,
	signingsLog,
	teamCapStatus,
} from "./capSheet.ts";
import { decideOptions, renounce, sign } from "./freeAgency.ts";
import { stageSettings } from "./stages.ts";

const importNode = (name: string) => import(/* @vite-ignore */ name);

const realRandom = Math.random;
const tid = 3;

beforeAll(async () => {
	await importNode("fake-indexeddb/auto");
	Math.random = makeSeededRandom(23);
	// The test setup shares modules between files, and another may have left a
	// stand-in database here; creating a league would try to close it.
	(idb as any).league = undefined;
	await createArenaLeague(9047);
	g.setWithoutSavingToDB("realisticFrontOffices", true);
	stageSettings.on = true;
}, 120_000);

const stagesWereOn = stageSettings.on;
afterAll(() => {
	Math.random = realRandom;
	stageSettings.on = stagesWereOn;
});

beforeEach(() => {
	g.setWithoutSavingToDB("phase", PHASE.FREE_AGENCY);
	g.setWithoutSavingToDB("faInStages", true);
	g.setWithoutSavingToDB("daysLeft", 3);
});

const lastEvent = async (pid: number) =>
	(await idb.cache.events.getAll())
		.filter((event) => event.pids?.includes(pid))
		.at(-1) as EventBBGM;

const freeAgent = async (i = 0) =>
	(await idb.cache.players.indexGetAll("playersByTid", PLAYER.FREE_AGENT))
		.sort((a, b) => b.value - a.value || a.pid - b.pid)
		.at(i)!;

test("a signing's event says how it was signed, in a few words", async () => {
	const p = await freeAgent();
	delete p.faOpenAsk;
	const season = g.get("season");
	await sign(
		p,
		tid,
		{ amount: 9100, exp: season + 3 },
		{ raise: 0.05, option: "team" },
		{ path: "midLevel" },
	);
	const event = await lastEvent(p.pid);
	assert.strictEqual(event.type, "freeAgent");
	if (event.type !== "freeAgent") {
		return;
	}
	assert.deepInclude(event.how, {
		path: "midLevel",
		option: "team",
		stage: 2,
		years: 3,
	});
	// The raises are in the total.
	assert.isAbove(event.how!.total!, 3 * 9100);
	const text = await formatEventText(event);
	assert.include(text, "/year through");
	assert.match(text, /\(mid-level, team option\)\.$/);

	// The contract has the same shape.
	const contract = getCurrentContract(p)!;
	assert.strictEqual(contract.option, "team");
	assert.strictEqual(contract.amounts.length, 3);
	assert.isAbove(contract.amounts[2]!, contract.amounts[0]!);

	// The season's signings log has it, by its method.
	const log = await signingsLog(season, tid);
	const row = log.find((x) => x.pid === p.pid)!;
	assert.strictEqual(row.type, "freeAgent");
	assert.strictEqual(row.when, "Decision day 2");
	assert.strictEqual(howMethod(row.how)?.short, "MLE");
	assert.strictEqual(row.total, event.how!.total);
});

test("an offer sheet matched, and a prove-it deal", async () => {
	const p = await freeAgent(1);
	p.faOpenAsk = 20_000;
	await sign(p, tid, { amount: 5000, exp: g.get("season") + 1 }, undefined, {
		path: "bird",
		offerSheet: "matched",
	});
	const event = await lastEvent(p.pid);
	const text = await formatEventText(event);
	assert.match(text, /\(matched offer sheet, Bird rights, prove-it deal\)\.$/);
	assert.isTrue(getCurrentContract(p)!.discount);
});

test("with the rules off, a signing's event is as in stock", async () => {
	stageSettings.on = false;
	try {
		const p = await freeAgent(2);
		await sign(p, tid, { amount: 3000, exp: g.get("season") + 2 });
		const event = await lastEvent(p.pid);
		assert.notProperty(event, "how");
		assert.match(await formatEventText(event), /through \d+\.$/);
	} finally {
		stageSettings.on = true;
	}
});

test("options decided and rights renounced are events", async () => {
	g.setWithoutSavingToDB("phase", PHASE.RESIGN_PLAYERS);
	const season = g.get("season");
	const [p] = await idb.cache.players.indexGetAll("playersByTid", tid);
	setTestContract(p!, { exp: season + 1 });
	getCurrentContract(p!)!.option = "team";
	await idb.cache.players.put(p!);
	await decideOptions();
	const event = await lastEvent(p!.pid);
	assert.strictEqual(event.type, "contract");
	assert.match(
		await formatEventText(event),
		/(picked up|declined) their team option on/,
	);

	// Renounced: his Bird rights and cap hold.
	g.setWithoutSavingToDB("phase", PHASE.FREE_AGENCY);
	const own = (
		await idb.cache.players.indexGetAll("playersByTid", PLAYER.FREE_AGENT)
	).find((x) => !x.contracts.at(-1)?.discount);
	assert.isDefined(own);
	if (own) {
		// His last season was with this team: its Bird rights, and his hold.
		own.stats.push({
			season,
			tid,
			playoffs: false,
			gp: 10,
			yearsWithTeam: 1,
		} as any);
		delete own.faRenounced;
		await idb.cache.players.put(own);
		const before = (await teamCapStatus(tid))!;
		assert.isTrue(before.holds.some((row) => row.pid === own.pid));
		assert.isUndefined(await renounce(tid, own.pid));
		const renounced = await lastEvent(own.pid);
		assert.strictEqual(renounced.type, "contract");
		assert.include(await formatEventText(renounced), "renounced their rights");
		const after = (await teamCapStatus(tid))!;
		assert.isFalse(after.holds.some((row) => row.pid === own.pid));
		assert.isAbove(after.capSpace, before.capSpace);
	}
});

test("the rules page has this league's numbers", () => {
	const salaryCap = g.get("salaryCap");
	const rules = contractRules();
	assert.strictEqual(rules.salaryCap, salaryCap);
	assert.strictEqual(rules.apron, g.get("luxuryPayroll"));
	assert.strictEqual(
		rules.hardCap,
		g.get("luxuryPayroll") + cbaSettings.hardCapOverTax * salaryCap,
	);
	assert.strictEqual(rules.midLevel, cbaSettings.midLevel * salaryCap);
	assert.strictEqual(rules.midLevelPct, 9);
	assert.strictEqual(rules.taxpayerMidLevelPct, 5);
	assert.deepEqual(
		rules.tiers.map((tier) => tier.seasons),
		["0-6", "7-9", "10+"],
	);
	assert.strictEqual(rules.tiers[0]!.amount, maxFor(0));
	assert.strictEqual(rules.tiers[2]!.amount, g.get("maxContract"));
	assert.strictEqual(rules.years.own, Math.min(5, g.get("maxContractLength")));
	assert.strictEqual(rules.raiseOwn, 8);
	assert.strictEqual(rules.raiseOther, 5);
	assert.strictEqual(rules.stages.length, 5);
});

test("cap status: under the cap, over it, over the apron, at the hard cap", () => {
	const salaryCap = g.get("salaryCap");
	const apron = g.get("luxuryPayroll");
	const hardCap = apron + cbaSettings.hardCapOverTax * salaryCap;
	assert.strictEqual(capStatusOf(tid, salaryCap - 1000), "under");
	assert.strictEqual(capStatusOf(tid, salaryCap - 1000, 2000), "over");
	assert.strictEqual(capStatusOf(tid, apron + 1000), "apron");
	assert.strictEqual(capStatusOf(tid, hardCap - 1), "hardCap");

	const t = { tid, midLevelSeason: undefined };
	assert.deepEqual(exceptionOf(t, salaryCap), {
		kind: "midLevel",
		amount: cbaSettings.midLevel * salaryCap,
	});
	assert.strictEqual(exceptionOf(t, apron + 1000).kind, "taxpayerMidLevel");
	assert.strictEqual(
		exceptionOf({ tid, midLevelSeason: g.get("season") + 1 }, salaryCap).kind,
		"used",
	);
});

test("the words for each way of signing", () => {
	assert.deepEqual(howPhrases(undefined), []);
	assert.deepEqual(howPhrases({ path: "taxpayerMidLevel" }), [
		"taxpayer mid-level",
	]);
	assert.deepEqual(
		howPhrases({ path: "room", option: "player", offerSheet: "notMatched" }),
		["offer sheet, not matched", "cap room", "player option"],
	);
	assert.strictEqual(howMethod({ path: "minimum" })?.short, "Min");
	assert.strictEqual(
		howMethod({ offerSheet: "matched", path: "bird" })?.short,
		"RFA match",
	);
});

test("his page: each deal as signed, each extension, and his deal now", async () => {
	const season = g.get("season");
	const [p0] = await idb.cache.players.indexGetAll("playersByTid", tid);
	const p = { ...p0!, tid };
	p.contracts = [
		{
			type: "freeAgent",
			eid: 1,
			start: season,
			amounts: [4000, 4000, 9000, 9000, 9000],
			payments: [],
			option: "player",
		},
	];
	const events = [
		{
			type: "freeAgent",
			eid: 1,
			season: season - 1,
			pids: [p.pid],
			tids: [tid],
			contract: { amount: 4000, exp: season + 1 },
			how: { path: "midLevel", years: 2, total: 8000 },
		},
		{
			type: "reSigned",
			eid: 2,
			season,
			pids: [p.pid],
			tids: [tid],
			contract: { amount: 9000, exp: season + 4 },
			how: {
				extension: true,
				path: "bird",
				option: "player",
				years: 3,
				total: 27_000,
			},
		},
	] as EventBBGM[];
	const card = contractCard(p, events)!;
	assert.deepEqual(
		card.history.map((row) => [row.method, row.years, row.total]),
		[
			["MLE", 2, 8000],
			["Ext", 3, 27_000],
		],
	);
	assert.deepInclude(card.deal, {
		verb: "extended",
		years: 3,
		total: 27_000,
		exp: season + 4,
		method: "Bird rights",
		bird: g.get("teamInfoCache")[tid]!.abbrev,
	});
	assert.deepEqual(card.marks[season + 4], ["PO"]);
});
