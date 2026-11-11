import { afterAll, assert, beforeAll, test } from "vitest";
import { PHASE, PLAYER } from "../../../common/constants.ts";
import { makeSeededRandom } from "../../../common/random.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { createArenaLeague } from "../../../test/arena/runLeague.ts";
import { vanillaPlusSettings } from "./settings.ts";
import { ensureLanes, type Lane } from "./lanes.ts";
import { tweaksFor } from "./tweaks.ts";
import { timingFactor } from "./timing.ts";
import { tradeValueCalculator } from "./index.ts";
import makeItWork from "../trade/makeItWork.ts";

const importNode = (name: string) => import(/* @vite-ignore */ name);
const realRandom = Math.random;
const wasOn = vanillaPlusSettings.on;

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
	// Fit alone: timing (when each helps) taken back out.
	const center =
		tweaks(allIn, asset(pair![0]), "add") / timingFactor(pair![0], "allIn");
	const guard =
		tweaks(allIn, asset(pair![1]), "add") / timingFactor(pair![1], "allIn");
	assert.isAbove(center, guard);
	assert.isAbove(center, 1);
	assert.isAtMost(center, 1.25);
});

test("a rebuilding team won't give its best player away", async () => {
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
});

test("what would make it work never leaves a roster past the limit", async () => {
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
});

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
	assert.isBelow(timingFactor(teen, "allIn"), 0.6);
	assert.isAbove(timingFactor(vet, "allIn"), 1.2);
	assert.isAbove(timingFactor(teen, "rebuild"), 1.2);
	assert.isBelow(timingFactor(vet, "rebuild"), 0.9);
	// In order across the lanes.
	const lanes = ["allIn", "playoff", "retool", "rebuild"] as const;
	for (let i = 1; i < lanes.length; i++) {
		assert.isAbove(
			timingFactor(teen, lanes[i]!),
			timingFactor(teen, lanes[i - 1]!),
		);
		assert.isBelow(
			timingFactor(vet, lanes[i]!),
			timingFactor(vet, lanes[i - 1]!),
		);
	}
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
