import { afterAll, assert, beforeAll, beforeEach, test } from "vitest";
import { PHASE, PLAYER } from "../../../common/constants.ts";
import type { Player } from "../../../common/types.ts";
import { makeSeededRandom } from "../../../common/random.ts";
import { idb } from "../../db/index.ts";
import { g, helpers } from "../../util/index.ts";
import { createArenaLeague } from "../../../test/arena/runLeague.ts";
import { makeOffer, reSignChance, withdrawOffer } from "./freeAgency.ts";
import {
	chanceOfOffer,
	instantSigningRefusal,
	lastChoices,
	offerScreen,
	offerTerms,
	offersList,
	reSigningOffers,
	wantedTerm,
} from "./offerScreen.ts";
import { wantedYears } from "./offers.ts";

const importNode = (name: string) => import(/* @vite-ignore */ name);

const userTid = 0;
const realRandom = Math.random;

beforeAll(async () => {
	await importNode("fake-indexeddb/auto");
	Math.random = makeSeededRandom(7);
	// The test setup shares modules between files, and another may have left a
	// stand-in database here; creating a league would try to close it.
	(idb as any).league = undefined;
	await createArenaLeague(9001);
	g.setWithoutSavingToDB("spectator", false);
	g.setWithoutSavingToDB("userTid", userTid);
	g.setWithoutSavingToDB("userTids", [userTid]);
	g.setWithoutSavingToDB("realisticFrontOffices", true);
}, 120_000);

afterAll(() => {
	Math.random = realRandom;
});

beforeEach(async () => {
	g.setWithoutSavingToDB("phase", PHASE.REGULAR_SEASON);
	g.setWithoutSavingToDB("realisticFrontOffices", true);
	await idb.cache.negotiations.clear();
	const t = (await idb.cache.teams.get(userTid))!;
	t.offerBoard = [];
	await idb.cache.teams.put(t);
});

const freeAgents = async () =>
	(await idb.cache.players.indexGetAll("playersByTid", PLAYER.FREE_AGENT)).sort(
		(a, b) => b.value - a.value,
	);

const askOf = (p: Player) => wantedTerm(p);

test("he asks his price for the length he wants, and more for other lengths", async () => {
	const [p] = await freeAgents();
	const terms = offerTerms(p!);
	assert.strictEqual(
		terms.length,
		g.get("maxContractLength") - g.get("minContractLength") + 1,
	);
	const wanted = wantedTerm(p!);
	assert.strictEqual(
		wanted.years,
		helpers.bound(
			wantedYears(p!),
			g.get("minContractLength"),
			g.get("maxContractLength"),
		),
	);
	for (const term of terms) {
		assert.isAtLeast(term.ask, wanted.ask);
		assert.isAtLeast(term.ask, g.get("minContract"));
		assert.isAtMost(term.ask, g.get("maxContract"));
		assert.strictEqual(term.exp, g.get("season") + term.years - 1);
	}
});

test("the offer screen shows your odds, and the offer once it is made", async () => {
	const [p] = await freeAgents();
	const before = await offerScreen(userTid, p!.pid);
	if (typeof before === "string") {
		throw new Error(before);
	}
	assert.isFalse(before.reSigning);
	assert.isUndefined(before.current);
	assert.isAbove(before.chance, 0);
	assert.isBelow(before.chance, 1);

	const { ask, exp } = askOf(p!);
	const error = await makeOffer(userTid, { pid: p!.pid, amount: ask, exp });
	assert.isUndefined(error);

	const after = await offerScreen(userTid, p!.pid);
	if (typeof after === "string") {
		throw new Error(after);
	}
	assert.deepEqual(after.current?.amount, ask);
	assert.strictEqual(after.rank, 1);
	assert.strictEqual(after.numOffers, 1);
	assert.closeTo(after.chance, before.chance, 1e-9);

	const list = await offersList(userTid);
	assert.deepEqual(
		list.map((row) => row.pid),
		[p!.pid],
	);
	assert.closeTo(list[0]!.chance, after.chance, 1e-9);

	await withdrawOffer(userTid, p!.pid);
	assert.deepEqual(await offersList(userTid), []);
});

test("more money is more likely to be taken", async () => {
	const [p] = await freeAgents();
	const { ask, exp } = askOf(p!);
	const low = await chanceOfOffer(userTid, { pid: p!.pid, amount: ask, exp });
	const high = await chanceOfOffer(userTid, {
		pid: p!.pid,
		amount: Math.min(g.get("maxContract"), ask * 1.3),
		exp,
	});
	assert.isAbove(high, low);
});

test("offers are listed in the team's order of priority", async () => {
	const [a, b] = await freeAgents();
	for (const p of [a!, b!]) {
		// At the minimum, which fits under any cap.
		assert.isUndefined(
			await makeOffer(userTid, {
				pid: p.pid,
				amount: g.get("minContract"),
				exp: askOf(p).exp,
			}),
		);
	}
	const t = (await idb.cache.teams.get(userTid))!;
	t.offerBoard = [...t.offerBoard!].reverse();
	await idb.cache.teams.put(t);
	assert.deepEqual(
		(await offersList(userTid)).map((row) => row.pid),
		[b!.pid, a!.pid],
	);
});

test("no offers outside the times free agents can sign, nor to players under contract", async () => {
	const [p] = await freeAgents();
	g.setWithoutSavingToDB("phase", PHASE.PLAYOFFS);
	assert.isString(await offerScreen(userTid, p!.pid));
	g.setWithoutSavingToDB("phase", PHASE.REGULAR_SEASON);
	const [signed] = await idb.cache.players.indexGetAll("playersByTid", 1);
	assert.isString(await offerScreen(userTid, signed!.pid));
});

test("no instant signing, except draft picks while re-signing", async () => {
	const [p] = await freeAgents();
	assert.isString(await instantSigningRefusal(p!.pid));
	g.setWithoutSavingToDB("realisticFrontOffices", false);
	assert.isUndefined(await instantSigningRefusal(p!.pid));
	g.setWithoutSavingToDB("realisticFrontOffices", true);

	g.setWithoutSavingToDB("phase", PHASE.RESIGN_PLAYERS);
	const draftYear = p!.draft.year;
	p!.draft.year = g.get("season");
	await idb.cache.negotiations.add({
		pid: p!.pid,
		tid: userTid,
		resigning: true,
	});
	try {
		assert.isUndefined(await instantSigningRefusal(p!.pid));
		// Nor is his an offer to work: he signs as in stock.
		assert.isFalse(
			(await reSigningOffers(userTid, [p!])).has(p!.pid),
			"draft pick",
		);
	} finally {
		p!.draft.year = draftYear;
	}
});

test("re-signing shows his chance of taking his team's offer rather than testing the market", async () => {
	const [p] = await freeAgents();
	g.setWithoutSavingToDB("phase", PHASE.RESIGN_PLAYERS);
	await idb.cache.negotiations.add({
		pid: p!.pid,
		tid: userTid,
		resigning: true,
	});
	const { ask, exp } = askOf(p!);
	assert.isUndefined(
		await makeOffer(userTid, { pid: p!.pid, amount: ask, exp }),
	);

	const screen = await offerScreen(userTid, p!.pid);
	if (typeof screen === "string") {
		throw new Error(screen);
	}
	assert.isTrue(screen.reSigning);
	const expected = await reSignChance(p!, userTid, { amount: ask, exp });
	assert.closeTo(screen.chance, expected, 1e-9);

	const rows = await reSigningOffers(userTid, [p!]);
	assert.strictEqual(rows.get(p!.pid)?.offer?.amount, ask);
	assert.closeTo(rows.get(p!.pid)!.chance!, expected, 1e-9);

	// Let go: no offer, no chance.
	await withdrawOffer(userTid, p!.pid);
	const letGo = await reSigningOffers(userTid, [p!]);
	assert.isUndefined(letGo.get(p!.pid)?.offer);
	assert.isUndefined(letGo.get(p!.pid)?.chance);
});

test("the last day's signings are the most recent ones recorded", async () => {
	const season = g.get("season");
	const item = {
		type: "choice" as const,
		pid: 1,
		name: "A B",
		offers: [
			{ tid: 3, abbrev: "XXX", amount: 5000, exp: season + 2, oddsPct: 60 },
		],
		waitPct: 40,
		roll: 12.3,
		winnerTid: 3,
		amount: 5000,
		exp: season + 2,
	};
	await idb.cache.faDayResults.put({
		key: "old",
		season,
		daysLeft: 20,
		items: [{ ...item, pid: 2 }],
		boards: [],
		at: 1,
	});
	await idb.cache.faDayResults.put({
		key: "new",
		season,
		daysLeft: 19,
		items: [item],
		boards: [],
		at: 2,
	});
	const results = await lastChoices();
	assert.strictEqual(results?.daysLeft, 19);
	assert.deepEqual(
		results?.items.map((x) => x.pid),
		[1],
	);
});
