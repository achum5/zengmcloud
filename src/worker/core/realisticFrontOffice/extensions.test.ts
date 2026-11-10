import { afterAll, assert, beforeAll, beforeEach, test } from "vitest";
import { PHASE } from "../../../common/constants.ts";
import {
	getContractExp,
	getCurrentContract,
} from "../../../common/contracts.ts";
import { makeSeededRandom } from "../../../common/random.ts";
import type { Contract, Player } from "../../../common/types.ts";
import { idb } from "../../db/index.ts";
import { g } from "../../util/index.ts";
import { createArenaLeague } from "../../../test/arena/runLeague.ts";
import { setTestContract } from "../../../test/helpers.ts";
import { team } from "../index.ts";
import { hardCap, maxFor, serviceYears } from "./cba.ts";
import {
	applyExtension,
	canExtend,
	extensionError,
	extensionProps,
	extensionRoll,
	extensionSettings,
	cpuExtensions,
	offerExtension,
} from "./extensions.ts";
import { stageSettings } from "./stages.ts";

const stagesWereOn = stageSettings.on;
const extensionsWereOn = extensionSettings.on;

const importNode = (name: string) => import(/* @vite-ignore */ name);

const userTid = 0;
const realRandom = Math.random;

beforeAll(async () => {
	await importNode("fake-indexeddb/auto");
	Math.random = makeSeededRandom(11);
	// The test setup shares modules between files, and another may have left a
	// stand-in database here; creating a league would try to close it.
	(idb as any).league = undefined;
	await createArenaLeague(9031);
	g.setWithoutSavingToDB("spectator", false);
	g.setWithoutSavingToDB("userTid", userTid);
	g.setWithoutSavingToDB("userTids", [userTid]);
	g.setWithoutSavingToDB("realisticFrontOffices", true);
	stageSettings.on = true;
	// Off in the game (owner's call); the engine is still tested.
	extensionSettings.on = true;
}, 120_000);

afterAll(() => {
	Math.random = realRandom;
	stageSettings.on = stagesWereOn;
	extensionSettings.on = extensionsWereOn;
});

beforeEach(() => {
	g.setWithoutSavingToDB("phase", PHASE.REGULAR_SEASON);
});

// The user's players, each set to be in the last season of his contract and
// past his first season.
const expiring = async () => {
	const season = g.get("season");
	const players = await idb.cache.players.indexGetAll("playersByTid", userTid);
	for (const p of players) {
		setTestContract(p, { exp: season });
		p.draft.year = Math.min(p.draft.year, season - 2);
		await idb.cache.players.put(p);
	}
	return players;
};

test("only in the last season of a contract, in the regular season, in stages", async () => {
	const season = g.get("season");
	const [p] = await expiring();
	assert.isTrue(canExtend(p!));

	setTestContract(p!, { exp: season + 1 });
	assert.isFalse(canExtend(p!), "a season left after this one");
	setTestContract(p!, { exp: season });

	p!.draft.year = season - 1;
	assert.isFalse(canExtend(p!), "his first season since the draft");
	p!.draft.year = season - 2;

	g.setWithoutSavingToDB("phase", PHASE.PLAYOFFS);
	assert.isFalse(canExtend(p!), "after the regular season");
	g.setWithoutSavingToDB("phase", PHASE.AFTER_TRADE_DEADLINE);
	assert.isTrue(canExtend(p!), "after the trade deadline");

	stageSettings.on = false;
	try {
		assert.isFalse(canExtend(p!), "switch off");
	} finally {
		stageSettings.on = true;
	}
});

test("the new seasons go on the end of his contract, with the raise and option", () => {
	const contract: Contract = {
		type: "rookie",
		start: 2030,
		amounts: [2000, 2500],
		payments: [],
	};
	applyExtension(contract, {
		amount: 10_000,
		years: 3,
		raise: 0.08,
		option: "player",
	});
	assert.deepEqual(contract.amounts, [2000, 2500, 10_000, 10_800, 11_600]);
	assert.strictEqual(contract.option, "player");
	assert.strictEqual(contract.type, "unknown", "no longer a rookie contract");

	const flat: Contract = {
		type: "freeAgent",
		start: 2030,
		amounts: [5000],
		payments: [],
		option: "team",
	};
	applyExtension(flat, { amount: 6000, years: 1, option: "team" });
	assert.deepEqual(flat.amounts, [5000, 6000]);
	assert.isUndefined(flat.option, "no option on a one-year extension");
	assert.strictEqual(flat.type, "freeAgent");
});

test(
	"next season's payroll stays within the hard cap",
	{ timeout: 300_000 },
	async () => {
		const season = g.get("season");
		const players = await expiring();
		const [p, other] = players.sort((a, b) => b.value - a.value);
		// Room under the hard cap next season: 10M, from a teammate signed on.
		setTestContract(other!, { exp: season + 2 });
		const amount = getCurrentContract(other!)!.amounts.at(-1)!;
		const room =
			hardCap(userTid) - (await team.getPayroll(userTid, season + 1));
		setTestContract(other!, { amount: amount + room - 10_000 });
		await idb.cache.players.put(other!);
		assert.closeTo(
			hardCap(userTid) - (await team.getPayroll(userTid, season + 1)),
			10_000,
			1,
		);

		assert.strictEqual(
			await extensionError(p!, userTid, { amount: 12_000, years: 2 }),
			"Over the hard cap.",
		);
		assert.strictEqual(
			await offerExtension(userTid, p!.pid, { amount: 12_000, years: 2 }),
			"Over the hard cap.",
		);
		assert.isUndefined(
			await extensionError(p!, userTid, { amount: 9000, years: 2 }),
		);
		assert.match(
			(await extensionError(p!, userTid, {
				amount: 9000,
				years: 2,
				raise: 0.1,
			}))!,
			/Raises are limited/,
		);
		assert.match(
			(await extensionError(p!, userTid, { amount: 9000, years: 6 }))!,
			/1 to 5 years/,
		);

		setTestContract(other!, { amount, exp: season });
		await idb.cache.players.put(other!);
	},
);

test(
	"he answers on his roll for the season; turned down three times, no more",
	{ timeout: 300_000 },
	async () => {
		const players = (await expiring()).sort(
			(a, b) => extensionRoll(a) - extensionRoll(b),
		);
		// His ask at every length, and the odds of an offer.
		const take = players[0]!;
		const props = await extensionProps(userTid, take.pid);
		if (typeof props === "string") {
			throw new Error(props);
		}
		assert.strictEqual(props.terms.length, 5);
		const wanted = props.terms.find(
			(term) => term.years === props.wantedYears,
		)!;
		const offer = { amount: props.maxContract, years: wanted.years };
		const result = await offerExtension(userTid, take.pid, offer);
		if (typeof result === "string") {
			throw new Error(result);
		}
		assert.strictEqual(result.accepted, extensionRoll(take) < result.chance);
		const contract = getCurrentContract(
			(await idb.cache.players.get(take.pid)) as Player,
		)!;
		if (result.accepted) {
			assert.strictEqual(
				getContractExp(contract),
				g.get("season") + wanted.years,
			);
			assert.isFalse(canExtend((await idb.cache.players.get(take.pid))!));
		}

		// The best player, offered the minimum: no chance.
		const star = players
			.filter((p) => p.pid !== take.pid)
			.sort((a, b) => b.value - a.value)[0]!;
		const lowball = { amount: g.get("minContract"), years: 1 };
		for (let i = 1; i <= extensionSettings.patience; i++) {
			const answer = await offerExtension(userTid, star.pid, lowball);
			if (typeof answer === "string") {
				throw new Error(answer);
			}
			assert.isFalse(answer.accepted);
			assert.strictEqual(answer.declined, i);
		}
		assert.match(
			(await offerExtension(userTid, star.pid, {
				amount: g.get("maxContract"),
				years: 2,
			})) as string,
			/won't talk/,
		);
		const after = await extensionProps(userTid, star.pid);
		assert.match(
			typeof after === "string" ? after : after.error!,
			/won't talk/,
		);
		assert.strictEqual(
			getContractExp(getCurrentContract(star)!),
			g.get("season"),
			"not extended",
		);
	},
);

test(
	"CPU teams extend by their valuation, within the rules",
	{ timeout: 300_000 },
	async () => {
		const season = g.get("season");
		const before = new Map<number, number>();
		let eligible = 0;
		for (const p of await idb.cache.players.indexGetAll("playersByTid", [
			1,
			Infinity,
		])) {
			if (canExtend(p)) {
				eligible += 1;
				before.set(p.pid, getCurrentContract(p)!.amounts.length);
			}
		}
		await cpuExtensions();
		let extended = 0;
		for (const [pid, length] of before) {
			const p = (await idb.cache.players.get(pid))!;
			const contract = getCurrentContract(p)!;
			if (contract.amounts.length > length) {
				extended += 1;
				assert.isAtMost(getContractExp(contract) - season, 5);
				assert.isAtMost(contract.amounts[length]!, maxFor(serviceYears(p)) + 1);
				assert.isAtMost(
					await team.getPayroll(p.tid, season + 1),
					hardCap(p.tid) + 1,
				);
			}
		}
		// Some, not all: by its valuation, given his odds.
		assert.isAbove(extended, 0);
		assert.isBelow(extended, eligible);
	},
);
